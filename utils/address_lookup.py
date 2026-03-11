"""
utils/address_lookup.py  v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Lookup de propietario por dirección física.

PRINCIPIO CLAVE:
  Los campos from_st / to_st / from_address_num / to_address_num en DataSF
  son strings ("85", "99", "100"). Comparar "85" <= "100" en SQL string
  falla porque lexicográficamente "85" > "100".
  
  SOLUCIÓN: consultar SOLO por nombre de calle, traer todos los registros,
  y filtrar por número de calle en Python con int().

FLUJO SF (3 estrategias en cascada):
  1. acdm-wktn → blklot → wv5m-vpq2 (owner)   [más preciso]
  2. wv5m-vpq2 directo por street name          [fallback]
  3. DataSF full-text search                    [último recurso]

OTROS CONDADOS:
  Alameda, ContraCosta, SantaClara → sus respectivos servicios GIS/API
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import re
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

EMPTY = {"owner_name": "", "mailing_address": "", "apn": "", "phone": ""}

# Sufijos de calle y sus equivalentes en DataSF
SUFFIX_MAP = {
    "STREET": ["ST", "STREET"],
    "ST":     ["ST", "STREET"],
    "AVENUE": ["AVE", "AV", "AVENUE"],
    "AVE":    ["AVE", "AV", "AVENUE"],
    "AV":     ["AV", "AVE", "AVENUE"],
    "BOULEVARD": ["BLVD", "BOULEVARD"],
    "BLVD":   ["BLVD", "BOULEVARD"],
    "DRIVE":  ["DR", "DRIVE"],
    "DR":     ["DR", "DRIVE"],
    "LANE":   ["LN", "LANE"],
    "LN":     ["LN", "LANE"],
    "ROAD":   ["RD", "ROAD"],
    "RD":     ["RD", "ROAD"],
    "COURT":  ["CT", "COURT"],
    "CT":     ["CT", "COURT"],
    "PLACE":  ["PL", "PLACE"],
    "PL":     ["PL", "PLACE"],
    "WAY":    ["WAY"],
    "TERRACE": ["TER", "TERRACE"],
    "TER":    ["TER", "TERRACE"],
}


# ─────────────────────────────────────────────────────────────────
# PARSER DE DIRECCIÓN
# ─────────────────────────────────────────────────────────────────

def parse_address(raw: str) -> dict:
    """
    Separa una dirección en número + nombre de calle + tipo.
    
    Ejemplos:
      "85 Central Av"         → num=85, name="CENTRAL", type=["AV","AVE"]
      "2050 33Rd Ave"         → num=2050, name="33RD", type=["AVE","AV"]
      "3151 Franklin St"      → num=3151, name="FRANKLIN", type=["ST"]
      "680 Folsom St"         → num=680, name="FOLSOM", type=["ST"]
      "3151 Franklin St, SF"  → (ignora la parte después de la coma)
    """
    # Quitar ciudad/estado que puede venir después de la coma
    cleaned = raw.strip().split(",")[0].strip()
    cleaned = re.sub(r"\s+", " ", cleaned.upper())

    m = re.match(r"^(\d+[A-Z]?)\s+(.+)$", cleaned)
    if not m:
        return {"num": 0, "num_str": "", "name": cleaned, "types": [], "raw": cleaned}

    num_str    = m.group(1)
    street_raw = m.group(2).strip()
    num        = _to_int(num_str) or 0

    # Separar sufijo del nombre
    words  = street_raw.split()
    suffix = words[-1] if words else ""
    types  = SUFFIX_MAP.get(suffix, [])

    if types:
        name = " ".join(words[:-1])
    else:
        name  = " ".join(words)
        types = []

    return {
        "num":     num,
        "num_str": num_str,
        "name":    name,     # "CENTRAL", "33RD", "FRANKLIN"
        "types":   types,    # ["AV", "AVE"] or []
        "raw":     cleaned,
    }


# ─────────────────────────────────────────────────────────────────
# ESTRATEGIA 1: acdm-wktn → blklot → wv5m-vpq2 (owner)
# ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1024)
def _get_blklot(street_name: str, street_type: str, num: int) -> str:
    """
    Busca blklot en dataset de parcelas activas (acdm-wktn).
    Consulta por nombre de calle, filtra por número en Python.
    """
    url = "https://data.sfgov.org/resource/acdm-wktn.json"

    # Construir where: nombre exacto + opcionalmente el tipo
    where = f"UPPER(street_name) = '{street_name}'"
    if street_type:
        where += f" AND (UPPER(street_type) = '{street_type}'"
        # DataSF usa tanto "ST" como "STREET" — incluir ambos
        alts = SUFFIX_MAP.get(street_type, [])
        for alt in alts:
            if alt != street_type:
                where += f" OR UPPER(street_type) = '{alt}'"
        where += ")"

    try:
        resp = requests.get(
            url,
            params={
                "$where":  where,
                "$limit":  500,   # traer todos para filtrar en Python
                "$select": "blklot,from_address_num,to_address_num,odd_even",
            },
            headers=HEADERS,
            timeout=12,
        )
        if not resp.ok:
            return ""

        records = resp.json()
        if not records:
            return ""

        for rec in records:
            from_n = _to_int(rec.get("from_address_num"))
            to_n   = _to_int(rec.get("to_address_num"))
            if from_n is None or to_n is None:
                continue
            if from_n <= num <= to_n:
                blklot = (rec.get("blklot") or "").strip()
                if blklot:
                    logger.debug(f"blklot found: {street_name} {num} → {blklot}")
                    return blklot

        # Si no hay match exacto de rango, devolver blklot del primer registro
        # (útil para calles de un solo tramo)
        for rec in records:
            blklot = (rec.get("blklot") or "").strip()
            if blklot:
                return blklot

    except Exception as e:
        logger.debug(f"acdm-wktn [{street_name}]: {e}")

    return ""


@lru_cache(maxsize=1024)
def _blklot_to_owner(blklot: str) -> dict:
    """wv5m-vpq2: blklot → owner_name + mail_address"""
    if not blklot:
        return EMPTY.copy()

    try:
        resp = requests.get(
            "https://data.sfgov.org/resource/wv5m-vpq2.json",
            params={
                "$where":  f"blklot = '{blklot}'",
                "$limit":  5,
                "$select": "blklot,owner_name,mail_address,mail_city,mail_zipcode",
            },
            headers=HEADERS,
            timeout=12,
        )
        if resp.ok:
            for rec in resp.json():
                owner = (rec.get("owner_name") or "").title().strip()
                if owner and len(owner) > 2:
                    mail = " ".join(filter(None, [
                        rec.get("mail_address", ""),
                        rec.get("mail_city", ""),
                        rec.get("mail_zipcode", ""),
                    ])).strip()
                    logger.debug(f"owner found: {blklot} → {owner}")
                    return {"owner_name": owner, "mailing_address": mail,
                            "apn": blklot, "phone": ""}
    except Exception as e:
        logger.debug(f"wv5m-vpq2 [{blklot}]: {e}")

    return EMPTY.copy()


# ─────────────────────────────────────────────────────────────────
# ESTRATEGIA 2: wv5m-vpq2 directo por nombre de calle
# ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def _sf_assessor_by_name(street_name: str, num: int) -> dict:
    """
    Busca directamente en wv5m-vpq2 filtrando por nombre de calle.
    El filtro de número se hace en Python para evitar comparación de strings.
    """
    try:
        resp = requests.get(
            "https://data.sfgov.org/resource/wv5m-vpq2.json",
            params={
                "$where":  f"UPPER(street) LIKE '%{street_name[:20]}%'",
                "$limit":  200,
                "$select": "blklot,street,from_st,to_st,owner_name,mail_address,mail_city,mail_zipcode",
            },
            headers=HEADERS,
            timeout=12,
        )
        if not resp.ok:
            return EMPTY.copy()

        records = resp.json()

        # Filtrar por número en Python
        for rec in records:
            from_n = _to_int(rec.get("from_st"))
            to_n   = _to_int(rec.get("to_st"))
            if from_n is not None and to_n is not None:
                if from_n <= num <= to_n:
                    owner = (rec.get("owner_name") or "").title().strip()
                    if owner and len(owner) > 2:
                        mail = " ".join(filter(None, [
                            rec.get("mail_address", ""),
                            rec.get("mail_city", ""),
                            rec.get("mail_zipcode", ""),
                        ])).strip()
                        return {"owner_name": owner, "mailing_address": mail,
                                "apn": rec.get("blklot", ""), "phone": ""}

        # Si no hay match por rango, devolver el primero con owner
        for rec in records:
            owner = (rec.get("owner_name") or "").title().strip()
            if owner and len(owner) > 2:
                mail = " ".join(filter(None, [
                    rec.get("mail_address", ""),
                    rec.get("mail_city", ""),
                    rec.get("mail_zipcode", ""),
                ])).strip()
                return {"owner_name": owner, "mailing_address": mail,
                        "apn": rec.get("blklot", ""), "phone": ""}

    except Exception as e:
        logger.debug(f"sf_assessor_by_name [{street_name}]: {e}")

    return EMPTY.copy()


# ─────────────────────────────────────────────────────────────────
# LOOKUP SF COMPLETO (cascada de 3 estrategias)
# ─────────────────────────────────────────────────────────────────

def lookup_owner_sf(address: str) -> dict:
    """
    Busca el propietario de una dirección en San Francisco.
    3 estrategias en cascada.
    """
    p = parse_address(address)
    if not p["num"] or not p["name"]:
        return EMPTY.copy()

    num  = p["num"]
    name = p["name"]

    # Lista de tipos a intentar (incluye el detectado + variantes)
    type_variants = [""] + p["types"]  # "" = sin filtro de tipo, como fallback

    # ── Estrategia 1: acdm-wktn → blklot → wv5m-vpq2 ────────────
    for stype in type_variants:
        blklot = _get_blklot(name, stype, num)
        if blklot:
            result = _blklot_to_owner(blklot)
            if result.get("owner_name"):
                logger.info(f"SF owner [S1]: {address} → {result['owner_name']}")
                return result

    # ── Estrategia 2: wv5m-vpq2 directo por nombre de calle ──────
    result = _sf_assessor_by_name(name, num)
    if result.get("owner_name"):
        logger.info(f"SF owner [S2]: {address} → {result['owner_name']}")
        return result

    # ── Estrategia 3: solo primeras letras del nombre de calle ───
    # Útil para nombres con ortografía diferente ("33RD" vs "33 RD")
    short_name = name[:6]  # "FRANKL", "CENTRA", "33RD"
    if short_name != name:
        result = _sf_assessor_by_name(short_name, num)
        if result.get("owner_name"):
            logger.info(f"SF owner [S3]: {address} → {result['owner_name']}")
            return result

    logger.debug(f"SF owner NOT FOUND: {address}")
    return EMPTY.copy()


# ─────────────────────────────────────────────────────────────────
# OTROS CONDADOS
# ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def lookup_owner_alameda(address: str) -> dict:
    p = parse_address(address)
    result = EMPTY.copy()
    try:
        resp = requests.get(
            "https://data.acgov.org/resource/a2sq-oaix.json",
            params={
                "$where":  f"UPPER(situs_addr) LIKE '%{p['name'][:20]}%'",
                "$limit":  100,
                "$select": "apn,situs_addr,owner_name,mail_addr_1,mail_city,mail_zip",
            },
            headers=HEADERS,
            timeout=12,
        )
        if resp.ok:
            recs = resp.json()
            # Filtrar por número en Python
            for rec in recs:
                situs = rec.get("situs_addr", "")
                first_word = situs.strip().split()[0] if situs.strip() else ""
                if _to_int(first_word) == p["num"]:
                    owner = (rec.get("owner_name") or "").title().strip()
                    if owner:
                        result.update({
                            "owner_name":      owner,
                            "mailing_address": " ".join(filter(None, [
                                rec.get("mail_addr_1", ""),
                                rec.get("mail_city", ""),
                                rec.get("mail_zip", ""),
                            ])).strip(),
                            "apn": rec.get("apn", ""),
                        })
                        return result
            # Fallback: primero con owner
            for rec in recs:
                owner = (rec.get("owner_name") or "").title().strip()
                if owner:
                    result.update({
                        "owner_name":      owner,
                        "mailing_address": " ".join(filter(None, [
                            rec.get("mail_addr_1", ""),
                            rec.get("mail_city", ""),
                            rec.get("mail_zip", ""),
                        ])).strip(),
                        "apn": rec.get("apn", ""),
                    })
                    return result
    except Exception as e:
        logger.debug(f"Alameda [{address}]: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_owner_contra_costa(address: str) -> dict:
    p      = parse_address(address)
    result = EMPTY.copy()
    query  = f"{p['num_str']} {p['name']}"[:25]
    try:
        resp = requests.get(
            "https://gis.ccmap.us/arcgis/rest/services/CCC/CCCParcel/MapServer/0/query",
            params={
                "where":             f"UPPER(SITUS_ADDR) LIKE '%{query}%'",
                "outFields":         "APN,OWNER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_ZIP",
                "f":                 "json",
                "resultRecordCount": 5,
            },
            headers=HEADERS,
            timeout=12,
        )
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr  = feats[0].get("attributes", {})
                owner = (attr.get("OWNER_NAME") or "").title()
                if owner:
                    result.update({
                        "owner_name":      owner,
                        "mailing_address": " ".join(filter(None, [
                            attr.get("MAIL_ADDR", ""),
                            attr.get("MAIL_CITY", ""),
                            attr.get("MAIL_ZIP", ""),
                        ])).strip(),
                        "apn": str(attr.get("APN", "")),
                    })
    except Exception as e:
        logger.debug(f"ContraCosta [{address}]: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_owner_santa_clara(address: str) -> dict:
    p      = parse_address(address)
    result = EMPTY.copy()
    query  = f"{p['num_str']} {p['name']}"[:25]
    try:
        resp = requests.get(
            "https://gis.sccgov.org/arcgis/rest/services/ParcelServices/SCC_ParcelQueryService/MapServer/0/query",
            params={
                "where":             f"UPPER(SITUS_ADDR) LIKE '%{query}%'",
                "outFields":         "APN_FORMATTED,TAXPAYER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
                "f":                 "json",
                "resultRecordCount": 3,
            },
            headers=HEADERS,
            timeout=12,
        )
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr  = feats[0].get("attributes", {})
                owner = (attr.get("TAXPAYER_NAME") or "").title()
                if owner:
                    result.update({
                        "owner_name":      owner,
                        "mailing_address": " ".join(filter(None, [
                            attr.get("MAIL_ADDR", ""),
                            attr.get("MAIL_CITY", ""),
                            attr.get("MAIL_STATE", ""),
                            attr.get("MAIL_ZIP", ""),
                        ])).strip(),
                        "apn": str(attr.get("APN_FORMATTED", "")),
                    })
    except Exception as e:
        logger.debug(f"SantaClara [{address}]: {e}")
    return result


# ─────────────────────────────────────────────────────────────────
# DISPATCHER PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def lookup_owner_by_address(address: str, city: str) -> dict:
    """
    Punto de entrada. Elige la fuente según la ciudad.
    Retorna: {owner_name, mailing_address, apn, phone}
    """
    if not address or not address.strip():
        return EMPTY.copy()

    c = city.lower()

    if "san francisco" in c:
        return lookup_owner_sf(address)

    if any(x in c for x in ["oakland", "berkeley", "alameda", "fremont", "hayward",
                              "livermore", "san leandro", "emeryville", "castro valley",
                              "dublin", "pleasanton", "union city"]):
        return lookup_owner_alameda(address)

    if any(x in c for x in ["walnut creek", "concord", "richmond", "lafayette",
                              "martinez", "antioch", "pittsburg", "brentwood",
                              "danville", "orinda", "moraga", "el cerrito"]):
        return lookup_owner_contra_costa(address)

    if any(x in c for x in ["san jose", "santa clara", "sunnyvale", "milpitas",
                              "campbell", "cupertino", "gilroy", "morgan hill",
                              "los altos", "mountain view", "los gatos"]):
        return lookup_owner_santa_clara(address)

    return EMPTY.copy()


# ─────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────

def _to_int(val) -> int | None:
    """Convierte cualquier valor a int de forma segura, ignorando letras."""
    if val is None:
        return None
    try:
        # Extraer solo dígitos iniciales (ej: "85A" → 85)
        m = re.match(r"^(\d+)", str(val).strip())
        return int(m.group(1)) if m else None
    except (ValueError, TypeError):
        return None
