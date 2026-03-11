"""
utils/contact_enricher.py  v5.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FUENTES DE DATOS (en orden de confiabilidad)

  PROPIETARIO
  ─────────────────────────────────────────────
  1. SF DBI Permit Contacts   (3pee-9qhc)   ← por número de permiso
  2. SF Planning GIS Parcels  (ArcGIS REST) ← por dirección, incluye comercial
  3. SF Assessor Tax Rolls    (wv5m-vpq2)   ← fallback residencial SF
  4. County Assessors         (Alameda / ContraCosta / SantaClara)

  CONTRATISTA / INSTALADOR
  ─────────────────────────────────────────────
  1. SF DBI Permit Contacts   (3pee-9qhc)   ← OWNER, CONTRACTOR, APPLICANT
  2. CSLB scrape               (por licencia o nombre)
  3. OpenCorporates            (directorio CA)

  SCORES
  ─────────────────────────────────────────────
  contact_score      (0-5)
  lead_quality_score (0-10) — con scoring especial para solar
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import re
import time
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────
MIN_LEAD_VALUE = 50_000

INSULATION_KEYWORDS = [
    "insul", "spray foam", "attic", "crawl", "vapor", "weatheri",
    "adu", "accessory dwelling", "addition", "new construction",
    "new building", "remodel", "garage conversion", "basement",
    "tenant improvement", "second unit", "granny flat",
]

PERMIT_TYPE_SCORES = {
    "new construction":   4,
    "new building":       4,
    "adu":                4,
    "accessory dwelling": 4,
    "addition":           3,
    "remodel":            2,
    "alteration":         2,
    "renovation":         2,
    "tenant improvement": 2,
    "garage conversion":  3,
    "basement":           3,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ═════════════════════════════════════════════════════════════════
# FUENTE 1: SF DBI Building Permits Contacts (DataSF 3pee-9qhc)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=1024)
def lookup_sf_dbi_contacts(permit_number: str) -> dict:
    """
    Dataset: SF Building Permits Contacts (3pee-9qhc)
    Cubre TODOS los tipos de permiso DBI: building, electrical, solar,
    plumbing, mechanical. Actualizado diariamente.

    Formatos de número que intenta:
      - Tal cual:        202603046914  /  S20260109428
      - Sin prefijo:     20260109428   (quita letra inicial)
      - Solo dígitos:    20260109428
      - LIKE parcial:    %09428        (últimos 5 dígitos)
    """
    empty = {
        "owner": "", "owner_phone": "", "owner_mail_addr": "",
        "contractor": "", "contractor_phone": "", "contractor_addr": "",
        "contractor_license": "", "applicant": "", "applicant_phone": "",
        "architect": "",
    }
    if not permit_number:
        return empty

    pnum = permit_number.strip()

    # Generar variantes del número de permiso
    variants = [pnum]
    digits_only = re.sub(r"[^0-9]", "", pnum)
    if digits_only and digits_only != pnum:
        variants.append(digits_only)
    if pnum != pnum.lstrip("0"):
        variants.append(pnum.lstrip("0"))

    url = "https://data.sfgov.org/resource/3pee-9qhc.json"
    select = (
        "permit_number,contact_type,contact_name,"
        "contact_address,contact_city,contact_state,"
        "contact_zip,contact_phone,license_number"
    )

    # Intento A: búsqueda exacta con cada variante
    for v in variants:
        result = _dbi_contacts_query(url, f"permit_number = '{v}'", select)
        if result.get("owner") or result.get("contractor"):
            return result

    # Intento B: LIKE con los últimos 8 dígitos (más flexible)
    if len(digits_only) >= 8:
        suffix = digits_only[-8:]
        result = _dbi_contacts_query(
            url, f"permit_number LIKE '%{suffix}'", select
        )
        if result.get("owner") or result.get("contractor"):
            return result

    return empty


def _dbi_contacts_query(url: str, where: str, select: str) -> dict:
    result = {
        "owner": "", "owner_phone": "", "owner_mail_addr": "",
        "contractor": "", "contractor_phone": "", "contractor_addr": "",
        "contractor_license": "", "applicant": "", "applicant_phone": "",
        "architect": "",
    }
    try:
        resp = requests.get(
            url,
            params={"$where": where, "$limit": 20, "$select": select},
            headers=HEADERS,
            timeout=12,
        )
        if not resp.ok:
            return result
        records = resp.json()
        if not records:
            return result

        for rec in records:
            ctype = (rec.get("contact_type") or "").upper().strip()
            name  = (rec.get("contact_name") or "").title().strip()
            phone = _clean_phone(rec.get("contact_phone") or "")
            addr  = _fmt_addr(
                rec.get("contact_address", ""),
                rec.get("contact_city", ""),
                rec.get("contact_state", ""),
                rec.get("contact_zip", ""),
            )
            lic = (rec.get("license_number") or "").strip()

            if ctype == "OWNER" and not result["owner"]:
                result["owner"]           = name
                result["owner_phone"]     = phone
                result["owner_mail_addr"] = addr

            elif ctype in ("CONTRACTOR", "OWNER_BUILDER") and not result["contractor"]:
                result["contractor"]         = name
                result["contractor_phone"]   = phone
                result["contractor_addr"]    = addr
                result["contractor_license"] = lic

            elif ctype == "APPLICANT" and not result["applicant"]:
                result["applicant"]       = name
                result["applicant_phone"] = phone
                if not result["contractor"]:
                    result["contractor"]       = name
                    result["contractor_phone"] = phone
                    result["contractor_addr"]  = addr

            elif ctype in ("ARCHITECT", "ENGINEER") and not result["architect"]:
                result["architect"] = name

    except Exception as e:
        logger.debug(f"DBI contacts query [{where[:50]}]: {e}")
    return result


# ═════════════════════════════════════════════════════════════════
# FUENTE 2: SF Planning GIS Parcels  (ArcGIS REST — sin API key)
# ═════════════════════════════════════════════════════════════════
# Cubre propiedades residenciales Y comerciales.
# Especialmente bueno para edificios grandes (680 Folsom, 1 Market, etc.)

@lru_cache(maxsize=512)
def lookup_sf_planning_gis(address: str) -> dict:
    """
    SF Planning Department GIS Parcel Service.
    Endpoint: MapServer/0 del servicio de parcelas públicas de SF.
    Devuelve: propietario, APN, dirección postal, uso del suelo.
    """
    result = {"owner_name": "", "mailing_address": "", "apn": "", "land_use": ""}
    if not address:
        return result

    clean = address.strip().upper()

    # Servicio GIS principal del Planning Department
    urls_to_try = [
        # SF Planning Parcels service
        "https://sfplanninggis.org/arcgis/rest/services/zoning/MapServer/1/query",
        # SF assessor parcel service alternativo
        "https://sfgis.org/arcgis/rest/services/SF_Parcels/MapServer/0/query",
    ]

    for base_url in urls_to_try:
        try:
            # Extraer número de calle y nombre
            parts   = clean.split()
            s_num   = parts[0] if parts else ""
            s_words = parts[1:3] if len(parts) > 1 else []

            params = {
                "where":             (
                    f"UPPER(ADDRESS) LIKE '%{s_num}%' AND "
                    + " AND ".join(
                        f"UPPER(ADDRESS) LIKE '%{w}%'" for w in s_words[:1]
                    ) if s_words else f"UPPER(ADDRESS) LIKE '%{s_num}%'"
                ),
                "outFields":         "*",
                "f":                 "json",
                "resultRecordCount": 3,
                "returnGeometry":    "false",
            }
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=10)
            if not resp.ok:
                continue

            feats = resp.json().get("features", [])
            if not feats:
                continue

            attr = feats[0].get("attributes", {})
            # Intentar múltiples nombres de campo (varían entre servicios)
            owner = (
                attr.get("OWNER") or attr.get("OWNNAME") or
                attr.get("OWNER_NAME") or attr.get("TAXPAYER") or ""
            ).title().strip()
            mail = _fmt_addr(
                attr.get("MAIL_ADDR") or attr.get("MAILADDR") or "",
                attr.get("MAIL_CITY") or attr.get("MAILCITY") or "",
                attr.get("MAIL_STATE") or "",
                attr.get("MAIL_ZIP") or attr.get("MAILZIP") or "",
            )
            apn = str(
                attr.get("APN") or attr.get("BLKLOT") or attr.get("PARCEL_NUM") or ""
            ).strip()

            if owner:
                result["owner_name"]      = owner
                result["mailing_address"] = mail
                result["apn"]             = apn
                result["land_use"]        = str(attr.get("LANDUSE") or attr.get("LAND_USE") or "")
                return result

        except Exception as e:
            logger.debug(f"SF Planning GIS [{base_url[:50]}]: {e}")

    return result


# ═════════════════════════════════════════════════════════════════
# FUENTE 3: SF Assessor Tax Rolls  (DataSF wv5m-vpq2)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=512)
def lookup_sf_assessor(address: str) -> dict:
    """
    SF Assessor-Recorder Historical Secured Property Tax Rolls.
    Mejor para residencial. Intenta 3 estrategias de query.
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    if not address:
        return result

    clean  = address.strip().upper()
    parts  = clean.split()
    s_num  = parts[0] if parts else ""
    # Extraer nombre de calle sin número y sufijo
    s_name = " ".join(parts[1:3]) if len(parts) > 1 else ""
    # Solo primer palabra del nombre de calle (más flexible)
    s_word = parts[1] if len(parts) > 1 else ""

    url    = "https://data.sfgov.org/resource/wv5m-vpq2.json"
    select = "blklot,street,owner_name,mail_address,mail_city,mail_zipcode"

    strategies = [
        # Estrategia 1: número exacto + nombre de calle
        (
            f"UPPER(street) LIKE '%{s_word}%' "
            f"AND from_st <= '{s_num}' AND to_st >= '{s_num}'",
        ),
        # Estrategia 2: solo primera palabra del street + número
        (f"UPPER(street) LIKE '%{s_word[:8]}%' AND from_st = '{s_num}'",),
        # Estrategia 3: búsqueda por nombre de calle sola (más amplia)
        (f"UPPER(street) LIKE '%{s_name[:12]}%'",),
    ]

    for (where,) in strategies:
        try:
            resp = requests.get(
                url,
                params={"$where": where, "$limit": 5, "$select": select},
                headers=HEADERS,
                timeout=10,
            )
            if not resp.ok:
                continue
            recs = resp.json()
            if not recs:
                continue

            # Priorizar el que tenga owner_name no vacío
            for rec in recs:
                owner = (rec.get("owner_name") or "").title().strip()
                if owner and len(owner) > 2:
                    result["owner_name"]      = owner
                    result["mailing_address"] = " ".join(filter(None, [
                        rec.get("mail_address", ""),
                        rec.get("mail_city", ""),
                        rec.get("mail_zipcode", ""),
                    ])).strip()
                    result["apn"] = rec.get("blklot", "")
                    return result

        except Exception as e:
            logger.debug(f"SF Assessor strategy: {e}")

    return result


# ═════════════════════════════════════════════════════════════════
# FUENTE 4: County Assessors (otros condados del Bay Area)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=256)
def lookup_alameda_assessor(address: str) -> dict:
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url    = "https://data.acgov.org/resource/a2sq-oaix.json"
        params = {
            "$where":  f"UPPER(situs_addr) LIKE '%{address.split(',')[0].strip().upper()[:30]}%'",
            "$limit":  3,
            "$select": "apn,owner_name,mail_addr_1,mail_city,mail_zip",
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            recs = resp.json()
            if recs:
                rec = recs[0]
                result["owner_name"]      = (rec.get("owner_name") or "").title()
                result["mailing_address"] = _fmt_addr(
                    rec.get("mail_addr_1", ""), rec.get("mail_city", ""),
                    "", rec.get("mail_zip", ""))
                result["apn"] = rec.get("apn", "")
    except Exception as e:
        logger.debug(f"Alameda Assessor: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_contra_costa_assessor(address: str) -> dict:
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url    = "https://gis.ccmap.us/arcgis/rest/services/CCC/CCCParcel/MapServer/0/query"
        params = {
            "where":             f"UPPER(SITUS_ADDR) LIKE '%{address.split(',')[0].strip().upper()[:25]}%'",
            "outFields":         "APN,OWNER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_ZIP",
            "f":                 "json",
            "resultRecordCount": 3,
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("OWNER_NAME") or "").title()
                result["mailing_address"] = _fmt_addr(
                    attr.get("MAIL_ADDR", ""), attr.get("MAIL_CITY", ""),
                    "", attr.get("MAIL_ZIP", ""))
                result["apn"] = attr.get("APN", "")
    except Exception as e:
        logger.debug(f"Contra Costa Assessor: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_santa_clara_assessor(address: str) -> dict:
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url    = "https://gis.sccgov.org/arcgis/rest/services/ParcelServices/SCC_ParcelQueryService/MapServer/0/query"
        params = {
            "where":             f"UPPER(SITUS_ADDR) LIKE '%{address.split(',')[0].strip().upper()[:25]}%'",
            "outFields":         "APN_FORMATTED,TAXPAYER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
            "f":                 "json",
            "resultRecordCount": 3,
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("TAXPAYER_NAME") or "").title()
                result["mailing_address"] = _fmt_addr(
                    attr.get("MAIL_ADDR", ""), attr.get("MAIL_CITY", ""),
                    attr.get("MAIL_STATE", ""), attr.get("MAIL_ZIP", ""))
                result["apn"] = attr.get("APN_FORMATTED", "")
    except Exception as e:
        logger.debug(f"Santa Clara Assessor: {e}")
    return result


def lookup_owner_sf(address: str) -> dict:
    """
    Cascada completa para SF: Planning GIS → Assessor Tax Rolls.
    Devuelve el primero que tenga owner_name.
    """
    # 1. SF Planning GIS (cubre commercial y residential)
    result = lookup_sf_planning_gis(address)
    if result.get("owner_name"):
        return result

    # 2. SF Assessor Tax Rolls (residencial)
    result = lookup_sf_assessor(address)
    if result.get("owner_name"):
        return result

    return {"owner_name": "", "mailing_address": "", "apn": ""}


def lookup_assessor_by_county(address: str, city: str) -> dict:
    c = city.lower()
    if "san francisco" in c:
        return lookup_owner_sf(address)
    if any(x in c for x in ["oakland", "berkeley", "alameda", "fremont",
                              "hayward", "livermore", "san leandro", "emeryville"]):
        return lookup_alameda_assessor(address)
    if any(x in c for x in ["walnut creek", "concord", "richmond", "lafayette",
                              "martinez", "antioch", "pittsburg", "brentwood"]):
        return lookup_contra_costa_assessor(address)
    if any(x in c for x in ["san jose", "santa clara", "sunnyvale", "milpitas",
                              "campbell", "cupertino", "gilroy", "morgan hill"]):
        return lookup_santa_clara_assessor(address)
    return {"owner_name": "", "mailing_address": "", "apn": ""}


# ═════════════════════════════════════════════════════════════════
# FUENTE 5: CSLB — Contratista (fallback para no-SF / sin DBI data)
# ═════════════════════════════════════════════════════════════════

def lookup_contractor_cslb(license_number: str = "", company_name: str = "") -> dict:
    empty = {
        "contractor_phone":   "",
        "contractor_addr":    "",
        "contractor_city":    "",
        "contractor_zip":     "",
        "contractor_status":  "",
        "contractor_license": license_number or "",
        "contractor_types":   "",
    }
    if license_number:
        r = _cslb_api_json(license_number)
        if r.get("contractor_phone") or r.get("contractor_addr"):
            return r
        r = _cslb_scrape_html(license_number)
        if r.get("contractor_phone") or r.get("contractor_addr"):
            return r
    if company_name:
        r = _opencorporates_lookup(company_name)
        if r.get("contractor_addr"):
            return r
    return empty


def _cslb_api_json(lic: str) -> dict:
    result = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
              "contractor_zip": "", "contractor_status": "", "contractor_license": lic,
              "contractor_types": ""}
    try:
        num  = lic.strip().lstrip("0")
        url  = (
            "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/"
            f"LicenseQueryHandler.ashx?LicenseNumber={num}&BoardType=C"
        )
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if not resp.ok:
            return result
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            data = resp.json()
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                result["contractor_phone"]  = _clean_phone(data.get("Phone", ""))
                result["contractor_addr"]   = data.get("Address", "")
                result["contractor_city"]   = data.get("City", "")
                result["contractor_zip"]    = data.get("Zip", "")
                result["contractor_status"] = data.get("LicenseStatus", "")
                result["contractor_types"]  = data.get("Classifications", "")
    except Exception as e:
        logger.debug(f"CSLB JSON: {e}")
    return result


def _cslb_scrape_html(lic: str) -> dict:
    result = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
              "contractor_zip": "", "contractor_status": "", "contractor_license": lic,
              "contractor_types": ""}
    try:
        num  = lic.strip().lstrip("0")
        url  = (
            "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/"
            f"CheckLicense.aspx?LicNum={num}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if not resp.ok:
            return result

        text = resp.text

        def _between(s, a, b):
            try:
                i = s.index(a) + len(a)
                j = s.index(b, i)
                return re.sub(r"<[^>]+>", "", s[i:j]).strip()
            except ValueError:
                return ""

        phone  = _between(text, "lblPhone", "</span>")
        addr   = _between(text, "lblAddress", "</span>")
        city   = _between(text, "lblCity", "</span>")
        status = _between(text, "lblLicenseStatus", "</span>")
        types  = _between(text, "lblClassification", "</span>")

        if not phone:
            m = re.search(r"\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", text)
            phone = m.group(0) if m else ""

        result["contractor_phone"]  = _clean_phone(phone)
        result["contractor_addr"]   = addr
        result["contractor_city"]   = city
        result["contractor_status"] = status
        result["contractor_types"]  = types
        time.sleep(0.4)
    except Exception as e:
        logger.debug(f"CSLB scrape: {e}")
    return result


def _opencorporates_lookup(company_name: str) -> dict:
    result = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
              "contractor_zip": "", "contractor_status": "", "contractor_license": "",
              "contractor_types": ""}
    try:
        url    = "https://api.opencorporates.com/v0.4/companies/search"
        params = {"q": company_name[:50], "jurisdiction_code": "us_ca", "per_page": 3}
        resp   = requests.get(url, params=params, timeout=10)
        if not resp.ok:
            return result
        companies = resp.json().get("results", {}).get("companies", [])
        if companies:
            comp     = companies[0].get("company", {})
            addr_obj = comp.get("registered_address", {})
            result["contractor_addr"]   = addr_obj.get("street_address", "")
            result["contractor_city"]   = addr_obj.get("locality", "")
            result["contractor_zip"]    = addr_obj.get("postal_code", "")
            result["contractor_status"] = comp.get("current_status", "")
    except Exception as e:
        logger.debug(f"OpenCorporates: {e}")
    return result


# ═════════════════════════════════════════════════════════════════
# FUENTE 6: Geocodificación — Nominatim (OSM)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=512)
def geocode_address(address: str, city: str = "CA") -> dict:
    result = {"lat": "", "lon": "", "maps_url": ""}
    try:
        url    = "https://nominatim.openstreetmap.org/search"
        params = {"q": f"{address}, {city}, California, USA", "format": "json", "limit": 1}
        resp   = requests.get(
            url, params=params,
            headers={"User-Agent": "InsulTechs-LeadAgent/5.0 admin@insultechs.com"},
            timeout=8,
        )
        if resp.ok:
            data = resp.json()
            if data:
                lat = data[0].get("lat", "")
                lon = data[0].get("lon", "")
                result["lat"]      = lat
                result["lon"]      = lon
                result["maps_url"] = f"https://maps.google.com/?q={lat},{lon}"
    except Exception as e:
        logger.debug(f"Geocode: {e}")
    return result


# ═════════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL: enrich_lead()
# ═════════════════════════════════════════════════════════════════

def enrich_lead(lead: dict, lead_type: str = "permit") -> dict:
    """
    Enriquece un lead con toda la información de contacto.

    lead_type: "permit" | "solar"
      - "solar": scoring especial por kW, sin filtro de $50K
    """
    address   = lead.get("address", "")
    city      = lead.get("city", "")
    permit_no = lead.get("permit_no", "")
    is_sf     = "san francisco" in city.lower()

    # ── 1. SF DBI Contacts ───────────────────────────────────────
    dbi_data = {}
    if is_sf and permit_no:
        try:
            dbi_data = lookup_sf_dbi_contacts(permit_no)
            logger.debug(
                f"DBI [{permit_no}] → "
                f"owner={dbi_data.get('owner') or '—'} "
                f"ctr={dbi_data.get('contractor') or '—'}"
            )
        except Exception as e:
            logger.debug(f"DBI error: {e}")

    # ── 2. Propietario: DBI → Assessor/GIS ──────────────────────
    existing_owner = (lead.get("owner") or "").strip().lower()
    blank_owner    = not existing_owner or existing_owner in ("no indicado", "n/a", "")

    if dbi_data.get("owner"):
        lead["owner"]           = dbi_data["owner"]
        lead["owner_mail_addr"] = dbi_data.get("owner_mail_addr", "")
        if not lead.get("owner_phone"):
            lead["owner_phone"] = dbi_data.get("owner_phone", "")
    elif blank_owner:
        try:
            assessor = lookup_assessor_by_county(address, city) if address else {}
        except Exception:
            assessor = {}
        lead["owner"]           = assessor.get("owner_name") or "No encontrado"
        lead["owner_mail_addr"] = assessor.get("mailing_address", "")
        if not lead.get("apn"):
            lead["apn"] = assessor.get("apn", "")
        # Guardar land_use si lo obtuvimos del GIS
        if assessor.get("land_use"):
            lead["land_use"] = assessor["land_use"]

    # ── 3. Contratista: DBI → CSLB ──────────────────────────────
    existing_ctr = (lead.get("contractor") or "").strip().lower()
    blank_ctr    = not existing_ctr or existing_ctr in ("no indicado", "n/a", "")

    if dbi_data.get("contractor"):
        if blank_ctr:
            lead["contractor"] = dbi_data["contractor"]
        if not lead.get("contractor_phone"):
            lead["contractor_phone"] = dbi_data.get("contractor_phone", "")
        if not lead.get("contractor_addr"):
            lead["contractor_addr"] = dbi_data.get("contractor_addr", "")
        if not lead.get("contractor_license"):
            lead["contractor_license"] = dbi_data.get("contractor_license", "")

    # CSLB si aún falta
    ctr_name = (lead.get("contractor") or "").strip()
    ctr_lic  = (lead.get("contractor_license") or "").strip()
    if ctr_name and ctr_name.lower() not in ("no indicado", "n/a", ""):
        if not lead.get("contractor_phone") or not lead.get("contractor_addr"):
            try:
                cslb = lookup_contractor_cslb(ctr_lic, ctr_name)
                if not lead.get("contractor_phone"):
                    lead["contractor_phone"] = cslb.get("contractor_phone", "")
                if not lead.get("contractor_addr"):
                    lead["contractor_addr"] = _fmt_addr(
                        cslb.get("contractor_addr", ""),
                        cslb.get("contractor_city", ""),
                        "",
                        cslb.get("contractor_zip", ""),
                    )
                if not lead.get("contractor_status"):
                    lead["contractor_status"] = cslb.get("contractor_status", "")
                if not lead.get("contractor_types"):
                    lead["contractor_types"]  = cslb.get("contractor_types", "")
            except Exception as e:
                logger.debug(f"CSLB: {e}")

    # Aplicante / Arquitecto del DBI
    if dbi_data.get("applicant") and not lead.get("applicant"):
        lead["applicant"]       = dbi_data["applicant"]
        lead["applicant_phone"] = dbi_data.get("applicant_phone", "")
    if dbi_data.get("architect") and not lead.get("architect"):
        lead["architect"] = dbi_data["architect"]

    # ── 4. Geocodificación ───────────────────────────────────────
    if address and not lead.get("maps_url"):
        try:
            geo = geocode_address(address, city)
            lead["maps_url"] = geo.get("maps_url", "")
            lead["lat"]      = geo.get("lat", "")
            lead["lon"]      = geo.get("lon", "")
        except Exception as e:
            logger.debug(f"Geocode: {e}")

    # ── 5. Scores ────────────────────────────────────────────────
    lead["contact_score"]      = calc_contact_score(lead)
    lead["lead_quality_score"] = calc_lead_quality_score(lead, lead_type)
    emoji, label               = lead_quality_label(lead["lead_quality_score"])
    lead["quality_emoji"]      = emoji
    lead["quality_label"]      = label

    return lead


# ═════════════════════════════════════════════════════════════════
# SCORING
# ═════════════════════════════════════════════════════════════════

def calc_contact_score(lead: dict) -> int:
    """Score 0-5: completitud de datos de contacto."""
    score = 0
    owner = (lead.get("owner") or "").strip()
    if owner and owner.lower() not in ("no indicado", "no encontrado", "n/a", ""):
        score += 1
    if lead.get("owner_mail_addr") or lead.get("owner_phone"):
        score += 1
    ctr = (lead.get("contractor") or "").strip()
    if ctr and ctr.lower() not in ("no indicado", "n/a", ""):
        score += 1
    if lead.get("contractor_phone"):
        score += 1
    if lead.get("maps_url"):
        score += 1
    return score


def calc_lead_quality_score(lead: dict, lead_type: str = "permit") -> int:
    """
    Score 0-10 de calidad del lead.

    Para "solar": scoring especial por kW instalado.
      Los permisos solares siempre reportan $1 de costo — ignorar costo.
      Puntuar por tamaño del sistema solar.

    Para "permit": scoring por valor + tipo de permiso + relevancia.
    """
    if lead_type == "solar":
        return _solar_quality_score(lead)
    return _permit_quality_score(lead)


def _solar_quality_score(lead: dict) -> int:
    """
    Solar: score por kW instalado + tipo de propiedad + contacto.
    """
    score = 0

    # kW del sistema
    kw_raw = (lead.get("kw_installed") or lead.get("description") or "")
    kw     = _parse_kw(kw_raw)
    if kw >= 20:    score += 4   # Sistema comercial grande
    elif kw >= 10:  score += 3   # Sistema residencial grande
    elif kw >= 5:   score += 2   # Sistema residencial estándar
    elif kw > 0:    score += 1   # Sistema pequeño
    else:           score += 2   # Desconocido → asumir residencial

    # Bonus por incluir batería / energy storage
    desc = (lead.get("description") or "").lower()
    if any(w in desc for w in ["battery", "storage", "powerwall", "enphase"]):
        score += 2  # Sistema premium = propietario consciente del ahorro

    # Tipo de propiedad (si disponible)
    if any(w in desc for w in ["commercial", "office", "retail", "industrial"]):
        score += 1   # Comercial → más potencial

    # Contacto disponible
    if calc_contact_score(lead) >= 3:
        score += 1
    elif calc_contact_score(lead) >= 1:
        score += 1

    # Siempre mínimo 5 (los solares son leads valiosos)
    return max(min(score, 10), 5)


def _permit_quality_score(lead: dict) -> int:
    """Scoring para permisos de construcción."""
    score = 0

    cost = _parse_cost(lead.get("estimated_cost", ""))
    if cost >= 500_000:   score += 4
    elif cost >= 200_000: score += 3
    elif cost >= 100_000: score += 2
    elif cost >= 50_000:  score += 1

    combined = (
        (lead.get("permit_type") or "") + " " +
        (lead.get("description") or "")
    ).lower()
    best = 0
    for kw, pts in PERMIT_TYPE_SCORES.items():
        if kw in combined:
            best = max(best, pts)
    score += min(best, 3)

    hits = sum(1 for kw in INSULATION_KEYWORDS if kw in combined)
    if hits >= 3:   score += 2
    elif hits >= 1: score += 1

    if calc_contact_score(lead) >= 3:
        score += 1

    return min(score, 10)


def lead_quality_label(score: int) -> tuple:
    if score >= 10: return "🌟", "EXCELENTE"
    if score >= 8:  return "🔥", "MUY BUENO"
    if score >= 6:  return "✅", "BUENO"
    if score >= 4:  return "⚠️", "REGULAR"
    return "❌", "DESCARTADO"


def should_send_lead(lead: dict, lead_type: str = "permit") -> bool:
    """
    Criterios para enviar:
      permit: valor >= $50K  Y  quality >= 6
      solar:  siempre quality >= 5 (sin filtro de $50K)
    """
    quality = lead.get("lead_quality_score", 0)
    if lead_type == "solar":
        return quality >= 5
    return (
        _parse_cost(lead.get("estimated_cost", "")) >= MIN_LEAD_VALUE
        and quality >= 6
    )


# ═════════════════════════════════════════════════════════════════
# HELPERS PÚBLICOS
# ═════════════════════════════════════════════════════════════════

def contact_score_label(score: int) -> str:
    return {
        0: "⬜ Sin datos",
        1: "🟥 Mínimo",
        2: "🟧 Básico",
        3: "🟨 Bueno",
        4: "🟩 Completo",
        5: "🌟 Completo+",
    }.get(score, "⬜ Sin datos")


# ═════════════════════════════════════════════════════════════════
# HELPERS PRIVADOS
# ═════════════════════════════════════════════════════════════════

def _parse_cost(raw) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def _parse_kw(raw: str) -> float:
    """Extrae kW de texto como '6.6 kW' o '6600 watts'."""
    if not raw:
        return 0.0
    # kW directo
    m = re.search(r"(\d+\.?\d*)\s*kw", str(raw), re.IGNORECASE)
    if m:
        return float(m.group(1))
    # Watts → kW
    m2 = re.search(r"(\d+)\s*w(?:att)?s?", str(raw), re.IGNORECASE)
    if m2:
        return float(m2.group(1)) / 1000
    return 0.0


def _clean_phone(raw: str) -> str:
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == "1":
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw.strip()


def _fmt_addr(*parts) -> str:
    return ", ".join(p.strip() for p in parts if p and p.strip())
