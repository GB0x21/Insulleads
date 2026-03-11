"""
utils/contact_enricher.py  v4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FUENTES DE ENRIQUECIMIENTO (en orden de confiabilidad):

  ┌─ PROPIETARIO ─────────────────────────────────┐
  │ 1. SF DBI Permit Contacts  (DataSF 3pee-9qhc) │  ← NUEVA, más confiable
  │    contact_type = "OWNER" → nombre + dirección │
  │ 2. SF Assessor Parcel      (DataSF wv5m-vpq2)  │
  │ 3. Alameda / ContraCosta / SantaClara Assessor │
  └───────────────────────────────────────────────┘

  ┌─ CONTRATISTA ─────────────────────────────────┐
  │ 1. SF DBI Permit Contacts  (DataSF 3pee-9qhc) │  ← NUEVA, más confiable
  │    contact_type IN ("CONTRACTOR","APPLICANT")  │
  │ 2. CSLB API JSON                               │
  │ 3. CSLB HTML scrape                            │
  │ 4. OpenCorporates                              │
  └───────────────────────────────────────────────┘

  ┌─ SCORE DE CALIDAD ─────────────────────────────┐
  │ contact_score     (0-5)                         │
  │ lead_quality_score (0-10)                       │
  └────────────────────────────────────────────────┘
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
    "tenant improvement", "second unit", "granny flat"
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
# Este dataset es el más completo: contiene TODOS los contactos
# asociados a cada permiso (owner, applicant, contractor, architect)
# con nombre, dirección, teléfono y número de licencia.
# Se actualiza diariamente por el DBI.

@lru_cache(maxsize=512)
def lookup_sf_dbi_contacts(permit_number: str) -> dict:
    """
    Consulta el dataset SF Building Permits Contacts (3pee-9qhc).
    Retorna un dict con owner y contractor separados.

    contact_type posibles: OWNER, APPLICANT, CONTRACTOR, ARCHITECT,
                           ENGINEER, OWNER_BUILDER, FIRE, etc.
    """
    result = {
        "owner":            "",
        "owner_phone":      "",
        "owner_mail_addr":  "",
        "contractor":       "",
        "contractor_phone": "",
        "contractor_addr":  "",
        "contractor_license": "",
        "applicant":        "",
        "applicant_phone":  "",
        "architect":        "",
    }

    if not permit_number:
        return result

    # El número de permiso en el dataset puede venir sin año o con formato diferente
    # Intentamos con el número tal cual y también sin ceros iniciales
    pnums_to_try = [permit_number.strip()]
    # SF format: 202603046914 → también probar 202603046914
    if len(permit_number) > 6:
        pnums_to_try.append(permit_number.lstrip("0"))

    url = "https://data.sfgov.org/resource/3pee-9qhc.json"

    for pnum in pnums_to_try:
        try:
            params = {
                "$where":  f"permit_number = '{pnum}'",
                "$limit":  20,
                "$select": (
                    "permit_number,contact_type,contact_name,"
                    "contact_address,contact_city,contact_state,"
                    "contact_zip,contact_phone,license_number"
                ),
            }
            resp = requests.get(url, params=params, headers=HEADERS, timeout=12)
            if not resp.ok:
                continue

            records = resp.json()
            if not records:
                continue

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
                lic   = (rec.get("license_number") or "").strip()

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
                    # Si no hay contratista, usar el aplicante como fallback
                    if not result["contractor"]:
                        result["contractor"]       = name
                        result["contractor_phone"] = phone
                        result["contractor_addr"]  = addr

                elif ctype in ("ARCHITECT", "ENGINEER") and not result["architect"]:
                    result["architect"] = name

            # Si encontramos datos, no seguir intentando
            if result["owner"] or result["contractor"]:
                break

        except Exception as e:
            logger.debug(f"DBI Contacts [{pnum}]: {e}")

    return result


# ═════════════════════════════════════════════════════════════════
# FUENTE 2: CSLB — Contratista (fallback para ciudades no-SF)
# ═════════════════════════════════════════════════════════════════

def lookup_contractor_cslb(license_number: str = "", company_name: str = "") -> dict:
    """Busca datos del contratista en el CSLB. 3 métodos en cascada."""
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
                start = s.index(a) + len(a)
                end   = s.index(b, start)
                return re.sub(r"<[^>]+>", "", s[start:end]).strip()
            except ValueError:
                return ""

        phone  = _between(text, "lblPhone", "</span>")
        addr   = _between(text, "lblAddress", "</span>")
        city   = _between(text, "lblCity", "</span>")
        status = _between(text, "lblLicenseStatus", "</span>")
        types  = _between(text, "lblClassification", "</span>")

        if not phone:
            phones = re.findall(r"\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", text)
            phone  = phones[0] if phones else ""

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
# FUENTE 3: County Assessors (propietario por dirección)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=512)
def lookup_sf_assessor(address: str) -> dict:
    """
    SF Assessor-Recorder: propietario + dirección postal.
    Dataset: https://data.sfgov.org/resource/wv5m-vpq2.json
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    if not address:
        return result

    clean  = address.strip().upper()
    parts  = clean.split()
    s_num  = parts[0] if parts else ""
    s_name = " ".join(parts[1:3]) if len(parts) > 1 else ""

    # Método A: buscar por número de calle + nombre de calle
    try:
        url    = "https://data.sfgov.org/resource/wv5m-vpq2.json"
        params = {
            "$where":  (
                f"UPPER(street) LIKE '%{s_name[:20]}%' "
                f"AND from_st <= '{s_num}' AND to_st >= '{s_num}'"
            ),
            "$limit":  5,
            "$select": "blklot,street,owner_name,mail_address,mail_city,mail_zipcode",
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            recs = resp.json()
            if recs:
                rec = recs[0]
                owner = (rec.get("owner_name") or "").title().strip()
                if owner:
                    result["owner_name"]      = owner
                    result["mailing_address"] = " ".join(filter(None, [
                        rec.get("mail_address", ""),
                        rec.get("mail_city", ""),
                        rec.get("mail_zipcode", ""),
                    ])).strip()
                    result["apn"] = rec.get("blklot", "")
                    return result
    except Exception as e:
        logger.debug(f"SF Assessor A: {e}")

    # Método B: búsqueda fulltext por dirección completa
    try:
        url    = "https://data.sfgov.org/resource/wv5m-vpq2.json"
        params = {
            "$where":  f"UPPER(from_address) = '{s_num}' AND UPPER(street) LIKE '%{s_name[:15]}%'",
            "$limit":  3,
            "$select": "blklot,owner_name,mail_address,mail_city,mail_zipcode",
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if resp.ok:
            recs = resp.json()
            if recs:
                rec = recs[0]
                result["owner_name"]      = (rec.get("owner_name") or "").title().strip()
                result["mailing_address"] = " ".join(filter(None, [
                    rec.get("mail_address", ""),
                    rec.get("mail_city", ""),
                    rec.get("mail_zipcode", ""),
                ])).strip()
                result["apn"] = rec.get("blklot", "")
    except Exception as e:
        logger.debug(f"SF Assessor B: {e}")

    return result


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
                    rec.get("mail_addr_1", ""), rec.get("mail_city", ""), "", rec.get("mail_zip", ""))
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
                    attr.get("MAIL_ADDR", ""), attr.get("MAIL_CITY", ""), "", attr.get("MAIL_ZIP", ""))
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


def lookup_assessor_by_county(address: str, city: str) -> dict:
    c = city.lower()
    if "san francisco" in c:
        return lookup_sf_assessor(address)
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
# FUENTE 4: Geocodificación — Nominatim (OSM)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=512)
def geocode_address(address: str, city: str = "CA") -> dict:
    result = {"lat": "", "lon": "", "maps_url": ""}
    try:
        url    = "https://nominatim.openstreetmap.org/search"
        params = {"q": f"{address}, {city}, California, USA", "format": "json", "limit": 1}
        headers = {"User-Agent": "InsulTechs-LeadAgent/4.0 admin@insultechs.com"}
        resp = requests.get(url, params=params, headers=headers, timeout=8)
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

def enrich_lead(lead: dict) -> dict:
    """
    Enriquece un lead con toda la información de contacto disponible.
    Prioridad de fuentes:
      SF leads  → DBI Contacts dataset (número de permiso) > Assessor
      Otros     → CSLB > Assessor > OpenCorporates
    """
    address    = lead.get("address", "")
    city       = lead.get("city", "")
    permit_no  = lead.get("permit_no", "")
    is_sf      = "san francisco" in city.lower()

    # ── 1. SF DBI Contacts (la fuente más completa para SF) ──────
    dbi_data = {}
    if is_sf and permit_no:
        try:
            dbi_data = lookup_sf_dbi_contacts(permit_no)
            logger.debug(
                f"DBI Contacts [{permit_no}]: "
                f"owner={dbi_data.get('owner')} "
                f"ctr={dbi_data.get('contractor')}"
            )
        except Exception as e:
            logger.debug(f"DBI contacts error: {e}")

    # ── 2. Propietario: DBI → Assessor → lo que ya tenía ────────
    existing_owner = (lead.get("owner") or "").strip().lower()
    blank_owner    = not existing_owner or existing_owner in ("no indicado", "n/a", "")

    if dbi_data.get("owner"):
        lead["owner"]          = dbi_data["owner"]
        lead["owner_mail_addr"] = dbi_data.get("owner_mail_addr", "")
        lead["owner_phone"]    = dbi_data.get("owner_phone", "") or lead.get("owner_phone", "")
    elif blank_owner:
        try:
            assessor = lookup_assessor_by_county(address, city) if address else {}
        except Exception:
            assessor = {}
        lead["owner"]          = assessor.get("owner_name") or "No encontrado"
        lead["owner_mail_addr"] = assessor.get("mailing_address", "")
        lead["apn"]             = assessor.get("apn", "")

    # ── 3. Contratista: DBI → CSLB → lo que ya tenía ────────────
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

    # Si aún falta info del contratista → CSLB
    ctr_name = (lead.get("contractor") or "").strip()
    ctr_lic  = (lead.get("contractor_license") or "").strip()
    needs_cslb = (
        ctr_name and ctr_name.lower() not in ("no indicado", "n/a", "") and
        (not lead.get("contractor_phone") or not lead.get("contractor_addr"))
    )
    if needs_cslb:
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
            logger.debug(f"CSLB enrichment: {e}")

    # Guardar el aplicante/arquitecto si los tenemos del DBI
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
    lead["lead_quality_score"] = calc_lead_quality_score(lead)
    emoji, label               = lead_quality_label(lead["lead_quality_score"])
    lead["quality_emoji"]      = emoji
    lead["quality_label"]      = label

    return lead


# ═════════════════════════════════════════════════════════════════
# SCORING
# ═════════════════════════════════════════════════════════════════

def calc_contact_score(lead: dict) -> int:
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


def calc_lead_quality_score(lead: dict) -> int:
    score = 0

    # Valor del permiso
    cost = _parse_cost(lead.get("estimated_cost", ""))
    if cost >= 500_000:   score += 4
    elif cost >= 200_000: score += 3
    elif cost >= 100_000: score += 2
    elif cost >= 50_000:  score += 1

    # Tipo de permiso
    combined = (
        (lead.get("permit_type") or "") + " " +
        (lead.get("description") or "")
    ).lower()
    best = 0
    for kw, pts in PERMIT_TYPE_SCORES.items():
        if kw in combined:
            best = max(best, pts)
    score += min(best, 3)

    # Relevancia para insulación
    hits = sum(1 for kw in INSULATION_KEYWORDS if kw in combined)
    if hits >= 3:   score += 2
    elif hits >= 1: score += 1

    # Bonus contacto completo
    if calc_contact_score(lead) >= 3:
        score += 1

    return min(score, 10)


def lead_quality_label(score: int) -> tuple:
    if score >= 10: return "🌟", "EXCELENTE"
    if score >= 8:  return "🔥", "MUY BUENO"
    if score >= 6:  return "✅", "BUENO"
    if score >= 4:  return "⚠️", "REGULAR"
    return "❌", "DESCARTADO"


def should_send_lead(lead: dict) -> bool:
    if _parse_cost(lead.get("estimated_cost", "")) < MIN_LEAD_VALUE:
        return False
    return lead.get("lead_quality_score", 0) >= 6


# ═════════════════════════════════════════════════════════════════
# HELPERS
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
    """Une partes de una dirección ignorando vacíos."""
    return ", ".join(p.strip() for p in parts if p and p.strip())
