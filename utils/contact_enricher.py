"""
utils/contact_enricher.py  v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Enriquece cada lead con:
  1. Propietario real  → SF/Alameda/ContraCosta/SantaClara Assessor
  2. Contratista       → CSLB (múltiples métodos)
  3. Geocodificación   → Nominatim (OSM) sin key
  4. contact_score     → cuántos datos tenemos (0-5)
  5. lead_quality_score → qué tan buen lead es (0-10)
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


# ─────────────────────────────────────────────
# 1. CSLB — Contratista (3 métodos en cascada)
# ─────────────────────────────────────────────

def lookup_contractor_cslb(license_number: str = "", company_name: str = "") -> dict:
    empty = {
        "contractor_phone":  "",
        "contractor_addr":   "",
        "contractor_city":   "",
        "contractor_zip":    "",
        "contractor_status": "",
        "contractor_license": license_number or "",
        "contractor_types":  "",
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
        num = lic.strip().lstrip("0")
        url = (
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
        num = lic.strip().lstrip("0")
        url = (
            "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/"
            f"CheckLicense.aspx?LicNum={num}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if not resp.ok:
            return result

        # Parseo sin BeautifulSoup para no requerir dep extra
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

        # Fallback: buscar patrones de teléfono en el HTML
        if not phone:
            phones = re.findall(r"\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", text)
            phone = phones[0] if phones else ""

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


# ─────────────────────────────────────────────
# 2. Assessors por condado
# ─────────────────────────────────────────────

@lru_cache(maxsize=512)
def lookup_sf_assessor(address: str) -> dict:
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    if not address:
        return result
    clean  = address.strip().upper()
    parts  = clean.split()
    s_num  = parts[0] if parts else ""
    s_name = " ".join(parts[1:3]) if len(parts) > 1 else ""
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
        resp = requests.get(url, params=params, timeout=10)
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
        logger.debug(f"SF Assessor: {e}")
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
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            recs = resp.json()
            if recs:
                rec = recs[0]
                result["owner_name"]      = (rec.get("owner_name") or "").title()
                result["mailing_address"] = " ".join(filter(None, [
                    rec.get("mail_addr_1", ""),
                    rec.get("mail_city", ""),
                    rec.get("mail_zip", ""),
                ])).strip()
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
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("OWNER_NAME") or "").title()
                result["mailing_address"] = " ".join(filter(None, [
                    attr.get("MAIL_ADDR", ""),
                    attr.get("MAIL_CITY", ""),
                    attr.get("MAIL_ZIP", ""),
                ])).strip()
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
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("TAXPAYER_NAME") or "").title()
                result["mailing_address"] = " ".join(filter(None, [
                    attr.get("MAIL_ADDR", ""),
                    attr.get("MAIL_CITY", ""),
                    attr.get("MAIL_STATE", ""),
                    attr.get("MAIL_ZIP", ""),
                ])).strip()
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


# ─────────────────────────────────────────────
# 3. Geocodificación — Nominatim (OSM)
# ─────────────────────────────────────────────

@lru_cache(maxsize=512)
def geocode_address(address: str, city: str = "CA") -> dict:
    result = {"lat": "", "lon": "", "maps_url": ""}
    try:
        url    = "https://nominatim.openstreetmap.org/search"
        params = {"q": f"{address}, {city}, California, USA", "format": "json", "limit": 1}
        headers = {"User-Agent": "InsulTechs-LeadAgent/3.0 admin@insultechs.com"}
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


# ─────────────────────────────────────────────
# 4. Scoring
# ─────────────────────────────────────────────

def calc_contact_score(lead: dict) -> int:
    score = 0
    owner = (lead.get("owner") or "").strip()
    if owner and owner.lower() not in ("no indicado", "no encontrado", "n/a", ""):
        score += 1
    if lead.get("owner_mail_addr"):
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
    """
    Score 0-10:
      Valor permiso  → 0-4 pts
      Tipo permiso   → 0-3 pts
      Relevancia     → 0-2 pts
      Contacto OK    → 0-1 pt
    """
    score = 0

    # Valor
    cost = _parse_cost(lead.get("estimated_cost", ""))
    if cost >= 500_000:   score += 4
    elif cost >= 200_000: score += 3
    elif cost >= 100_000: score += 2
    elif cost >= 50_000:  score += 1

    # Tipo
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

    # Bonus contacto
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
    """
    Solo enviar si:
      • Valor >= $50K
      • Lead quality score >= 6 (BUENO o mejor)
    """
    if _parse_cost(lead.get("estimated_cost", "")) < MIN_LEAD_VALUE:
        return False
    return lead.get("lead_quality_score", 0) >= 6


# ─────────────────────────────────────────────
# 5. Función principal
# ─────────────────────────────────────────────

def enrich_lead(lead: dict) -> dict:
    address = lead.get("address", "")
    city    = lead.get("city", "")

    # Propietario
    try:
        assessor = lookup_assessor_by_county(address, city) if address else {}
    except Exception:
        assessor = {}

    existing_owner = (lead.get("owner") or "").strip().lower()
    if not existing_owner or existing_owner in ("no indicado", "n/a", ""):
        lead["owner"] = assessor.get("owner_name") or "No encontrado"
    lead["owner_mail_addr"] = assessor.get("mailing_address", "")
    lead["apn"]             = assessor.get("apn", "")

    # Contratista
    ctr_name = (lead.get("contractor") or "").strip()
    ctr_lic  = (lead.get("contractor_license") or "").strip()
    if ctr_name and ctr_name.lower() not in ("no indicado", "n/a", ""):
        try:
            cslb = lookup_contractor_cslb(ctr_lic, ctr_name)
            if not lead.get("contractor_phone"):
                lead["contractor_phone"] = cslb.get("contractor_phone", "")
            lead["contractor_addr"]   = _fmt_addr(
                cslb.get("contractor_addr", ""),
                cslb.get("contractor_city", ""),
                cslb.get("contractor_zip", ""),
            )
            lead["contractor_status"] = cslb.get("contractor_status", "")
            lead["contractor_types"]  = cslb.get("contractor_types", "")
        except Exception as e:
            logger.debug(f"CSLB enrichment: {e}")

    # Geo
    if address and not lead.get("maps_url"):
        try:
            geo = geocode_address(address, city)
            lead["maps_url"] = geo.get("maps_url", "")
            lead["lat"]      = geo.get("lat", "")
            lead["lon"]      = geo.get("lon", "")
        except Exception as e:
            logger.debug(f"Geo: {e}")

    # Scores
    lead["contact_score"]      = calc_contact_score(lead)
    lead["lead_quality_score"] = calc_lead_quality_score(lead)
    emoji, label               = lead_quality_label(lead["lead_quality_score"])
    lead["quality_emoji"]      = emoji
    lead["quality_label"]      = label

    return lead


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

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
    return ", ".join(p.strip() for p in parts if p and p.strip())
