"""
utils/contact_enricher.py  v5.2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Orquesta todas las fuentes de datos para enriquecer un lead.

PROPIETARIO  (en orden de prioridad)
  1. address_lookup.lookup_owner_by_address()  ← usa blklot SF, GIS otros
  2. SF DBI Permit Contacts (3pee-9qhc)       ← fallback para leads con permit_no

CONTRATISTA
  1. SF DBI Permit Contacts (3pee-9qhc)
  2. CSLB (scrape por licencia / nombre)
  3. OpenCorporates

SCORING
  contact_score (0-5) | lead_quality_score (0-10)
  Solar tiene scoring especial por kW (ignora el $1 de costo)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import re
import time
import logging
import requests
from functools import lru_cache

from utils.address_lookup import lookup_owner_by_address

logger = logging.getLogger(__name__)

MIN_LEAD_VALUE = 50_000

INSULATION_KEYWORDS = [
    "insul", "spray foam", "attic", "crawl", "vapor", "weatheri",
    "adu", "accessory dwelling", "addition", "new construction",
    "new building", "remodel", "garage conversion", "basement",
    "tenant improvement", "second unit", "granny flat",
]

PERMIT_TYPE_SCORES = {
    "new construction": 4, "new building": 4,
    "adu": 4, "accessory dwelling": 4,
    "addition": 3, "garage conversion": 3, "basement": 3,
    "remodel": 2, "alteration": 2, "renovation": 2, "tenant improvement": 2,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ═════════════════════════════════════════════════════════════════
# SF DBI Permit Contacts  (para contratista principalmente)
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=1024)
def lookup_sf_dbi_contacts(permit_number: str) -> dict:
    empty = {
        "owner": "", "owner_phone": "", "owner_mail_addr": "",
        "contractor": "", "contractor_phone": "", "contractor_addr": "",
        "contractor_license": "", "applicant": "", "applicant_phone": "",
        "architect": "",
    }
    if not permit_number:
        return empty

    pnum        = permit_number.strip()
    digits_only = re.sub(r"[^0-9]", "", pnum)
    variants    = list(dict.fromkeys([pnum, digits_only]))

    url    = "https://data.sfgov.org/resource/3pee-9qhc.json"
    select = (
        "permit_number,contact_type,contact_name,"
        "contact_address,contact_city,contact_state,"
        "contact_zip,contact_phone,license_number"
    )

    for v in variants:
        if not v:
            continue
        r = _dbi_query(url, f"permit_number = '{v}'", select)
        if r.get("owner") or r.get("contractor"):
            return r

    # Último recurso: LIKE con últimos 8 dígitos
    if len(digits_only) >= 8:
        r = _dbi_query(url, f"permit_number LIKE '%{digits_only[-8:]}'", select)
        if r.get("owner") or r.get("contractor"):
            return r

    return empty


def _dbi_query(url: str, where: str, select: str) -> dict:
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
        logger.debug(f"DBI query [{where[:40]}]: {e}")
    return result


# ═════════════════════════════════════════════════════════════════
# CSLB — Contratista
# ═════════════════════════════════════════════════════════════════

def lookup_contractor_cslb(license_number: str = "", company_name: str = "") -> dict:
    empty = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
             "contractor_zip": "", "contractor_status": "",
             "contractor_license": license_number or "", "contractor_types": ""}
    if license_number:
        r = _cslb_json(license_number)
        if r.get("contractor_phone") or r.get("contractor_addr"):
            return r
        r = _cslb_html(license_number)
        if r.get("contractor_phone") or r.get("contractor_addr"):
            return r
    if company_name:
        r = _opencorporates(company_name)
        if r.get("contractor_addr"):
            return r
    return empty


def _cslb_json(lic: str) -> dict:
    r = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
         "contractor_zip": "", "contractor_status": "", "contractor_license": lic,
         "contractor_types": ""}
    try:
        resp = requests.get(
            f"https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/"
            f"LicenseQueryHandler.ashx?LicenseNumber={lic.strip().lstrip('0')}&BoardType=C",
            headers=HEADERS, timeout=12,
        )
        if resp.ok and "json" in resp.headers.get("content-type", ""):
            data = resp.json()
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                r["contractor_phone"]  = _clean_phone(data.get("Phone", ""))
                r["contractor_addr"]   = data.get("Address", "")
                r["contractor_city"]   = data.get("City", "")
                r["contractor_status"] = data.get("LicenseStatus", "")
                r["contractor_types"]  = data.get("Classifications", "")
    except Exception as e:
        logger.debug(f"CSLB JSON: {e}")
    return r


def _cslb_html(lic: str) -> dict:
    r = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
         "contractor_zip": "", "contractor_status": "", "contractor_license": lic,
         "contractor_types": ""}
    try:
        resp = requests.get(
            f"https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/"
            f"CheckLicense.aspx?LicNum={lic.strip().lstrip('0')}",
            headers=HEADERS, timeout=12,
        )
        if not resp.ok:
            return r
        text = resp.text

        def _b(a, b):
            try:
                i = text.index(a) + len(a)
                j = text.index(b, i)
                return re.sub(r"<[^>]+>", "", text[i:j]).strip()
            except ValueError:
                return ""

        r["contractor_phone"]  = _clean_phone(_b("lblPhone", "</span>"))
        r["contractor_addr"]   = _b("lblAddress", "</span>")
        r["contractor_city"]   = _b("lblCity", "</span>")
        r["contractor_status"] = _b("lblLicenseStatus", "</span>")
        r["contractor_types"]  = _b("lblClassification", "</span>")
        if not r["contractor_phone"]:
            m = re.search(r"\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}", text)
            r["contractor_phone"] = _clean_phone(m.group(0) if m else "")
        time.sleep(0.4)
    except Exception as e:
        logger.debug(f"CSLB HTML: {e}")
    return r


def _opencorporates(company_name: str) -> dict:
    r = {"contractor_phone": "", "contractor_addr": "", "contractor_city": "",
         "contractor_zip": "", "contractor_status": "", "contractor_license": "",
         "contractor_types": ""}
    try:
        resp = requests.get(
            "https://api.opencorporates.com/v0.4/companies/search",
            params={"q": company_name[:50], "jurisdiction_code": "us_ca", "per_page": 3},
            timeout=10,
        )
        if resp.ok:
            companies = resp.json().get("results", {}).get("companies", [])
            if companies:
                comp     = companies[0].get("company", {})
                addr_obj = comp.get("registered_address", {})
                r["contractor_addr"]   = addr_obj.get("street_address", "")
                r["contractor_city"]   = addr_obj.get("locality", "")
                r["contractor_zip"]    = addr_obj.get("postal_code", "")
                r["contractor_status"] = comp.get("current_status", "")
    except Exception as e:
        logger.debug(f"OpenCorporates: {e}")
    return r


# ═════════════════════════════════════════════════════════════════
# Geocodificación
# ═════════════════════════════════════════════════════════════════

@lru_cache(maxsize=512)
def geocode_address(address: str, city: str = "CA") -> dict:
    result = {"lat": "", "lon": "", "maps_url": ""}
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{address}, {city}, California, USA",
                    "format": "json", "limit": 1},
            headers={"User-Agent": "InsulTechs-LeadAgent/5.2 admin@insultechs.com"},
            timeout=8,
        )
        if resp.ok:
            data = resp.json()
            if data:
                lat, lon           = data[0].get("lat", ""), data[0].get("lon", "")
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
    address   = lead.get("address", "")
    city      = lead.get("city", "")
    permit_no = lead.get("permit_no", "")
    is_sf     = "san francisco" in city.lower()

    # ── 1. PROPIETARIO: address_lookup primero ───────────────────
    existing_owner = (lead.get("owner") or "").strip().lower()
    blank_owner    = not existing_owner or existing_owner in ("no indicado", "n/a", "")

    if blank_owner and address:
        try:
            o = lookup_owner_by_address(address, city)
            if o.get("owner_name"):
                lead["owner"]           = o["owner_name"]
                lead["owner_mail_addr"] = o.get("mailing_address", "")
                if not lead.get("apn"):
                    lead["apn"] = o.get("apn", "")
                logger.info(f"Owner [lookup]: {address} → {lead['owner']}")
        except Exception as e:
            logger.debug(f"address_lookup error: {e}")

    # ── 2. DBI Contacts (SF): propietario fallback + contratista ─
    dbi = {}
    if is_sf and permit_no:
        try:
            dbi = lookup_sf_dbi_contacts(permit_no)
        except Exception as e:
            logger.debug(f"DBI: {e}")

    # Usar DBI owner si address_lookup no encontró nada
    current_owner = (lead.get("owner") or "").strip()
    no_owner = not current_owner or current_owner.lower() in ("no indicado", "no encontrado", "n/a", "")
    if no_owner and dbi.get("owner"):
        lead["owner"]           = dbi["owner"]
        lead["owner_mail_addr"] = dbi.get("owner_mail_addr", "")
        if not lead.get("owner_phone"):
            lead["owner_phone"] = dbi.get("owner_phone", "")
        logger.info(f"Owner [DBI]: {permit_no} → {lead['owner']}")

    if not lead.get("owner"):
        lead["owner"] = "No encontrado"

    # ── 3. CONTRATISTA: DBI → CSLB ──────────────────────────────
    blank_ctr = not (lead.get("contractor") or "").strip() or \
                (lead.get("contractor") or "").lower().strip() in ("no indicado", "n/a", "")

    if dbi.get("contractor"):
        if blank_ctr:
            lead["contractor"] = dbi["contractor"]
        if not lead.get("contractor_phone"):
            lead["contractor_phone"] = dbi.get("contractor_phone", "")
        if not lead.get("contractor_addr"):
            lead["contractor_addr"] = dbi.get("contractor_addr", "")
        if not lead.get("contractor_license"):
            lead["contractor_license"] = dbi.get("contractor_license", "")

    if not lead.get("applicant") and dbi.get("applicant"):
        lead["applicant"]       = dbi["applicant"]
        lead["applicant_phone"] = dbi.get("applicant_phone", "")
    if not lead.get("architect") and dbi.get("architect"):
        lead["architect"] = dbi["architect"]

    # CSLB si aún falta info del contratista
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
                        cslb.get("contractor_city", ""), "",
                        cslb.get("contractor_zip", ""),
                    )
                if not lead.get("contractor_status"):
                    lead["contractor_status"] = cslb.get("contractor_status", "")
                if not lead.get("contractor_types"):
                    lead["contractor_types"]  = cslb.get("contractor_types", "")
            except Exception as e:
                logger.debug(f"CSLB: {e}")

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
    if lead_type == "solar":
        return _solar_score(lead)
    return _permit_score(lead)


def _solar_score(lead: dict) -> int:
    score = 0
    kw    = _parse_kw(lead.get("kw_installed") or lead.get("description") or "")
    if kw >= 20:    score += 4
    elif kw >= 10:  score += 3
    elif kw >= 5:   score += 2
    elif kw > 0:    score += 1
    else:           score += 2  # desconocido → asumir residencial
    desc = (lead.get("description") or "").lower()
    if any(w in desc for w in ["battery", "storage", "powerwall", "enphase"]):
        score += 2
    if any(w in desc for w in ["commercial", "office", "retail"]):
        score += 1
    if calc_contact_score(lead) >= 1:
        score += 1
    return max(min(score, 10), 5)  # mínimo 5 para todos los solares


def _permit_score(lead: dict) -> int:
    score = 0
    cost  = _parse_cost(lead.get("estimated_cost", ""))
    if cost >= 500_000:   score += 4
    elif cost >= 200_000: score += 3
    elif cost >= 100_000: score += 2
    elif cost >= 50_000:  score += 1

    combined = ((lead.get("permit_type") or "") + " " +
                (lead.get("description") or "")).lower()
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
    quality = lead.get("lead_quality_score", 0)
    if lead_type == "solar":
        return quality >= 5
    return (_parse_cost(lead.get("estimated_cost", "")) >= MIN_LEAD_VALUE
            and quality >= 6)


def contact_score_label(score: int) -> str:
    return {
        0: "⬜ Sin datos", 1: "🟥 Mínimo", 2: "🟧 Básico",
        3: "🟨 Bueno",    4: "🟩 Completo", 5: "🌟 Completo+",
    }.get(score, "⬜ Sin datos")


# ═════════════════════════════════════════════════════════════════
# HELPERS
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
    if not raw:
        return 0.0
    m = re.search(r"(\d+\.?\d*)\s*kw", str(raw), re.IGNORECASE)
    if m:
        return float(m.group(1))
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
