"""
utils/contact_enricher.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Enriquece cada lead con información de contacto real:

1. CSLB  — California Contractors State License Board
           Busca teléfono y dirección del contratista por licencia o nombre
           https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx

2. County Assessor APIs  — Busca propietario real por dirección
   • SF:    https://data.sfgov.org/resource/wv5m-vpq2.json  (Assessor parcels)
   • Alameda: https://data.acgov.org
   • Contra Costa: datos públicos del condado

3. White Pages / USPS  — Validación de dirección (sin key)

4. Google Maps Geocoding  — lat/lng para mapa (sin key, usando nominatim)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import re
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# 1. CSLB — Datos del contratista
# ─────────────────────────────────────────────────────────────────

def lookup_contractor_cslb(license_number: str = "", company_name: str = "") -> dict:
    """
    Busca datos del contratista en la base pública del CSLB de California.
    Retorna dict con phone, address, city, zip, status, classifications.
    
    La CSLB expone un endpoint de búsqueda en formato JSON no oficial
    pero accesible públicamente (no requiere API key).
    """
    result = {
        "contractor_phone":   "",
        "contractor_address": "",
        "contractor_city":    "",
        "contractor_zip":     "",
        "contractor_status":  "",
        "contractor_license": license_number,
        "contractor_types":   "",
    }

    # Intentar por número de licencia primero (más preciso)
    if license_number:
        url = "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseQueryHandler.ashx"
        params = {
            "LicenseNumber": license_number.strip().lstrip("0"),
            "BoardType": "C",
        }
        try:
            resp = requests.get(url, params=params, timeout=10,
                                headers={"User-Agent": "Mozilla/5.0"})
            if resp.ok and resp.text.strip():
                data = resp.json() if "json" in resp.headers.get("content-type","") else {}
                if data:
                    result["contractor_phone"]   = data.get("Phone", "")
                    result["contractor_address"] = data.get("Address", "")
                    result["contractor_city"]    = data.get("City", "")
                    result["contractor_status"]  = data.get("Status", "")
                    result["contractor_types"]   = data.get("Classifications", "")
        except Exception as e:
            logger.debug(f"CSLB license lookup failed: {e}")

    # Fallback: búsqueda por nombre en API alternativa del CSLB
    if not result["contractor_phone"] and company_name:
        try:
            url2 = "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseQueryHandler.ashx"
            params2 = {"BusinessName": company_name[:40], "BoardType": "C"}
            resp2 = requests.get(url2, params=params2, timeout=10,
                                 headers={"User-Agent": "Mozilla/5.0"})
            if resp2.ok:
                try:
                    records = resp2.json()
                    if isinstance(records, list) and records:
                        first = records[0]
                        result["contractor_phone"]   = first.get("Phone", "")
                        result["contractor_address"] = first.get("Address", "")
                        result["contractor_city"]    = first.get("City", "")
                        result["contractor_status"]  = first.get("LicenseStatus", "")
                        result["contractor_license"] = first.get("LicenseNumber", license_number)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"CSLB name lookup failed: {e}")

    return result


# ─────────────────────────────────────────────────────────────────
# 2. SF Assessor — Datos del propietario por dirección/APN
# ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def lookup_sf_assessor(address: str) -> dict:
    """
    Busca el propietario real de una propiedad en SF usando el
    Assessor-Recorder API público de DataSF.
    Retorna: owner_name, mailing_address, parcel_number (APN)
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    if not address:
        return result

    # Normalizar dirección
    clean = address.strip().upper()
    street_num  = clean.split()[0] if clean.split() else ""
    street_name = " ".join(clean.split()[1:]).replace(",", "")

    url = "https://data.sfgov.org/resource/wv5m-vpq2.json"
    params = {
        "$where": f"blklot IS NOT NULL AND from_st <= '{street_num}' AND to_st >= '{street_num}'",
        "$limit": 5,
        "$select": "blklot,from_address,to_address,street,owner_name,mail_address,mail_city,mail_zipcode",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        records = resp.json()
        if records:
            # Buscar el más cercano al street name
            for rec in records:
                if any(word in (rec.get("street", "").upper()) for word in street_name.split()[:2]):
                    result["owner_name"]      = rec.get("owner_name", "").title()
                    result["mailing_address"] = f"{rec.get('mail_address','')} {rec.get('mail_city','')} {rec.get('mail_zipcode','')}".strip()
                    result["apn"]             = rec.get("blklot", "")
                    break
            # Si no hay match exacto, tomar el primero
            if not result["owner_name"] and records:
                rec = records[0]
                result["owner_name"]      = rec.get("owner_name", "").title()
                result["mailing_address"] = f"{rec.get('mail_address','')} {rec.get('mail_city','')} {rec.get('mail_zipcode','')}".strip()
                result["apn"]             = rec.get("blklot", "")
    except Exception as e:
        logger.debug(f"SF Assessor lookup failed for '{address}': {e}")

    return result


# ─────────────────────────────────────────────────────────────────
# 3. County Assessor — Alameda, Contra Costa, Santa Clara
# ─────────────────────────────────────────────────────────────────

def lookup_assessor_by_county(address: str, city: str) -> dict:
    """
    Dispatcher: elige el assessor API correcto según ciudad.
    """
    city_lower = city.lower()

    if "san francisco" in city_lower:
        return lookup_sf_assessor(address)

    if any(c in city_lower for c in ["oakland", "berkeley", "alameda", "fremont",
                                      "hayward", "livermore", "san leandro"]):
        return lookup_alameda_assessor(address)

    if any(c in city_lower for c in ["walnut creek", "concord", "richmond",
                                      "lafayette", "martinez", "antioch", "pittsburg"]):
        return lookup_contra_costa_assessor(address)

    if any(c in city_lower for c in ["san jose", "santa clara", "sunnyvale",
                                      "milpitas", "campbell", "cupertino", "gilroy"]):
        return lookup_santa_clara_assessor(address)

    return {"owner_name": "", "mailing_address": "", "apn": ""}


@lru_cache(maxsize=256)
def lookup_alameda_assessor(address: str) -> dict:
    """
    Alameda County Assessor — API pública
    https://data.acgov.org/resource/
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url = "https://data.acgov.org/resource/a2sq-oaix.json"
        params = {
            "$where": f"UPPER(situs_addr) LIKE '%{address.split(',')[0].strip().upper()[:30]}%'",
            "$limit": 3,
            "$select": "apn,situs_addr,owner_name,mail_addr_1,mail_city,mail_zip",
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            records = resp.json()
            if records:
                rec = records[0]
                result["owner_name"]      = rec.get("owner_name", "").title()
                result["mailing_address"] = f"{rec.get('mail_addr_1','')} {rec.get('mail_city','')} {rec.get('mail_zip','')}".strip()
                result["apn"]             = rec.get("apn", "")
    except Exception as e:
        logger.debug(f"Alameda assessor failed: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_contra_costa_assessor(address: str) -> dict:
    """
    Contra Costa County — datos del assessor público
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url = "https://gis.ccmap.us/arcgis/rest/services/CCC/CCCParcel/MapServer/0/query"
        params = {
            "where": f"UPPER(SITUS_ADDR) LIKE '%{address.split(',')[0].strip().upper()[:25]}%'",
            "outFields": "APN,OWNER_NAME,SITUS_ADDR,MAIL_ADDR,MAIL_CITY,MAIL_ZIP",
            "f": "json",
            "resultRecordCount": 3,
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("OWNER_NAME") or "").title()
                result["mailing_address"] = f"{attr.get('MAIL_ADDR','')} {attr.get('MAIL_CITY','')} {attr.get('MAIL_ZIP','')}".strip()
                result["apn"]             = attr.get("APN", "")
    except Exception as e:
        logger.debug(f"Contra Costa assessor failed: {e}")
    return result


@lru_cache(maxsize=256)
def lookup_santa_clara_assessor(address: str) -> dict:
    """
    Santa Clara County Assessor — GIS público
    """
    result = {"owner_name": "", "mailing_address": "", "apn": ""}
    try:
        url = "https://gis.sccgov.org/arcgis/rest/services/ParcelServices/SCC_ParcelQueryService/MapServer/0/query"
        params = {
            "where": f"UPPER(SITUS_ADDR) LIKE '%{address.split(',')[0].strip().upper()[:25]}%'",
            "outFields": "APN_FORMATTED,TAXPAYER_NAME,MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP",
            "f": "json",
            "resultRecordCount": 3,
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.ok:
            feats = resp.json().get("features", [])
            if feats:
                attr = feats[0].get("attributes", {})
                result["owner_name"]      = (attr.get("TAXPAYER_NAME") or "").title()
                result["mailing_address"] = f"{attr.get('MAIL_ADDR','')} {attr.get('MAIL_CITY','')} {attr.get('MAIL_STATE','')} {attr.get('MAIL_ZIP','')}".strip()
                result["apn"]             = attr.get("APN_FORMATTED", "")
    except Exception as e:
        logger.debug(f"Santa Clara assessor failed: {e}")
    return result


# ─────────────────────────────────────────────────────────────────
# 4. Geocodificación gratuita — Nominatim (OSM)
# ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def geocode_address(address: str, city: str = "CA") -> dict:
    """
    Convierte dirección a lat/lng usando Nominatim (OpenStreetMap).
    Completamente gratuito, sin API key.
    """
    result = {"lat": "", "lon": "", "maps_url": ""}
    query = f"{address}, {city}, California, USA"
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": query, "format": "json", "limit": 1}
        headers = {"User-Agent": "InsulTechs-LeadAgent/1.0 admin@insultechs.com"}
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        if resp.ok:
            data = resp.json()
            if data:
                result["lat"]      = data[0].get("lat", "")
                result["lon"]      = data[0].get("lon", "")
                result["maps_url"] = f"https://maps.google.com/?q={result['lat']},{result['lon']}"
    except Exception as e:
        logger.debug(f"Geocoding failed for '{address}': {e}")
    return result


# ─────────────────────────────────────────────────────────────────
# 5. Función principal de enriquecimiento
# ─────────────────────────────────────────────────────────────────

def enrich_lead(lead: dict) -> dict:
    """
    Enriquece un lead con toda la información de contacto disponible.
    Modifica el dict in-place y lo retorna.
    
    Agrega:
      - owner_name_real  (del assessor)
      - owner_mail_addr  (dirección postal del propietario)
      - apn              (número de parcela)
      - contractor_phone (del CSLB)
      - contractor_addr  (del CSLB)
      - maps_url         (Google Maps)
      - contact_score    (0-5: qué tan completo está el contacto)
    """
    address = lead.get("address", "")
    city    = lead.get("city", "")

    # 1. Datos del propietario desde assessor
    assessor_data = {}
    if address:
        try:
            assessor_data = lookup_assessor_by_county(address, city)
        except Exception as e:
            logger.debug(f"Assessor enrichment failed: {e}")

    # Si el lead ya tenía owner del permiso, solo rellenar lo que falta
    if not lead.get("owner") or lead.get("owner") == "No indicado":
        lead["owner"] = assessor_data.get("owner_name", "No encontrado")

    lead["owner_mail_addr"] = assessor_data.get("mailing_address", "")
    lead["apn"]             = assessor_data.get("apn", "")

    # 2. Datos del contratista desde CSLB
    contractor_name    = lead.get("contractor", "")
    contractor_license = lead.get("contractor_license", "")

    if contractor_name and contractor_name not in ("No indicado", ""):
        try:
            cslb_data = lookup_contractor_cslb(
                license_number=contractor_license,
                company_name=contractor_name
            )
            if not lead.get("contractor_phone"):
                lead["contractor_phone"] = cslb_data.get("contractor_phone", "")
            lead["contractor_addr"]   = cslb_data.get("contractor_address", "")
            lead["contractor_city"]   = cslb_data.get("contractor_city", "")
            lead["contractor_status"] = cslb_data.get("contractor_status", "")
            lead["contractor_types"]  = cslb_data.get("contractor_types", "")
        except Exception as e:
            logger.debug(f"CSLB enrichment failed: {e}")

    # 3. Geocodificación
    if address and not lead.get("maps_url"):
        try:
            geo = geocode_address(address, city)
            lead["maps_url"] = geo.get("maps_url", "")
            if not lead.get("lat"):
                lead["lat"] = geo.get("lat", "")
                lead["lon"] = geo.get("lon", "")
        except Exception as e:
            logger.debug(f"Geocoding failed: {e}")

    # 4. Score de contacto (cuánta info tenemos)
    score = 0
    if lead.get("owner") not in ("", "No indicado", "No encontrado", None):      score += 1
    if lead.get("owner_mail_addr"):                                                score += 1
    if lead.get("contractor") not in ("", "No indicado", None):                   score += 1
    if lead.get("contractor_phone"):                                               score += 1
    if lead.get("maps_url"):                                                       score += 1
    lead["contact_score"] = score

    return lead


def contact_score_label(score: int) -> str:
    """Convierte score numérico en etiqueta visual."""
    labels = {0: "⬜ Sin datos", 1: "🟥 Mínimo",
               2: "🟧 Básico",   3: "🟨 Bueno",
               4: "🟩 Completo", 5: "🌟 Excelente"}
    return labels.get(score, "⬜")
