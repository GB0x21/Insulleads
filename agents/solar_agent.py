"""
agents/solar_agent.py  v8
━━━━━━━━━━━━━━━━━━━━━━━
☀️ Instalaciones Solares — Bay Area

MEJORAS v8 (APIs de pago):
  ✅ Google Solar API ($0.40/request) — potencial solar a nivel de edificio
     Datos: panel count, sqft de techo, ahorro anual estimado, orientación
  ✅ Aurora Solar API ($100+/mes) — pipeline de proyectos solares activos
     Detecta propuestas en progreso = instalación inminente
  ✅ EnergySage API — marketplace de solar, compradores activos buscando
     cotizaciones = oportunidad inmediata de cross-sell insulación
  ✅ OpenEI Utility Rates API (gratuita) — tarifas eléctricas por zona
     Áreas con tarifa alta = mayor incentivo para solar+insulación

Previas (v7):
  ✅ 7 ciudades con permisos solares (Socrata/CKAN)
  ✅ NREL Solar Resource API + keywords ampliados + fetch paralelo
"""

import os
import re
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact
from utils.lead_scoring import score_lead, format_score_line
from utils.notifications import notify_multichannel

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT   = int(os.getenv("SOURCE_TIMEOUT", "45"))
MIN_PERMIT_VALUE = float(os.getenv("MIN_PERMIT_VALUE", "50000"))
PERMIT_MONTHS    = int(os.getenv("PERMIT_MONTHS", "3"))
PARALLEL_SOLAR   = int(os.getenv("PARALLEL_SOLAR", "6"))
NREL_API_KEY     = os.getenv("NREL_API_KEY", "")        # Free: https://developer.nrel.gov/signup/
GOOGLE_SOLAR_KEY = os.getenv("GOOGLE_SOLAR_API_KEY", "")  # $0.40/request
AURORA_API_KEY   = os.getenv("AURORA_API_KEY", "")       # Aurora Solar
ENERGYSAGE_KEY   = os.getenv("ENERGYSAGE_API_KEY", "")   # EnergySage marketplace

# Keywords ampliados — solar + proyectos relacionados con energía
SOLAR_KW = [
    "SOLAR", "PHOTOVOLTAIC", "PV SYSTEM", "PV PANEL", "PANEL SOLAR",
    "ROOFTOP PV", "ROOFTOP SOLAR",
    "BATTERY STORAGE", "ENERGY STORAGE", "POWERWALL",
    "EV CHARGER", "EV CHARGING", "ELECTRIC VEHICLE CHARG",
    "NET ZERO", "NET-ZERO", "ENERGY UPGRADE",
]


def _cutoff_ymd() -> str:
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%d")

def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%dT00:00:00")

def _is_solar(rec: dict) -> bool:
    haystack = " ".join([
        str(rec.get("WORKDESCRIPTION") or ""),
        str(rec.get("FOLDERNAME")      or ""),
        str(rec.get("description")     or ""),
        str(rec.get("permit_type_definition") or ""),
        str(rec.get("permit_description") or ""),
        str(rec.get("project_description") or ""),
        str(rec.get("work_description") or ""),
    ]).upper()
    return any(kw in haystack for kw in SOLAR_KW)

def _parse_value(v) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", str(v) or "") or "0")
    except Exception:
        return 0.0


# ── Fuentes de datos solares — 7 ciudades Bay Area ──────────────────

SOLAR_SOURCES = [
    # ── San Francisco — Socrata con filtro solar server-side ──────
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.sfgov.org/resource/i98e-djp9.json",
        "params": {
            "$limit": 100,
            "$order": "filed_date DESC",
            "$where": (
                "status IN('issued','complete') "
                "AND filed_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%' "
                "OR UPPER(description) LIKE '%BATTERY STORAGE%' "
                "OR UPPER(description) LIKE '%EV CHARG%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"street_number","address2":"street_name",
            "desc":"description","status":"status","date":"filed_date",
            "contractor":"contractor_company_name","lic":"contractor_license",
            "owner":"owner","value":"estimated_cost",
        },
    },
    # ── Oakland — Socrata ────────────────────────────────────────
    {
        "city":    "Oakland",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.oaklandca.gov/resource/uymu-f5cz.json",
        "params": {
            "$limit": 100,
            "$order": "application_date DESC",
            "$where": (
                "application_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV %' "
                "OR UPPER(description) LIKE '%BATTERY STORAGE%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"application_date",
            "contractor":"contractor_name","lic":"contractor_license_number",
            "owner":"owner_name","value":"job_value",
        },
    },
    # ── Sunnyvale — Socrata ──────────────────────────────────────
    {
        "city":    "Sunnyvale",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.sunnyvale.ca.gov/resource/irbr-7ykz.json",
        "params": {
            "$limit": 100,
            "$order": "issueddate DESC",
            "$where": (
                "issueddate >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issueddate",
            "contractor":"contractor","lic":"license_number",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Santa Clara — Socrata ────────────────────────────────────
    {
        "city":    "Santa Clara",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.santaclaraca.gov/resource/rg5i-sfiv.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(permit_description) LIKE '%SOLAR%' "
                "OR UPPER(permit_description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(permit_description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"permit_description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Berkeley — Socrata ───────────────────────────────────────
    {
        "city":    "Berkeley",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.cityofberkeley.info/resource/k92i-t48y.json",
        "params": {
            "$limit": 100,
            "$order": "issue_date DESC",
            "$where": (
                "issue_date >= '{cutoff_iso}' "
                "AND (UPPER(project_description) LIKE '%SOLAR%' "
                "OR UPPER(project_description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(project_description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"record_number","address":"address",
            "desc":"project_description","status":"record_status","date":"issue_date",
            "contractor":"contractor","lic":"contractor_license",
            "owner":"owner","value":"job_value",
        },
    },
    # ── Richmond — Socrata ───────────────────────────────────────
    {
        "city":    "Richmond",
        "engine":  "socrata",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.ci.richmond.ca.us/resource/bm7q-witt.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(work_description) LIKE '%SOLAR%' "
                "OR UPPER(work_description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(work_description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"work_description","status":"status","date":"issued_date",
            "contractor":"contractor","lic":"license_number",
            "owner":"owner_name","value":"valuation",
        },
    },
    # ── San Jose — CKAN datastore_search con filtro fecha ─────────
    {
        "city":    "San Jose",
        "engine":  "ckan_search",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.sanjoseca.gov/api/3/action/datastore_search",
        "params": {
            "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904",
            "limit":       500,
            "sort":        "ISSUEDATE desc",
        },
        "field_map": {
            "id":"FOLDERNUMBER","address":"gx_location","address2":None,
            "desc":"WORKDESCRIPTION","status":"Status","date":"ISSUEDATE",
            "contractor":"CONTRACTOR","lic":None,
            "owner":"OWNERNAME","value":"PERMITVALUATION",
        },
        "_date_cutoff": None,
        "_date_field":  "ISSUEDATE",
    },
    # ── Contra Costa County — Socrata ────────────────────────────
    {
        "city": "Contra Costa County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.contracosta.gov/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── San Mateo County — Socrata ───────────────────────────────
    {
        "city": "San Mateo County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.smcgov.org/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Solano County — Socrata ──────────────────────────────────
    {
        "city": "Solano County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.solanocounty.com/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Marin County — Socrata ───────────────────────────────────
    {
        "city": "Marin County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.marincounty.org/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Napa County — Socrata ────────────────────────────────────
    {
        "city": "Napa County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.countyofnapa.org/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Sonoma County — Socrata ──────────────────────────────────
    {
        "city": "Sonoma County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.sonomacounty.ca.gov/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── San Joaquin County — Socrata ─────────────────────────────
    {
        "city": "San Joaquin County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.sjgov.org/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
    # ── Alameda County — Socrata ─────────────────────────────────
    {
        "city": "Alameda County",
        "engine": "socrata",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "url": "https://data.acgov.org/resource/building-permits.json",
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%SOLAR%' "
                "OR UPPER(description) LIKE '%PHOTOVOLTAIC%' "
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"address",
            "desc":"description","status":"status","date":"issued_date",
            "contractor":"contractor_name","lic":"contractor_license",
            "owner":"owner","value":"valuation",
        },
    },
]


# ── NREL Solar Resource API (gratuita) ──────────────────────────────
# Enriquece leads con datos de irradiación solar de la zona.
# Signup gratis: https://developer.nrel.gov/signup/

def _get_solar_potential(lat: float, lon: float) -> dict | None:
    """
    Consulta NREL Solar Resource API para obtener potencial solar.
    Retorna dict con ghi_annual (irradiación global horizontal) y
    capacity_factor, o None si falla/no hay API key.
    """
    if not NREL_API_KEY:
        return None
    try:
        resp = requests.get(
            "https://developer.nrel.gov/api/solar/solar_resource/v1.json",
            params={
                "api_key": NREL_API_KEY,
                "lat": lat,
                "lon": lon,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        outputs = data.get("outputs", {})
        avg_ghi = outputs.get("avg_ghi", {})
        return {
            "ghi_annual": avg_ghi.get("annual", 0),
            "solar_rating": "⭐⭐⭐" if avg_ghi.get("annual", 0) > 5.5
                           else "⭐⭐" if avg_ghi.get("annual", 0) > 4.5
                           else "⭐",
        }
    except Exception as e:
        logger.debug(f"[NREL] Error: {e}")
        return None


# ── Bay Area geocoding aproximado por ciudad ─────────────────────────
_CITY_COORDS = {
    "San Francisco": (37.7749, -122.4194),
    "Oakland":       (37.8044, -122.2712),
    "San Jose":      (37.3382, -121.8863),
    "Sunnyvale":     (37.3688, -122.0363),
    "Santa Clara":   (37.3541, -121.9552),
    "Berkeley":      (37.8716, -122.2727),
    "Richmond":      (37.9358, -122.3477),
    "Contra Costa County": (37.9535, -122.0311),
    "Alameda County":      (37.6017, -121.7195),
    "San Mateo County":    (37.5630, -122.3255),
    "Solano County":       (38.2494, -121.9400),
    "Marin County":        (38.0834, -122.7633),
    "Napa County":         (38.2975, -122.2869),
    "Sonoma County":       (38.2921, -122.4580),
    "San Joaquin County":  (37.9577, -121.2908),
    "Walnut Creek":   (37.9101, -122.0652),
    "Concord":        (37.9780, -122.0311),
    "Pleasanton":     (37.6624, -121.8747),
    "Hayward":        (37.6688, -122.0808),
    "Fremont":        (37.5485, -121.9886),
    "Daly City":      (37.6879, -122.4702),
    "San Mateo":      (37.5630, -122.3255),
    "Vallejo":        (38.1041, -122.2566),
    "Fairfield":      (38.2494, -122.0400),
    "Napa":           (38.2975, -122.2869),
    "San Rafael":     (37.9735, -122.5311),
    "Novato":         (38.1074, -122.5697),
    "Petaluma":       (38.2324, -122.6367),
    "Tracy":          (37.7397, -121.4252),
    "Stockton":       (37.9577, -121.2908),
    "Antioch":        (38.0049, -121.8058),
    "Pittsburg":      (38.0280, -121.8847),
    "Dublin":         (37.7022, -121.9358),
    "Livermore":      (37.6819, -121.7680),
}


# ── Google Solar API ($0.40/request) ─────────────────────────────────
# Potencial solar a nivel de edificio individual.
# Datos: orientación de techo, panel count, ahorro anual, CO₂ offset.
# Requiere: Google Cloud + Solar API habilitada.

_google_solar_cache: dict = {}

def _google_solar_lookup(lat: float, lon: float) -> dict:
    """
    Google Solar API — Building Insights.
    Retorna potencial solar del edificio específico.
    """
    if not GOOGLE_SOLAR_KEY:
        return {}

    cache_key = f"{lat:.5f},{lon:.5f}"
    if cache_key in _google_solar_cache:
        return _google_solar_cache[cache_key]

    try:
        resp = requests.get(
            "https://solar.googleapis.com/v1/buildingInsights:findClosest",
            params={
                "location.latitude": lat,
                "location.longitude": lon,
                "requiredQuality": "MEDIUM",
                "key": GOOGLE_SOLAR_KEY,
            },
            timeout=12,
        )
        if resp.status_code != 200:
            _google_solar_cache[cache_key] = {}
            return {}

        data = resp.json()
        solar_potential = data.get("solarPotential", {})
        best_config = (solar_potential.get("solarPanelConfigs") or [{}])[-1]  # Max config

        result = {
            "max_panels":       solar_potential.get("maxArrayPanelsCount", 0),
            "max_area_sqft":    round(solar_potential.get("maxArrayAreaMeters2", 0) * 10.764, 0),
            "roof_sqft":        round(solar_potential.get("wholeRoofStats", {}).get("areaMeters2", 0) * 10.764, 0),
            "annual_kwh":       round(best_config.get("yearlyEnergyDcKwh", 0), 0),
            "panel_capacity_w": solar_potential.get("panelCapacityWatts", 400),
            "max_sunshine_hrs": solar_potential.get("maxSunshineHoursPerYear", 0),
            "carbon_offset_kg": round(solar_potential.get("carbonOffsetFactorKgPerMwh", 0) *
                                     best_config.get("yearlyEnergyDcKwh", 0) / 1000, 0),
            "source": "Google Solar",
        }
        _google_solar_cache[cache_key] = result
        return result

    except Exception as e:
        logger.debug(f"[Google Solar] Error: {e}")
        _google_solar_cache[cache_key] = {}
        return {}


# ── Aurora Solar API ($100+/mes) ─────────────────────────────────────
# Plataforma de diseño solar. Su API expone propuestas/proyectos activos.
# Un proyecto en Aurora = instalación solar inminente = oportunidad.

def _fetch_aurora_projects(city: str = "") -> list:
    """
    Aurora Solar API — proyectos activos de diseño solar.
    Requiere API key de Aurora Solar (tier Business+).
    """
    if not AURORA_API_KEY:
        return []
    try:
        resp = requests.get(
            "https://api-sandbox.aurorasolar.com/v2019.01.01/projects",
            headers={
                "Authorization": f"Bearer {AURORA_API_KEY}",
                "Accept": "application/json",
            },
            params={
                "status": "active,proposal_sent",
                "sort": "-created_at",
                "per_page": 50,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        projects = data.get("projects", data) if isinstance(data, (dict, list)) else []

        # Filtrar por ciudad si se especifica
        if city and isinstance(projects, list):
            projects = [p for p in projects
                       if city.lower() in (p.get("city", "") or "").lower()
                       or city.lower() in (p.get("address", "") or "").lower()]

        return projects if isinstance(projects, list) else []
    except Exception as e:
        logger.debug(f"[Aurora/{city}] {e}")
        return []


# ── EnergySage API — marketplace de compradores de solar ─────────────
# Personas que están ACTIVAMENTE buscando cotizaciones de solar.
# Cross-sell: "Si vas a instalar solar, ¿ya revisaste tu insulación?"

def _fetch_energysage_leads(zip_code: str = "") -> list:
    """
    EnergySage API — leads activos en el marketplace.
    Requiere partner API key.
    """
    if not ENERGYSAGE_KEY:
        return []
    try:
        params = {"status": "active", "per_page": 25}
        if zip_code:
            params["zip_code"] = zip_code

        resp = requests.get(
            "https://api.energysage.com/v1/leads",
            headers={
                "Authorization": f"Token {ENERGYSAGE_KEY}",
                "Accept": "application/json",
            },
            params=params,
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("results", data) if isinstance(data, (dict, list)) else []
    except Exception as e:
        logger.debug(f"[EnergySage] {e}")
        return []


# ── OpenEI Utility Rate API (gratuita) ──────────────────────────────
# Tarifas eléctricas por utility/zona. Tarifa alta = más incentivo
# para solar + insulación (mayor ahorro potencial).

_utility_rate_cache: dict = {}

def _get_utility_rate(lat: float, lon: float) -> dict:
    """
    OpenEI Utility Rate Database API — tarifa eléctrica de la zona.
    Gratuita, no requiere API key.
    """
    cache_key = f"{lat:.2f},{lon:.2f}"
    if cache_key in _utility_rate_cache:
        return _utility_rate_cache[cache_key]

    try:
        resp = requests.get(
            "https://developer.nrel.gov/api/utility_rates/v3.json",
            params={
                "api_key": NREL_API_KEY or "DEMO_KEY",
                "lat": lat,
                "lon": lon,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return {}

        data = resp.json()
        outputs = data.get("outputs", {})

        residential_rate = outputs.get("residential", 0)
        commercial_rate = outputs.get("commercial", 0)
        utility_name = outputs.get("utility_name", "")

        result = {
            "residential_rate": residential_rate,  # $/kWh
            "commercial_rate":  commercial_rate,
            "utility_name":     utility_name,
            "rate_tier": (
                "🔴 MUY ALTA" if residential_rate > 0.30 else
                "🟠 ALTA" if residential_rate > 0.20 else
                "🟡 MEDIA" if residential_rate > 0.12 else
                "🟢 BAJA"
            ),
        }
        _utility_rate_cache[cache_key] = result
        return result

    except Exception as e:
        logger.debug(f"[OpenEI] Error: {e}")
        return {}


# ── Fetchers ─────────────────────────────────────────────────────────

def _fetch_socrata(source: dict) -> list:
    cutoff_iso = _cutoff_iso()
    params = {
        k: v.replace("{cutoff_iso}", cutoff_iso) if isinstance(v, str) else v
        for k, v in source["params"].items()
    }
    token = os.getenv("SOCRATA_APP_TOKEN", "")
    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token
    resp = requests.get(source["url"], params=params,
                        timeout=source.get("timeout", 30), headers=headers)
    if resp.status_code == 400:
        logger.warning(f"[Solar/{source['city']}] 400 Bad Request — posible dataset inválido")
        return []
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_ckan_search(source: dict) -> list:
    cutoff_ymd = _cutoff_ymd()
    resp = requests.get(
        source["url"], params=source["params"],
        timeout=source.get("timeout", 30),
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"CKAN error: {data.get('error','unknown')}")

    records    = data.get("result", {}).get("records", [])
    date_field = source.get("_date_field", "")

    result = []
    for r in records:
        date_val = (r.get(date_field) or "")[:10]
        if date_val and date_val < cutoff_ymd:
            continue
        if _is_solar(r):
            result.append(r)
    return result


def _fetch_source(source: dict) -> tuple[str, list]:
    """Fetch una fuente individual. Retorna (city, records)."""
    engine = source.get("engine", "socrata")
    if engine == "ckan_search":
        records = _fetch_ckan_search(source)
    else:
        records = _fetch_socrata(source)
    return source["city"], records


class SolarAgent(BaseAgent):
    name      = "☀️ Instalaciones Solares — Bay Area"
    emoji     = "☀️"
    agent_key = "solar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()
        # Caches para evitar llamadas repetidas por ciudad
        self._nrel_cache: dict = {}
        self._utility_cache: dict = {}

    def _get_nrel_for_city(self, city: str) -> dict | None:
        """NREL lookup con cache por ciudad."""
        if city in self._nrel_cache:
            return self._nrel_cache[city]
        coords = _CITY_COORDS.get(city)
        if not coords:
            return None
        result = _get_solar_potential(coords[0], coords[1])
        self._nrel_cache[city] = result
        return result

    def _get_utility_rate_for_city(self, city: str) -> dict:
        """Utility rate lookup con cache por ciudad."""
        if city in self._utility_cache:
            return self._utility_cache[city]
        coords = _CITY_COORDS.get(city)
        if not coords:
            return {}
        result = _get_utility_rate(coords[0], coords[1])
        self._utility_cache[city] = result
        return result

    def fetch_leads(self) -> list:
        leads = []

        # ⚡ Fetch paralelo de todas las ciudades
        with ThreadPoolExecutor(max_workers=PARALLEL_SOLAR) as executor:
            futures = {
                executor.submit(_fetch_source, src): src
                for src in SOLAR_SOURCES
            }
            for fut in as_completed(futures):
                src = futures[fut]
                try:
                    city, records = fut.result()
                    fm  = src["field_map"]
                    get = lambda r, k, _fm=fm: r.get(_fm.get(k) or "", "") or ""

                    for raw in records:
                        addr = get(raw, "address")
                        if fm.get("address2") and raw.get(fm["address2"]):
                            addr = f"{addr} {raw[fm['address2']]}".strip()

                        val = _parse_value(get(raw, "value"))
                        lead = {
                            "id":          f"{city}_{get(raw,'id')}",
                            "city":        city,
                            "address":     addr,
                            "description": get(raw, "desc"),
                            "status":      get(raw, "status"),
                            "date":        get(raw, "date")[:10] if get(raw, "date") else "",
                            "contractor":  get(raw, "contractor"),
                            "lic_number":  get(raw, "lic"),
                            "owner":       get(raw, "owner"),
                            "value":       get(raw, "value"),
                            "value_float": val,
                        }

                        # Enriquecer con datos de contacto
                        match = lookup_contact(lead["contractor"], self._contacts)
                        if match:
                            lead["contact_phone"]  = match.get("phone", "")
                            lead["contact_email"]  = match.get("email", "")
                            lead["contact_source"] = f"CSV ({match['source']})"

                        # Enriquecer con potencial solar NREL
                        nrel = self._get_nrel_for_city(city)
                        if nrel:
                            lead["solar_potential"] = nrel.get("solar_rating", "")
                            lead["ghi_annual"]      = nrel.get("ghi_annual", 0)

                        # ── Google Solar API: potencial a nivel edificio ─
                        coords = _CITY_COORDS.get(city)
                        if GOOGLE_SOLAR_KEY and coords:
                            gsolar = _google_solar_lookup(coords[0], coords[1])
                            if gsolar:
                                lead["max_panels"]    = gsolar.get("max_panels", 0)
                                lead["annual_kwh"]    = gsolar.get("annual_kwh", 0)
                                lead["roof_sqft"]     = gsolar.get("roof_sqft", 0)
                                lead["carbon_offset"] = gsolar.get("carbon_offset_kg", 0)

                        # ── OpenEI: tarifa eléctrica de la zona ─────────
                        if coords:
                            rate_info = self._get_utility_rate_for_city(city)
                            if rate_info:
                                lead["utility_rate"]  = rate_info.get("residential_rate", 0)
                                lead["rate_tier"]     = rate_info.get("rate_tier", "")
                                lead["utility_name"]  = rate_info.get("utility_name", "")
                                # Estimar ahorro anual si hay kWh y tarifa
                                if lead.get("annual_kwh") and lead["utility_rate"]:
                                    lead["annual_savings"] = round(
                                        lead["annual_kwh"] * lead["utility_rate"], 0
                                    )

                        # Lead scoring
                        lead["_agent_key"] = "solar"
                        scoring = score_lead(lead)
                        lead["_scoring"] = scoring

                        leads.append(lead)

                    logger.info(f"[Solar/{city}] {len(records)} permisos solares")

                except Exception as e:
                    logger.error(f"[Solar/{src['city']}] Error: {e}")

        # ── Aurora Solar: proyectos activos de diseño (pago) ─────
        if AURORA_API_KEY:
            for city_name in ["San Francisco", "Oakland", "San Jose",
                                "Fremont", "Hayward", "Concord", "Walnut Creek",
                                "Richmond", "Berkeley", "Sunnyvale", "Santa Clara",
                                "Daly City", "Vallejo"]:
                try:
                    projects = _fetch_aurora_projects(city_name)
                    for proj in projects:
                        lead = {
                            "id":          f"aurora_{proj.get('id', '')}",
                            "city":        city_name,
                            "address":     proj.get("address", ""),
                            "description": f"Proyecto solar Aurora — {proj.get('status', '')}",
                            "status":      proj.get("status", ""),
                            "date":        (proj.get("created_at") or "")[:10],
                            "contractor":  proj.get("installer_name", ""),
                            "owner":       proj.get("customer_name", ""),
                            "value_float": float(proj.get("system_cost", 0) or 0),
                            "annual_kwh":  float(proj.get("annual_production_kwh", 0) or 0),
                            "system_size_kw": float(proj.get("system_size_kw", 0) or 0),
                            "source":      "Aurora Solar",
                            "_agent_key":  "solar",
                        }
                        if proj.get("customer_email"):
                            lead["contact_email"]  = proj["customer_email"]
                            lead["contact_source"] = "Aurora Solar"

                        scoring = score_lead(lead)
                        scoring["reasons"].insert(0, "🔥 Proyecto solar activo en Aurora")
                        scoring["score"] = min(scoring["score"] + 15, 100)
                        lead["_scoring"] = scoring
                        leads.append(lead)

                    if projects:
                        logger.info(f"[Aurora/{city_name}] {len(projects)} proyectos")
                except Exception as e:
                    logger.debug(f"[Aurora/{city_name}] {e}")

        # ── EnergySage: compradores activos (pago) ──────────────
        if ENERGYSAGE_KEY:
            bay_area_zips = ["94102", "94607", "95112", "94538", "94704",
                             "94087", "94805", "94025", "94014",
                             "94520", "94596", "94590", "94903", "94558",
                             "94501", "94568", "94588", "95376", "94533"]
            for zip_code in bay_area_zips:
                try:
                    es_leads = _fetch_energysage_leads(zip_code)
                    for es in es_leads:
                        lead = {
                            "id":          f"energysage_{es.get('id', '')}",
                            "city":        es.get("city", zip_code),
                            "address":     es.get("address", "") or f"ZIP {zip_code}",
                            "description": "Comprador activo buscando cotización solar",
                            "status":      "Buscando cotización",
                            "date":        (es.get("created_at") or "")[:10],
                            "owner":       es.get("name", ""),
                            "value_float": float(es.get("estimated_system_cost", 0) or 0),
                            "system_size_kw": float(es.get("system_size_kw", 0) or 0),
                            "source":      "EnergySage",
                            "_agent_key":  "solar",
                        }
                        if es.get("email"):
                            lead["contact_email"]  = es["email"]
                            lead["contact_source"] = "EnergySage"
                        if es.get("phone"):
                            lead["contact_phone"]  = es["phone"]
                            lead["contact_source"] = "EnergySage"

                        scoring = score_lead(lead)
                        scoring["reasons"].insert(0, "🎯 Comprador activo en EnergySage")
                        scoring["score"] = min(scoring["score"] + 20, 100)
                        lead["_scoring"] = scoring
                        leads.append(lead)

                    if es_leads:
                        logger.info(f"[EnergySage/{zip_code}] {len(es_leads)} compradores")
                except Exception as e:
                    logger.debug(f"[EnergySage/{zip_code}] {e}")

        # Ordenar por score, luego por valor
        leads.sort(key=lambda l: (
            -l.get("_scoring", {}).get("score", 0),
            -l.get("value_float", 0),
        ))
        return leads

    def notify(self, lead: dict):
        scoring    = lead.get("_scoring", {})
        score_line = format_score_line(scoring) if scoring else ""
        phone      = lead.get("contact_phone") or "No disponible"
        source     = lead.get("contact_source", "")
        value      = lead.get("value_float", 0)

        fields = {
            "📍 Ciudad":           lead.get("city"),
            "📝 Descripción":      (lead.get("description") or "")[:200],
            "📊 Estado":           lead.get("status"),
            "📅 Fecha":            lead.get("date"),
            "👷 Contratista (GC)": lead.get("contractor") or "—",
            "📞 Teléfono GC":      f"{phone}  _(via {source})_" if source else phone,
            "✉️  Email GC":        lead.get("contact_email") or "—",
            "👤 Propietario":      lead.get("owner") or "—",
            "💰 Valor":            f"${value:,.0f}" if value else "—",
        }

        # Potencial solar (NREL)
        solar_pot = lead.get("solar_potential")
        if solar_pot:
            ghi = lead.get("ghi_annual", 0)
            fields["☀️ Potencial Solar"] = f"{solar_pot} (GHI: {ghi:.1f} kWh/m²/día)"

        # Google Solar API — datos de edificio
        if lead.get("max_panels"):
            fields["🔋 Capacidad Techo"] = (
                f"{lead['max_panels']} paneles / {lead.get('roof_sqft', 0):,.0f} sqft"
            )
        if lead.get("annual_kwh"):
            fields["⚡ Producción Anual"] = f"{lead['annual_kwh']:,.0f} kWh"
        if lead.get("carbon_offset"):
            fields["🌍 CO₂ Offset"] = f"{lead['carbon_offset']:,.0f} kg/año"

        # Tarifa eléctrica (OpenEI)
        if lead.get("rate_tier"):
            rate = lead.get("utility_rate", 0)
            fields["💡 Tarifa Eléctrica"] = f"{lead['rate_tier']} (${rate:.3f}/kWh)"
        if lead.get("annual_savings"):
            fields["💰 Ahorro Estimado"] = f"${lead['annual_savings']:,.0f}/año"
        if lead.get("utility_name"):
            fields["🏢 Utility"] = lead["utility_name"]

        # Sistema solar (Aurora/EnergySage)
        if lead.get("system_size_kw"):
            fields["🔌 Sistema"] = f"{lead['system_size_kw']:.1f} kW"

        # Fuente especial
        if lead.get("source") in ("Aurora Solar", "EnergySage"):
            fields["📡 Fuente"] = f"🎯 {lead['source']}"

        if score_line:
            fields["🎯 Lead Score"] = score_line

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta="☀️ Solar nuevo = oportunidad de mejorar aislamiento. ¡Contáctalos!",
        )

        # Multi-canal para leads calientes
        if scoring.get("score", 0) >= 70:
            notify_multichannel(lead, scoring)
