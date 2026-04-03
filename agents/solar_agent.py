"""
agents/solar_agent.py  v7
━━━━━━━━━━━━━━━━━━━━━━━
☀️ Instalaciones Solares — Bay Area

MEJORAS v7:
  ✅ +5 ciudades Bay Area (Oakland, Sunnyvale, Santa Clara, Berkeley, Richmond)
     vía Socrata con filtro solar server-side
  ✅ Integración NREL Solar Resource API (gratuita con API key)
     para enriquecer leads con potencial solar de la zona
  ✅ Fetch paralelo de todas las ciudades (ThreadPoolExecutor)
  ✅ Keywords solares ampliados (battery storage, EV charger, energy upgrade)
  ✅ Score de prioridad basado en valor + potencial solar
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

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT   = int(os.getenv("SOURCE_TIMEOUT", "45"))
MIN_PERMIT_VALUE = float(os.getenv("MIN_PERMIT_VALUE", "50000"))
PERMIT_MONTHS    = int(os.getenv("PERMIT_MONTHS", "3"))
PARALLEL_SOLAR   = int(os.getenv("PARALLEL_SOLAR", "6"))
NREL_API_KEY     = os.getenv("NREL_API_KEY", "")  # Free: https://developer.nrel.gov/signup/

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
}


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
        # Cache NREL para evitar llamadas repetidas por ciudad
        self._nrel_cache: dict = {}

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

                        leads.append(lead)

                    logger.info(f"[Solar/{city}] {len(records)} permisos solares")

                except Exception as e:
                    logger.error(f"[Solar/{src['city']}] Error: {e}")

        # Ordenar por valor (mayor primero) para priorizar leads valiosos
        leads.sort(key=lambda l: l.get("value_float", 0), reverse=True)
        return leads

    def notify(self, lead: dict):
        phone  = lead.get("contact_phone") or "No disponible"
        source = lead.get("contact_source", "")
        value  = lead.get("value_float", 0)

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

        # Agregar potencial solar si disponible (NREL)
        solar_pot = lead.get("solar_potential")
        if solar_pot:
            ghi = lead.get("ghi_annual", 0)
            fields["☀️ Potencial Solar"] = f"{solar_pot} (GHI: {ghi:.1f} kWh/m²/día)"

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta="☀️ Solar nuevo = oportunidad de mejorar aislamiento. ¡Contáctalos!",
        )
