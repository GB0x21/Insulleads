"""
agents/construction_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚧 Construcciones Activas — Bay Area

Monitorea inspecciones de construcción para detectar proyectos
EN PROGRESO y su fase actual. El timing es clave:

  Fase de CIMENTACIÓN  → muy temprano, proyecto recién comienza
  Fase de ESTRUCTURA   → ¡CONTACTAR AHORA! insulación es el siguiente paso
  Fase de INSULACIÓN   → ya están comprando, ¿quién es el proveedor?
  Fase de DRYWALL      → tarde pero aún posible para blown-in
  Fase de FINAL        → oportunidad perdida para nuevo, pero upgrades futuros

Fuentes gratuitas:
  1. SF Building Inspections (Socrata) — inspecciones programadas/realizadas
  2. San Jose Inspections (CKAN) — registros de inspección
  3. Oakland Inspections (Socrata) — data abierta
  4. Sunnyvale/Berkeley/Richmond inspections (Socrata)

Fuentes de pago:
  5. BuildZoom API — tracking avanzado de proyectos ($100-300/mes)
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

SOURCE_TIMEOUT      = int(os.getenv("SOURCE_TIMEOUT", "45"))
CONSTRUCTION_MONTHS = int(os.getenv("CONSTRUCTION_MONTHS", "1"))
PARALLEL_INSPECT    = int(os.getenv("PARALLEL_INSPECT", "6"))
BUILDZOOM_API_KEY   = os.getenv("BUILDZOOM_API_KEY", "")


def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * CONSTRUCTION_MONTHS)).strftime("%Y-%m-%dT00:00:00")

def _cutoff_ymd() -> str:
    return (datetime.utcnow() - timedelta(days=30 * CONSTRUCTION_MONTHS)).strftime("%Y-%m-%d")

def _parse_value(v) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", str(v) or "") or "0")
    except Exception:
        return 0.0


# ── Fases de construcción y su relevancia para insulación ────────────

CONSTRUCTION_PHASES = {
    # Fase temprana — proyecto arrancando
    "foundation": {
        "keywords": ["FOUNDATION", "FOOTING", "SLAB", "EXCAVATION", "GRADING",
                      "CIMENTACION", "LOSA"],
        "phase_order": 1,
        "timing":      "🔵 Temprano",
        "action":      "Proyecto iniciando. Contactar para cotización anticipada.",
        "priority":    2,
    },
    # Fase estructural — TIMING PERFECTO
    "framing": {
        "keywords": ["FRAMING", "FRAME", "ROUGH FRAME", "STRUCTURAL",
                      "SHEATHING", "SHEAR WALL", "ESTRUCTURA", "FRAME ROUGH",
                      "TOP OUT", "ROUGH FRAMING"],
        "phase_order": 2,
        "timing":      "🔥 AHORA",
        "action":      "¡Estructura lista! Insulación es el SIGUIENTE paso. ¡CONTACTAR YA!",
        "priority":    5,
    },
    # Fase de sistemas MEP — oportunidad activa
    "rough_mep": {
        "keywords": ["ROUGH PLUMBING", "ROUGH ELECTRIC", "ROUGH MECHANICAL",
                      "ROUGH MEP", "MEP ROUGH", "ROUGH-IN", "HVAC ROUGH",
                      "DUCTWORK", "DUCTOS"],
        "phase_order": 3,
        "timing":      "🟠 Oportunidad",
        "action":      "MEP en progreso. Insulación se instala junto o justo después.",
        "priority":    4,
    },
    # Fase de insulación — ver quién lo está haciendo
    "insulation": {
        "keywords": ["INSULATION", "INSULATE", "BATT INSUL", "SPRAY FOAM",
                      "BLOWN IN", "THERMAL", "R-VALUE", "VAPOR BARRIER",
                      "AISLAMIENTO", "INSULACION", "ENERGY COMPLIANCE",
                      "TITLE 24"],
        "phase_order": 4,
        "timing":      "⚡ EN CURSO",
        "action":      "Insulación en progreso. ¿Quién es el subcontratista? Ofrecer alternativa.",
        "priority":    3,
    },
    # Fase de cierre — tarde pero posible
    "drywall": {
        "keywords": ["DRYWALL", "LATH", "PLASTER", "STUCCO", "EXTERIOR FINISH",
                      "WALL COVER", "SHEETROCK"],
        "phase_order": 5,
        "timing":      "🟡 Último chance",
        "action":      "Paredes cerrándose. Aún posible blown-in o correcciones.",
        "priority":    2,
    },
    # Inspección final — oportunidad para upgrades
    "final": {
        "keywords": ["FINAL INSPECTION", "FINAL BLDG", "FINAL BUILDING",
                      "FINAL COMBO", "CERTIFICATE OF OCCUPANCY", "C OF O",
                      "TCO", "FINAL SIGN"],
        "phase_order": 6,
        "timing":      "✅ Completado",
        "action":      "Proyecto terminado. Ofrecer mejoras futuras / mantenimiento.",
        "priority":    1,
    },
}


def _classify_phase(inspection_text: str) -> dict | None:
    """Identifica la fase de construcción a partir del texto de inspección."""
    upper = (inspection_text or "").upper()
    for phase_name, phase_info in CONSTRUCTION_PHASES.items():
        if any(kw in upper for kw in phase_info["keywords"]):
            return {
                "phase":       phase_name,
                "phase_order": phase_info["phase_order"],
                "timing":      phase_info["timing"],
                "action":      phase_info["action"],
                "priority":    phase_info["priority"],
            }
    return None


# ── Fuentes de inspecciones — ciudades Bay Area ─────────────────────

INSPECTION_SOURCES = [
    # ── SF Building Inspections ──────────────────────────────────
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/biys-ruxt.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 200,
            "$order": "inspection_date DESC",
            "$where": (
                "inspection_date >= '{cutoff_iso}' "
                "AND (UPPER(inspection_type_description) LIKE '%FRAME%' "
                "OR UPPER(inspection_type_description) LIKE '%INSULATION%' "
                "OR UPPER(inspection_type_description) LIKE '%ROUGH%' "
                "OR UPPER(inspection_type_description) LIKE '%DRYWALL%' "
                "OR UPPER(inspection_type_description) LIKE '%FOUNDATION%' "
                "OR UPPER(inspection_type_description) LIKE '%FINAL%')"
            ),
        },
        "field_map": {
            "id":           "complaint_number",
            "permit_id":    "permit_number",
            "address":      "block",
            "address2":     "lot",
            "inspection":   "inspection_type_description",
            "status":       "inspection_status",
            "date":         "inspection_date",
            "inspector":    "inspector",
            "result":       "inspection_status",
            "contractor":   "contractor_name",
            "owner":        "property_owner",
        },
    },
    # ── SF Permit Activity (complementario) ──────────────────────
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/i98e-djp9.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND status IN('issued','complete') "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%' "
                "OR UPPER(description) LIKE '%STRUCTURE%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "street_number",
            "address2":     "street_name",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_company_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "estimated_cost",
        },
        "_is_permit": True,
    },
    # ── San Jose Inspections — CKAN ──────────────────────────────
    {
        "city":    "San Jose",
        "engine":  "ckan",
        "url":     "https://data.sanjoseca.gov/api/3/action/datastore_search",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904",
            "limit":       300,
            "sort":        "ISSUEDATE desc",
        },
        "field_map": {
            "id":           "FOLDERNUMBER",
            "permit_id":    "FOLDERNUMBER",
            "address":      "gx_location",
            "inspection":   "WORKDESCRIPTION",
            "status":       "Status",
            "date":         "ISSUEDATE",
            "contractor":   "CONTRACTOR",
            "owner":        "OWNERNAME",
            "value":        "PERMITVALUATION",
        },
        "_filter_phases": True,
    },
    # ── Oakland ──────────────────────────────────────────────────
    {
        "city":    "Oakland",
        "engine":  "socrata",
        "url":     "https://data.oaklandca.gov/resource/uymu-f5cz.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "application_date DESC",
            "$where": (
                "application_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "application_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license_number",
            "owner":        "owner_name",
            "value":        "job_value",
        },
        "_skip_if_no_data": True,
    },
    # ── Sunnyvale ────────────────────────────────────────────────
    {
        "city":    "Sunnyvale",
        "engine":  "socrata",
        "url":     "https://data.sunnyvale.ca.gov/resource/irbr-7ykz.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "issueddate DESC",
            "$where": (
                "issueddate >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issueddate",
            "contractor":   "contractor",
            "lic_number":   "license_number",
            "owner":        "owner",
            "value":        "valuation",
        },
        "_skip_if_no_data": True,
    },
    # ── Berkeley ─────────────────────────────────────────────────
    {
        "city":    "Berkeley",
        "engine":  "socrata",
        "url":     "https://data.cityofberkeley.info/resource/k92i-t48y.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "issue_date DESC",
            "$where": (
                "issue_date >= '{cutoff_iso}' "
                "AND (UPPER(project_description) LIKE '%FRAME%' "
                "OR UPPER(project_description) LIKE '%INSULATION%' "
                "OR UPPER(project_description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "record_number",
            "permit_id":    "record_number",
            "address":      "address",
            "inspection":   "project_description",
            "status":       "record_status",
            "date":         "issue_date",
            "contractor":   "contractor",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "job_value",
        },
        "_skip_if_no_data": True,
    },
    # ── Contra Costa County — Socrata ────────────────────────────
    {
        "city":    "Contra Costa County",
        "engine":  "socrata",
        "url":     "https://data.contracosta.gov/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── Alameda County — Socrata ─────────────────────────────────
    {
        "city":    "Alameda County",
        "engine":  "socrata",
        "url":     "https://data.acgov.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── San Mateo County — Socrata ───────────────────────────────
    {
        "city":    "San Mateo County",
        "engine":  "socrata",
        "url":     "https://data.smcgov.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── Solano County — Socrata ──────────────────────────────────
    {
        "city":    "Solano County",
        "engine":  "socrata",
        "url":     "https://data.solanocounty.com/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── Marin County — Socrata ───────────────────────────────────
    {
        "city":    "Marin County",
        "engine":  "socrata",
        "url":     "https://data.marincounty.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── Napa County — Socrata ────────────────────────────────────
    {
        "city":    "Napa County",
        "engine":  "socrata",
        "url":     "https://data.countyofnapa.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── Sonoma County — Socrata ──────────────────────────────────
    {
        "city":    "Sonoma County",
        "engine":  "socrata",
        "url":     "https://data.sonomacounty.ca.gov/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
    # ── San Joaquin County — Socrata ─────────────────────────────
    {
        "city":    "San Joaquin County",
        "engine":  "socrata",
        "url":     "https://data.sjgov.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%INSULATION%' "
                "OR UPPER(description) LIKE '%ROUGH%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "address",
            "inspection":   "description",
            "status":       "status",
            "date":         "issued_date",
            "contractor":   "contractor_name",
            "lic_number":   "contractor_license",
            "owner":        "owner",
            "value":        "valuation",
        },
    },
]


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
        logger.warning(f"[Construction/{source['city']}] 400 Bad Request")
        return []
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_ckan(source: dict) -> list:
    resp = requests.get(
        source["url"], params=source["params"],
        timeout=source.get("timeout", 30),
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return []

    records = data.get("result", {}).get("records", [])

    # Filtrar por fase de construcción + fecha
    if source.get("_filter_phases"):
        cutoff = _cutoff_ymd()
        filtered = []
        for r in records:
            date_val = (r.get("ISSUEDATE") or "")[:10]
            if date_val and date_val < cutoff:
                continue
            text = str(r.get("WORKDESCRIPTION") or "")
            if _classify_phase(text):
                filtered.append(r)
        return filtered

    return records


def _fetch_source(source: dict) -> tuple[str, list]:
    engine = source.get("engine", "socrata")
    if engine == "ckan":
        records = _fetch_ckan(source)
    else:
        records = _fetch_socrata(source)
    return source["city"], records


# ── BuildZoom API (pago, opcional) ───────────────────────────────────

def _fetch_buildzoom_projects(city: str, state: str = "CA") -> list:
    """
    BuildZoom API — tracking avanzado de proyectos de construcción.
    Proporciona timeline de proyecto, contratista, valor, y estado actual.
    Requiere API key ($100-300/mes).
    """
    if not BUILDZOOM_API_KEY:
        return []

    try:
        resp = requests.get(
            "https://api.buildzoom.com/v1/projects",
            headers={
                "Authorization": f"Bearer {BUILDZOOM_API_KEY}",
                "Accept": "application/json",
            },
            params={
                "city": city,
                "state": state,
                "status": "active",
                "sort": "start_date",
                "order": "desc",
                "per_page": 50,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("projects", data) if isinstance(data, (dict, list)) else []
    except Exception as e:
        logger.debug(f"[BuildZoom/{city}] {e}")
        return []


class ConstructionAgent(BaseAgent):
    name      = "🚧 Construcciones Activas — Bay Area"
    emoji     = "🚧"
    agent_key = "construction"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        leads = []

        # ⚡ Fetch paralelo de inspecciones públicas
        with ThreadPoolExecutor(max_workers=PARALLEL_INSPECT) as executor:
            futures = {
                executor.submit(_fetch_source, src): src
                for src in INSPECTION_SOURCES
            }
            for fut in as_completed(futures):
                src = futures[fut]
                try:
                    city, records = fut.result()
                    fm = src["field_map"]
                    get = lambda r, k, _fm=fm: r.get(_fm.get(k) or "", "") or ""

                    for raw in records:
                        # Clasificar fase de construcción
                        inspection_text = get(raw, "inspection")
                        phase_info = _classify_phase(inspection_text)

                        if not phase_info:
                            continue

                        addr = get(raw, "address")
                        if fm.get("address2") and raw.get(fm.get("address2", "") or ""):
                            addr = f"{addr} {raw[fm['address2']]}".strip()

                        value = _parse_value(get(raw, "value"))

                        lead = {
                            "id":           f"{city}_insp_{get(raw, 'id')}_{phase_info['phase']}",
                            "city":         city,
                            "address":      addr,
                            "permit_id":    get(raw, "permit_id"),
                            "description":  inspection_text[:200],
                            "status":       get(raw, "status"),
                            "date":         get(raw, "date")[:10] if get(raw, "date") else "",
                            "contractor":   get(raw, "contractor"),
                            "lic_number":   get(raw, "lic_number"),
                            "owner":        get(raw, "owner"),
                            "value":        str(value) if value else "",
                            "value_float":  value,
                            "inspector":    get(raw, "inspector"),
                            "result":       get(raw, "result"),
                            # Fase de construcción
                            "phase":        phase_info["phase"],
                            "phase_order":  phase_info["phase_order"],
                            "timing":       phase_info["timing"],
                            "action":       phase_info["action"],
                            "phase_priority": phase_info["priority"],
                            "_agent_key":   "construction",
                        }

                        # Enriquecer contacto GC
                        contractor = lead["contractor"]
                        if contractor:
                            match = lookup_contact(contractor, self._contacts)
                            if match:
                                lead["contact_phone"]  = match.get("phone", "")
                                lead["contact_email"]  = match.get("email", "")
                                lead["contact_source"] = f"CSV ({match['source']})"

                        # Lead scoring
                        scoring = score_lead(lead)
                        # Boost score por fase de construcción
                        phase_boost = phase_info["priority"] * 5
                        scoring["score"] = min(scoring["score"] + phase_boost, 100)
                        if phase_info["phase"] == "framing":
                            scoring["reasons"].insert(0, "🔥 Fase FRAMING — insulación es siguiente")
                            scoring["grade"] = "HOT"
                            scoring["grade_emoji"] = "🔥"
                        elif phase_info["phase"] == "rough_mep":
                            scoring["reasons"].insert(0, "🟠 Fase MEP — insulación inminente")
                        lead["_scoring"] = scoring

                        leads.append(lead)

                    logger.info(f"[Construction/{city}] {len(records)} inspecciones procesadas")

                except Exception as e:
                    if not src.get("_skip_if_no_data"):
                        logger.error(f"[Construction/{src['city']}] Error: {e}")
                    else:
                        logger.debug(f"[Construction/{src['city']}] {e}")

        # ── BuildZoom (pago, opcional) ───────────────────────────
        if BUILDZOOM_API_KEY:
            for city in ["San Francisco", "Oakland", "San Jose", "Berkeley", "Richmond",
                         "Fremont", "Hayward", "Concord", "Walnut Creek", "Vallejo",
                         "Daly City", "San Mateo", "Livermore", "Pleasanton",
                         "San Rafael", "Napa", "Fairfield"]:
                try:
                    projects = _fetch_buildzoom_projects(city)
                    for proj in projects:
                        lead = self._buildzoom_to_lead(proj, city)
                        if lead:
                            leads.append(lead)
                    logger.info(f"[BuildZoom/{city}] {len(projects)} proyectos activos")
                except Exception as e:
                    logger.debug(f"[BuildZoom/{city}] {e}")

        # Ordenar por prioridad de fase (framing primero) y luego por valor
        leads.sort(key=lambda l: (
            -l.get("phase_priority", 0),
            -l.get("value_float", 0),
        ))

        return leads

    def _buildzoom_to_lead(self, project: dict, city: str) -> dict | None:
        """Convierte un proyecto BuildZoom a lead normalizado."""
        address = project.get("address", "")
        if not address:
            return None

        phase_text = project.get("current_phase") or project.get("status", "")
        phase_info = _classify_phase(phase_text)
        if not phase_info:
            return None

        value = _parse_value(project.get("value") or project.get("estimated_cost", ""))

        lead = {
            "id":           f"bz_{city}_{project.get('id', '')}",
            "city":         city,
            "address":      address,
            "permit_id":    project.get("permit_number", ""),
            "description":  project.get("description", "")[:200],
            "status":       project.get("status", ""),
            "date":         (project.get("start_date") or "")[:10],
            "contractor":   project.get("contractor_name", ""),
            "owner":        project.get("owner_name", ""),
            "value":        str(value) if value else "",
            "value_float":  value,
            "phase":        phase_info["phase"],
            "phase_order":  phase_info["phase_order"],
            "timing":       phase_info["timing"],
            "action":       phase_info["action"],
            "phase_priority": phase_info["priority"],
            "source":       "BuildZoom",
            "_agent_key":   "construction",
        }

        # Contacto
        if lead["contractor"]:
            match = lookup_contact(lead["contractor"], self._contacts)
            if match:
                lead["contact_phone"]  = match.get("phone", "")
                lead["contact_email"]  = match.get("email", "")
                lead["contact_source"] = f"CSV ({match['source']})"

        scoring = score_lead(lead)
        lead["_scoring"] = scoring
        return lead

    def notify(self, lead: dict):
        scoring = lead.get("_scoring", {})
        score_line = format_score_line(scoring) if scoring else ""

        phase = lead.get("phase", "unknown")
        timing = lead.get("timing", "")
        value = lead.get("value_float", 0)

        # Header con fase prominente
        phase_display = {
            "foundation": "🔵 CIMENTACIÓN",
            "framing":    "🔥 ESTRUCTURA (FRAMING)",
            "rough_mep":  "🟠 MEP ROUGH-IN",
            "insulation":  "⚡ INSULACIÓN",
            "drywall":    "🟡 DRYWALL/CIERRE",
            "final":      "✅ INSPECCIÓN FINAL",
        }.get(phase, phase.upper())

        fields = {
            "📍 Ciudad":            lead.get("city"),
            "🚧 Fase Actual":      phase_display,
            "⏱️ Timing":           timing,
            "📝 Inspección":       (lead.get("description") or "")[:150],
            "📊 Estado":           lead.get("status") or lead.get("result") or "—",
            "📅 Fecha":            lead.get("date"),
        }

        if lead.get("permit_id"):
            fields["🔖 Permiso"] = lead["permit_id"]

        if value:
            fields["💰 Valor"] = f"${value:,.0f}"

        if lead.get("contractor"):
            fields["👷 Contratista (GC)"] = lead["contractor"]
        if lead.get("lic_number"):
            fields["🪪 Licencia CSLB"] = lead["lic_number"]
        if lead.get("owner"):
            fields["👤 Propietario"] = lead["owner"]

        if lead.get("contact_phone"):
            src = lead.get("contact_source", "")
            fields["📞 Teléfono GC"] = (
                f"{lead['contact_phone']}  _(via {src})_" if src
                else lead["contact_phone"]
            )
        if lead.get("contact_email"):
            fields["✉️  Email GC"] = lead["contact_email"]

        if score_line:
            fields["🎯 Lead Score"] = score_line

        if lead.get("source") == "BuildZoom":
            fields["📡 Fuente"] = "BuildZoom"

        action = lead.get("action", "")

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta=f"🚧 {action}",
        )

        # Multi-canal para leads HOT (framing/rough_mep)
        if phase in ("framing", "rough_mep", "insulation"):
            notify_multichannel(lead, scoring)
