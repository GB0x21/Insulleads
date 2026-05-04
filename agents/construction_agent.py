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
import time
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact
from utils.lead_scoring import score_lead, format_score_line
from utils.notifications import notify_multichannel
from utils.inspection_schedule import get_next_visit_window
from utils import cslb_bulk
from utils.contact_enrichment import enrich_contact as _ext_enrich

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT      = int(os.getenv("SOURCE_TIMEOUT", "45"))
CONSTRUCTION_MONTHS = int(os.getenv("CONSTRUCTION_MONTHS", "1"))
PARALLEL_INSPECT    = int(os.getenv("PARALLEL_INSPECT", "6"))
BUILDZOOM_API_KEY   = os.getenv("BUILDZOOM_API_KEY", "")

_CSLB_URL = "https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
_CSLB_HDR = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}


def _cslb_scrape(license_number: str = None, company_name: str = None) -> dict:
    """CSLB web scraper — by license number or company name. Returns phone, name, city, status."""
    result = {}
    try:
        s = requests.Session()
        s.headers.update(_CSLB_HDR)
        r = s.get(_CSLB_URL, timeout=10)
        r.raise_for_status()
        hidden = {t.get("name", ""): t.get("value", "")
                  for t in BeautifulSoup(r.text, "html.parser").find_all("input", {"type": "hidden"})}
        if license_number and re.match(r"^\d{4,}$", str(license_number).strip()):
            val, typ = str(license_number).strip(), "License"
        elif company_name:
            val, typ = company_name.strip()[:50], "Business"
        else:
            return result
        payload = {
            **hidden,
            "ctl00$ContentPlaceHolder1$RadioButtonList1": typ,
            "ctl00$ContentPlaceHolder1$TextBox1": val,
            "ctl00$ContentPlaceHolder1$Button1": "Submit",
        }
        r2 = s.post(_CSLB_URL, data=payload, timeout=10)
        r2.raise_for_status()
        soup2 = BeautifulSoup(r2.text, "html.parser")
        table = (soup2.find("table", {"id": re.compile(r"Grid|Results|License", re.I)})
                 or soup2.find("table"))
        if table:
            for row in table.find_all("tr")[1:2]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 3:
                    result = {
                        "phone":       cells[3] if len(cells) > 3 else "",
                        "cslb_name":   cells[1] if len(cells) > 1 else "",
                        "cslb_city":   cells[2] if len(cells) > 2 else "",
                        "cslb_status": cells[4] if len(cells) > 4 else "",
                    }
    except Exception as e:
        logger.debug(f"[CSLB] scrape error: {e}")
    return result


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
    # Fase estructural — TIMING PERFECTO para insulación
    "framing": {
        "keywords": ["FRAMING", "FRAME", "ROUGH FRAME", "STRUCTURAL",
                      "SHEATHING", "SHEAR WALL", "ESTRUCTURA", "FRAME ROUGH",
                      "TOP OUT", "ROUGH FRAMING"],
        "phase_order": 1,
        "timing":      "🔥 AHORA",
        "action":      "¡Estructura lista! Insulación es el SIGUIENTE paso. ¡CONTACTAR YA!",
        "priority":    5,
    },
    # Fase de sistemas MEP — ventana activa
    "rough_mep": {
        "keywords": ["ROUGH PLUMBING", "ROUGH ELECTRIC", "ROUGH MECHANICAL",
                      "ROUGH MEP", "MEP ROUGH", "ROUGH-IN", "HVAC ROUGH",
                      "DUCTWORK", "DUCTOS"],
        "phase_order": 2,
        "timing":      "🟠 Oportunidad",
        "action":      "MEP en progreso. Insulación se instala justo después. Contactar HOY.",
        "priority":    4,
    },
    # Fases excluidas — insulación ya fue instalada o aún no hay estructura:
    # foundation (muy temprano), insulation (ya en curso), drywall (tarde),
    # final (terminado). No generan leads útiles para subcontratista.
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
    # ── SF Permit Activity — fuente principal SF ──────────────────
    # Dataset i98e-djp9: permisos emitidos con tipo de trabajo en descripción.
    # biys-ruxt fue retirado por SF DBI (devuelve 404 desde 2026).
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/i98e-djp9.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 200,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND status IN('issued','complete') "
                "AND (UPPER(description) LIKE '%FRAME%' "
                "OR UPPER(description) LIKE '%ROUGH PLUMBING%' "
                "OR UPPER(description) LIKE '%ROUGH ELECTRIC%' "
                "OR UPPER(description) LIKE '%ROUGH MECHANICAL%' "
                "OR UPPER(description) LIKE '%ROUGH-IN%' "
                "OR UPPER(description) LIKE '%HVAC ROUGH%' "
                "OR UPPER(description) LIKE '%STRUCTURAL%' "
                "OR UPPER(description) LIKE '%SHEATHING%')"
            ),
        },
        "field_map": {
            "id":           "permit_number",
            "permit_id":    "permit_number",
            "address":      "street_number",
            "address2":     "street_name",
            "address3":     "street_suffix",
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
        self._contacts   = load_all_contacts()
        self._cslb_cache = {}

    def _enrich_gc(self, lead: dict) -> dict:
        """Three-tier contact enrichment cascade.

        Tier 1: Local CSV (by contractor name)
        Tier 2: CSLB bulk index → scraper (by license number, then company)
        Tier 3+4: Hunter.io / Apollo.io (email finder)
        """
        contractor  = (lead.get("contractor") or "").strip()
        lic_number  = (lead.get("lic_number") or "").strip()
        owner       = (lead.get("owner") or "").strip()
        search_name = contractor or owner
        cache_key   = lic_number or search_name
        if not cache_key:
            return {}
        if cache_key in self._cslb_cache:
            return self._cslb_cache[cache_key]

        enrichment: dict = {}

        # Tier 1: local CSV (name fuzzy match)
        if search_name:
            match = lookup_contact(search_name, self._contacts)
            if match:
                if match.get("phone"):
                    enrichment["contact_phone"] = match["phone"]
                if match.get("email"):
                    enrichment["contact_email"] = match["email"]
                enrichment["contact_source"] = f"CSV ({match['source']})"

        # Tier 2: CSLB bulk index first, then web scraper
        if lic_number or search_name:
            cslb: dict = {}
            if lic_number:
                cslb = cslb_bulk.lookup(license_number=lic_number)
            if not cslb.get("phone") and contractor:
                cslb = cslb_bulk.lookup(company_name=contractor)
            # Fallback: CSLB web scraper
            if not cslb:
                time.sleep(0.3)
                if lic_number:
                    cslb = _cslb_scrape(license_number=lic_number)
                if not cslb.get("phone") and contractor:
                    cslb = _cslb_scrape(company_name=contractor)
                if not cslb.get("phone") and owner and owner != contractor:
                    cslb = _cslb_scrape(company_name=owner)
            if cslb.get("phone") and not enrichment.get("contact_phone"):
                enrichment["contact_phone"] = cslb["phone"]
                src = enrichment.get("contact_source")
                enrichment["contact_source"] = f"{src} + CSLB" if src else "CSLB"

        # Tier 3+4: Hunter.io / Apollo.io (email only when still missing)
        if not enrichment.get("contact_email") and search_name:
            try:
                ext = _ext_enrich(company_name=search_name, person_name=contractor)
                if ext.get("email"):
                    enrichment["contact_email"] = ext["email"]
                    if ext.get("phone") and not enrichment.get("contact_phone"):
                        enrichment["contact_phone"] = ext["phone"]
                    src = enrichment.get("contact_source")
                    tag = ext.get("source", "Hunter/Apollo")
                    enrichment["contact_source"] = f"{src} + {tag}" if src else tag
            except Exception as e:
                logger.debug(f"[Hunter/Apollo] error: {e}")

        self._cslb_cache[cache_key] = enrichment
        return enrichment

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
                        if fm.get("address3") and raw.get(fm.get("address3", "") or ""):
                            addr = f"{addr} {raw[fm['address3']]}".strip()

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

                        # Enriquecer contacto GC — CSV → CSLB → Hunter/Apollo
                        enrichment = self._enrich_gc(lead)
                        for _k in ("contact_phone", "contact_email", "contact_source"):
                            if enrichment.get(_k):
                                lead[_k] = enrichment[_k]

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

        # Enriquecer contacto GC
        enrichment = self._enrich_gc(lead)
        for _k in ("contact_phone", "contact_email", "contact_source"):
            if enrichment.get(_k):
                lead[_k] = enrichment[_k]

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

        # Lookup inspection schedule for visit timing
        visit = get_next_visit_window(lead)
        if visit:
            lead["_next_inspection"] = visit
            fields["🗓️ Proxima Inspeccion"] = visit.get("best_visit", "")
            if visit.get("type"):
                fields["🔍 Tipo Inspeccion"] = visit["type"]

        action = lead.get("action", "")
        cta = f"🚧 {action}"
        if visit:
            cta = f"🚧 Visita la obra: {visit.get('best_visit', '')} — el GC estara en sitio"

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta=cta,
        )

        # Multi-canal para leads HOT (framing/rough_mep)
        if phase in ("framing", "rough_mep", "insulation"):
            notify_multichannel(lead, scoring)
