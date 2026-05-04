"""
agents/permits_agent.py  v8
━━━━━━━━━━━━━━━━━━━━━━━━━━

ROOT CAUSE ANALYSIS v8:

  ❌ SF 400 Bad Request — DOS causas en $select:
     1. 'street_sfx' NO existe → campo real es 'street_suffix'
     2. 'contact_1_*' NO existen en i98e-djp9 → son un dataset separado
     FIX: Eliminar $select completamente (evitar el problema de raíz)
           Dejar que la API devuelva todos los campos.
           Mapear 'street_suffix' correctamente en field_map.

  ❌ SJ tarda exactamente 45s (= SOURCE_TIMEOUT) → el endpoint
     ckan_search tarda en responder con 200 registros sin filtro de fecha.
     FIX: Usar el endpoint SQL (datastore_search_sql) con WHERE en ISSUEDATE
          para filtrar server-side y recibir solo registros del período.
          Timeout separado de 30s para SJ (más agresivo).

  ❌ Datos de contacto SF ausentes:
     contractor_company_name existe pero frecuentemente vacío.
     Los datos reales de contacto están en dataset separado (kvek-u79k).
     FIX: Para GCs sin datos, enriquecer via CSLB usando el owner name
          como último recurso si no hay contractor.
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
from utils.inspection_schedule import get_next_visit_window, format_visit_info
from outreach.lead_quality import contact_quality_label, contact_quality_score

logger = logging.getLogger(__name__)

PARALLEL_CITIES  = int(os.getenv("PARALLEL_CITIES", "6"))
MIN_PERMIT_VALUE = float(os.getenv("MIN_PERMIT_VALUE", "50000"))
PERMIT_MONTHS    = int(os.getenv("PERMIT_MONTHS", "3"))
SOURCE_TIMEOUT   = int(os.getenv("SOURCE_TIMEOUT", "45"))
HTTP_RETRIES     = int(os.getenv("HTTP_RETRIES", "2"))

# Tier-5 (LLM web_search) is the most expensive enrichment step in the
# cascade — one Anthropic call per uncached contractor. Off-by-default
# would change behaviour silently; on-by-default but easy to disable.
_LLM_ENRICH_ENABLED = os.getenv(
    "PERMITS_LLM_ENRICH", "true"
).lower() in ("1", "true", "yes")


def _merge_source(current: str | None, new: str) -> str:
    """Append a backend tag to the human-readable contact_source string
    without duplicating it (e.g. "CSV (...) + CSLB + Hunter.io")."""
    if not current:
        return new
    if not new or new in current:
        return current
    return f"{current} + {new}"


class _LegacyLeadStub:
    """Duck-typed Lead-like object so the pydantic-ai LLM enricher
    (which expects a Django ``Lead``) can be invoked from the legacy
    dict-based agent path. Exposes only the attributes
    :func:`outreach.llm.prompts.lead_payload` and the adapter's
    ``logger.warning(... lead.pk ...)`` actually read."""

    __slots__ = (
        "pk", "title", "address", "city", "project_type", "project_value",
        "description", "contact_company", "contact_name", "contact_phone",
        "contact_email", "lead_score", "source",
    )

    def __init__(self, lead: dict, search_name: str, enrichment: dict):
        self.pk = lead.get("id") or 0
        addr_bits = [b for b in (lead.get("address"), lead.get("city")) if b]
        self.title = (
            lead.get("description") or lead.get("permit_type") or "permit"
        )
        self.address = lead.get("address", "") or ""
        self.city = lead.get("city", "") or ""
        self.project_type = lead.get("permit_type", "") or ""
        try:
            self.project_value = float(lead.get("value_float", 0) or 0)
        except (ValueError, TypeError):
            self.project_value = 0.0
        self.description = " · ".join(addr_bits) + (
            f" — {lead.get('description')}" if lead.get("description") else ""
        )
        self.contact_company = search_name
        self.contact_name = enrichment.get("contact_name", "")
        self.contact_phone = enrichment.get("contact_phone", "")
        self.contact_email = enrichment.get("contact_email", "")
        self.lead_score = 0
        self.source = type("S", (), {"kind": "permits"})()


def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%dT00:00:00")

def _cutoff_ymd() -> str:
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%d")


def _parse_value(v) -> float:
    if not v:
        return 0.0
    try:
        return float(re.sub(r"[^\d.]", "", str(v)) or "0")
    except Exception:
        return 0.0


def _build_sources() -> list:
    cutoff_iso = _cutoff_iso()
    cutoff_ymd = _cutoff_ymd()

    return [

        # ── San Francisco ─────────────────────────────────────────
        # ✅ FIX: Sin $select — evita el 400 por campos inválidos.
        #    Solo usamos $where y $order que son campos válidos confirmados.
        #    Campos reales del dataset: street_suffix (no street_sfx),
        #    contractor_company_name, contractor_license (cuando existen).
        {
            "city": "San Francisco", "engine": "socrata",
            "url":  "https://data.sfgov.org/resource/i98e-djp9.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200,
                "$order": "issued_date DESC",
                "$where": (
                    f"status IN('issued','complete') "
                    f"AND issued_date >= '{cutoff_iso}' "
                    f"AND permit_type_definition IN("
                    f"'additions alterations or repairs',"
                    f"'new construction wood frame',"
                    f"'otc additions',"
                    f"'accessory dwelling units',"
                    f"'new construction - wood frame')"
                ),
                # ✅ SIN $select — la API devuelve todos los campos disponibles
            },
            "field_map": {
                "id":          "permit_number",
                "address":     "street_number",
                "address_sfx": "street_number_suffix",  # ej: "1/2"
                "address2":    "street_name",
                "address_type":"street_suffix",          # ✅ FIX: "street_suffix" no "street_sfx"
                "permit_type": "permit_type_definition",
                "description": "description",
                "status":      "status",
                "filed_date":  "filed_date",
                "issued_date": "issued_date",
                "contractor":  "contractor_company_name",
                "lic_number":  "contractor_license",     # campo real en el dataset
                "owner":       "owner",
                "value":       "estimated_cost",
                "url_tpl":     "https://sfdbi.org/permit/{permit_number}",
                # ✅ Eliminados contact_1_* que no existen en i98e-djp9
            },
        },

        # ── San Jose — SQL server-side con filtro de fecha ────────
        # ✅ FIX: Usar datastore_search_sql con WHERE en ISSUEDATE
        #    para filtrar en el servidor y evitar descargar 1500+ registros.
        #    Timeout reducido a 30s (más agresivo que el global).
        {
            "city": "San Jose", "engine": "ckan_sql",
            "url":  "https://data.sanjoseca.gov/api/3/action/datastore_search_sql",
            "timeout": 30,
            "params": {
                "sql": (
                    f'SELECT "FOLDERNUMBER","gx_location","FOLDERNAME","WORKDESCRIPTION",'
                    f'"Status","ISSUEDATE","CONTRACTOR","OWNERNAME","PERMITVALUATION" '
                    f'FROM "761b7ae8-3be1-4ad6-923d-c7af6404a904" '
                    f'WHERE "ISSUEDATE" >= \'{cutoff_ymd}\' '
                    f'ORDER BY "ISSUEDATE" DESC '
                    f'LIMIT 200'
                )
            },
            "field_map": {
                "id":          "FOLDERNUMBER",
                "address":     "gx_location",
                "address2":    None,
                "permit_type": "FOLDERNAME",
                "description": "WORKDESCRIPTION",
                "status":      "Status",
                "filed_date":  None,
                "issued_date": "ISSUEDATE",
                "contractor":  "CONTRACTOR",
                "lic_number":  None,
                "owner":       "OWNERNAME",
                "value":       "PERMITVALUATION",
                "url_tpl":     "https://www.sjpermits.org/",
            },
        },

        # ── Sunnyvale ─────────────────────────────────────────────
        {
            "city": "Sunnyvale", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.sunnyvale.ca.gov/resource/7xm5-teup.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"permit_status='Issued' AND issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description",
                "status":"permit_status","filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license_number",
                "owner":"property_owner","value":"project_value",
                "url_tpl":"https://sunapps.sunnyvale.ca.gov/pds/",
            },
        },

        # ── Santa Clara ───────────────────────────────────────────
        {
            "city": "Santa Clara", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.santa-clara.ca.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issue_date DESC",
                "$where": f"status='Issued' AND issue_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issue_date",
                "contractor":"contractor","lic_number":"license_number",
                "owner":"owner","value":"value",
                "url_tpl":"https://www.santaclaraca.gov/government/departments/community-development/building-division",
            },
        },

        # ── Richmond ──────────────────────────────────────────────
        {
            "city": "Richmond", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.ci.richmond.ca.us/resource/permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "date_issued DESC",
                "$where": f"status='ISSUED' AND date_issued >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"site_address","address2":None,
                "permit_type":"permit_type","description":"work_description","status":"status",
                "filed_date":"application_date","issued_date":"date_issued",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner_name","value":"declared_valuation",
                "url_tpl":"https://www.ci.richmond.ca.us/1357/Building-Permits",
            },
        },

        # ── Fremont ───────────────────────────────────────────────
        {
            "city": "Fremont", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://www.fremont.gov/CivicAlerts.aspx",
            "timeout": SOURCE_TIMEOUT, "params": {},
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issue_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.fremont.gov/government/departments/building-services",
            },
        },

        # ── Hayward ───────────────────────────────────────────────
        {
            "city": "Hayward", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://hayward.permitportal.us/api/permits",
            "timeout": SOURCE_TIMEOUT,
            "params": {"status":"Issued","type":"Building","limit":200},
            "field_map": {
                "id":"permit_number","address":"location","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"applied","issued_date":"issued",
                "contractor":"contractor","lic_number":"lic_no",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://hayward.permitportal.us/permit/{permit_number}",
            },
        },

        # ── Oakland (Accela, sin API pública) ─────────────────────
        {
            "city": "Oakland", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.oaklandca.gov/resource/p8h7-gzqg.json",
            "timeout": 10, "params": {"$limit": 1},
            "field_map": {
                "id":"permit_number","address":"site_address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"applied_date","issued_date":"issue_date",
                "contractor":"primary_contractor","lic_number":"contractor_lic_number",
                "owner":"owner_name","value":"valuation",
                "url_tpl":"https://aca-prod.accela.com/OAKLAND/",
            },
        },

        # ── Berkeley (requiere Socrata token en .env) ─────────────
        {
            "city": "Berkeley", "engine": "socrata",
            "_skip_if_no_data": True, "_requires_token": True,
            "url": "https://data.cityofberkeley.info/resource/cqze-unm8.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "date_issued DESC",
                "$where": f"permit_status IN('ISSUED','FINALED') AND date_issued >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"location_address","address2":None,
                "permit_type":"permit_type","description":"permit_description","status":"permit_status",
                "filed_date":"date_filed","issued_date":"date_issued",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"property_owner","value":"project_valuation",
                "url_tpl":"https://permits.cityofberkeley.info/eTRAKiT/",
            },
        },

        # ── Palo Alto ─────────────────────────────────────────────
        {
            "city": "Palo Alto", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.cityofpaloalto.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"property_owner","value":"valuation",
                "url_tpl":"https://www.cityofpaloalto.org/Departments/Development-Services",
            },
        },

        # ── Mountain View ─────────────────────────────────────────
        {
            "city": "Mountain View", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.mountainview.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issue_date DESC",
                "$where": f"issue_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issue_date",
                "contractor":"contractor","lic_number":"license_number",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.mountainview.gov/depts/comdev/building/",
            },
        },

        # ── Redwood City ──────────────────────────────────────────
        {
            "city": "Redwood City", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.redwoodcity.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"applied_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner_name","value":"job_value",
                "url_tpl":"https://www.redwoodcity.org/departments/community-development-department/building-division",
            },
        },

        # ── Daly City ─────────────────────────────────────────────
        {
            "city": "Daly City", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.dalycity.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor","lic_number":"license_number",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.dalycity.org/405/Building-Division",
            },
        },

        # ── San Mateo ─────────────────────────────────────────────
        {
            "city": "San Mateo", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.cityofsanmateo.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.cityofsanmateo.org/3012/Building",
            },
        },

        # ── Concord ───────────────────────────────────────────────
        {
            "city": "Concord", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.concordca.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner_name","value":"valuation",
                "url_tpl":"https://www.cityofconcord.org/349/Building-Division",
            },
        },

        # ── Walnut Creek ──────────────────────────────────────────
        {
            "city": "Walnut Creek", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.walnut-creek.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor","lic_number":"license_number",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.walnut-creek.org/departments/community-development/building",
            },
        },

        # ══════════════════════════════════════════════════════════
        #  COUNTY-LEVEL PORTALS  (cover multiple cities each)
        # ══════════════════════════════════════════════════════════

        # ── Contra Costa County ───────────────────────────────────
        # Covers: Pleasant Hill, Martinez, Clayton, Pittsburg, Lafayette,
        #         Orinda, Antioch, Moraga, Alamo, Danville, Hercules,
        #         Pinole, Oakley, San Ramon, Brentwood, El Cerrito
        # (Walnut Creek, Concord, Richmond already have individual entries)
        {
            "city": "Contra Costa County (Pleasant Hill, Martinez, Clayton, Pittsburg, Lafayette, Orinda, Antioch, Moraga, Alamo, Danville, Hercules, Pinole, Oakley, San Ramon, Brentwood, El Cerrito)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.contracosta.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.contracosta.ca.gov/4839/Building-Inspection",
            },
        },

        # ── Alameda County ────────────────────────────────────────
        # Covers: Dublin, Alameda, San Leandro, Pleasanton, Livermore,
        #         Newark, Castro Valley, San Lorenzo, Emeryville, Albany, Union City
        # (Oakland, Berkeley, Fremont, Hayward already have individual entries)
        {
            "city": "Alameda County (Dublin, San Leandro, Pleasanton, Livermore, Newark, Castro Valley, San Lorenzo, Emeryville, Albany, Union City)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.acgov.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.acgov.org/building/",
            },
        },

        # ── Alameda (City) ────────────────────────────────────────
        {
            "city": "Alameda", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.alamedaca.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.alamedaca.gov/Departments/Building",
            },
        },

        # ── San Mateo County ──────────────────────────────────────
        # Covers: South San Francisco, San Bruno, Millbrae, Burlingame
        # (Daly City, San Mateo, Redwood City already have individual entries)
        {
            "city": "San Mateo County (South San Francisco, San Bruno, Millbrae, Burlingame)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.smcgov.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.smcgov.org/planning-building",
            },
        },

        # ── Solano County ─────────────────────────────────────────
        # Covers: Benicia, Fairfield, Suisun City, Rio Vista, Vacaville
        # (Vallejo has its own individual entry below)
        {
            "city": "Solano County (Benicia, Fairfield, Suisun City, Rio Vista, Vacaville)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.solanocounty.com/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.solanocounty.com/depts/resource_mgmt/building/",
            },
        },

        # ── Vallejo (City) ────────────────────────────────────────
        {
            "city": "Vallejo", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.cityofvallejo.net/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.cityofvallejo.net/city_hall/departments___divisions/building_division",
            },
        },

        # ── Marin County ─────────────────────────────────────────
        # Covers: Novato, San Rafael
        {
            "city": "Marin County (Novato, San Rafael)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.marincounty.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.marincounty.org/depts/cd/divisions/building-and-safety",
            },
        },

        # ── Napa County ──────────────────────────────────────────
        # Covers: Napa
        {
            "city": "Napa County (Napa)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.countyofnapa.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.countyofnapa.org/191/Building",
            },
        },

        # ── Sonoma County ─────────────────────────────────────────
        # Covers: Sonoma, Petaluma
        {
            "city": "Sonoma County (Sonoma, Petaluma)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.sonomacounty.ca.gov/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://sonomacounty.ca.gov/development-services/permit-sonoma",
            },
        },

        # ── San Joaquin County ────────────────────────────────────
        # Covers: Tracy, Stockton
        {
            "city": "San Joaquin County (Tracy, Stockton)",
            "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.sjgov.org/resource/building-permits.json",
            "timeout": SOURCE_TIMEOUT,
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": f"issued_date >= '{cutoff_iso}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.sjgov.org/department/cd/building/",
            },
        },
    ]


INSULATION_KEYWORDS = [
    "insulation","insulate","adu","accessory dwelling","addition","remodel",
    "renovation","attic","crawl","energy","retrofit","new construction",
    "garage conversion","dwelling","residential","hvac","weatherization",
]


# ─── Quality filters (anti-junk) ────────────────────────────────────
# These run after `_is_relevant` and drop leads that match a positive
# keyword by accident (e.g. "new construction" in a "PLAN A LOT N"
# planning-stage permit). Each pattern is matched as a substring against
# the lower-cased combined permit_type + description haystack.

# Permit type patterns that indicate the permit is NOT actual building
# work — enforcement actions, planning-only approvals, status checks,
# or scope-of-trade work that's outside our cross-sell.
_NON_CONSTRUCTION_PERMIT_TYPES = (
    "code investigation",
    "code compliance",
    "code enforcement",
    "complaint",
    "no entry",
    "courtesy inspection",
    "plan check only",
    "plan review only",
    "plan a (",        # San Jose "PLAN A (BEMP …) LOT N" — master plan only
    "plan b (",
    "plan c (",
    "site plan",
    "subdivision",
    "tentative map",
    "final map",
    "lot line",
    "fence",
    "sign permit",
    "pool only",
    "spa only",
    "driveway only",
    "landscape only",
    "tree removal",
    "demolition only",
)

# Description patterns that override an otherwise-matching keyword.
# Conservative — only matches when the description is essentially _just_
# the bad-pattern phrase, not when it's mentioned alongside real scope.
_NON_CONSTRUCTION_DESCRIPTIONS = (
    "code investigation",
    "code compliance",
    "code enforcement",
    "no entry",
    "no action",
    "investigation only",
)


# Inspection types that mean an enforcement / non-construction visit —
# advertising these as a "site visit window" is what surfaced the
# 4/10/2018 Code Investigation in the operator's Telegram.
_ENFORCEMENT_INSPECTION_TYPES = (
    "code investigation",
    "code compliance",
    "code enforcement",
    "complaint",
    "no entry",
    "courtesy",
    "no action",
)


def _is_enforcement_inspection(visit: dict) -> bool:
    itype = (visit.get("type") or "").lower()
    return any(p in itype for p in _ENFORCEMENT_INSPECTION_TYPES)


def _is_future_inspection(visit: dict) -> bool:
    """True iff `visit['best_visit']` parses to today-or-later. The
    helper produces strings like 'Wed 4/10/2018, 7:00 AM - 10:00 AM' —
    we extract the date token and reuse `_parse_date`."""
    txt = (visit.get("best_visit") or "").strip()
    if not txt:
        return False
    # Pull out the slashed-date token.
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", txt)
    if not m:
        # Helper sometimes serves an ISO timestamp directly.
        m = re.search(r"(\d{4}-\d{2}-\d{2})", txt)
        if not m:
            return False
    parsed = _parse_date(m.group(1))
    if not parsed:
        return False
    return parsed.date() >= datetime.utcnow().date()


def _is_quality_permit(lead: dict) -> tuple[bool, str]:
    """Return (passes, reason). Drops:

      - Enforcement permits (Code Investigation, complaints, …).
      - Planning-only / lot-approval permits (San Jose "PLAN A LOT N").
      - Out-of-trade scope (fences, signs, pool-only, landscape-only, …).

    The legacy `_is_relevant` is whitelist-only, so a lead whose
    description happens to contain "new construction" passes even when
    the permit_type is "PLAN A (BEMP 100%) LOT 16". This adds the
    blacklist."""
    permit_type = (lead.get("permit_type") or "").lower()
    description = (lead.get("description") or "").lower().strip()

    for pat in _NON_CONSTRUCTION_PERMIT_TYPES:
        if pat in permit_type:
            return False, f"permit_type matches '{pat}'"

    # Description-only check — only fires when the description IS the
    # bad pattern (length-bounded so "blah code compliance work" still
    # passes).
    if description and len(description) <= 60:
        for pat in _NON_CONSTRUCTION_DESCRIPTIONS:
            if pat in description:
                return False, f"description matches '{pat}'"

    return True, ""


# ─── CSLB fallback ──────────────────────────────────────────────────
_CSLB_URL = "https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
_CSLB_HDR = {"User-Agent": "Mozilla/5.0 (compatible; LeadBot/1.0)"}

def _cslb_lookup(license_number: str = None, company_name: str = None) -> dict:
    result = {}
    try:
        s = requests.Session()
        s.headers.update(_CSLB_HDR)
        r = s.get(_CSLB_URL, timeout=10)
        r.raise_for_status()
        hidden = {t.get("name",""):t.get("value","")
                  for t in BeautifulSoup(r.text,"html.parser").find_all("input",{"type":"hidden"})}
        if license_number and re.match(r"^\d{4,}$", str(license_number).strip()):
            val, typ = str(license_number).strip(), "License"
        elif company_name:
            val, typ = company_name.strip()[:50], "Business"
        else:
            return result
        payload = {**hidden,
                   "ctl00$ContentPlaceHolder1$RadioButtonList1": typ,
                   "ctl00$ContentPlaceHolder1$TextBox1": val,
                   "ctl00$ContentPlaceHolder1$Button1": "Submit"}
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
        logger.debug(f"CSLB error: {e}")
    return result


# ── Parsers ────────────────────────────────────────────────────────

def _http_get(url: str, *, params=None, headers=None, timeout: int = 30) -> requests.Response:
    """requests.get with exponential-backoff retry on transient failures.

    Retries on: Timeout, ConnectionError, and HTTP 429 (rate limit).
    All other HTTP errors propagate immediately.
    """
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(HTTP_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.debug("[http] 429 from %s — waiting %ds (attempt %d)", url, wait, attempt + 1)
                if attempt < HTTP_RETRIES:
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < HTTP_RETRIES:
                wait = 2 ** attempt
                logger.debug("[http] %s on %s — retrying in %ds", type(exc).__name__, url, wait)
                time.sleep(wait)
    raise last_exc


def _fetch_socrata(source: dict) -> list:
    headers = {"Accept": "application/json"}
    token = os.getenv("SOCRATA_APP_TOKEN", "")
    if token:
        headers["X-App-Token"] = token
    resp = _http_get(source["url"], params=source["params"],
                     timeout=source.get("timeout", 30), headers=headers)
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_ckan_sql(source: dict) -> list:
    """CKAN datastore_search_sql — filtra server-side con WHERE."""
    resp = _http_get(
        source["url"],
        params=source["params"],
        timeout=source.get("timeout", 30),
        headers={"Accept": "application/json"},
    )
    data = resp.json()
    if not data.get("success"):
        err = data.get("error", {})
        raise ValueError(f"CKAN SQL error: {err}")
    return data.get("result", {}).get("records", [])


def _fetch_source(source: dict) -> tuple:
    city = source["city"]
    try:
        engine = source.get("engine", "socrata")
        if engine == "ckan_sql":
            records = _fetch_ckan_sql(source)
        else:
            records = _fetch_socrata(source)
        return (city, records, None)
    except Exception as e:
        return (city, [], e)


# ── Normalización ──────────────────────────────────────────────────

def _normalize_permit(raw: dict, field_map: dict, city: str) -> dict:
    """
    Construye el lead normalizado.
    Para SF construye dirección completa con street_suffix.
    """
    get = lambda k: raw.get(field_map.get(k, "") or "", "") or ""

    # Dirección
    parts = [get("address").strip()]
    if field_map.get("address_sfx"):
        sfx = raw.get(field_map.get("address_sfx","") or "", "")
        if sfx:
            parts.append(str(sfx).strip())
    if field_map.get("address2") and raw.get(field_map.get("address2") or ""):
        parts.append(raw[field_map["address2"]].strip())
    if field_map.get("address_type"):
        atype = raw.get(field_map.get("address_type","") or "", "")
        if atype:
            parts.append(str(atype).strip())
    address = " ".join(p for p in parts if p).strip()

    permit_id = get("id")
    raw_vals  = {v: raw.get(v, "") for k, v in field_map.items()
                 if v and k != "url_tpl" and not k.startswith("address")}
    try:
        permit_url = field_map.get("url_tpl", "").format(**raw_vals)
    except KeyError:
        permit_url = field_map.get("url_tpl", "")

    return {
        "id":          f"{city}_{permit_id}",
        "city":        city,
        "address":     address,
        "permit_type": get("permit_type"),
        "description": get("description"),
        "status":      get("status"),
        "filed_date":  get("filed_date")[:10] if get("filed_date") else "",
        "issued_date": get("issued_date")[:10] if get("issued_date") else "",
        "contractor":  get("contractor").strip(),
        "lic_number":  get("lic_number").strip(),
        "owner":       get("owner").strip(),
        "value":       get("value"),
        "value_float": _parse_value(get("value")),
        "permit_url":  permit_url,
    }


def _is_relevant(lead: dict) -> bool:
    if lead["value_float"] < MIN_PERMIT_VALUE:
        return False
    haystack = ((lead.get("description") or "") + " " + (lead.get("permit_type") or "")).lower()
    return any(kw in haystack for kw in INSULATION_KEYWORDS)


def _is_recent(lead: dict) -> bool:
    """True iff issued_date is parseable AND within PERMIT_MONTHS.

    We try several formats because the upstream agencies serve dates in
    different shapes (ISO, US slashed, with/without a time component).
    Unparseable dates return False — pre-v9 we returned True on parse
    failure, which let stale leads sneak through any time the source
    served a non-ISO format."""
    date_str = (lead.get("issued_date") or lead.get("filed_date") or "").strip()
    if not date_str:
        return False
    issued = _parse_date(date_str)
    if not issued:
        return False
    return issued >= (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS))


def _parse_date(text: str) -> datetime | None:
    """Best-effort multi-format date parser. Tries ISO (with/without
    time), US-slashed (`m/d/Y` or `m/d/y`), and ISO-with-T."""
    if not text:
        return None
    text = text.strip()
    # ISO timestamp — keep first 10 chars.
    candidates = (text[:10], text[:19], text)
    fmts = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y", "%-m/%-d/%Y", "%m/%d/%y", "%-m/%-d/%y")
    for cand in candidates:
        for fmt in fmts:
            try:
                return datetime.strptime(cand, fmt)
            except (ValueError, TypeError):
                continue
    return None


# ── AGENTE ─────────────────────────────────────────────────────────

class PermitsAgent(BaseAgent):
    name      = "🏗️ Permisos de Construcción — Bay Area"
    emoji     = "🏗️"
    agent_key = "permits"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts   = load_all_contacts()
        self._cslb_cache = {}

    def _enrich_gc(self, lead: dict) -> dict:
        """Five-tier fill-missing enrichment cascade for the GC contact.

        Each tier is consulted in order and *adds the fields it can find
        without overwriting fields already filled by an earlier tier*.
        Pre-v9 the cascade short-circuited at the first phone/email hit,
        which caused the CSLB / external-API / LLM tiers to be skipped
        the moment the local CSV produced a phone — losing license
        numbers, emails and websites along the way.

        Tiers:
          1. Local CSV (``utils.contacts_loader.lookup_contact``) — fast,
             free; gives phone / email / canonical name.
          2. CSLB (``_cslb_lookup``) — by license, then company, then
             owner; recovers license number, contractor city, and
             license status, plus a phone fallback.
          3. Hunter.io (``utils.contact_enrichment``) — best-effort
             email finder; needs ``HUNTER_API_KEY``.
          4. Apollo.io (same module) — decision-maker email + name;
             needs ``APOLLO_API_KEY``.
          5. LLM web_search (``outreach.llm.get_adapter().enrich_contact``)
             — last-resort fallback for whatever's still missing
             (website, decision-maker name, license number). Only fires
             when ``adapter.name != "noop"`` and at least one of phone /
             email / website is still missing. Disable with
             ``PERMITS_LLM_ENRICH=false``."""
        contractor = lead.get("contractor", "").strip()
        lic        = lead.get("lic_number", "").strip()
        owner      = lead.get("owner", "").strip()

        search_name = contractor or owner
        cache_key   = lic or search_name
        if not cache_key:
            return {}
        if cache_key in self._cslb_cache:
            return self._cslb_cache[cache_key]

        enrichment: dict = {}

        # ── Tier 1: CSV local ────────────────────────────────────────
        if search_name:
            match = lookup_contact(search_name, self._contacts)
            if match:
                if match.get("phone"):
                    enrichment["contact_phone"] = match["phone"]
                if match.get("email"):
                    enrichment["contact_email"] = match["email"]
                enrichment["contact_name"]   = match["raw_name"]
                enrichment["contact_source"] = f"CSV ({match['source']})"

        # ── Tier 2: CSLB — always tried so we don't lose license / city
        # / status even when CSV already gave us a phone. We only skip
        # when there's literally nothing to look up. The bulk index
        # (utils.cslb_bulk) is consulted first; the legacy scraper is
        # the fallback for licenses missing from the dump.
        if (lic or contractor or owner) and not enrichment.get("cslb_status"):
            from utils import cslb_bulk

            cslb = {}
            if lic:
                cslb = cslb_bulk.lookup(license_number=lic)
            if not cslb.get("phone") and contractor:
                cslb = cslb_bulk.lookup(company_name=contractor)
            if not cslb.get("phone") and owner and owner != contractor:
                cslb = cslb_bulk.lookup(company_name=owner)
            # Fallback to the scraper only when the bulk index isn't
            # loaded or doesn't have this contractor.
            if not cslb:
                time.sleep(0.3)
                if lic:
                    cslb = _cslb_lookup(license_number=lic)
                if not cslb.get("phone") and contractor:
                    cslb = _cslb_lookup(company_name=contractor)
                if not cslb.get("phone") and owner and owner != contractor:
                    cslb = _cslb_lookup(company_name=owner)
            if cslb:
                if cslb.get("phone") and not enrichment.get("contact_phone"):
                    enrichment["contact_phone"] = cslb["phone"]
                if not enrichment.get("contact_name"):
                    enrichment["contact_name"] = cslb.get("cslb_name", search_name)
                enrichment["cslb_city"]   = cslb.get("cslb_city", "")
                enrichment["cslb_status"] = cslb.get("cslb_status", "")
                enrichment["contact_source"] = _merge_source(
                    enrichment.get("contact_source"), "CSLB"
                )

        # ── Tier 3+4: Hunter.io / Apollo.io ──────────────────────────
        # Only when email is still missing AND the relevant API key is
        # configured (the helper short-circuits internally if not).
        if not enrichment.get("contact_email") and search_name:
            try:
                from utils.contact_enrichment import enrich_contact as ext_enrich

                ext = ext_enrich(
                    company_name=search_name, person_name=contractor
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[Hunter/Apollo] error: {exc}")
                ext = {}
            if ext:
                if ext.get("email") and not enrichment.get("contact_email"):
                    enrichment["contact_email"] = ext["email"]
                if ext.get("phone") and not enrichment.get("contact_phone"):
                    enrichment["contact_phone"] = ext["phone"]
                if ext.get("position"):
                    enrichment["position"] = ext["position"]
                if ext.get("linkedin_url"):
                    enrichment["linkedin"] = ext["linkedin_url"]
                if ext.get("source"):
                    enrichment["contact_source"] = _merge_source(
                        enrichment.get("contact_source"), ext["source"]
                    )

        # ── Tier 5: LLM web_search (outreach/llm/) ────────────────────
        # Skipped when the adapter is noop (no API key), when the
        # operator opted out via PERMITS_LLM_ENRICH=false, or when the
        # critical fields are already filled.
        if _LLM_ENRICH_ENABLED and search_name:
            still_missing = (
                not enrichment.get("contact_email")
                or not enrichment.get("contact_phone")
                or not enrichment.get("website")
            )
            if still_missing:
                try:
                    from outreach.llm import get_adapter

                    adapter = get_adapter()
                    if adapter.name != "noop":
                        stub = _LegacyLeadStub(lead, search_name, enrichment)
                        payload = adapter.enrich_contact(stub) or {}
                        for key in (
                            "contact_phone",
                            "contact_email",
                            "website",
                            "contact_name",
                            "cslb_license",
                            "linkedin",
                        ):
                            val = (payload.get(key) or "").strip()
                            if val and not enrichment.get(key):
                                enrichment[key] = val
                        # If the LLM filled the license number AND the
                        # original lead row didn't have one, surface it
                        # so the notify card shows it.
                        if payload.get("cslb_license") and not lic:
                            enrichment["lic_number"] = payload["cslb_license"]
                        if payload:
                            enrichment["contact_source"] = _merge_source(
                                enrichment.get("contact_source"),
                                f"llm:{adapter.name}",
                            )
                except Exception as exc:  # noqa: BLE001
                    logger.debug(f"[LLM enricher] error: {exc}")

        self._cslb_cache[cache_key] = enrichment
        return enrichment

    def fetch_leads(self) -> list:
        all_leads = []
        sources   = _build_sources()
        active    = [s for s in sources
                     if not (s.get("_requires_token") and not os.getenv("SOCRATA_APP_TOKEN"))]

        with ThreadPoolExecutor(max_workers=PARALLEL_CITIES) as executor:
            futures = {executor.submit(_fetch_source, s): s for s in active}
            for fut in as_completed(futures):
                source       = futures[fut]
                city         = source["city"]
                skip_on_fail = source.get("_skip_if_no_data", False)
                _, records, error = fut.result()

                if error:
                    (logger.debug if skip_on_fail else logger.error)(
                        f"[{city}] {'Omitido' if skip_on_fail else 'Error'}: {error}"
                    )
                    continue

                city_n = 0
                rejected_quality = 0
                rejected_stale = 0
                for raw in records:
                    lead = _normalize_permit(raw, source["field_map"], city)
                    if not _is_relevant(lead):
                        continue
                    if not _is_recent(lead):
                        rejected_stale += 1
                        continue
                    quality_ok, reason = _is_quality_permit(lead)
                    if not quality_ok:
                        rejected_quality += 1
                        logger.debug("[%s] dropped %s: %s",
                                     city, lead.get("id"), reason)
                        continue
                    lead.update(self._enrich_gc(lead))
                    all_leads.append(lead)
                    city_n += 1

                logger.info(
                    f"[{city}] {len(records)} registros → "
                    f"{city_n} leads (>${MIN_PERMIT_VALUE/1000:.0f}K, "
                    f"últimos {PERMIT_MONTHS} meses; "
                    f"dropped: {rejected_stale} stale, "
                    f"{rejected_quality} non-construction)"
                )

        # Send the most-reachable leads first. Within equal contact
        # quality, fall back to project value so big jobs still bubble
        # up. Operator's Telegram limit / digest mode then trims.
        all_leads.sort(key=lambda l: (
            -contact_quality_score(l),
            -float(l.get("value_float") or 0),
        ))
        return all_leads

    def notify(self, lead: dict):
        phone  = lead.get("contact_phone") or "—"
        email  = lead.get("contact_email") or "—"
        source = lead.get("contact_source", "")
        value  = lead.get("value_float", 0)

        fields = {
            "📍 Ciudad":           lead.get("city"),
            "🔖 Tipo de Permiso":  lead.get("permit_type"),
            "📝 Descripción":      (lead.get("description") or "—")[:200],
            "📅 Fecha Emisión":    lead.get("issued_date"),
            "💰 Valor Estimado":   f"${value:,.0f}" if value else "—",
            "👷 Contratista (GC)": lead.get("contractor") or "—",
            "🪪 Licencia CSLB":    lead.get("lic_number") or "—",
            "📞 Teléfono GC":      f"{phone}  _(via {source})_" if source and phone != "—" else phone,
            "✉️  Email GC":        email,
            "👤 Propietario":      lead.get("owner") or "—",
        }
        # Surface anything filled by the new tier-3/4/5 enrichment so
        # the operator sees the new info on Telegram. Every line is
        # conditional so leads that *do* still come back partial don't
        # show "—" rows.
        if lead.get("website"):
            fields["🌐 Website"] = lead["website"]
        if lead.get("position"):
            fields["💼 Cargo"] = lead["position"]
        if lead.get("linkedin"):
            fields["🔗 LinkedIn"] = lead["linkedin"]
        # Additional contacts from the enrichment cascade
        extra = lead.get("additional_contacts") or (
            lead.get("enrichment_log") or {}
        ).get("additional_contacts") or []
        for i, c in enumerate(extra[:4], start=2):
            name = c.get("name") or ""
            pos = c.get("position") or ""
            label = f"{name} ({pos})" if pos else (name or f"Contacto {i}")
            parts = []
            if c.get("phone"):
                parts.append(f"📞 {c['phone']}")
            if c.get("email"):
                parts.append(f"✉️ {c['email']}")
            if c.get("linkedin_url"):
                parts.append(f"🔗 {c['linkedin_url']}")
            if parts:
                fields[f"👤 {label}"] = "  ".join(parts)
        # Reachability score — how many independent channels we have
        # to reach this GC (phone / email / website / linkedin).
        fields["📊 Calidad de contacto"] = contact_quality_label(
            contact_quality_score(lead)
        )
        if lead.get("contact_source") == "CSLB":
            if lead.get("cslb_city"):
                fields["🏢 Ciudad GC (CSLB)"] = lead["cslb_city"]
            if lead.get("cslb_status"):
                fields["✅ Estado Licencia"]   = lead["cslb_status"]

        # Lookup inspection schedule for visit timing. Drop the block if
        # the helper returned a stale (in-the-past) or unparseable date,
        # AND drop blocks whose `type` field reads as enforcement —
        # otherwise we end up advertising "🔍 Code Investigation" as a
        # site-visit window, which is exactly the junk lead the operator
        # sees on Telegram.
        visit = get_next_visit_window(lead)
        visit_is_useful = bool(visit) and _is_future_inspection(visit) \
            and not _is_enforcement_inspection(visit)
        if visit_is_useful:
            lead["_next_inspection"] = visit
            fields["🗓️ Proxima Inspeccion"] = visit.get("best_visit", "")
            if visit.get("type"):
                fields["🔍 Tipo Inspeccion"] = visit["type"]

        cta = "📲 Contacta al GC y ofrece insulación para el proyecto"
        if visit_is_useful:
            cta = f"📲 Visita la obra: {visit.get('best_visit', '')} — el GC estara en sitio"

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields, url=lead.get("permit_url"),
            cta=cta,
        )
