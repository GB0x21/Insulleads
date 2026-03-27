"""
agents/solar_agent.py  v6
━━━━━━━━━━━━━━━━━━━━━━━
☀️ Instalaciones Solares — Bay Area

FIXES v6:
  ✅ SJ — Migrado de dump completo (30+ minutos) a datastore_search
           con filtro de fecha + limit. Máx 200 registros.
  ✅ Timeout por fuente: ninguna puede bloquear más de SOURCE_TIMEOUT s
  ✅ Filtro solar aplicado server-side en SF (Socrata $where)
     y client-side en SJ después de descargar
"""

import os
import re
import logging
import requests

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT   = int(os.getenv("SOURCE_TIMEOUT", "45"))
MIN_PERMIT_VALUE = float(os.getenv("MIN_PERMIT_VALUE", "50000"))
PERMIT_MONTHS    = int(os.getenv("PERMIT_MONTHS", "3"))

SOLAR_KW = ["SOLAR", "PHOTOVOLTAIC", "PV SYSTEM", "PANEL SOLAR", "ROOFTOP PV"]


def _cutoff_ymd() -> str:
    from datetime import datetime, timedelta
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%d")

def _cutoff_iso() -> str:
    from datetime import datetime, timedelta
    return (datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%Y-%m-%dT00:00:00")

def _is_solar(rec: dict) -> bool:
    haystack = " ".join([
        str(rec.get("WORKDESCRIPTION") or ""),
        str(rec.get("FOLDERNAME")      or ""),
        str(rec.get("description")     or ""),
        str(rec.get("permit_type_definition") or ""),
    ]).upper()
    return any(kw in haystack for kw in SOLAR_KW)

def _parse_value(v) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", str(v) or "") or "0")
    except Exception:
        return 0.0


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
                "OR UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":"permit_number","address":"street_number","address2":"street_name",
            "desc":"description","status":"status","date":"filed_date",
            "contractor":"contractor_company_name","lic":"contractor_license",
            "owner":"owner","value":"estimated_cost",
        },
    },
    # ── San Jose — CKAN datastore_search con filtro fecha ─────────
    # ✅ FIX: usa datastore_search REST (no dump). Limit=200, sort por fecha.
    #    Filtro solar aplicado client-side con _is_solar().
    {
        "city":    "San Jose",
        "engine":  "ckan_search",
        "timeout": SOURCE_TIMEOUT,
        "url":     "https://data.sanjoseca.gov/api/3/action/datastore_search",
        "params": {
            "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904",
            "limit":       500,   # descargamos 500 y filtramos solar
            "sort":        "ISSUEDATE desc",
        },
        "field_map": {
            "id":"FOLDERNUMBER","address":"gx_location","address2":None,
            "desc":"WORKDESCRIPTION","status":"Status","date":"ISSUEDATE",
            "contractor":"CONTRACTOR","lic":None,
            "owner":"OWNERNAME","value":"PERMITVALUATION",
        },
        "_date_cutoff": None,   # se rellena en runtime
        "_date_field":  "ISSUEDATE",
    },
]


def _fetch_socrata(source: dict) -> list:
    cutoff_iso = _cutoff_iso()
    # Interpolar la fecha en el $where si tiene placeholder
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

    # Filtrar por fecha y keyword solar en una pasada
    result = []
    for r in records:
        date_val = (r.get(date_field) or "")[:10]
        if date_val and date_val < cutoff_ymd:
            continue
        if _is_solar(r):
            result.append(r)
    return result


class SolarAgent(BaseAgent):
    name      = "☀️ Instalaciones Solares — Bay Area"
    emoji     = "☀️"
    agent_key = "solar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        leads = []
        for src in SOLAR_SOURCES:
            try:
                engine = src.get("engine", "socrata")
                if engine == "ckan_search":
                    records = _fetch_ckan_search(src)
                else:
                    records = _fetch_socrata(src)

                fm  = src["field_map"]
                get = lambda r, k: r.get(fm.get(k) or "", "") or ""

                for raw in records:
                    addr = get(raw, "address")
                    if fm.get("address2") and raw.get(fm["address2"]):
                        addr = f"{addr} {raw[fm['address2']]}".strip()

                    val = _parse_value(get(raw, "value"))
                    lead = {
                        "id":          f"{src['city']}_{get(raw,'id')}",
                        "city":        src["city"],
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

                    match = lookup_contact(lead["contractor"], self._contacts)
                    if match:
                        lead["contact_phone"]  = match.get("phone", "")
                        lead["contact_email"]  = match.get("email", "")
                        lead["contact_source"] = f"CSV ({match['source']})"

                    leads.append(lead)

                logger.info(f"[Solar/{src['city']}] {len(records)} permisos solares")

            except Exception as e:
                logger.error(f"[Solar/{src['city']}] Error: {e}")

        return leads

    def notify(self, lead: dict):
        phone  = lead.get("contact_phone") or "No disponible"
        source = lead.get("contact_source", "")
        value  = lead.get("value_float", 0)
        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields={
                "📍 Ciudad":           lead.get("city"),
                "📝 Descripción":      (lead.get("description") or "")[:200],
                "📊 Estado":           lead.get("status"),
                "📅 Fecha":            lead.get("date"),
                "👷 Contratista (GC)": lead.get("contractor") or "—",
                "📞 Teléfono GC":      f"{phone}  _(via {source})_" if source else phone,
                "✉️  Email GC":        lead.get("contact_email") or "—",
                "👤 Propietario":      lead.get("owner") or "—",
                "💰 Valor":            f"${value:,.0f}" if value else "—",
            },
            cta="☀️ Solar nuevo = oportunidad de mejorar aislamiento. ¡Contáctalos!",
        )
