"""
agents/solar_agent.py  v2
━━━━━━━━━━━━━━━━━━━━━━━
☀️ Instalaciones Solares — Bay Area
Fuentes: SF DataSF + San Jose CKAN
Solar nuevo = necesitan mejorar aislamiento para maximizar ahorro energético

✅ FIX v2: removido Oakland (dataset Socrata 404, migró a Accela)
"""

import logging
import requests

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

SOLAR_SOURCES = [
    # ── San Francisco — Socrata ───────────────────────────────────
    {
        "city":   "San Francisco",
        "engine": "socrata",
        "url":    "https://data.sfgov.org/resource/i98e-djp9.json",
        "params": {
            "$limit": 30,
            "$order": "filed_date DESC",
            "$where": (
                "status IN('issued','complete') AND "
                "(UPPER(description) LIKE '%SOLAR%' OR "
                " UPPER(description) LIKE '%PHOTOVOLTAIC%' OR "
                " UPPER(description) LIKE '%PV%')"
            ),
        },
        "field_map": {
            "id":         "permit_number",
            "address":    "street_number",
            "address2":   "street_name",
            "desc":       "description",
            "status":     "status",
            "date":       "filed_date",
            "contractor": "contractor_company_name",
            "lic":        "contractor_license",
            "owner":      "owner",
            "value":      "estimated_cost",
        },
    },
    # ── San Jose — CKAN JSON ──────────────────────────────────────
    {
        "city":   "San Jose",
        "engine": "ckan_json",
        "url":    "https://data.sanjoseca.gov/datastore/dump/761b7ae8-3be1-4ad6-923d-c7af6404a904",
        "params": {"format": "json"},
        "filter_fn": lambda r: any(kw in (r.get("WORKDESCRIPTION") or "").upper()
                                   for kw in ["SOLAR", "PHOTOVOLTAIC", "PV"]),
        "field_map": {
            "id":         "FOLDERNUMBER",
            "address":    "gx_location",
            "address2":   None,
            "desc":       "WORKDESCRIPTION",
            "status":     "Status",
            "date":       "ISSUEDATE",
            "contractor": "CONTRACTOR",
            "lic":        None,
            "owner":      "OWNERNAME",
            "value":      "PERMITVALUATION",
        },
    },
]


def _fetch_records(source: dict) -> list[dict]:
    """Descarga registros según el engine de la fuente."""
    resp = requests.get(source["url"], params=source["params"], timeout=30,
                        headers={"Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()

    if source["engine"] == "ckan_json":
        fields = [f["id"] for f in data.get("fields", [])]
        raw    = data.get("records", [])
        result = []
        for row in raw:
            rec = dict(zip(fields, row)) if isinstance(row, list) else row
            # Filtro solar inline si viene definido
            if source.get("filter_fn") and not source["filter_fn"](rec):
                continue
            result.append(rec)
        return result
    else:
        return data if isinstance(data, list) else []


class SolarAgent(BaseAgent):
    name      = "☀️ Instalaciones Solares — Bay Area"
    emoji     = "☀️"
    agent_key = "solar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()  # cache de módulo

    def fetch_leads(self) -> list:
        leads = []
        for src in SOLAR_SOURCES:
            try:
                records = _fetch_records(src)
                fm  = src["field_map"]
                get = lambda r, k: r.get(fm.get(k) or "", "") or ""

                for raw in records:
                    addr = get(raw, "address")
                    if fm.get("address2") and raw.get(fm["address2"]):
                        addr = f"{addr} {raw[fm['address2']]}".strip()

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
        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
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
                "💰 Valor":            f"${float(lead['value']):,.0f}" if lead.get("value") else "—",
            },
            cta="☀️ Solar nuevo = oportunidad de mejorar aislamiento. ¡Contáctalos!",
        )
