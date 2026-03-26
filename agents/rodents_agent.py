"""
agents/rodents_agent.py
🐀 Reportes 311 Roedores — Bay Area
Fuentes: SF 311, Oakland SeeClickFix, SJ 311
Roedores = daño a insulación de ático/crawlspace
"""

import logging
import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)

RODENT_SOURCES = [
    {
        "city": "San Francisco",
        "url":  "https://data.sfgov.org/resource/vw6y-z8j6.json",
        "params": {
            "$limit": 30,
            "$order": "requested_datetime DESC",
            "$where": (
                "UPPER(service_name) LIKE '%RODENT%' OR "
                "UPPER(service_name) LIKE '%PEST%' OR "
                "UPPER(service_subtype) LIKE '%RAT%'"
            ),
        },
        "field_map": {
            "id":      "service_request_id",
            "address": "address",
            "desc":    "service_name",
            "status":  "status_description",
            "date":    "requested_datetime",
            "lat":     "lat",
            "lon":     "long",
        },
    },
    {
        "city": "Oakland",
        "url":  "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "oakland",
            "request_type": "Rats/Rodents",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
]


class RodentsAgent(BaseAgent):
    name      = "🐀 Reportes de Roedores — Bay Area"
    emoji     = "🐀"
    agent_key = "rodents"

    def fetch_leads(self) -> list:
        leads = []
        for src in RODENT_SOURCES:
            try:
                resp = requests.get(src["url"], params=src["params"], timeout=15,
                                    headers={"Accept": "application/json"})
                resp.raise_for_status()
                data = resp.json()

                # Algunos endpoints anidan los resultados
                root = src.get("_root")
                records = data.get(root, data) if root else data
                if not isinstance(records, list):
                    continue

                fm = src["field_map"]
                get = lambda r, k: r.get(fm.get(k) or "", "") or ""

                for raw in records:
                    lead = {
                        "id":      f"{src['city']}_{get(raw,'id')}",
                        "city":    src["city"],
                        "address": get(raw, "address"),
                        "desc":    get(raw, "desc"),
                        "status":  get(raw, "status"),
                        "date":    get(raw, "date")[:10] if get(raw, "date") else "",
                        "lat":     get(raw, "lat"),
                        "lon":     get(raw, "lon"),
                    }
                    leads.append(lead)
                logger.info(f"[Rodents/{src['city']}] {len(records)} reportes")
            except Exception as e:
                logger.debug(f"[Rodents/{src['city']}] {e}")
        return leads

    def notify(self, lead: dict):
        maps_url = (
            f"https://maps.google.com/?q={lead.get('lat')},{lead.get('lon')}"
            if lead.get("lat") and lead.get("lon") else None
        )
        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields={
                "📍 Ciudad":    lead.get("city"),
                "📝 Reporte":   lead.get("desc"),
                "📊 Estado":    lead.get("status"),
                "📅 Fecha":     lead.get("date"),
            },
            url=maps_url,
            cta="🐀 Roedores = insulación dañada. Contacta al propietario para inspección.",
        )
