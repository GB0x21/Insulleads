"""
agents/rodents_agent.py
🐀 Reportes 311 Plagas y Roedores — Bay Area & Contra Costa
Objetivo Insulleads: Plagas = daño a insulación de ático/crawlspace = Oportunidad de venta
"""

import logging
import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)

# Palabras clave de búsqueda optimizadas y centralizadas (fauna destructora de áticos)
SEARCH_TERMS = "rat OR rodent OR mice OR mouse OR raccoon OR squirrel OR skunk OR pest"

# 1. Fuente única: San Francisco (Socrata API)
RODENT_SOURCES = [
    {
        "city": "San Francisco",
        "url":  "https://data.sfgov.org/resource/vw6y-z8j6.json",
        "params": {
            "$limit": 1000,
            "$order": "requested_datetime DESC",
            "$where": (
                "UPPER(service_name) LIKE '%RODENT%' OR "
                "UPPER(service_name) LIKE '%PEST%' OR "
                "UPPER(service_subtype) LIKE '%RAT%' OR "
                "UPPER(service_name) LIKE '%MICE%' OR "
                "UPPER(service_name) LIKE '%MOUSE%' OR "
                "UPPER(service_name) LIKE '%RACCOON%' OR "
                "UPPER(service_name) LIKE '%SQUIRREL%' OR "
                "UPPER(service_name) LIKE '%SKUNK%'"
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
    }
]

# 2. Fuentes SeeClickFix: Generación dinámica para múltiples ciudades
SEECLICKFIX_CITIES = [
    # East Bay (Alameda County)
    {"city": "Oakland", "place_url": "oakland"},
    {"city": "Berkeley", "place_url": "berkeley"},
    {"city": "Alameda", "place_url": "alameda"},
    
    # Contra Costa County
    {"city": "Antioch", "place_url": "antioch"},
    {"city": "Walnut Creek", "place_url": "walnut-creek"},
    {"city": "Concord", "place_url": "concord"},
    {"city": "Richmond", "place_url": "richmond"},
    
    # North Bay & Peninsula
    {"city": "Vallejo", "place_url": "vallejo"},
    {"city": "San Rafael", "place_url": "san-rafael"},
    {"city": "South San Francisco", "place_url": "south-san-francisco"},
    {"city": "San Mateo County", "place_url": "san-mateo-county"}
]

# Construimos los diccionarios para SeeClickFix automáticamente
for scf in SEECLICKFIX_CITIES:
    RODENT_SOURCES.append({
        "city": scf["city"],
        "url":  "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   scf["place_url"],
            "search":      SEARCH_TERMS,
            "per_page":    100,
            "sort":        "created_at",
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
    })

class RodentsAgent(BaseAgent):
    name      = "🐀 Reportes de Plagas/Roedores — Insulleads"
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
                logger.info(f"[Rodents/{src['city']}] {len(records)} reportes obtenidos")
            except Exception as e:
                # Si alguna ciudad cambia su URL o tiene una caída temporal, el script lo ignorará y continuará
                logger.debug(f"[Rodents/{src['city']}] Error o sin resultados: {e}")
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
            cta="🐀 Posible daño a insulación. ¡El cliente necesita inspección de ático/crawlspace!",
        )