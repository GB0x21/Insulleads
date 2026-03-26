"""
agents/flood_agent.py
🌊 Alertas NOAA de Inundación — Bay Area
Agua/humedad = crawlspace insulation dañada
"""

import logging
import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)

# Zonas NOAA para Bay Area
# https://api.weather.gov/zones?type=forecast&area=CA
NOAA_ZONES = [
    {"zone": "CAZ006", "name": "San Francisco"},
    {"zone": "CAZ007", "name": "Alameda / Oakland"},
    {"zone": "CAZ508", "name": "Santa Clara Valley"},
    {"zone": "CAZ511", "name": "East Bay Interior"},
    {"zone": "CAZ013", "name": "Contra Costa"},
]

NOAA_BASE = "https://api.weather.gov/alerts/active"
FLOOD_EVENTS = {
    "Flood Warning", "Flood Watch", "Flash Flood Warning",
    "Flash Flood Watch", "Coastal Flood Warning", "Coastal Flood Advisory",
    "Flood Advisory",
}


class FloodAgent(BaseAgent):
    name      = "🌊 Alertas NOAA Inundación — Bay Area"
    emoji     = "🌊"
    agent_key = "flood"

    def fetch_leads(self) -> list:
        leads = []
        for zone_info in NOAA_ZONES:
            try:
                resp = requests.get(
                    NOAA_BASE,
                    params={"zone": zone_info["zone"]},
                    headers={"User-Agent": "InsulTechs-LeadBot/1.0 (contact@insultechs.com)"},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                features = data.get("features", [])

                for feat in features:
                    props = feat.get("properties", {})
                    event = props.get("event", "")
                    if event not in FLOOD_EVENTS:
                        continue
                    lead = {
                        "id":       feat.get("id", "").split("/")[-1],
                        "city":     zone_info["name"],
                        "event":    event,
                        "headline": props.get("headline", ""),
                        "severity": props.get("severity", ""),
                        "urgency":  props.get("urgency", ""),
                        "areas":    props.get("areaDesc", ""),
                        "onset":    (props.get("onset") or "")[:10],
                        "expires":  (props.get("expires") or "")[:10],
                        "url":      props.get("@id", ""),
                    }
                    leads.append(lead)
                logger.info(f"[Flood/{zone_info['name']}] {len(features)} alertas activas")
            except Exception as e:
                logger.debug(f"[Flood/{zone_info['name']}] {e}")
        return leads

    def notify(self, lead: dict):
        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=f"⚠️ {lead['event']} — {lead['city']}",
            fields={
                "📍 Zona":        lead.get("city"),
                "🚨 Alerta":      lead.get("event"),
                "📋 Resumen":     lead.get("headline"),
                "⚠️  Severidad":  lead.get("severity"),
                "⏰ Urgencia":    lead.get("urgency"),
                "📌 Área":        lead.get("areas"),
                "📅 Inicio":      lead.get("onset"),
                "📅 Expira":      lead.get("expires"),
            },
            url=lead.get("url"),
            cta="🌊 Inundación = crawlspace dañado. Prospecta en el área afectada.",
        )
