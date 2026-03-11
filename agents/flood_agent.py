"""
agents/flood_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 4 — ALERTAS POST-INUNDACIÓN (NOAA)

Fuentes:
  • NOAA Weather API — Alertas activas Bay Area
  • NWS (National Weather Service) — Flood warnings

Lógica:
  Tormenta / inundación en Bay Area → 
  Crawlspace insulation y vapor barrier se dañan.
  Lanzar campaña geolocal en las 48–72 horas post-evento.

Pitch: "¿Tuviste agua en el sótano o crawlspace? La humedad daña
la insulación y puede generar moho. Evaluación gratuita esta semana."

Zonas objetivo: condados de Bay Area
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import requests
from datetime import datetime
from agents.base import BaseAgent
from utils.telegram import send_lead

# NOAA Zone IDs para el Bay Area
# Docs: https://api.weather.gov/zones/forecast
BAY_AREA_ZONES = [
    "CAZ006",  # San Francisco / San Mateo
    "CAZ007",  # East Bay / Alameda / Contra Costa
    "CAZ008",  # Santa Clara Valley
    "CAZ512",  # North Bay Interior (Marin, Sonoma)
    "CAZ513",  # Napa / Lake
    "CAZ511",  # Bay Area coast
]

FLOOD_EVENT_TYPES = [
    "Flood", "Flash Flood", "Coastal Flood",
    "High Wind", "Winter Storm", "Excessive Rainfall",
    "Dense Fog",  # condensación → humedad → daño a insulación
]


class FloodAgent(BaseAgent):
    name      = "🌊 Alertas de Inundación NOAA"
    emoji     = "🌊"
    agent_key = "flood"

    def fetch_leads(self) -> list[dict]:
        leads = []
        leads += self._fetch_noaa_alerts()
        leads += self._fetch_past_events()
        return leads

    # ── Alertas activas NOAA ──────────────────────────────────────
    def _fetch_noaa_alerts(self) -> list[dict]:
        """
        NOAA Weather API — Alertas activas en Bay Area
        Docs: https://api.weather.gov/alerts/active
        """
        url = "https://api.weather.gov/alerts/active"
        params = {
            "area": "CA",
            "status": "actual",
        }
        headers = {"User-Agent": "InsulTechs-LeadAgent/1.0 (admin@insultechs.com)"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            features = resp.json().get("features", [])
        except Exception:
            return []

        leads = []
        for feat in features:
            props = feat.get("properties", {})
            event = props.get("event", "")

            # Solo eventos de inundación o lluvia
            if not any(et.lower() in event.lower() for et in FLOOD_EVENT_TYPES):
                continue

            # Solo Bay Area (por zona o área)
            zones = props.get("affectedZones", [])
            area_desc = props.get("areaDesc", "")
            
            is_bay_area = any(z.split("/")[-1] in BAY_AREA_ZONES for z in zones)
            if not is_bay_area:
                # Check por texto también
                bay_keywords = ["Francisco", "Oakland", "San Jose", "Marin",
                                "Alameda", "Contra Costa", "Santa Clara", "Napa", "Sonoma"]
                is_bay_area = any(kw in area_desc for kw in bay_keywords)

            if not is_bay_area:
                continue

            alert_id = props.get("id", feat.get("id", ""))

            leads.append({
                "id":          f"noaa_{alert_id}",
                "type":        "alert",
                "event":       event,
                "area":        area_desc,
                "headline":    props.get("headline", "")[:150],
                "description": props.get("description", "")[:200],
                "severity":    props.get("severity", "Unknown"),
                "certainty":   props.get("certainty", "Unknown"),
                "onset":       props.get("onset", "")[:16].replace("T", " "),
                "expires":     props.get("expires", "")[:16].replace("T", " "),
                "instruction": props.get("instruction", "")[:150],
                "sender":      props.get("senderName", "NOAA/NWS"),
            })
        return leads

    # ── Eventos pasados (últimas 72h) ─────────────────────────────
    def _fetch_past_events(self) -> list[dict]:
        """
        Busca eventos que YA OCURRIERON para hacer follow-up
        en los días posteriores (cuando el daño ya se manifestó).
        """
        url = "https://api.weather.gov/alerts"
        params = {
            "area": "CA",
            "status": "actual",
            "message_type": "cancel,update",  # alertas que ya cerraron
        }
        headers = {"User-Agent": "InsulTechs-LeadAgent/1.0 (admin@insultechs.com)"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            features = resp.json().get("features", [])
        except Exception:
            return []

        leads = []
        for feat in features:
            props = feat.get("properties", {})
            event = props.get("event", "")
            area_desc = props.get("areaDesc", "")

            if not any(et.lower() in event.lower() for et in ["Flood", "Flash Flood", "Excessive Rainfall"]):
                continue

            bay_keywords = ["Francisco", "Oakland", "San Jose", "Marin",
                            "Alameda", "Contra Costa", "Santa Clara", "Napa", "Sonoma"]
            if not any(kw in area_desc for kw in bay_keywords):
                continue

            alert_id = props.get("id", feat.get("id", ""))

            leads.append({
                "id":          f"noaa_past_{alert_id}",
                "type":        "past_event",
                "event":       f"[POST-EVENTO] {event}",
                "area":        area_desc,
                "headline":    props.get("headline", "")[:150],
                "description": "Evento de lluvia/inundación reciente — oportunidad de follow-up",
                "severity":    props.get("severity", "Unknown"),
                "certainty":   "Confirmed",
                "onset":       props.get("onset", "")[:16].replace("T", " "),
                "expires":     props.get("expires", "")[:16].replace("T", " "),
                "instruction": "Lanzar campaña de Meta Ads o contacto directo en la zona",
                "sender":      props.get("senderName", "NOAA/NWS"),
            })
        return leads

    # ── Telegram notify ───────────────────────────────────────────
    def notify(self, lead: dict):
        is_past = lead.get("type") == "past_event"
        
        if is_past:
            cta = (
                "📣 ACCIÓN INMEDIATA: Lanzar campaña de Meta Ads en estas zonas "
                "con el mensaje: '¿Tuviste agua en el crawlspace o sótano? "
                "La humedad destruye la insulación. Evaluación GRATUITA esta semana.' "
                "Llamar también a clientes anteriores en esas áreas."
            )
        else:
            cta = (
                "⚡ PRE-ALERTA: Prepara campaña de seguimiento. "
                "En 48-72 hrs post-tormenta, los propietarios detectarán daños. "
                "Sé el primero en aparecer con una solución."
            )

        send_lead(
            agent_name="Alerta Meteorológica — Bay Area",
            emoji="🌊",
            title=f"{lead['event']}",
            fields={
                "Zona Afectada":   lead["area"],
                "Titular":         lead["headline"],
                "Severidad":       lead["severity"],
                "Inicio":          lead["onset"],
                "Vence":           lead["expires"],
                "Fuente":          lead["sender"],
                "Descripción":     lead["description"],
            },
            cta=cta
        )
