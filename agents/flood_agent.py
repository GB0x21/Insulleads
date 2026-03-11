"""
agents/flood_agent.py  v2.0
Alertas NOAA + historial 3 meses + enriquecimiento de contacto
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import geocode_address, contact_score_label

BAY_AREA_ZONES = ["CAZ006","CAZ007","CAZ008","CAZ512","CAZ513","CAZ511"]
BAY_KEYWORDS   = ["Francisco","Oakland","San Jose","Marin","Alameda",
                   "Contra Costa","Santa Clara","Napa","Sonoma","Vallejo",
                   "Concord","Richmond","Livermore","Walnut Creek"]
FLOOD_EVENTS   = ["Flood","Flash Flood","Coastal Flood","High Wind",
                   "Winter Storm","Excessive Rainfall","Dense Fog","Debris Flow"]
LOOKBACK_DAYS  = 90


class FloodAgent(BaseAgent):
    name      = "Alertas de Inundacion NOAA"
    emoji     = "🌊"
    agent_key = "flood"

    def fetch_leads(self) -> list:
        leads = []
        leads += self._fetch_noaa_active()
        leads += self._fetch_noaa_recent_closed()
        return leads

    def _fetch_noaa_active(self) -> list:
        url = "https://api.weather.gov/alerts/active"
        params = {"area": "CA", "status": "actual"}
        headers = {"User-Agent": "InsulTechs-LeadAgent/1.0 (admin@insultechs.com)"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            features = resp.json().get("features", [])
        except Exception:
            return []

        leads = []
        for feat in features:
            props     = feat.get("properties", {})
            event     = props.get("event", "")
            area_desc = props.get("areaDesc", "")

            if not any(et.lower() in event.lower() for et in FLOOD_EVENTS):
                continue

            zones     = props.get("affectedZones", [])
            is_bay    = any(z.split("/")[-1] in BAY_AREA_ZONES for z in zones)
            if not is_bay:
                is_bay = any(kw in area_desc for kw in BAY_KEYWORDS)
            if not is_bay:
                continue

            alert_id = props.get("id", feat.get("id", ""))
            # Geocodificar zona afectada
            geo = geocode_address(area_desc.split(";")[0][:40], "CA") if area_desc else {}

            lead = {
                "id":          "noaa_" + str(alert_id),
                "type":        "active",
                "event":       event,
                "area":        area_desc,
                "headline":    (props.get("headline", "") or "")[:150],
                "description": (props.get("description", "") or "")[:200],
                "severity":    props.get("severity", "Unknown"),
                "certainty":   props.get("certainty", "Unknown"),
                "onset":       (props.get("onset", "") or "")[:16].replace("T", " "),
                "expires":     (props.get("expires", "") or "")[:16].replace("T", " "),
                "sender":      props.get("senderName", "NOAA/NWS"),
                "maps_url":    geo.get("maps_url", ""),
                "contact_score": 1 if geo.get("maps_url") else 0,
            }
            leads.append(lead)
        return leads

    def _fetch_noaa_recent_closed(self) -> list:
        """Eventos cerrados recientes (follow-up post-tormenta)."""
        url = "https://api.weather.gov/alerts"
        params = {"area": "CA", "status": "actual", "message_type": "cancel,update"}
        headers = {"User-Agent": "InsulTechs-LeadAgent/1.0 (admin@insultechs.com)"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            features = resp.json().get("features", [])
        except Exception:
            return []

        leads = []
        for feat in features:
            props     = feat.get("properties", {})
            event     = props.get("event", "")
            area_desc = props.get("areaDesc", "")

            if not any(et.lower() in event.lower() for et in ["Flood","Flash Flood","Excessive Rainfall"]):
                continue
            if not any(kw in area_desc for kw in BAY_KEYWORDS):
                continue

            alert_id = props.get("id", feat.get("id", ""))
            geo = geocode_address(area_desc.split(";")[0][:40], "CA") if area_desc else {}

            lead = {
                "id":          "noaa_past_" + str(alert_id),
                "type":        "past_event",
                "event":       "[POST-EVENTO] " + event,
                "area":        area_desc,
                "headline":    (props.get("headline", "") or "")[:150],
                "description": "Evento de lluvia/inundacion reciente — momento ideal para contactar propietarios en zona afectada",
                "severity":    props.get("severity", "Unknown"),
                "certainty":   "Confirmado",
                "onset":       (props.get("onset", "") or "")[:16].replace("T", " "),
                "expires":     (props.get("expires", "") or "")[:16].replace("T", " "),
                "sender":      props.get("senderName", "NOAA/NWS"),
                "maps_url":    geo.get("maps_url", ""),
                "contact_score": 1 if geo.get("maps_url") else 0,
            }
            leads.append(lead)
        return leads

    def notify(self, lead: dict):
        is_past = lead.get("type") == "past_event"
        if is_past:
            cta = ("ACCION INMEDIATA: Lanzar Meta Ads en esta zona: "
                   "'Tuviste agua en el crawlspace? La humedad destruye la insulacion. "
                   "Evaluacion GRATUITA esta semana.' "
                   "Tambien llamar a clientes anteriores en estas areas.")
        else:
            cta = ("PRE-ALERTA: Prepara campana de seguimiento. "
                   "En 48-72 hrs post-tormenta los propietarios detectan danos. "
                   "Se el primero en aparecer con solucion.")

        send_lead(
            agent_name="Alerta Meteorologica Bay Area",
            emoji="🌊",
            title=lead["event"],
            fields={
                "Zona Afectada":  lead["area"],
                "Titular":        lead["headline"],
                "Severidad":      lead["severity"],
                "Certeza":        lead["certainty"],
                "Inicio":         lead["onset"],
                "Vence":          lead["expires"],
                "Fuente":         lead["sender"],
                "Ver Zona Maps":  lead.get("maps_url") or "No disponible",
            },
            cta=cta
        )
