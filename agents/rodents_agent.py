"""
agents/rodents_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 3 — REPORTES 311 DE ROEDORES / PLAGAS

Fuentes:
  • SF 311 Open Data (DataSF)
  • Oakland 311 (Seeclickfix API)
  • San Jose 311

Lógica:
  Propietario reporta roedores en la ciudad → 
  Necesita attic cleaning + re-insulation + rodent proofing.
  Es el servicio de mayor ticket de Insul-Techs.

Pitch: "Vimos que hay una solicitud activa por plagas en tu zona.
Ofrecemos limpieza de ático, eliminación de insulation dañada e
instalación nueva. Garantizamos el trabajo."
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead

RODENT_KEYWORDS = [
    "rodent", "rat", "mice", "mouse", "vermin", "pest",
    "infestation", "roedor", "raton", "rata", "plaga"
]


class RodentsAgent(BaseAgent):
    name      = "🐀 Reportes 311 Roedores"
    emoji     = "🐀"
    agent_key = "rodents"

    def fetch_leads(self) -> list[dict]:
        leads = []
        leads += self._fetch_sf_311()
        leads += self._fetch_oakland_311()
        leads += self._fetch_sj_311()
        return leads

    # ── San Francisco 311 ─────────────────────────────────────────
    def _fetch_sf_311(self) -> list[dict]:
        """
        API: SF 311 Cases (DataSF)
        Docs: https://data.sfgov.org/resource/vw6y-z8j6.json
        Categories: Pest Control, Rodent Complaints
        """
        since = (datetime.now() - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/vw6y-z8j6.json"
        params = {
            "$limit": 100,
            "$where": (
                f"requested_datetime >= '{since}' AND "
                f"(UPPER(service_name) LIKE '%RODENT%' OR "
                f" UPPER(service_name) LIKE '%PEST%' OR "
                f" UPPER(service_subtype) LIKE '%RODENT%')"
            ),
            "$order": "requested_datetime DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            address = item.get("address", "No indicada")
            # Filtrar genéricos sin dirección
            if not address or address in ("", "N/A"):
                continue

            leads.append({
                "id":           f"sf311_{item.get('service_request_id', item.get('case_id', ''))}",
                "city":         "San Francisco",
                "address":      address.title(),
                "category":     item.get("service_name", "Roedores/Plagas"),
                "subcategory":  item.get("service_subtype", ""),
                "description":  item.get("service_details", "Sin detalle")[:120],
                "reported_date":item.get("requested_datetime", "")[:10],
                "status":       item.get("status_description", "Abierto"),
                "neighborhood": item.get("neighborhoods_sffind_boundaries", ""),
                "lat":          item.get("lat", ""),
                "lon":          item.get("long", ""),
                "source_url":   f"https://sf311.org/requests/{item.get('service_request_id', '')}",
            })
        return leads

    # ── Oakland 311 ───────────────────────────────────────────────
    def _fetch_oakland_311(self) -> list[dict]:
        """
        Oakland usa SeeClickFix API (pública).
        Docs: https://seeclickfix.com/open311/v2
        """
        since = (datetime.now() - timedelta(hours=72)).isoformat()
        url = "https://seeclickfix.com/open311/v2/requests.json"
        params = {
            "jurisdiction_id": "seeclickfix.com",
            "lat": 37.8044,
            "long": -122.2711,
            "radius": 25000,   # metros (~15 millas)
            "service_code": "pest_control",
            "requested_datetime_start": since,
            "page_size": 100,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            summary = (item.get("summary", "") or "").lower()
            if not any(kw in desc or kw in summary for kw in RODENT_KEYWORDS):
                continue

            leads.append({
                "id":            f"oak311_{item.get('service_request_id', '')}",
                "city":          "Oakland",
                "address":       item.get("address", "No indicada").title(),
                "category":      item.get("service_name", "Control de Plagas"),
                "subcategory":   item.get("agency_responsible", ""),
                "description":   item.get("description", "Sin detalle")[:120],
                "reported_date": item.get("requested_datetime", "")[:10],
                "status":        item.get("status", "Abierto"),
                "neighborhood":  item.get("ward", ""),
                "lat":           item.get("lat", ""),
                "lon":           item.get("long", ""),
                "source_url":    item.get("media_url", "https://seeclickfix.com"),
            })
        return leads

    # ── San José 311 ─────────────────────────────────────────────
    def _fetch_sj_311(self) -> list[dict]:
        """
        San Jose usa el mismo Open311 estándar.
        """
        since = (datetime.now() - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://311.sanjoseca.gov/open311/v2/requests.json"
        params = {
            "start_date":  since,
            "page_size":   100,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            service = (item.get("service_name", "") or "").lower()
            desc = (item.get("description", "") or "").lower()
            if not any(kw in service or kw in desc for kw in RODENT_KEYWORDS):
                continue

            leads.append({
                "id":            f"sj311_{item.get('service_request_id', '')}",
                "city":          "San José",
                "address":       item.get("address", "No indicada").title(),
                "category":      item.get("service_name", "Control de Plagas"),
                "subcategory":   "",
                "description":   item.get("description", "Sin detalle")[:120],
                "reported_date": item.get("requested_datetime", "")[:10],
                "status":        item.get("status", "Abierto"),
                "neighborhood":  item.get("ward", ""),
                "lat":           item.get("lat", ""),
                "lon":           item.get("long", ""),
                "source_url":    "https://311.sanjoseca.gov",
            })
        return leads

    # ── Telegram notify ───────────────────────────────────────────
    def notify(self, lead: dict):
        maps_link = ""
        if lead.get("lat") and lead.get("lon"):
            maps_link = f"https://maps.google.com/?q={lead['lat']},{lead['lon']}"

        send_lead(
            agent_name="Reporte 311 — Roedores/Plagas",
            emoji="🐀",
            title=f"{lead['city']} — {lead['address']}",
            fields={
                "Tipo de Reporte":  lead["category"],
                "Descripción":      lead["description"],
                "Barrio":           lead.get("neighborhood") or "No indicado",
                "Fecha Reporte":    lead["reported_date"],
                "Estado":           lead["status"],
                "Ver en Maps":      maps_link or "No disponible",
                "Reporte Oficial":  lead["source_url"],
            },
            cta=(
                "🎯 PITCH: 'Ofrecemos limpieza completa de ático, "
                "remoción de insulación dañada por roedores e instalación nueva. "
                "Garantía incluida. ¿Cuándo podemos pasar a evaluar?'"
            )
        )
