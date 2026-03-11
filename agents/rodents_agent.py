"""
agents/rodents_agent.py  v2.0
Ventana: 3 meses — con enriquecimiento de contacto completo
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import enrich_lead, geocode_address, contact_score_label

RODENT_KEYWORDS = ["rodent","rat","mice","mouse","vermin","pest","infestation","roedor","raton","rata","plaga"]
LOOKBACK_DAYS = 90


class RodentsAgent(BaseAgent):
    name      = "Reportes 311 Roedores"
    emoji     = "🐀"
    agent_key = "rodents"

    def fetch_leads(self) -> list:
        leads = []
        leads += self._fetch_sf_311()
        leads += self._fetch_oakland_311()
        leads += self._fetch_sj_311()
        return leads

    def _fetch_sf_311(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/vw6y-z8j6.json"
        params = {
            "$limit": 500,
            "$where": (
                f"requested_datetime >= '{since}' AND "
                "(UPPER(service_name) LIKE '%RODENT%' OR "
                " UPPER(service_name) LIKE '%PEST%' OR "
                " UPPER(service_subtype) LIKE '%RODENT%')"
            ),
            "$order": "requested_datetime DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            address = (item.get("address", "") or "").title()
            if not address:
                continue
            lead = {
                "id":            "sf311_" + item.get("service_request_id", item.get("case_id", "")),
                "city":          "San Francisco",
                "address":       address,
                "category":      item.get("service_name", "Roedores/Plagas"),
                "subcategory":   item.get("service_subtype", ""),
                "description":   (item.get("service_details", "") or "Sin detalle")[:120],
                "reported_date": (item.get("requested_datetime", "") or "")[:10],
                "status":        item.get("status_description", "Abierto"),
                "neighborhood":  item.get("neighborhoods_sffind_boundaries", ""),
                "lat":           item.get("lat", ""),
                "lon":           item.get("long", ""),
                "owner":         "",
                "contractor":    "",
            }
            lead = enrich_lead(lead)
            # Para 311 el maps_url puede venir del lat/lon original
            if item.get("lat") and item.get("long") and not lead.get("maps_url"):
                lead["maps_url"] = f"https://maps.google.com/?q={item['lat']},{item['long']}"
            leads.append(lead)
        return leads

    def _fetch_oakland_311(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).isoformat()
        url = "https://seeclickfix.com/open311/v2/requests.json"
        params = {
            "jurisdiction_id": "seeclickfix.com",
            "lat": 37.8044, "long": -122.2711, "radius": 25000,
            "requested_datetime_start": since, "page_size": 200,
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            desc    = (item.get("description", "") or "").lower()
            summary = (item.get("summary", "") or "").lower()
            if not any(kw in desc or kw in summary for kw in RODENT_KEYWORDS):
                continue
            address = (item.get("address", "") or "No indicada").title()
            lead = {
                "id":            "oak311_" + item.get("service_request_id", ""),
                "city":          "Oakland",
                "address":       address,
                "category":      item.get("service_name", "Control de Plagas"),
                "subcategory":   item.get("agency_responsible", ""),
                "description":   (item.get("description", "") or "")[:120],
                "reported_date": (item.get("requested_datetime", "") or "")[:10],
                "status":        item.get("status", "Abierto"),
                "neighborhood":  item.get("ward", ""),
                "lat":           item.get("lat", ""),
                "lon":           item.get("long", ""),
                "owner":         "",
                "contractor":    "",
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _fetch_sj_311(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://311.sanjoseca.gov/open311/v2/requests.json"
        params = {"start_date": since, "page_size": 200}
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            service = (item.get("service_name", "") or "").lower()
            desc    = (item.get("description", "") or "").lower()
            if not any(kw in service or kw in desc for kw in RODENT_KEYWORDS):
                continue
            address = (item.get("address", "") or "No indicada").title()
            lead = {
                "id":            "sj311_" + item.get("service_request_id", ""),
                "city":          "San Jose",
                "address":       address,
                "category":      item.get("service_name", "Control de Plagas"),
                "subcategory":   "",
                "description":   desc[:120],
                "reported_date": (item.get("requested_datetime", "") or "")[:10],
                "status":        item.get("status", "Abierto"),
                "neighborhood":  item.get("ward", ""),
                "lat":           item.get("lat", ""),
                "lon":           item.get("long", ""),
                "owner":         "",
                "contractor":    "",
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def notify(self, lead: dict):
        score_label = contact_score_label(lead.get("contact_score", 0))
        send_lead(
            agent_name="Reporte 311 Roedores/Plagas",
            emoji="🐀",
            title=lead["city"] + " — " + lead["address"],
            fields={
                "Tipo de Reporte":   lead["category"],
                "Descripcion":       lead["description"],
                "Barrio":            lead.get("neighborhood") or "No indicado",
                "Fecha Reporte":     lead["reported_date"],
                "Estado":            lead["status"],
                "Propietario Real":  lead.get("owner") or "No encontrado",
                "Dir. Postal Owner": lead.get("owner_mail_addr") or "No disponible",
                "APN Parcela":       lead.get("apn") or "N/A",
                "Ver en Maps":       lead.get("maps_url") or "No disponible",
                "Info Contacto":     score_label,
            },
            cta="PITCH: Ofrecemos limpieza completa de atico, remocion de insulacion danada por roedores e instalacion nueva. Garantia incluida."
        )
