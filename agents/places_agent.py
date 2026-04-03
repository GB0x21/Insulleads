"""
agents/places_agent.py
━━━━━━━━━━━━━━━━━━━━━━
📍 Google Places — Negocios de Construcción Activos

Fuente: Google Places API (Nearby Search)
  - Free tier: $200/mes en créditos (= ~5,000 búsquedas)
  - Busca negocios activos de construcción, remodelación, HVAC

Lógica: Negocios de construcción activos en Bay Area = potenciales
clientes o referidos para servicios de insulación.
"""

import os
import logging
import requests
from datetime import datetime

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact
from utils.lead_scoring import score_lead, format_score_line

logger = logging.getLogger(__name__)

GOOGLE_PLACES_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# Coordenadas centrales de ciudades Bay Area + radio de búsqueda
_SEARCH_LOCATIONS = [
    {"city": "San Francisco", "lat": 37.7749, "lon": -122.4194, "radius": 8000},
    {"city": "Oakland",       "lat": 37.8044, "lon": -122.2712, "radius": 8000},
    {"city": "San Jose",      "lat": 37.3382, "lon": -121.8863, "radius": 10000},
    {"city": "Fremont",       "lat": 37.5485, "lon": -121.9886, "radius": 6000},
    {"city": "Berkeley",      "lat": 37.8716, "lon": -122.2727, "radius": 5000},
]

# Keywords de búsqueda para negocios relacionados
_SEARCH_KEYWORDS = [
    "general contractor",
    "home remodeling",
    "HVAC contractor",
    "roofing contractor",
    "construction company",
]


class PlacesAgent(BaseAgent):
    name      = "📍 Negocios Construcción — Bay Area"
    emoji     = "📍"
    agent_key = "places"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        if not GOOGLE_PLACES_KEY:
            logger.info("[Places] GOOGLE_PLACES_API_KEY no configurado — omitido")
            return []

        leads = []
        seen_ids = set()

        for location in _SEARCH_LOCATIONS:
            for keyword in _SEARCH_KEYWORDS:
                try:
                    results = self._search_nearby(
                        location["lat"], location["lon"],
                        location["radius"], keyword,
                    )
                    for place in results:
                        place_id = place.get("place_id", "")
                        if not place_id or place_id in seen_ids:
                            continue
                        seen_ids.add(place_id)

                        lead = self._place_to_lead(place, location["city"], keyword)
                        if lead:
                            leads.append(lead)

                except Exception as e:
                    logger.debug(f"[Places/{location['city']}/{keyword}] {e}")

        logger.info(f"[Places] {len(leads)} negocios encontrados en {len(_SEARCH_LOCATIONS)} ciudades")

        # Ordenar por rating (mejor primero)
        leads.sort(key=lambda l: l.get("rating", 0), reverse=True)
        return leads

    def _search_nearby(self, lat: float, lon: float,
                       radius: int, keyword: str) -> list:
        """Google Places Nearby Search."""
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={
                "location": f"{lat},{lon}",
                "radius": radius,
                "keyword": keyword,
                "type": "general_contractor",
                "key": GOOGLE_PLACES_KEY,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        if data.get("status") != "OK":
            return []

        return data.get("results", [])

    def _place_to_lead(self, place: dict, city: str, keyword: str) -> dict | None:
        """Convierte un resultado de Google Places a lead."""
        name = place.get("name", "")
        address = place.get("vicinity", "") or place.get("formatted_address", "")
        place_id = place.get("place_id", "")
        rating = place.get("rating", 0)
        total_ratings = place.get("user_ratings_total", 0)

        if not name or not address:
            return None

        # Obtener detalles del negocio (teléfono, website)
        details = self._get_place_details(place_id)

        lead = {
            "id":             f"places_{place_id}",
            "city":           city,
            "address":        address,
            "business_name":  name,
            "description":    f"{keyword.title()} — {name}",
            "rating":         rating,
            "total_reviews":  total_ratings,
            "phone":          details.get("phone", ""),
            "website":        details.get("website", ""),
            "business_status": place.get("business_status", ""),
            "search_keyword": keyword,
            "_agent_key":     "places",
        }

        # Contacto via CSV
        match = lookup_contact(name, self._contacts)
        if match:
            lead["contact_phone"]  = match.get("phone", "")
            lead["contact_email"]  = match.get("email", "")
            lead["contact_source"] = f"CSV ({match['source']})"
        elif details.get("phone"):
            lead["contact_phone"] = details["phone"]
            lead["contact_source"] = "Google Places"

        return lead

    def _get_place_details(self, place_id: str) -> dict:
        """Google Places Details — obtiene teléfono y website."""
        try:
            resp = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "formatted_phone_number,website,opening_hours",
                    "key": GOOGLE_PLACES_KEY,
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return {}
            data = resp.json()
            result = data.get("result", {})
            return {
                "phone":   result.get("formatted_phone_number", ""),
                "website": result.get("website", ""),
            }
        except Exception:
            return {}

    def notify(self, lead: dict):
        rating = lead.get("rating", 0)
        reviews = lead.get("total_reviews", 0)

        star_str = "⭐" * int(rating) if rating else "Sin rating"

        fields = {
            "📍 Ciudad":       lead.get("city"),
            "🏢 Negocio":     lead.get("business_name"),
            "🔍 Categoría":   lead.get("search_keyword", "").title(),
            "⭐ Rating":       f"{rating}/5 ({reviews} reviews)" if rating else "—",
        }

        if lead.get("contact_phone"):
            src = lead.get("contact_source", "")
            fields["📞 Teléfono"] = (
                f"{lead['contact_phone']}  _(via {src})_" if src
                else lead["contact_phone"]
            )

        if lead.get("contact_email"):
            fields["✉️  Email"] = lead["contact_email"]

        if lead.get("website"):
            fields["🌐 Website"] = lead["website"]

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['business_name']}",
            fields=fields,
            cta="📍 Contratista activo = potencial cliente o referido para insulación.",
        )
