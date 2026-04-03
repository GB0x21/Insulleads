"""
agents/yelp_agent.py
━━━━━━━━━━━━━━━━━━━━
⭐ Yelp Fusion — Reviews de Contratistas Bay Area

Fuente: Yelp Fusion API (gratuita: 5,000 calls/día)
  https://docs.developer.yelp.com/reference/v3_business_search

Lógica:
  1. Buscar contratistas HVAC, roofing, remodeling activos
  2. Reviews recientes mencionan proyectos = cross-sell insulación
  3. Contratistas con alto rating = mejores referidos
"""

import os
import logging
import requests
from datetime import datetime

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

YELP_API_KEY = os.getenv("YELP_API_KEY", "")  # Gratis: https://www.yelp.com/developers/v3/manage_app

# Categorías de Yelp relacionadas con insulación
_YELP_CATEGORIES = [
    "contractors",          # General contractors
    "hvac",                 # HVAC
    "roofing",              # Roofing
    "insulation_installation",  # Insulación directa
    "handyman",             # Handyman (remodeling)
    "home_energy_auditors", # Auditorías energéticas
]

# Ciudades Bay Area para búsqueda
_SEARCH_CITIES = [
    "San Francisco, CA",
    "Oakland, CA",
    "San Jose, CA",
    "Fremont, CA",
    "Berkeley, CA",
    "Hayward, CA",
    "Richmond, CA",
    "Sunnyvale, CA",
]


class YelpAgent(BaseAgent):
    name      = "⭐ Contratistas Yelp — Bay Area"
    emoji     = "⭐"
    agent_key = "yelp"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        if not YELP_API_KEY:
            logger.info("[Yelp] YELP_API_KEY no configurado — omitido")
            return []

        leads = []
        seen_ids = set()

        for city in _SEARCH_CITIES:
            for category in _YELP_CATEGORIES:
                try:
                    businesses = self._search_yelp(city, category)
                    for biz in businesses:
                        biz_id = biz.get("id", "")
                        if not biz_id or biz_id in seen_ids:
                            continue
                        seen_ids.add(biz_id)

                        lead = self._biz_to_lead(biz, city, category)
                        if lead:
                            leads.append(lead)

                except Exception as e:
                    logger.debug(f"[Yelp/{city}/{category}] {e}")

        logger.info(f"[Yelp] {len(leads)} negocios encontrados")

        # Ordenar por rating * review_count (popularidad ponderada)
        leads.sort(
            key=lambda l: (l.get("rating", 0) * min(l.get("review_count", 0), 100)),
            reverse=True,
        )
        return leads

    def _search_yelp(self, location: str, category: str) -> list:
        """Yelp Fusion Business Search."""
        resp = requests.get(
            "https://api.yelp.com/v3/businesses/search",
            headers={"Authorization": f"Bearer {YELP_API_KEY}"},
            params={
                "location": location,
                "categories": category,
                "sort_by": "rating",
                "limit": 10,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("businesses", [])

    def _biz_to_lead(self, biz: dict, search_city: str,
                     category: str) -> dict | None:
        """Convierte un negocio Yelp a lead."""
        name = biz.get("name", "")
        if not name:
            return None

        location = biz.get("location", {})
        address = location.get("address1", "")
        city = location.get("city", search_city.split(",")[0])

        phone = biz.get("display_phone", "") or biz.get("phone", "")
        rating = biz.get("rating", 0)
        review_count = biz.get("review_count", 0)

        # Categorías del negocio
        categories = [c.get("title", "") for c in biz.get("categories", [])]
        cat_str = ", ".join(categories[:3])

        lead = {
            "id":             f"yelp_{biz.get('id', '')}",
            "city":           city,
            "address":        address,
            "business_name":  name,
            "description":    f"{cat_str} — {name}",
            "rating":         rating,
            "review_count":   review_count,
            "phone":          phone,
            "yelp_url":       biz.get("url", ""),
            "category":       category,
            "categories_str": cat_str,
            "is_closed":      biz.get("is_closed", False),
            "_agent_key":     "yelp",
        }

        # No incluir negocios cerrados
        if lead["is_closed"]:
            return None

        # Enriquecer contacto via CSV
        match = lookup_contact(name, self._contacts)
        if match:
            lead["contact_phone"]  = match.get("phone", "") or phone
            lead["contact_email"]  = match.get("email", "")
            lead["contact_source"] = f"CSV ({match['source']})"
        elif phone:
            lead["contact_phone"]  = phone
            lead["contact_source"] = "Yelp"

        return lead

    def notify(self, lead: dict):
        rating = lead.get("rating", 0)
        reviews = lead.get("review_count", 0)

        fields = {
            "📍 Ciudad":       lead.get("city"),
            "🏢 Negocio":     lead.get("business_name"),
            "🔍 Categoría":   lead.get("categories_str") or lead.get("category", ""),
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

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['business_name']}",
            fields=fields,
            url=lead.get("yelp_url"),
            cta="⭐ Contratista activo con buen rating = potencial referido para insulación.",
        )
