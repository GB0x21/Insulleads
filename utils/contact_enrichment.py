"""
utils/contact_enrichment.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Enriquecimiento de contactos vía APIs externas.

Soporta:
  1. Hunter.io   — Email Finder (100 búsquedas/mes gratis)
  2. Apollo.io   — People Enrichment (gratis con API key)
  3. Clearbit    — Company Enrichment (fallback)

Prioridad: CSV local → Hunter.io → Apollo.io → CSLB (existente)
"""

import os
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

HUNTER_API_KEY  = os.getenv("HUNTER_API_KEY", "")
APOLLO_API_KEY  = os.getenv("APOLLO_API_KEY", "")

# Cache en memoria para evitar llamadas repetidas
_enrichment_cache: dict = {}


def enrich_contact(company_name: str = "", domain: str = "",
                   person_name: str = "") -> dict:
    """
    Busca datos de contacto adicionales vía APIs de enriquecimiento.
    Retorna dict con phone, email, title, linkedin_url, o vacío.

    Usa cache para evitar llamadas duplicadas en el mismo ciclo.
    """
    cache_key = f"{company_name}|{domain}|{person_name}"
    if cache_key in _enrichment_cache:
        return _enrichment_cache[cache_key]

    result = {}

    # ── 1. Hunter.io — Email Finder ──────────────────────────────
    if HUNTER_API_KEY and (domain or company_name):
        result = _hunter_lookup(company_name, domain)

    # ── 2. Apollo.io — People/Company Enrichment ─────────────────
    if not result.get("email") and APOLLO_API_KEY:
        apollo = _apollo_lookup(company_name, person_name, domain)
        if apollo:
            result = {**result, **apollo}

    _enrichment_cache[cache_key] = result
    return result


def _hunter_lookup(company_name: str, domain: str = "") -> dict:
    """
    Hunter.io Domain Search / Email Finder.
    Free tier: 25 búsquedas/mes (verificaciones) + 50 búsquedas.
    """
    try:
        if domain:
            # Domain Search — encuentra emails del dominio
            resp = requests.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain": domain,
                    "api_key": HUNTER_API_KEY,
                    "limit": 3,
                },
                timeout=10,
            )
        else:
            # Company Search — busca por nombre de empresa
            resp = requests.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "company": company_name,
                    "api_key": HUNTER_API_KEY,
                    "limit": 3,
                },
                timeout=10,
            )

        if resp.status_code != 200:
            return {}

        data = resp.json().get("data", {})
        emails = data.get("emails", [])

        if not emails:
            return {}

        # Priorizar decisores: owner, manager, director
        priority_titles = ["owner", "manager", "director", "president", "ceo", "founder"]
        best = emails[0]
        for e in emails:
            pos = (e.get("position") or "").lower()
            if any(t in pos for t in priority_titles):
                best = e
                break

        return {
            "email":       best.get("value", ""),
            "first_name":  best.get("first_name", ""),
            "last_name":   best.get("last_name", ""),
            "position":    best.get("position", ""),
            "phone":       best.get("phone_number", ""),
            "confidence":  best.get("confidence", 0),
            "source":      "Hunter.io",
        }
    except Exception as e:
        logger.debug(f"[Hunter.io] Error: {e}")
        return {}


def _apollo_lookup(company_name: str = "", person_name: str = "",
                   domain: str = "") -> dict:
    """
    Apollo.io People Enrichment API.
    Free tier: 10,000 créditos/mes.
    """
    try:
        # Organization search
        if company_name:
            resp = requests.post(
                "https://api.apollo.io/v1/mixed_people/search",
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                },
                json={
                    "api_key": APOLLO_API_KEY,
                    "q_organization_name": company_name,
                    "page": 1,
                    "per_page": 3,
                    "person_titles": ["owner", "manager", "president", "director"],
                },
                timeout=10,
            )

            if resp.status_code != 200:
                return {}

            data = resp.json()
            people = data.get("people", [])

            if not people:
                return {}

            person = people[0]
            org = person.get("organization", {})

            return {
                "email":        person.get("email", ""),
                "first_name":   person.get("first_name", ""),
                "last_name":    person.get("last_name", ""),
                "phone":        (person.get("phone_numbers") or [{}])[0].get("sanitized_number", "") if person.get("phone_numbers") else "",
                "position":     person.get("title", ""),
                "linkedin_url": person.get("linkedin_url", ""),
                "company_size": org.get("estimated_num_employees", ""),
                "company_revenue": org.get("annual_revenue_printed", ""),
                "source":       "Apollo.io",
            }
    except Exception as e:
        logger.debug(f"[Apollo.io] Error: {e}")
    return {}


def get_enrichment_stats() -> dict:
    """Retorna estadísticas del cache de enriquecimiento."""
    total = len(_enrichment_cache)
    with_email = sum(1 for v in _enrichment_cache.values() if v.get("email"))
    with_phone = sum(1 for v in _enrichment_cache.values() if v.get("phone"))
    return {
        "total_lookups": total,
        "with_email": with_email,
        "with_phone": with_phone,
        "hit_rate": f"{(with_email/total*100):.0f}%" if total else "0%",
    }


def clear_cache():
    """Limpia el cache de enriquecimiento entre ciclos."""
    _enrichment_cache.clear()
