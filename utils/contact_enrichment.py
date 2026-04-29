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
            # Merge additional_contacts from both sources
            hunter_additional = result.pop("additional_contacts", [])
            apollo_additional = apollo.pop("additional_contacts", [])
            result = {**result, **apollo}
            # Combine additional contacts, dedupe by email, max 4
            seen = {(result.get("email") or "").lower()}
            merged_additional = []
            for c in hunter_additional + apollo_additional:
                ce = (c.get("email") or "").lower()
                if len(merged_additional) >= 4:
                    break
                if ce and ce not in seen:
                    merged_additional.append(c)
                    seen.add(ce)
                elif not ce and len(merged_additional) < 4:
                    merged_additional.append(c)
            if merged_additional:
                result["additional_contacts"] = merged_additional
    elif result.get("additional_contacts"):
        # Hunter only, keep its additional contacts but cap at 4
        extras = result.get("additional_contacts", [])
        if len(extras) > 4:
            result["additional_contacts"] = extras[:4]

    _enrichment_cache[cache_key] = result
    return result


def _hunter_lookup(company_name: str, domain: str = "") -> dict:
    """
    Hunter.io Domain Search / Email Finder.
    Free tier: 25 búsquedas/mes (verificaciones) + 50 búsquedas.
    """
    try:
        params = {"api_key": HUNTER_API_KEY, "limit": 10}
        if domain:
            params["domain"] = domain
        else:
            params["company"] = company_name
        resp = requests.get("https://api.hunter.io/v2/domain-search", params=params, timeout=10)
        if resp.status_code != 200:
            return {}
        data = resp.json().get("data", {})
        emails = data.get("emails", [])
        if not emails:
            return {}
        priority_titles = ["owner", "manager", "director", "president", "ceo", "founder"]
        # Sort: decision-makers first, then by confidence desc
        def _priority(e):
            pos = (e.get("position") or "").lower()
            return (0 if any(t in pos for t in priority_titles) else 1, -(e.get("confidence") or 0))
        emails.sort(key=_priority)
        primary = emails[0]
        additional = []
        seen_emails = {primary.get("value", "").lower()}
        for e in emails[1:]:
            ev = (e.get("value") or "").lower()
            if ev and ev not in seen_emails and len(additional) < 4:
                additional.append({
                    "email": e.get("value", ""),
                    "name": f"{e.get('first_name','')} {e.get('last_name','')}".strip(),
                    "position": e.get("position", ""),
                    "phone": e.get("phone_number", ""),
                    "source": "Hunter.io",
                })
                seen_emails.add(ev)
        result = {
            "email": primary.get("value", ""),
            "first_name": primary.get("first_name", ""),
            "last_name": primary.get("last_name", ""),
            "position": primary.get("position", ""),
            "phone": primary.get("phone_number", ""),
            "confidence": primary.get("confidence", 0),
            "source": "Hunter.io",
        }
        if additional:
            result["additional_contacts"] = additional
        return result
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
                    "per_page": 5,
                    "person_titles": ["owner", "manager", "president", "director", "superintendent", "project manager", "estimator"],
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return {}
            people = resp.json().get("people", [])
            if not people:
                return {}
            person = people[0]
            org = person.get("organization", {})
            additional = []
            seen_emails = {(person.get("email") or "").lower()}
            for p in people[1:]:
                pe = (p.get("email") or "").lower()
                if len(additional) >= 4:
                    break
                phone = ""
                if p.get("phone_numbers"):
                    phone = p["phone_numbers"][0].get("sanitized_number", "")
                additional.append({
                    "email": p.get("email", ""),
                    "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                    "position": p.get("title", ""),
                    "phone": phone,
                    "linkedin_url": p.get("linkedin_url", ""),
                    "source": "Apollo.io",
                })
            result = {
                "email": person.get("email", ""),
                "first_name": person.get("first_name", ""),
                "last_name": person.get("last_name", ""),
                "phone": (person.get("phone_numbers") or [{}])[0].get("sanitized_number", "") if person.get("phone_numbers") else "",
                "position": person.get("title", ""),
                "linkedin_url": person.get("linkedin_url", ""),
                "company_size": org.get("estimated_num_employees", ""),
                "company_revenue": org.get("annual_revenue_printed", ""),
                "source": "Apollo.io",
            }
            if additional:
                result["additional_contacts"] = additional
            return result
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
