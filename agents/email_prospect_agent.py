"""
agents/email_prospect_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Email prospect discovery agent.

Unlike the permit/solar/rodent agents that find leads from public data sources,
this agent finds *decision-maker email contacts* at GC and contractor companies
and creates Lead records pre-populated with contact emails.

Discovery cascade (first non-empty result wins per company):
  1. Local contacts/ CSV cache (already loaded by ContactsLoader)
  2. Hunter.io Domain/Company Search (100 free/mo)
  3. Apollo.io People Search (10 000 free credits/mo)
  4. Claude web_search fallback (when LLM adapter is Anthropic)

The agent is designed to run inside the Django-managed daemon as a source
of kind="email_prospect", but can also be called standalone via main.py.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger("agents.email_prospect")

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

# Rate-limit: Hunter free tier allows ~1 req/s; we stay conservative.
_HUNTER_DELAY_S = float(os.getenv("HUNTER_DELAY_S", "1.2"))
_APOLLO_DELAY_S = float(os.getenv("APOLLO_DELAY_S", "0.5"))

# Target decision-maker titles (in priority order)
_DECISION_TITLES = [
    "owner", "president", "ceo", "founder",
    "principal", "managing partner", "director",
    "project manager", "superintendent",
    "estimator",
]

# Bay Area cities used as a geographic filter when the CSV has city info
BAY_AREA_CITIES = {
    "san francisco", "oakland", "berkeley", "san jose", "palo alto",
    "san mateo", "fremont", "hayward", "concord", "walnut creek",
    "santa clara", "sunnyvale", "mountain view", "redwood city",
    "south san francisco", "daly city", "san leandro",
}


class EmailProspectAgent:
    """
    Discovers email contacts for GC / contractor prospects.

    Usage (standalone):
        agent = EmailProspectAgent(max_companies=50)
        leads = agent.fetch_leads()

    Usage (from Django pipeline):
        Called by outreach/pipeline/sources.py wrapping this class.
    """

    name = "Email Prospect Agent"
    emoji = "📧"
    agent_key = "email_prospect"

    def __init__(self, max_companies: int = 50) -> None:
        self.max_companies = max_companies

    # ─── Public API ───────────────────────────────────────────────

    def fetch_leads(self) -> list[dict[str, Any]]:
        """Return a list of lead dicts with email contacts pre-populated."""
        companies = self._load_companies()
        leads: list[dict[str, Any]] = []

        for company in companies[: self.max_companies]:
            contact = self._find_email(company)
            if not contact.get("email"):
                continue

            lead = self._build_lead(company, contact)
            leads.append(lead)
            logger.info(
                "[email_prospect] found email for %s → %s",
                company.get("name", "?"),
                contact["email"],
            )

        logger.info(
            "[email_prospect] scanned %d companies, found %d email contacts",
            len(companies[: self.max_companies]),
            len(leads),
        )
        return leads

    # ─── Internal helpers ─────────────────────────────────────────

    def _load_companies(self) -> list[dict[str, Any]]:
        """Pull company records from the shared ContactsLoader cache."""
        try:
            from utils.contacts_loader import load_all_contacts

            raw = load_all_contacts()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[email_prospect] contacts loader unavailable: %s", exc)
            raw = []

        companies: list[dict[str, Any]] = []
        seen: set[str] = set()

        for row in raw:
            # ContactsLoader records use raw_name (or name fallback)
            name = (row.get("raw_name") or row.get("name") or "").strip()
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue

            # Skip contacts that already have an email in the CSV
            if row.get("email"):
                continue

            seen.add(key)
            companies.append(
                {
                    "name": name,
                    "domain": "",  # ContactsLoader doesn't store domains
                    "phone": (row.get("phone") or "").strip(),
                    "city": "",
                    "license": "",
                }
            )

        return companies

    def _find_email(self, company: dict[str, Any]) -> dict[str, Any]:
        """Try each discovery source and return the first non-empty result."""
        name = company["name"]
        domain = company.get("domain", "")

        # 1. Hunter.io
        if HUNTER_API_KEY and (domain or name):
            result = self._hunter_search(name, domain)
            if result.get("email"):
                return result
            time.sleep(_HUNTER_DELAY_S)

        # 2. Apollo.io
        if APOLLO_API_KEY and name:
            result = self._apollo_search(name)
            if result.get("email"):
                return result
            time.sleep(_APOLLO_DELAY_S)

        # 3. Claude web_search fallback
        result = self._llm_search(company)
        if result.get("email"):
            return result

        return {}

    def _hunter_search(self, company_name: str, domain: str = "") -> dict[str, Any]:
        params: dict[str, Any] = {
            "api_key": HUNTER_API_KEY,
            "limit": 5,
        }
        if domain:
            params["domain"] = domain
        else:
            params["company"] = company_name

        try:
            resp = requests.get(
                "https://api.hunter.io/v2/domain-search",
                params=params,
                timeout=10,
            )
            if resp.status_code != 200:
                return {}
            data = resp.json().get("data", {})
            emails = data.get("emails", [])
            if not emails:
                return {}

            best = self._pick_decision_maker(emails)
            return {
                "email": best.get("value", ""),
                "first_name": best.get("first_name", ""),
                "last_name": best.get("last_name", ""),
                "position": best.get("position", ""),
                "confidence": best.get("confidence", 0),
                "source": "hunter",
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("[email_prospect] hunter error for %s: %s", company_name, exc)
            return {}

    def _apollo_search(self, company_name: str) -> dict[str, Any]:
        try:
            resp = requests.post(
                "https://api.apollo.io/v1/mixed_people/search",
                headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
                json={
                    "api_key": APOLLO_API_KEY,
                    "q_organization_name": company_name,
                    "page": 1,
                    "per_page": 5,
                    "person_titles": _DECISION_TITLES,
                },
                timeout=10,
            )
            if resp.status_code != 200:
                return {}
            people = resp.json().get("people", [])
            if not people:
                return {}

            person = people[0]
            org = person.get("organization") or {}
            phones = person.get("phone_numbers") or []
            return {
                "email": person.get("email", ""),
                "first_name": person.get("first_name", ""),
                "last_name": person.get("last_name", ""),
                "position": person.get("title", ""),
                "phone": phones[0].get("sanitized_number", "") if phones else "",
                "linkedin": person.get("linkedin_url", ""),
                "company_size": org.get("estimated_num_employees", ""),
                "source": "apollo",
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("[email_prospect] apollo error for %s: %s", company_name, exc)
            return {}

    def _llm_search(self, company: dict[str, Any]) -> dict[str, Any]:
        """Use the configured LLM adapter's enrich_contact() as a fallback."""
        try:
            from outreach.llm import get_adapter

            adapter = get_adapter()
            if adapter.name == "noop":
                return {}

            # Build a minimal mock lead so we can reuse enrich_contact().
            class _FakeLead:
                contact_company = company.get("name", "")
                contact_name = ""
                contact_phone = company.get("phone", "")
                contact_email = ""
                address = ""
                city = company.get("city", "")
                project_type = "general contracting"
                title = contact_company
                pk = "email_prospect_lookup"

            payload = adapter.enrich_contact(_FakeLead())
            email = (payload.get("contact_email") or "").strip()
            if not email:
                return {}
            return {
                "email": email,
                "first_name": "",
                "last_name": "",
                "position": "",
                "phone": (payload.get("contact_phone") or "").strip(),
                "source": f"llm:{adapter.name}",
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("[email_prospect] llm search error: %s", exc)
            return {}

    @staticmethod
    def _pick_decision_maker(emails: list[dict[str, Any]]) -> dict[str, Any]:
        """Return the email entry with the highest-priority title."""
        for title in _DECISION_TITLES:
            for e in emails:
                if title in (e.get("position") or "").lower():
                    return e
        return emails[0]

    @staticmethod
    def _build_lead(
        company: dict[str, Any], contact: dict[str, Any]
    ) -> dict[str, Any]:
        name = company["name"]
        first = contact.get("first_name", "")
        last = contact.get("last_name", "")
        contact_name = f"{first} {last}".strip() or ""
        position = contact.get("position", "")

        uid = hashlib.md5(
            f"email_prospect:{name}:{contact['email']}".encode()
        ).hexdigest()[:16]

        return {
            "id": uid,
            "title": f"Email prospect — {name}",
            "address": "",
            "city": company.get("city", ""),
            "project_type": "general contracting",
            "project_value": None,
            "description": (
                f"Decision-maker contact found via {contact.get('source', 'unknown')}. "
                f"{position + ' at ' if position else ''}{name}."
            ),
            "contact_company": name,
            "contact_name": contact_name,
            "contact_email": contact["email"],
            "contact_phone": contact.get("phone", "") or company.get("phone", ""),
            "contact_source": contact.get("source", "email_prospect"),
            "raw": {
                "linkedin": contact.get("linkedin", ""),
                "company_size": contact.get("company_size", ""),
                "email_confidence": contact.get("confidence", 0),
                "discovery_source": contact.get("source", ""),
            },
        }
