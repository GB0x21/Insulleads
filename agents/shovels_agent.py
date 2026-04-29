"""
agents/shovels_agent.py
━━━━━━━━━━━━━━━━━━━━━━
🪏 Shovels.ai — national US building-permit aggregator

API docs: https://docs.shovels.ai/

Why this matters for the project:
  - The legacy `permits_agent` covers ~25 individual Bay Area Socrata /
    CKAN endpoints. Shovels indexes thousands of jurisdictions
    nationally with a single, normalized schema — adding statewide CA
    coverage (LA, San Diego, Sacramento, OC, Inland Empire) plus the
    rest of the country with one source.
  - It also returns contractor identifiers (license number, business
    name) that feed straight into the existing 5-tier enrichment
    cascade.

Soft-fail contract:
  - When ``SHOVELS_API_KEY`` is missing, ``fetch_leads()`` logs a
    warning and returns ``[]`` — same pattern as crawl4ai. The daemon
    keeps running and the source just produces zero leads until you
    drop the key in ``.env``.

Free tier signup: https://shovels.ai/

Env knobs:
  SHOVELS_API_KEY        required for any non-empty result
  SHOVELS_BASE_URL       default https://api.shovels.ai
  SHOVELS_API_VERSION    path segment, default "v2" — bump if Shovels
                         publishes a breaking new version
  SHOVELS_STATES         comma-separated 2-letter states (default 'CA')
  SHOVELS_PERMIT_TYPES   comma-separated permit-type filter
                         (default: residential / additions / new
                         construction / re-roof — the work classes
                         where insulation is in scope)
  SHOVELS_MIN_VALUE      minimum estimated_cost (default 50_000)
  SHOVELS_DAYS_BACK      issued_date window (default 90)
  SHOVELS_MAX_PER_STATE  cap per state per fetch (default 200)
  SHOVELS_TIMEOUT_S      request timeout (default 45)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)


SHOVELS_API_KEY = os.getenv("SHOVELS_API_KEY", "")
SHOVELS_BASE_URL = os.getenv("SHOVELS_BASE_URL", "https://api.shovels.ai")
SHOVELS_API_VERSION = os.getenv("SHOVELS_API_VERSION", "v2")
SHOVELS_STATES = [
    s.strip().upper() for s in os.getenv("SHOVELS_STATES", "CA").split(",") if s.strip()
]
SHOVELS_PERMIT_TYPES = [
    t.strip() for t in os.getenv(
        "SHOVELS_PERMIT_TYPES",
        "RESIDENTIAL,ADDITION,NEW_CONSTRUCTION,REROOF,REMODEL",
    ).split(",") if t.strip()
]
SHOVELS_MIN_VALUE = float(os.getenv("SHOVELS_MIN_VALUE", "50000"))
SHOVELS_DAYS_BACK = int(os.getenv("SHOVELS_DAYS_BACK", "90"))
SHOVELS_MAX_PER_STATE = int(os.getenv("SHOVELS_MAX_PER_STATE", "200"))
SHOVELS_TIMEOUT = int(os.getenv("SHOVELS_TIMEOUT_S", "45"))


# ─── HTTP helpers ─────────────────────────────────────────────────


def _endpoint(path: str) -> str:
    return f"{SHOVELS_BASE_URL.rstrip('/')}/{SHOVELS_API_VERSION}{path}"


def _headers() -> dict[str, str]:
    return {
        "X-API-Key": SHOVELS_API_KEY,
        "Accept": "application/json",
        "User-Agent": "Insulleads/shovels",
    }


def _fetch_state_permits(state: str) -> list[dict]:
    """Search Shovels.ai for recent permits in a single state."""
    issued_after = (
        datetime.now(timezone.utc) - timedelta(days=SHOVELS_DAYS_BACK)
    ).strftime("%Y-%m-%d")
    params = {
        "state": state,
        "issued_after": issued_after,
        "min_estimated_cost": int(SHOVELS_MIN_VALUE),
        "limit": SHOVELS_MAX_PER_STATE,
    }
    if SHOVELS_PERMIT_TYPES:
        params["permit_type"] = ",".join(SHOVELS_PERMIT_TYPES)

    resp = requests.get(
        _endpoint("/permits/search"),
        params=params,
        headers=_headers(),
        timeout=SHOVELS_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    # Shovels' v2 returns either a top-level list or a `{"permits": [...]}`
    # envelope depending on the endpoint family. Tolerate both.
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("permits", "results", "data", "items"):
            v = data.get(key)
            if isinstance(v, list):
                return v
    return []


# ─── Lead normalization ──────────────────────────────────────────


def _addr_join(p: dict) -> str:
    parts = [
        p.get("street_number"),
        p.get("street_name"),
        p.get("street_type"),
    ]
    out = " ".join(str(x).strip() for x in parts if x)
    return out or (p.get("address") or p.get("street_address") or "").strip()


def _value(p: dict) -> float:
    for k in ("estimated_cost", "valuation", "value", "project_value", "cost"):
        v = p.get(k)
        if v in (None, ""):
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _date(p: dict) -> str:
    for k in ("issued_date", "issued", "issue_date", "date_issued"):
        v = p.get(k)
        if v:
            return str(v)[:10]
    return ""


def _normalize(p: dict) -> dict | None:
    addr = _addr_join(p)
    city = p.get("city") or p.get("jurisdiction") or ""
    if not addr or not city:
        return None

    permit_id = p.get("permit_number") or p.get("id") or p.get("permit_id")
    contractor = (
        p.get("contractor_name")
        or (p.get("contractor") or {}).get("name", "")
        or ""
    )
    lic_number = (
        p.get("contractor_license")
        or p.get("license_number")
        or (p.get("contractor") or {}).get("license_number", "")
        or ""
    )
    description = (p.get("description") or p.get("work_description") or "")
    permit_type = (p.get("permit_type") or p.get("type") or "")
    state = (p.get("state") or "").upper()

    return {
        "id": f"shovels_{state}_{permit_id}" if permit_id else f"shovels_{state}_{abs(hash(addr+city)) & 0xFFFFFF}",
        "address": addr,
        "city": city,
        "permit_type": permit_type,
        "description": description,
        "issued_date": _date(p),
        "value": p.get("estimated_cost") or "",
        "value_float": _value(p),
        "contractor": contractor.strip(),
        "lic_number": str(lic_number).strip(),
        "owner": (p.get("owner_name") or "").strip(),
        "permit_url": p.get("url") or p.get("permit_url") or "",
        "state": state,
        "_agent_key": "shovels",
    }


# ─── Agent ────────────────────────────────────────────────────────


class ShovelsAgent(BaseAgent):
    name = "🪏 Permits — Shovels.ai (national)"
    emoji = "🪏"
    agent_key = "shovels"

    def fetch_leads(self) -> list:
        if not SHOVELS_API_KEY:
            logger.info(
                "[shovels] SHOVELS_API_KEY not set — skipping. "
                "Sign up free at https://shovels.ai/"
            )
            return []

        leads: list[dict] = []
        for state in SHOVELS_STATES:
            try:
                permits = _fetch_state_permits(state)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[shovels/%s] fetch failed: %s", state, exc)
                continue
            kept = 0
            for p in permits:
                lead = _normalize(p)
                if lead:
                    leads.append(lead)
                    kept += 1
            logger.info(
                "[shovels/%s] %d permits → %d leads (≥$%.0fk, last %dd)",
                state, len(permits), kept,
                SHOVELS_MIN_VALUE / 1000, SHOVELS_DAYS_BACK,
            )

        # High-value first.
        leads.sort(key=lambda l: -l.get("value_float", 0))
        return leads

    def notify(self, lead: dict):
        value = lead.get("value_float", 0)
        fields = {
            "📍 Ciudad":          lead.get("city"),
            "🌎 Estado":          lead.get("state") or "—",
            "🔖 Tipo de Permiso": lead.get("permit_type") or "—",
            "📝 Descripción":     (lead.get("description") or "—")[:200],
            "📅 Fecha Emisión":   lead.get("issued_date") or "—",
            "💰 Valor Estimado":  f"${value:,.0f}" if value else "—",
            "👷 Contratista":     lead.get("contractor") or "—",
            "🪪 Licencia":        lead.get("lic_number") or "—",
            "👤 Propietario":     lead.get("owner") or "—",
        }
        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields,
            url=lead.get("permit_url"),
            cta="📲 Permit nacional vía Shovels.ai — contactar al GC.",
        )
