"""
Pipeline sources — thin adapters that wrap the legacy Insulleads agents
(`agents/*.py`) so the daemon can treat them as discovery backends.

Each source exposes a `.fetch() -> Iterable[dict]` that returns lead dicts
normalized for the Django Lead model. The raw dict from the underlying
agent is kept in `raw` so no data is lost.

Adapted from OpenOutreach's `linkedin.pipeline` (candidate sourcing).
"""
from __future__ import annotations

import logging
from typing import Iterable, Protocol

from outreach.models import Source

logger = logging.getLogger("outreach.pipeline.sources")


class DiscoverySource(Protocol):
    key: str

    def fetch(self) -> Iterable[dict]: ...


# ─── Lazy agent instantiation ───────────────────────────────────────
_AGENT_CACHE: dict[str, object] = {}


def _get_agent(source: Source):
    """Import & instantiate the backend (legacy agent or ScrapySource) for a source."""
    kind = source.kind

    # Scrapy sources are cached per-key because two sources of kind="scrapy"
    # can run entirely different spiders.
    if kind == "scrapy":
        cache_key = f"scrapy:{source.key}"
        if cache_key in _AGENT_CACHE:
            return _AGENT_CACHE[cache_key]
        from outreach.pipeline.scrapy_source import ScrapySource

        config = source.config or {}
        spider_name = config.get("spider")
        if not spider_name:
            raise ValueError(
                f"Source {source.key!r} is kind=scrapy but config.spider is missing"
            )
        backend = ScrapySource(
            spider_name=spider_name,
            spider_args=config.get("spider_args", {}),
            timeout=config.get("timeout"),
            source_key=source.key,
        )
        _AGENT_CACHE[cache_key] = backend
        return backend

    if kind in _AGENT_CACHE:
        return _AGENT_CACHE[kind]

    # Legacy agents live in the top-level `agents/` package. Import lazily
    # so `setup_crm` / `migrate` don't require API keys just to run.
    if kind == "permits":
        from agents.permits_agent import PermitsAgent as Cls
    elif kind == "solar":
        from agents.solar_agent import SolarAgent as Cls
    elif kind == "rodents":
        from agents.rodents_agent import RodentsAgent as Cls
    elif kind == "flood":
        from agents.flood_agent import FloodAgent as Cls
    elif kind == "construction":
        from agents.construction_agent import ConstructionAgent as Cls
    elif kind == "deconstruction":
        from agents.deconstruction_agent import DeconstuctionAgent as Cls
    elif kind == "realestate":
        from agents.realestate_agent import RealEstateAgent as Cls
    elif kind == "energy":
        from agents.energy_agent import EnergyAgent as Cls
    elif kind == "places":
        from agents.places_agent import PlacesAgent as Cls
    elif kind == "yelp":
        from agents.yelp_agent import YelpAgent as Cls
    elif kind == "thermal":
        from agents.thermal_agent import ThermalAgent as Cls
    elif kind == "email_prospect":
        from agents.email_prospect_agent import EmailProspectAgent as Cls
    else:
        raise ValueError(f"Unknown source kind: {kind}")

    _AGENT_CACHE[kind] = Cls()
    return _AGENT_CACHE[kind]


# ─── Normalization ──────────────────────────────────────────────────
def _normalize(raw: dict, source: Source) -> dict:
    """Project a legacy agent dict onto Lead model fields."""
    ext_id = str(
        raw.get("id")
        or raw.get("permit_id")
        or raw.get("external_id")
        or raw.get("case_id")
        or f"{source.key}-{hash(frozenset((k, str(v)) for k, v in raw.items()))}"
    )

    return {
        "external_id": ext_id,
        "title": raw.get("title")
        or raw.get("description", "")[:200]
        or f"{source.get_kind_display()} lead",
        "address": raw.get("address") or raw.get("street_address", ""),
        "city": raw.get("city", ""),
        "project_value": raw.get("value") or raw.get("estimated_cost") or None,
        "project_type": raw.get("permit_type")
        or raw.get("project_type")
        or raw.get("category", ""),
        "description": raw.get("description", ""),
        "latitude": raw.get("latitude") or raw.get("lat"),
        "longitude": raw.get("longitude") or raw.get("lng") or raw.get("lon"),
        "contact_name": raw.get("contact_name") or raw.get("contractor_name", ""),
        "contact_company": raw.get("contact_company") or raw.get("contractor") or raw.get("company", ""),
        "contact_phone": raw.get("phone") or raw.get("contact_phone", ""),
        "contact_email": raw.get("email") or raw.get("contact_email", ""),
        "contact_source": raw.get("contact_source", ""),
        "lead_score": int(raw.get("lead_score") or raw.get("score") or 0),
        "thermal_anomaly_k": raw.get("thermal_anomaly_k"),
        "building_footprint_id": raw.get("building_footprint_id", ""),
        "raw": raw,
    }


# ─── Public API ─────────────────────────────────────────────────────
def fetch_source(source: Source) -> list[dict]:
    """Run the underlying agent and return normalized lead dicts."""
    try:
        agent = _get_agent(source)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Source %s unavailable: %s", source.kind, exc)
        return []

    try:
        raw_leads = list(agent.fetch_leads())
    except Exception as exc:  # noqa: BLE001
        logger.exception("Source %s fetch failed: %s", source.kind, exc)
        raise

    return [_normalize(r, source) for r in raw_leads if r]
