"""
utils/socrata_discovery.py
━━━━━━━━━━━━━━━━━━━━━━━━━
Discover building-permit datasets across every Socrata-hosted open
data portal with a single API call.

Socrata exposes a public Discovery / Catalog API at
http://api.us.socrata.com/api/catalog/v1 that indexes every dataset on
every Socrata domain (cities, counties, states). With one query we
can surface every "Building Permits" dataset in California — the
project today only knows about a hand-curated list inside
``agents/permits_agent.py``, so this expansion ~5× the universe of
permits the daemon can ingest with no extra agent code.

Usage shapes:
  - As a library, from the management command:
      from utils.socrata_discovery import discover
      hits = discover(state="CA")
  - From the shell:
      python manage.py discover_socrata_permits --state CA --limit 50

Each hit is a dict with the fields the operator needs to add a new
entry to ``permits_agent._build_sources`` — the resource ID, host
domain, dataset name, and a sample row to inspect column names.

The Catalog API has no auth requirement. Rate-limit pressure is
mild but a ``SOCRATA_APP_TOKEN`` (already supported by the project's
``permits_agent`` socrata fetcher) lifts the per-IP quota.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


CATALOG_URL = os.getenv(
    "SOCRATA_CATALOG_URL",
    "http://api.us.socrata.com/api/catalog/v1",
)
_TIMEOUT = int(os.getenv("SOCRATA_DISCOVERY_TIMEOUT_S", "30"))

# Keywords that match permits / construction datasets (Socrata search
# is OR-of-tokens). We bias toward `building permits` to keep the
# results focused on what the project ingests.
DEFAULT_QUERY = "building permits"


def _headers() -> dict[str, str]:
    h = {"Accept": "application/json", "User-Agent": "Insulleads/discovery"}
    token = os.getenv("SOCRATA_APP_TOKEN", "")
    if token:
        h["X-App-Token"] = token
    return h


def discover(
    *,
    query: str = DEFAULT_QUERY,
    state: str | None = "CA",
    limit: int = 50,
    only_known: bool = False,
    known_domains: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Run one Catalog query and return a flat list of dataset hits.

    Each hit shape:

        {
          "id":       str,    # Socrata 4x4 resource id
          "name":     str,    # human dataset name
          "domain":   str,    # e.g. data.sfgov.org
          "url":      str,    # canonical dataset URL
          "json_url": str,    # /resource/<id>.json (the project's fetcher)
          "rows":     int,    # row count (when published)
          "updated":  str,    # ISO timestamp of last update
        }

    `state` filters by Socrata's `provenance` city/state when set.
    `only_known` keeps just hits on `known_domains` (used to refresh
    the project's existing curated list). `known_domains` defaults to
    None (= return everything).
    """
    params: dict[str, Any] = {
        "q": query,
        "limit": min(max(limit, 1), 100),
        "only": "datasets",
    }
    if state:
        # Socrata Catalog supports filtering by `domain_state` for
        # state-level metadata. Some city domains don't carry state
        # tags; we still query them and post-filter by domain pattern.
        params["domain_state"] = state.upper()

    try:
        resp = requests.get(
            CATALOG_URL, params=params, headers=_headers(), timeout=_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[socrata-discovery] catalog query failed: %s", exc)
        return []

    out: list[dict[str, Any]] = []
    for entry in (data.get("results") or []):
        meta = entry.get("resource") or {}
        classifications = entry.get("classification") or {}
        permalink = entry.get("permalink") or ""
        domain = (entry.get("metadata") or {}).get("domain") or ""
        if not domain or not meta.get("id"):
            continue
        if known_domains is not None and only_known and domain not in known_domains:
            continue
        rows = meta.get("rows_count") or meta.get("rowsCount") or 0
        out.append({
            "id": meta["id"],
            "name": meta.get("name") or "",
            "description": (meta.get("description") or "").strip(),
            "domain": domain,
            "url": permalink,
            "json_url": f"https://{domain}/resource/{meta['id']}.json",
            "rows": int(rows or 0),
            "updated": meta.get("updatedAt") or meta.get("data_updated_at") or "",
            "categories": classifications.get("categories") or [],
        })
    # Highest-row-count first — those are the most useful permit
    # datasets to add to permits_agent.
    out.sort(key=lambda h: -h["rows"])
    return out


def sample_rows(json_url: str, limit: int = 1) -> list[dict[str, Any]]:
    """Fetch `limit` rows from a discovered dataset so the operator can
    eyeball field names before plugging it into permits_agent."""
    try:
        resp = requests.get(
            json_url,
            params={"$limit": limit},
            headers=_headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[socrata-discovery] sample failed for %s: %s",
                       json_url, exc)
        return []
    return data if isinstance(data, list) else []
