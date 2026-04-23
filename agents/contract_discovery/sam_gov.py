"""
SAM.gov opportunities API agent.

Uses the public-beta Opportunities v2 API:
  https://open.gsa.gov/api/get-opportunities-public-api/

Requires a free API key at https://api.data.gov/signup.
Set ``SAM_GOV_API_KEY`` in .env. NAICS filter defaults to 238310
(insulation contractors) + 238990 (other specialty trades); override
with ``SAM_GOV_NAICS_CODES`` or ``source.config["naics"]``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable

import requests
from django.conf import settings

from .base import BidCandidate, ContractDiscoveryAgent

logger = logging.getLogger("agents.contract_discovery.sam_gov")

SAM_GOV_URL = "https://api.sam.gov/opportunities/v2/search"
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_LIMIT = 50
REQUEST_TIMEOUT = 30


class SamGovAgent(ContractDiscoveryAgent):
    """Polls SAM.gov opportunities matching our NAICS codes."""

    provider = "sam_gov"

    def discover(self, config: dict | None = None) -> Iterable[BidCandidate]:
        cfg = config or {}
        contracts_cfg = getattr(settings, "CONTRACTS", {}) or {}
        api_key = cfg.get("api_key") or contracts_cfg.get("SAM_GOV_API_KEY", "")
        if not api_key:
            logger.info("SAM_GOV_API_KEY not configured — skipping")
            return

        naics = cfg.get("naics") or contracts_cfg.get("SAM_GOV_NAICS_CODES") or []
        limit = int(cfg.get("limit") or DEFAULT_LIMIT)
        lookback = int(cfg.get("lookback_days") or DEFAULT_LOOKBACK_DAYS)

        posted_to = datetime.now(timezone.utc)
        posted_from = posted_to - timedelta(days=lookback)

        params = {
            "api_key": api_key,
            "limit": limit,
            "postedFrom": posted_from.strftime("%m/%d/%Y"),
            "postedTo": posted_to.strftime("%m/%d/%Y"),
            "ptype": cfg.get("ptype", "o,k,p"),  # o=solicitation, k=combined synopsis, p=presolicitation
        }
        if naics:
            params["ncode"] = ",".join(str(n) for n in naics)

        try:
            resp = requests.get(SAM_GOV_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("SAM.gov request failed: %s", exc)
            return

        payload = resp.json() or {}
        for item in payload.get("opportunitiesData") or []:
            yield _to_candidate(item)


def _to_candidate(item: dict) -> BidCandidate:
    deadline = _parse_datetime(item.get("responseDeadLine"))
    estimated = _parse_decimal(item.get("award", {}).get("amount") if isinstance(item.get("award"), dict) else None)

    file_urls: list[str] = []
    for res in item.get("resourceLinks") or []:
        if isinstance(res, str) and res:
            file_urls.append(res)
        elif isinstance(res, dict) and res.get("url"):
            file_urls.append(res["url"])

    agency = ""
    office = item.get("officeAddress") or {}
    if isinstance(office, dict):
        agency = item.get("fullParentPathName") or office.get("name") or ""

    city = ""
    pop = item.get("placeOfPerformance")
    if isinstance(pop, dict):
        city = (pop.get("city") or {}).get("name") if isinstance(pop.get("city"), dict) else pop.get("city") or ""

    return BidCandidate(
        external_id=str(item.get("noticeId") or item.get("solicitationNumber") or ""),
        provider="sam_gov",
        title=item.get("title") or "(untitled SAM.gov notice)",
        agency=agency or item.get("department", ""),
        source_url=item.get("uiLink") or "",
        deadline=deadline,
        estimated_value_usd=estimated,
        description=item.get("description", ""),
        city=city or "",
        file_urls=file_urls,
        raw_payload=item,
    )


def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _parse_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
