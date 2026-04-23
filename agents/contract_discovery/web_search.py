"""
Claude web_search discovery agent.

Reuses the Anthropic client already wired in :mod:`outreach.llm.client`:
the same ``web_search_20250305`` tool the enricher uses. We ask Claude to
find open insulation/construction bids posted recently and return a JSON
list of candidates that we turn into :class:`BidCandidate` rows.

No web scraping — Anthropic's tool handles retrieval. The call is capped
at a small number of tool uses to bound latency and cost.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable

from django.conf import settings

from outreach.llm import get_adapter

from .base import BidCandidate, ContractDiscoveryAgent

logger = logging.getLogger("agents.contract_discovery.web_search")


SYSTEM_PROMPT = """You are a bid-discovery scout for a U.S. insulation and \
construction contracting business. Use web search to find CURRENTLY OPEN \
bids, RFPs, or solicitations (posted in the last 14 days, not yet awarded) \
that match the contractor's scope: commercial insulation, weatherization, \
HVAC, building envelope, energy retrofits, federal/state construction.

Rules:
- Prioritise official portals: SAM.gov, state procurement sites, city \
  e-procurement systems. Aggregators like BidNet or ConstructConnect are ok.
- Only include bids with a clear title, issuing agency, and source URL.
- Skip anything already awarded, cancelled, or past its response deadline.
- Never invent URLs or dollar amounts.

Return ONLY a JSON array, no prose, each item shaped:

{
  "external_id":  "<solicitation number or stable id, required>",
  "title":        "<required>",
  "agency":       "<issuing agency>",
  "source_url":   "<https://...>",
  "deadline":     "<ISO-8601 date if known, else empty string>",
  "estimated_value_usd": "<number or empty string>",
  "city":         "<location if known>",
  "description":  "<1-2 sentence summary>"
}
"""


class WebSearchBidsAgent(ContractDiscoveryAgent):
    """Bid discovery via Claude's web_search tool."""

    provider = "web_search"

    def discover(self, config: dict | None = None) -> Iterable[BidCandidate]:
        cfg = config or {}
        contracts_cfg = getattr(settings, "CONTRACTS", {}) or {}
        if not contracts_cfg.get("WEB_SEARCH_ENABLED", True):
            return

        adapter = get_adapter()
        # Only the AnthropicAdapter exposes ``_client`` + web_search — skip
        # otherwise so the task loop never crashes with NoopAdapter.
        client = getattr(adapter, "_client", None)
        if client is None:
            logger.info("web_search bids: LLM adapter is %s, skipping", adapter.name)
            return

        model = contracts_cfg.get("ORCHESTRATOR_MODEL") or "claude-opus-4-6"
        user_prompt = cfg.get("query") or (
            "Find open bids / RFPs posted in the last 14 days for commercial "
            "insulation, weatherization, HVAC, or building-envelope scope in "
            "the United States. Return up to 12 results."
        )

        try:
            resp = client.messages.create(
                model=model,
                max_tokens=4000,
                system=[
                    {"type": "text", "text": SYSTEM_PROMPT}
                ],
                tools=[
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": int(cfg.get("max_uses") or 6),
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("web_search bids call failed: %s", exc)
            return

        text_blob = _collect_text(resp)
        items = _parse_json_list(text_blob)
        for item in items:
            cand = _to_candidate(item)
            if cand is not None:
                yield cand


def _collect_text(response) -> str:
    out = []
    for block in response.content:
        text = getattr(block, "text", None)
        if text:
            out.append(text)
    return "".join(out).strip()


def _parse_json_list(text: str) -> list[dict]:
    text = (text or "").strip()
    if not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if not match:
            return []
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError:
            return []
    if isinstance(value, dict):
        value = value.get("results") or value.get("items") or []
    return [v for v in value if isinstance(v, dict)]


def _to_candidate(item: dict) -> BidCandidate | None:
    ext = (item.get("external_id") or item.get("id") or item.get("source_url") or "").strip()
    title = (item.get("title") or "").strip()
    if not ext or not title:
        return None
    return BidCandidate(
        external_id=ext[:200],
        provider="web_search",
        title=title[:500],
        agency=(item.get("agency") or "")[:300],
        source_url=(item.get("source_url") or "")[:1000],
        deadline=_parse_date(item.get("deadline")),
        estimated_value_usd=_parse_decimal(item.get("estimated_value_usd")),
        description=(item.get("description") or "")[:2000],
        city=(item.get("city") or "")[:128],
        file_urls=[u for u in (item.get("file_urls") or []) if isinstance(u, str)],
        raw_payload=item,
    )


def _parse_date(value) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_decimal(value) -> Decimal | None:
    if value in (None, "", "null"):
        return None
    try:
        return Decimal(str(value).replace(",", "").replace("$", ""))
    except (InvalidOperation, TypeError, ValueError):
        return None
