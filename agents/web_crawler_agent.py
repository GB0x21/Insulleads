"""
agents/web_crawler_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
🕸️  Crawl4AI — LLM-friendly web crawler for sites without an API.

Wraps `crawl4ai.AsyncWebCrawler` with `JsonCssExtractionStrategy` so the
Insulleads daemon can scrape contractor directories, association pages and
similar HTML-only targets, then emit lead dicts compatible with the legacy
`BaseAgent.fetch_leads()` contract used by `outreach/pipeline/sources.py`.

Configuration is driven entirely by the `Source.config` JSON (or by kwargs
when called standalone), so the agent itself stays generic:

    {
      "urls": ["https://example.com/contractors"],
      "schema": {
        "name": "Contractors",
        "baseSelector": ".contractor-card",
        "fields": [
          {"name": "business_name", "selector": "h3", "type": "text"},
          {"name": "address",       "selector": ".addr", "type": "text"},
          {"name": "phone",         "selector": ".tel",  "type": "text"},
          {"name": "website",       "selector": "a",     "type": "attribute",
           "attribute": "href"}
        ]
      },
      "city_default": "San Francisco",
      "max_concurrent": 4,
      "timeout_s": 60,
      "http_only": false
    }

When `http_only=True`, the agent uses `AsyncHTTPCrawlerStrategy` (pure
aiohttp, no Playwright/Chromium) — much faster for static / server-rendered
HTML, and the only viable mode in environments where Chromium isn't
available. Default is `http_only=False`, which uses the full Playwright
stack so JS-rendered sites work too.

When `crawl4ai` is not installed, `fetch_leads()` logs a warning and returns
an empty list, matching the soft-fail pattern used by the rest of the agent
stack (see `agents/yelp_agent.py`, `agents/places_agent.py`, ...).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any

logger = logging.getLogger("agents.web_crawler")


def _stable_id(url: str, item: dict[str, Any]) -> str:
    """Deterministic id from (url, business_name|first non-empty field)."""
    name = (
        item.get("business_name")
        or item.get("name")
        or item.get("title")
        or next((str(v) for v in item.values() if v), "")
    )
    digest = hashlib.sha1(f"{url}|{name}".encode("utf-8")).hexdigest()[:16]
    return f"web_{digest}"


class WebCrawlerAgent:
    """
    Generic crawl4ai-backed discovery agent.

    Usage (standalone):
        agent = WebCrawlerAgent(
            urls=["https://example.com/contractors"],
            schema={...},
            city_default="San Francisco",
        )
        leads = agent.fetch_leads()

    Usage (Django pipeline):
        Source.kind="web_crawler" with the same kwargs in `Source.config`.
    """

    name = "🕸️ Web Crawler (crawl4ai)"
    emoji = "🕸️"
    agent_key = "web_crawler"

    def __init__(
        self,
        urls: list[str] | None = None,
        schema: dict[str, Any] | None = None,
        city_default: str = "",
        max_concurrent: int = 4,
        timeout_s: int = 60,
        http_only: bool = False,
    ) -> None:
        self.urls = list(urls or [])
        self.schema = schema or {}
        self.city_default = city_default
        self.max_concurrent = max(1, int(max_concurrent))
        self.timeout_s = max(5, int(timeout_s))
        self.http_only = bool(http_only)

    # ─── Public API (matches BaseAgent contract) ──────────────────────

    def fetch_leads(self) -> list[dict[str, Any]]:
        if not self.urls:
            logger.info("[web_crawler] no urls configured — skipping")
            return []
        if not self.schema:
            logger.info("[web_crawler] no extraction schema configured — skipping")
            return []

        try:
            return asyncio.run(self._crawl_all())
        except RuntimeError as exc:
            # asyncio.run inside an existing loop (e.g. some test harnesses).
            if "running event loop" in str(exc):
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(self._crawl_all())
                finally:
                    loop.close()
            raise

    # ─── Internals ────────────────────────────────────────────────────

    async def _crawl_all(self) -> list[dict[str, Any]]:
        try:
            from crawl4ai import (  # type: ignore[import-not-found]
                AsyncWebCrawler,
                CrawlerRunConfig,
                JsonCssExtractionStrategy,
            )
        except ImportError:
            logger.warning(
                "[web_crawler] crawl4ai not installed — "
                "`pip install crawl4ai && crawl4ai-setup`. Returning 0 leads."
            )
            return []

        strategy = JsonCssExtractionStrategy(self.schema)
        run_config = CrawlerRunConfig(extraction_strategy=strategy)

        crawler_kwargs: dict[str, Any] = {}
        if self.http_only:
            try:
                from crawl4ai.async_crawler_strategy import (  # type: ignore[import-not-found]
                    AsyncHTTPCrawlerStrategy,
                )
                crawler_kwargs["crawler_strategy"] = AsyncHTTPCrawlerStrategy()
            except ImportError:
                logger.warning(
                    "[web_crawler] http_only=True but AsyncHTTPCrawlerStrategy "
                    "is unavailable — falling back to default Playwright strategy."
                )

        sem = asyncio.Semaphore(self.max_concurrent)
        leads: list[dict[str, Any]] = []

        async with AsyncWebCrawler(**crawler_kwargs) as crawler:
            async def _one(url: str) -> list[dict[str, Any]]:
                async with sem:
                    try:
                        result = await asyncio.wait_for(
                            crawler.arun(url=url, config=run_config),
                            timeout=self.timeout_s,
                        )
                    except asyncio.TimeoutError:
                        logger.warning("[web_crawler] timeout on %s", url)
                        return []
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("[web_crawler] %s failed: %s", url, exc)
                        return []
                return self._result_to_leads(url, result)

            tasks = [_one(u) for u in self.urls]
            for batch in await asyncio.gather(*tasks):
                leads.extend(batch)

        logger.info("[web_crawler] %d leads from %d urls", len(leads), len(self.urls))
        return leads

    def _result_to_leads(self, url: str, result: Any) -> list[dict[str, Any]]:
        """Parse a crawl4ai result into normalized lead dicts."""
        raw = getattr(result, "extracted_content", None)
        if not raw:
            return []

        # extracted_content is a JSON string when JsonCssExtractionStrategy
        # is used; fall back to passing through dicts/lists if a future
        # crawl4ai version returns Python objects directly.
        if isinstance(raw, str):
            import json

            try:
                items = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[web_crawler] non-JSON extraction from %s", url)
                return []
        else:
            items = raw

        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            return []

        leads: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            lead = self._item_to_lead(url, item)
            if lead:
                leads.append(lead)
        return leads

    def _item_to_lead(self, url: str, item: dict[str, Any]) -> dict[str, Any] | None:
        name = (
            item.get("business_name")
            or item.get("name")
            or item.get("title")
        )
        if not name:
            return None

        return {
            "id":             _stable_id(url, item),
            "external_id":    _stable_id(url, item),
            "business_name":  str(name).strip(),
            "title":          str(name).strip(),
            "city":           item.get("city") or self.city_default,
            "address":        item.get("address") or item.get("street_address", ""),
            "phone":          item.get("phone", ""),
            "contact_phone":  item.get("phone", ""),
            "contact_email":  item.get("email", ""),
            "contact_company": item.get("company") or str(name).strip(),
            "website":        item.get("website") or item.get("url", ""),
            "description":    item.get("description", ""),
            "category":       item.get("category", ""),
            "source_url":     url,
            "contact_source": "web_crawler",
            "_agent_key":     self.agent_key,
            "raw":            item,
        }
