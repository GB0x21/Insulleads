"""
Tests for the `web_crawler` discovery kind (crawl4ai-backed).

We don't import crawl4ai itself — the unit tests stub the async crawl so
they run anywhere, even without Playwright/Chromium installed. The routing
test mirrors `test_sources_routing.py` for the scrapy kind.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from agents.web_crawler_agent import WebCrawlerAgent
from outreach.pipeline import sources as sources_mod


# ─── Fakes ────────────────────────────────────────────────────────────


class _FakeSource:
    def __init__(self, key, kind, config=None):
        self.key = key
        self.kind = kind
        self.config = config or {}


def _fake_result(items: list[dict]):
    class _R:
        extracted_content = json.dumps(items)

    return _R()


# ─── Setup ────────────────────────────────────────────────────────────


def setup_function(_fn):
    sources_mod._AGENT_CACHE.clear()


# ─── Routing ──────────────────────────────────────────────────────────


def test_get_agent_returns_web_crawler():
    src = _FakeSource(
        key="bay-contractors",
        kind="web_crawler",
        config={
            "urls": ["https://example.com/list"],
            "schema": {
                "name": "Contractors",
                "baseSelector": ".card",
                "fields": [{"name": "business_name", "selector": "h3", "type": "text"}],
            },
            "city_default": "San Francisco",
        },
    )

    backend = sources_mod._get_agent(src)

    assert isinstance(backend, WebCrawlerAgent)
    assert backend.urls == ["https://example.com/list"]
    assert backend.city_default == "San Francisco"
    assert backend.schema["baseSelector"] == ".card"


def test_get_agent_rejects_web_crawler_without_urls():
    src = _FakeSource(key="broken", kind="web_crawler", config={})
    with pytest.raises(ValueError, match="config.urls"):
        sources_mod._get_agent(src)


def test_get_agent_caches_per_source_key():
    src = _FakeSource(
        key="a",
        kind="web_crawler",
        config={"urls": ["https://example.com/a"], "schema": {"baseSelector": "x"}},
    )
    a1 = sources_mod._get_agent(src)
    a2 = sources_mod._get_agent(src)
    assert a1 is a2


# ─── Lead normalization ───────────────────────────────────────────────


def test_result_to_leads_parses_json_extraction():
    agent = WebCrawlerAgent(
        urls=["https://example.com/list"],
        schema={"baseSelector": ".x"},
        city_default="Oakland",
    )
    result = _fake_result(
        [
            {"business_name": "Acme Insulation", "phone": "555-1234"},
            {"name": "Beta Roofing", "address": "1 Main St"},
            {},  # filtered out (no name)
        ]
    )

    leads = agent._result_to_leads("https://example.com/list", result)

    assert len(leads) == 2
    assert leads[0]["business_name"] == "Acme Insulation"
    assert leads[0]["contact_phone"] == "555-1234"
    assert leads[0]["city"] == "Oakland"  # falls back to city_default
    assert leads[0]["source_url"] == "https://example.com/list"
    assert leads[0]["id"].startswith("web_")
    assert leads[1]["business_name"] == "Beta Roofing"
    assert leads[1]["address"] == "1 Main St"


def test_result_to_leads_handles_non_json():
    agent = WebCrawlerAgent(urls=["x"], schema={"baseSelector": "x"})

    class _R:
        extracted_content = "not json"

    assert agent._result_to_leads("x", _R()) == []


def test_stable_id_is_deterministic():
    from agents.web_crawler_agent import _stable_id

    a = _stable_id("https://e.com", {"business_name": "Acme"})
    b = _stable_id("https://e.com", {"business_name": "Acme"})
    c = _stable_id("https://e.com", {"business_name": "Beta"})
    assert a == b
    assert a != c


# ─── Soft-fail when crawl4ai is missing ───────────────────────────────


def test_fetch_leads_no_urls_returns_empty():
    assert WebCrawlerAgent(urls=[], schema={"x": 1}).fetch_leads() == []


def test_fetch_leads_no_schema_returns_empty():
    assert WebCrawlerAgent(urls=["https://x"], schema={}).fetch_leads() == []


def test_fetch_leads_missing_crawl4ai_returns_empty():
    """If crawl4ai isn't installed, the agent must return [] without raising."""
    import builtins

    real_import = builtins.__import__

    def _fail_crawl4ai(name, *a, **kw):
        if name == "crawl4ai" or name.startswith("crawl4ai."):
            raise ImportError("crawl4ai not installed")
        return real_import(name, *a, **kw)

    agent = WebCrawlerAgent(
        urls=["https://example.com"], schema={"baseSelector": "x"}
    )
    with patch.object(builtins, "__import__", side_effect=_fail_crawl4ai):
        assert agent.fetch_leads() == []
