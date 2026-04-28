"""
Verify `_get_agent` dispatches kind="scrapy" to a ScrapySource without
touching the legacy `agents/` package.
"""
from __future__ import annotations

import pytest

from outreach.pipeline import sources as sources_mod
from outreach.pipeline.scrapy_source import ScrapySource


class _FakeSource:
    def __init__(self, key, kind, config=None):
        self.key = key
        self.kind = kind
        self.config = config or {}


def setup_function(_fn):
    sources_mod._AGENT_CACHE.clear()


def test_get_agent_returns_scrapy_source():
    src = _FakeSource(
        key="cslb-demo",
        kind="scrapy",
        config={"spider": "cslb_contractors", "spider_args": {"license_numbers": "1"}},
    )

    backend = sources_mod._get_agent(src)

    assert isinstance(backend, ScrapySource)
    assert backend.spider_name == "cslb_contractors"
    assert backend.spider_args == {"license_numbers": "1"}
    assert backend.source_key == "cslb-demo"


def test_get_agent_rejects_scrapy_source_without_spider():
    src = _FakeSource(key="broken", kind="scrapy", config={})
    with pytest.raises(ValueError, match="config.spider"):
        sources_mod._get_agent(src)


def test_get_agent_caches_per_source_key():
    src_a = _FakeSource(
        key="a", kind="scrapy", config={"spider": "cslb_contractors"}
    )
    src_b = _FakeSource(
        key="b", kind="scrapy", config={"spider": "cslb_contractors"}
    )
    a1 = sources_mod._get_agent(src_a)
    a2 = sources_mod._get_agent(src_a)
    b1 = sources_mod._get_agent(src_b)
    assert a1 is a2
    assert a1 is not b1


# ─── Quality filter at the pipeline boundary ────────────────────────


class _StubPermitsAgent:
    """In-memory stand-in for `agents.permits_agent.PermitsAgent` —
    `fetch_source` calls `.fetch_leads()` so we just return the canned
    list of raw permit dicts."""

    def __init__(self, leads):
        self._leads = leads

    def fetch_leads(self):
        return list(self._leads)


def test_pipeline_drops_planning_only_permits(monkeypatch):
    """Reproduces the operator's Telegram regression: the permit
    'PLAN A (BEMP 100%) LOT 16 / New Construction' should be dropped
    BEFORE it becomes a Lead row, while a real construction permit
    next to it survives."""
    junk = {
        "id": "SJ_PLANA_LOT16",
        "city": "San Jose",
        "permit_type": "PLAN A (BEMP 100%) LOT 16",
        "description": "New Construction",
        "value_float": 316_000,
        "address": "29 Silver Elm Ct",
    }
    real = {
        "id": "SF_REAL_001",
        "city": "San Francisco",
        "permit_type": "additions alterations or repairs",
        "description": "Rough-in attic insulation R-38, walls R-15",
        "value_float": 80_000,
        "address": "100 Main St",
    }

    src = _FakeSource(key="permits", kind="permits")
    src.get_kind_display = lambda: "Building permits"  # used by _normalize

    sources_mod._AGENT_CACHE["permits"] = _StubPermitsAgent([junk, real])
    try:
        leads = sources_mod.fetch_source(src)
    finally:
        sources_mod._AGENT_CACHE.pop("permits", None)

    assert len(leads) == 1
    assert leads[0]["external_id"] == "SF_REAL_001"
