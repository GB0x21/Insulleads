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
