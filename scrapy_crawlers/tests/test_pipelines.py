"""
Unit tests for `scrapy_crawlers.pipelines`. No network, no Django.
"""
from __future__ import annotations

import pytest
from scrapy.exceptions import DropItem

from scrapy_crawlers.items import LeadItem
from scrapy_crawlers.pipelines import DedupPipeline, NormalizePipeline


def _item(**fields) -> LeadItem:
    item = LeadItem()
    for k, v in fields.items():
        item[k] = v
    return item


def test_normalize_pipeline_fills_defaults_and_coerces_types():
    pipeline = NormalizePipeline()
    item = _item(
        external_id=42,
        title="  Long " + "x" * 400 + "  ",
        project_value="$ 1,234.56",
        latitude="37.77",
        longitude=None,
        lead_score="5",
        raw={"source_field": "value"},
    )

    out = pipeline.process_item(item, spider=None)

    assert out["external_id"] == "42"
    assert out["title"].startswith("Long")
    assert len(out["title"]) == 200
    # The NormalizePipeline does not strip currency formatting on its own;
    # spiders are expected to use `BaseInsulleadsSpider.parse_money`. We just
    # verify that a non-numeric project_value becomes None rather than raising.
    assert out["project_value"] is None
    assert out["latitude"] == pytest.approx(37.77)
    assert out["longitude"] is None
    assert out["lead_score"] == 5
    assert out["address"] == ""
    assert out["raw"] == {"source_field": "value"}


def test_normalize_pipeline_drops_items_without_external_id():
    pipeline = NormalizePipeline()
    with pytest.raises(DropItem):
        pipeline.process_item(_item(title="no id here"), spider=None)


def test_dedup_pipeline_drops_repeats():
    pipeline = DedupPipeline()
    pipeline.process_item(_item(external_id="a"), spider=None)
    with pytest.raises(DropItem):
        pipeline.process_item(_item(external_id="a"), spider=None)
    # Different id passes through.
    out = pipeline.process_item(_item(external_id="b"), spider=None)
    assert out["external_id"] == "b"
