"""
Item pipelines shared by every Insulleads spider.

- NormalizePipeline: coerce types, fill defaults, trim overlong strings so the
  Django `Lead` model never sees a malformed payload.
- DedupPipeline: drop repeated `external_id`s inside a single crawl (DB-level
  dedup is handled by `Lead.objects.update_or_create`).
"""
from __future__ import annotations

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


def _to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value, default=0):
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


class NormalizePipeline:
    TEXT_DEFAULTS = (
        "title",
        "address",
        "city",
        "project_type",
        "description",
        "contact_name",
        "contact_company",
        "contact_phone",
        "contact_email",
        "contact_source",
    )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        external_id = adapter.get("external_id")
        if not external_id:
            raise DropItem("missing external_id")
        adapter["external_id"] = str(external_id)

        for key in self.TEXT_DEFAULTS:
            adapter[key] = (adapter.get(key) or "").strip()

        if adapter.get("title"):
            adapter["title"] = adapter["title"][:200]

        adapter["project_value"] = _to_float(adapter.get("project_value"))
        adapter["latitude"] = _to_float(adapter.get("latitude"))
        adapter["longitude"] = _to_float(adapter.get("longitude"))
        adapter["lead_score"] = _to_int(adapter.get("lead_score"), default=0)

        raw = adapter.get("raw")
        adapter["raw"] = dict(raw) if isinstance(raw, dict) else {}

        return item


class DedupPipeline:
    def __init__(self):
        self._seen: set[str] = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        ext_id = adapter["external_id"]
        if ext_id in self._seen:
            raise DropItem(f"duplicate external_id: {ext_id}")
        self._seen.add(ext_id)
        return item
