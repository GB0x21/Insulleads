"""
Shared base spider for Insulleads crawlers.

Provides a couple of helpers so individual spiders only need to implement
`parse`. The `source_key` argument is passed from `ScrapySource` so items can
be traced back to the Django `Source` row that triggered the crawl.
"""
from __future__ import annotations

import hashlib
import re

import scrapy


class BaseInsulleadsSpider(scrapy.Spider):
    source_key: str = ""

    def __init__(self, source_key: str = "", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_key = source_key or self.name

    @staticmethod
    def parse_money(value) -> float | None:
        if value in (None, ""):
            return None
        cleaned = re.sub(r"[^\d.]", "", str(value))
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    def build_external_id(self, *parts: str) -> str:
        joined = "|".join(p for p in parts if p)
        if not joined:
            joined = self.name
        digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
        return f"{self.name}-{digest}"
