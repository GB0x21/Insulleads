"""
Scrapy settings for the Insulleads crawler package.

Tuned for polite, low-volume crawling of public data sources (permits portals,
CSLB license search, contractor directories). Values can be overridden per run
via env vars so the daemon can pass SCRAPY_LOG_LEVEL / SCRAPY_USER_AGENT
without touching this file.
"""
from __future__ import annotations

import os
from pathlib import Path

BOT_NAME = "insulleads"

SPIDER_MODULES = ["scrapy_crawlers.spiders"]
NEWSPIDER_MODULE = "scrapy_crawlers.spiders"

ROBOTSTXT_OBEY = True

USER_AGENT = os.getenv(
    "SCRAPY_USER_AGENT",
    "Insulleads-Crawler/1.0 (+https://github.com/gb0x21/insulleads)",
)

CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_DELAY = 0.5

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 20.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
HTTPCACHE_ENABLED = os.getenv("SCRAPY_HTTPCACHE", "1") == "1"
HTTPCACHE_DIR = str(_PROJECT_ROOT / "data" / ".scrapy_httpcache")
HTTPCACHE_EXPIRATION_SECS = 60 * 60 * 6
HTTPCACHE_IGNORE_HTTP_CODES = [429, 500, 502, 503, 504]

ITEM_PIPELINES = {
    "scrapy_crawlers.pipelines.NormalizePipeline": 300,
    "scrapy_crawlers.pipelines.DedupPipeline": 400,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = os.getenv("SCRAPY_LOG_LEVEL", "INFO")
