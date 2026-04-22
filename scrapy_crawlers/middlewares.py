"""
Optional downloader middleware stubs.

The default Scrapy stack (RetryMiddleware + AutoThrottle + HttpCache) already
covers our current needs. This module exists as a hook point for future
additions (proxy rotation, captcha handling, etc.) without needing to touch
`settings.py`.
"""
from __future__ import annotations


class NoopDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response
