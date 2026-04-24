"""
State/local bid aggregator — stub.

Most state e-procurement portals (CA Cal eProcure, TX ESBD, NY OGS,
FL DMS, etc.) either expose no public JSON API or require a login plus
per-portal scraping. Rather than shipping a broken scraper we ship a
safe no-op stub and route the same work through SAM.gov + web_search,
which already cover a large fraction of state bids through aggregation.

To implement a real state scraper, add a Scrapy spider under
``scrapy_crawlers/spiders/`` and register it as a separate
``Source(kind="scrapy")`` — that path is already exercised by
``outreach/pipeline/scrapy_source.py``.
"""
from __future__ import annotations

import logging
from typing import Iterable

from .base import BidCandidate, ContractDiscoveryAgent

logger = logging.getLogger("agents.contract_discovery.state_bids")


class StateBidsAgent(ContractDiscoveryAgent):
    provider = "state_bids"

    def discover(self, config: dict | None = None) -> Iterable[BidCandidate]:
        logger.info(
            "state_bids is a stub — use a Scrapy spider Source for a specific "
            "state portal, or rely on SAM.gov + web_search coverage."
        )
        return []
