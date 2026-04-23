"""
Commercial construction-bid aggregator — stub.

ConstructConnect, Dodge Data, BidClerk, iSqFt all gate their APIs and web
UIs behind paid subscriptions. We ship a safe no-op stub and leave it to
the operator to plug in credentials: either fill in this agent with an
authenticated client, or run a ``Source(kind="scrapy")`` spider that
handles login. For now the web_search agent + SAM.gov cover most openly
available construction bids.
"""
from __future__ import annotations

import logging
from typing import Iterable

from .base import BidCandidate, ContractDiscoveryAgent

logger = logging.getLogger("agents.contract_discovery.construction")


class ConstructionBidsAgent(ContractDiscoveryAgent):
    provider = "construction"

    def discover(self, config: dict | None = None) -> Iterable[BidCandidate]:
        logger.info(
            "construction bids agent is a stub — add credentials and an "
            "authenticated HTTP client, or wire a Scrapy spider."
        )
        return []
