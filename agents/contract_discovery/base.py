"""
Shared base classes for contract-discovery agents.

Each concrete agent implements :meth:`discover` which yields
:class:`BidCandidate` rows — a normalised shape the ``discover_contracts``
task persists into Lead + Contract. The factory :func:`get_discovery_agent`
is the single dispatch point, keyed on ``source.config["provider"]`` for
``Source(kind="contract_bids")`` rows.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Iterable


@dataclass
class BidCandidate:
    """One open bid / RFP emitted by a discovery agent.

    ``external_id`` must be stable across polls — the pipeline uses
    ``(source, external_id)`` to de-duplicate across runs.
    """

    external_id: str
    provider: str
    title: str
    agency: str = ""
    source_url: str = ""
    deadline: datetime | None = None
    estimated_value_usd: Decimal | None = None
    description: str = ""
    city: str = ""
    file_urls: list[str] = field(default_factory=list)
    raw_payload: dict = field(default_factory=dict)


class ContractDiscoveryAgent(ABC):
    """Abstract interface every contract-discovery agent must satisfy."""

    provider: str = "base"

    @abstractmethod
    def discover(self, config: dict | None = None) -> Iterable[BidCandidate]:
        """Yield candidate bids. May hit the network. Raise on hard errors."""


def get_discovery_agent(provider: str) -> ContractDiscoveryAgent:
    """Dispatch to a concrete agent by provider name.

    Imports are lazy so configurations that don't use a given provider
    don't pay the import cost (SDKs, credentials, etc.).
    """
    key = (provider or "").lower().strip()
    if key == "sam_gov":
        from .sam_gov import SamGovAgent

        return SamGovAgent()
    if key == "web_search":
        from .web_search import WebSearchBidsAgent

        return WebSearchBidsAgent()
    if key == "state_bids":
        from .state_bids import StateBidsAgent

        return StateBidsAgent()
    if key in ("construction", "construction_bids"):
        from .construction import ConstructionBidsAgent

        return ConstructionBidsAgent()
    raise ValueError(
        f"Unknown contract-discovery provider {provider!r}. Known: "
        "sam_gov, web_search, state_bids, construction"
    )
