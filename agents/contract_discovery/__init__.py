"""
Contract-discovery agents.

Each agent discovers open bids / RFPs from a specific provider (SAM.gov,
state portals, construction aggregators, Claude web_search) and yields
:class:`BidCandidate` rows. The ``discover_contracts`` task consumes those
rows, persists Lead + Contract, and enqueues the analysis task.
"""
from .base import BidCandidate, ContractDiscoveryAgent, get_discovery_agent

__all__ = ["BidCandidate", "ContractDiscoveryAgent", "get_discovery_agent"]
