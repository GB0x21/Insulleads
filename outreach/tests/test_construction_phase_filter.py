"""
Tests for the construction-agent phase whitelist + the
pipeline-boundary mirror of it.

The bug under test: pre-v9, construction-kind leads with `phase` in
('drywall', 'insulation', 'final') were sent to Telegram even though
by drywall the wall is closed and by `final` the project is done —
useless to an insulation sub. The fix is a whitelist
(`PHASES_INCLUDE`) defaulting to foundation/framing/rough_mep.
"""
from __future__ import annotations

import pytest


def test_default_phases_include_only_pre_insulation():
    """The shipped default whitelist drops drywall / insulation /
    final — those are the phases the operator complained about."""
    from agents import construction_agent as ca

    assert "framing" in ca.PHASES_INCLUDE
    assert "rough_mep" in ca.PHASES_INCLUDE
    assert "foundation" in ca.PHASES_INCLUDE
    # The bad ones must be absent by default.
    assert "drywall" not in ca.PHASES_INCLUDE
    assert "insulation" not in ca.PHASES_INCLUDE
    assert "final" not in ca.PHASES_INCLUDE


def test_pipeline_drops_construction_lead_outside_whitelist(monkeypatch):
    """Defense-in-depth: even if a Lead with phase=drywall sneaks into
    the pipeline (CSV import, future agent, …), the boundary filter
    drops it before it becomes a Lead row."""
    from outreach.pipeline import sources as sources_mod
    from agents import construction_agent as ca

    # Force the whitelist to a known shape so the test is deterministic
    # regardless of env state.
    monkeypatch.setattr(ca, "PHASES_INCLUDE", {"foundation", "framing", "rough_mep"})

    junk = {
        "id": "SF_drywall_001",
        "city": "San Francisco",
        "phase": "drywall",
        "address": "100 Main St",
        "value_float": 250_000,
    }
    real = {
        "id": "SF_framing_002",
        "city": "San Francisco",
        "phase": "framing",
        "address": "200 Oak Ave",
        "value_float": 250_000,
    }

    class _StubAgent:
        def fetch_leads(self):
            return [junk, real]

    class _FakeSource:
        key = "construction"
        kind = "construction"
        config = {}

        def get_kind_display(self):
            return "Active construction"

    sources_mod._AGENT_CACHE["construction"] = _StubAgent()
    try:
        leads = sources_mod.fetch_source(_FakeSource())
    finally:
        sources_mod._AGENT_CACHE.pop("construction", None)

    assert len(leads) == 1
    assert leads[0]["external_id"] == "SF_framing_002"


def test_pipeline_keeps_construction_lead_with_no_phase(monkeypatch):
    """If `phase` is missing, we don't apply the whitelist — the lead
    came from a different shape and we can't make a judgement."""
    from outreach.pipeline import sources as sources_mod

    raw = {
        "id": "x", "city": "SF", "phase": "", "address": "1 Main",
        "value_float": 100_000,
    }

    class _StubAgent:
        def fetch_leads(self):
            return [raw]

    class _FakeSource:
        key = "construction"
        kind = "construction"
        config = {}

        def get_kind_display(self):
            return "Active construction"

    sources_mod._AGENT_CACHE["construction"] = _StubAgent()
    try:
        leads = sources_mod.fetch_source(_FakeSource())
    finally:
        sources_mod._AGENT_CACHE.pop("construction", None)

    assert len(leads) == 1
