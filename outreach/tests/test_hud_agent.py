"""
Tests for `agents.hud_agent` (LIHTC + HUD multifamily).
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from agents import hud_agent as ha


_LIHTC = {
    "features": [
        # CA, ≥8 units, old (rehab candidate)
        {"attributes": {
            "OBJECTID": 1,
            "PROJ_ST": "CA",
            "PROJECT": "Sunset Apartments",
            "PROJ_ADD": "100 Sunset Blvd",
            "PROJ_CTY": "Oakland",
            "N_UNITS": 80,
            "YR_PIS": 1995,
            "PROJ_SPONSOR": "Bay Area Housing LLC",
        }},
        # CA, big, recent — kept but NOT rehab candidate
        {"attributes": {
            "OBJECTID": 2,
            "PROJ_ST": "CA",
            "PROJECT": "Marina View",
            "PROJ_ADD": "50 Marina Way",
            "PROJ_CTY": "San Francisco",
            "N_UNITS": 120,
            "YR_PIS": 2018,
            "PROJ_SPONSOR": "MV Sponsor LP",
        }},
        # Wrong state — dropped
        {"attributes": {
            "OBJECTID": 3,
            "PROJ_ST": "TX",
            "PROJECT": "Houston Lofts",
            "PROJ_ADD": "1 X St",
            "PROJ_CTY": "Houston",
            "N_UNITS": 200,
            "YR_PIS": 1990,
        }},
        # Too small — dropped
        {"attributes": {
            "OBJECTID": 4,
            "PROJ_ST": "CA",
            "PROJECT": "Tiny",
            "PROJ_ADD": "1 Side St",
            "PROJ_CTY": "Oakland",
            "N_UNITS": 4,
            "YR_PIS": 2010,
        }},
    ]
}

_MF = {
    "features": [
        {"attributes": {
            "OBJECTID": 10,
            "STATE": "CA",
            "PROPERTY_NAME_TEXT": "Section 8 Tower",
            "STD_ADDR": "200 Tower Rd",
            "STD_CITY": "Berkeley",
            "TOTAL_ASSISTED_UNIT_COUNT": 60,
            "OWNER_ORG_NAME": "Tower Owner LP",
            "ASSISTANCE_PROGRAM": "Section 8",
        }},
        {"attributes": {
            "OBJECTID": 11,
            "STATE": "CA",
            "PROPERTY_NAME_TEXT": "Studio Cottages",
            "STD_ADDR": "5 Cottage Ln",
            "STD_CITY": "Berkeley",
            "TOTAL_ASSISTED_UNIT_COUNT": 6,  # below 8 — dropped
        }},
    ]
}


@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    monkeypatch.setattr(ha, "HUD_STATE", "CA")
    monkeypatch.setattr(ha, "HUD_MIN_UNITS", 8)
    monkeypatch.setattr(ha, "HUD_REHAB_WINDOW", 20)


# ─── _normalize_lihtc ────────────────────────────────────────────


def test_lihtc_keeps_ca_property_with_units_and_marks_rehab():
    lead = ha._normalize_lihtc(_LIHTC["features"][0]["attributes"], {})
    assert lead is not None
    assert lead["city"] == "Oakland"
    assert lead["units"] == 80
    assert lead["placed_in_service"] == 1995
    assert lead["rehab_candidate"] is True
    assert "rehab window" in lead["description"]
    assert lead["owner"] == "Bay Area Housing LLC"


def test_lihtc_recent_property_is_not_rehab_candidate():
    lead = ha._normalize_lihtc(_LIHTC["features"][1]["attributes"], {})
    assert lead is not None
    assert lead["rehab_candidate"] is False


def test_lihtc_drops_out_of_state():
    assert ha._normalize_lihtc(_LIHTC["features"][2]["attributes"], {}) is None


def test_lihtc_drops_below_min_units():
    assert ha._normalize_lihtc(_LIHTC["features"][3]["attributes"], {}) is None


# ─── _normalize_assisted ─────────────────────────────────────────


def test_multifamily_keeps_section8_with_units():
    lead = ha._normalize_assisted(_MF["features"][0]["attributes"], {})
    assert lead is not None
    assert lead["city"] == "Berkeley"
    assert lead["units"] == 60
    assert lead["program"] == "Section 8"
    assert lead["owner"] == "Tower Owner LP"


def test_multifamily_drops_below_min_units():
    assert ha._normalize_assisted(_MF["features"][1]["attributes"], {}) is None


# ─── End-to-end fetch_leads ──────────────────────────────────────


def test_fetch_leads_combines_both_layers_and_sorts_rehab_first():
    agent = ha.HUDMultifamilyAgent()

    def _fake_get(url, **_kw):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "LIHTC" in url:
            resp.json.return_value = _LIHTC
        else:
            resp.json.return_value = _MF
        return resp

    with patch.object(ha.requests, "get", side_effect=_fake_get):
        leads = agent.fetch_leads()

    # 2 LIHTC kept (Sunset 1995, Marina 2018) + 1 MF kept (Tower)
    assert len(leads) == 3
    # Rehab-window leads come first.
    assert leads[0]["rehab_candidate"] is True
    assert leads[0]["city"] == "Oakland"


def test_fetch_leads_handles_lihtc_failure_independently_of_multifamily():
    agent = ha.HUDMultifamilyAgent()

    def _fake_get(url, **_kw):
        if "LIHTC" in url:
            raise RuntimeError("lihtc layer down")
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = _MF
        return resp

    with patch.object(ha.requests, "get", side_effect=_fake_get):
        leads = agent.fetch_leads()

    assert len(leads) == 1
    assert leads[0]["address"] == "200 Tower Rd"


# ─── Pipeline routing ────────────────────────────────────────────


def test_pipeline_routes_hud_multifamily_kind():
    from outreach.pipeline import sources as sources_mod

    class _FakeSource:
        key = "hud-test"
        kind = "hud_multifamily"
        config = {}

    sources_mod._AGENT_CACHE.clear()
    try:
        backend = sources_mod._get_agent(_FakeSource())
        assert isinstance(backend, ha.HUDMultifamilyAgent)
    finally:
        sources_mod._AGENT_CACHE.clear()
