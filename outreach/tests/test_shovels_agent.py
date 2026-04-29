"""
Tests for `agents.shovels_agent`.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agents import shovels_agent as sa


_PERMITS_LIST = [
    {
        "permit_number": "SF-2026-001",
        "street_number": "100",
        "street_name": "MAIN",
        "street_type": "ST",
        "city": "San Francisco",
        "state": "CA",
        "permit_type": "ADDITION",
        "description": "Bedroom + bath addition, attic R-38",
        "issued_date": "2026-04-15",
        "estimated_cost": 250_000,
        "contractor_name": "Acme Builders Inc",
        "contractor_license": "1098765",
        "owner_name": "Jane Doe",
        "url": "https://city.example/permit/001",
    },
    {
        "permit_id": "LA-2026-901",
        "street_number": "55",
        "street_name": "Sunset",
        "street_type": "Blvd",
        "city": "Los Angeles",
        "state": "CA",
        "permit_type": "REROOF",
        "description": "Re-roof + radiant barrier",
        "issued_date": "2026-03-20",
        "estimated_cost": 80_000,
        "contractor": {"name": "Beta Roofing", "license_number": "555111"},
    },
    # Drop: missing address
    {
        "permit_number": "SAC-2026-X",
        "city": "Sacramento",
        "permit_type": "ADDITION",
        "estimated_cost": 90_000,
    },
]


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setattr(sa, "SHOVELS_API_KEY", "test-key")
    monkeypatch.setattr(sa, "SHOVELS_STATES", ["CA"])
    monkeypatch.setattr(sa, "SHOVELS_MIN_VALUE", 50_000.0)
    monkeypatch.setattr(sa, "SHOVELS_MAX_PER_STATE", 200)


def _mock_response(payload):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


# ─── _normalize ───────────────────────────────────────────────────


def test_normalize_full_record():
    lead = sa._normalize(_PERMITS_LIST[0])
    assert lead["address"] == "100 MAIN ST"
    assert lead["city"] == "San Francisco"
    assert lead["state"] == "CA"
    assert lead["permit_type"] == "ADDITION"
    assert lead["contractor"] == "Acme Builders Inc"
    assert lead["lic_number"] == "1098765"
    assert lead["value_float"] == 250_000
    assert lead["id"].startswith("shovels_CA_SF-2026-001")


def test_normalize_handles_nested_contractor_object():
    lead = sa._normalize(_PERMITS_LIST[1])
    assert lead["contractor"] == "Beta Roofing"
    assert lead["lic_number"] == "555111"


def test_normalize_drops_record_without_address():
    assert sa._normalize(_PERMITS_LIST[2]) is None


# ─── End-to-end fetch_leads ──────────────────────────────────────


def test_fetch_leads_no_api_key_returns_empty(monkeypatch):
    monkeypatch.setattr(sa, "SHOVELS_API_KEY", "")
    assert sa.ShovelsAgent().fetch_leads() == []


def test_fetch_leads_normalizes_top_level_list_payload():
    agent = sa.ShovelsAgent()
    with patch.object(sa.requests, "get", return_value=_mock_response(_PERMITS_LIST)):
        leads = agent.fetch_leads()
    assert len(leads) == 2  # 3 permits, one drops for missing address
    # High value first.
    assert leads[0]["address"] == "100 MAIN ST"
    assert leads[1]["address"] == "55 Sunset Blvd"


def test_fetch_leads_normalizes_envelope_payload():
    """Shovels v2 sometimes wraps results in `{"permits": [...]}`."""
    agent = sa.ShovelsAgent()
    envelope = {"permits": _PERMITS_LIST}
    with patch.object(sa.requests, "get", return_value=_mock_response(envelope)):
        leads = agent.fetch_leads()
    assert len(leads) == 2


def test_fetch_leads_swallows_state_failure():
    agent = sa.ShovelsAgent()
    with patch.object(sa.requests, "get", side_effect=RuntimeError("503")):
        assert agent.fetch_leads() == []


# ─── Pipeline routing ────────────────────────────────────────────


def test_pipeline_routes_shovels_kind():
    from outreach.pipeline import sources as sources_mod

    class _FakeSource:
        key = "shovels-test"
        kind = "shovels"
        config = {}

    sources_mod._AGENT_CACHE.clear()
    try:
        backend = sources_mod._get_agent(_FakeSource())
        assert isinstance(backend, sa.ShovelsAgent)
    finally:
        sources_mod._AGENT_CACHE.clear()
