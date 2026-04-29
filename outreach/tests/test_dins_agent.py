"""
Tests for `agents.dins_agent`.

Cal Fire's ArcGIS endpoints are unreachable from the test sandbox, so
we feed the agent canned ArcGIS-shaped JSON via a fake `requests.get`.
The behaviour under test: every "Destroyed" / "Major" damage row turns
into one normalized lead with a usable address; lower-damage rows are
filtered out by the threshold.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agents import dins_agent as da


# ─── Fixture: ArcGIS-shaped feature collection ────────────────────


_FEATURES = {
    "features": [
        {
            "attributes": {
                "OBJECTID": 1,
                "STREETNUMBER": "100",
                "STREETNAME": "MAIN",
                "STREETTYPE": "ST",
                "CITY": "Paradise",
                "COUNTY": "Butte",
                "INCIDENTNAME": "Park Fire",
                "DAMAGE": "Destroyed (>50%)",
                "STRUCTURETYPE": "Single Family Residence Single Story",
            },
            "geometry": {"x": -121.62, "y": 39.76},
        },
        {
            "attributes": {
                "OBJECTID": 2,
                "Street_Number": "200",
                "Street_Name": "OAK",
                "Street_Type": "AVE",
                "City": "Magalia",
                "County": "Butte",
                "Incident_Name": "Park Fire",
                "Damage": "Major (26-50%)",
                "Structure_Category": "Multi Family Residence",
            },
            "geometry": {"x": -121.58, "y": 39.81},
        },
        # Filtered out by damage threshold (default = 'major')
        {
            "attributes": {
                "OBJECTID": 3,
                "STREETNUMBER": "300",
                "STREETNAME": "PINE",
                "STREETTYPE": "RD",
                "CITY": "Chico",
                "DAMAGE": "Affected (1-9%)",
                "STRUCTURETYPE": "SFR",
            },
            "geometry": {"x": -121.83, "y": 39.72},
        },
        # Filtered out for missing address
        {
            "attributes": {"OBJECTID": 4, "DAMAGE": "Destroyed (>50%)"},
            "geometry": {},
        },
    ]
}


@pytest.fixture(autouse=True)
def _set_layers(monkeypatch):
    monkeypatch.setattr(da, "DINS_LAYERS", [
        "https://example.test/arcgis/rest/services/DINS_2024_Public_View/FeatureServer/0",
    ])


def _mock_response():
    resp = MagicMock()
    resp.json.return_value = _FEATURES
    resp.raise_for_status.return_value = None
    return resp


# ─── _normalize ───────────────────────────────────────────────────


def test_normalize_keeps_destroyed_with_full_address():
    feat = _FEATURES["features"][0]
    out = da._normalize(feat, "https://x/services/DINS_2024_Public_View/FeatureServer/0")
    assert out is not None
    assert out["address"] == "100 MAIN ST"
    assert out["city"] == "Paradise"
    assert out["county"] == "Butte"
    assert out["incident"] == "Park Fire"
    assert "Destroyed" in out["damage"]
    assert out["latitude"] == 39.76
    assert out["longitude"] == -121.62
    assert out["id"].startswith("dins_DINS_2024_Public_View_")
    assert "wildfire rebuild" in out["permit_type"].lower()


def test_normalize_handles_alternate_field_names():
    feat = _FEATURES["features"][1]  # Street_Number / Street_Name etc.
    out = da._normalize(feat, "https://x/services/DINS_2024_Public_View/FeatureServer/0")
    assert out is not None
    assert out["address"] == "200 OAK AVE"
    assert out["city"] == "Magalia"


def test_normalize_drops_affected_below_threshold():
    feat = _FEATURES["features"][2]
    assert da._normalize(feat, "url") is None


def test_normalize_drops_records_without_address():
    feat = _FEATURES["features"][3]
    assert da._normalize(feat, "url") is None


# ─── End-to-end fetch_leads ───────────────────────────────────────


def test_fetch_leads_returns_only_high_damage_rows():
    agent = da.DINSAgent()
    with patch.object(da.requests, "get", return_value=_mock_response()):
        leads = agent.fetch_leads()

    assert len(leads) == 2  # Destroyed + Major; Affected and no-address dropped
    assert {l["city"] for l in leads} == {"Paradise", "Magalia"}
    assert all("Park Fire" == l["incident"] for l in leads)


def test_fetch_leads_no_layers_returns_empty(monkeypatch):
    monkeypatch.setattr(da, "DINS_LAYERS", [])
    assert da.DINSAgent().fetch_leads() == []


def test_fetch_leads_swallows_layer_errors():
    agent = da.DINSAgent()
    failing = MagicMock(side_effect=RuntimeError("network down"))
    with patch.object(da.requests, "get", failing):
        # Soft-fail — return [] without raising.
        assert agent.fetch_leads() == []


# ─── Pipeline routing ─────────────────────────────────────────────


def test_pipeline_routes_dins_kind_to_dins_agent():
    from outreach.pipeline import sources as sources_mod

    class _FakeSource:
        key = "dins-test"
        kind = "dins"
        config = {}

    sources_mod._AGENT_CACHE.clear()
    try:
        backend = sources_mod._get_agent(_FakeSource())
        assert isinstance(backend, da.DINSAgent)
    finally:
        sources_mod._AGENT_CACHE.clear()


# ─── Damage threshold env override ────────────────────────────────


def test_damage_threshold_default_is_major():
    assert da._damage_threshold() == 3


def test_damage_value_substring_match_is_case_insensitive():
    assert da._damage_value("DESTROYED (>50%)") == 4
    assert da._damage_value("Major (26-50%)") == 3
    assert da._damage_value("Minor (10-25%)") == 2
    assert da._damage_value("Affected (1-9%)") == 1
    assert da._damage_value("No Damage") == 0
    assert da._damage_value("") == 0
