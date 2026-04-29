"""
Tests for `utils.socrata_discovery`.

We mock the Socrata Catalog response so the test runs anywhere — the
sandbox has no internet, and even with internet the live catalog
shape can shift week to week. Behaviour under test: shape parsing,
state + only_known filtering, and the highest-rows-first sort.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from utils import socrata_discovery as sd


_CATALOG = {
    "results": [
        {
            "resource": {
                "id": "abcd-1234",
                "name": "Building Permits — San Francisco",
                "rows_count": 500_000,
                "updatedAt": "2026-04-20T10:00:00Z",
                "description": "All building permits issued by SF DBI.",
            },
            "metadata": {"domain": "data.sfgov.org"},
            "permalink": "https://data.sfgov.org/d/abcd-1234",
            "classification": {"categories": ["Public Safety"]},
        },
        {
            "resource": {
                "id": "wxyz-5678",
                "name": "Construction Permits — Sacramento",
                "rows_count": 80_000,
                "updatedAt": "2026-04-15T08:00:00Z",
            },
            "metadata": {"domain": "data.cityofsacramento.org"},
            "permalink": "https://data.cityofsacramento.org/d/wxyz-5678",
        },
        {
            "resource": {
                "id": "qqqq-9999",
                "name": "Permits — Long Beach",
                "rows_count": 200_000,
            },
            "metadata": {"domain": "data.longbeach.gov"},
            "permalink": "https://data.longbeach.gov/d/qqqq-9999",
        },
        # Bad: missing id / domain
        {"resource": {"name": "Bad"}, "metadata": {"domain": ""}},
    ]
}


def _resp(payload):
    r = MagicMock()
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


# ─── discover() ───────────────────────────────────────────────────


def test_discover_returns_normalized_hits_sorted_by_rows():
    with patch.object(sd.requests, "get", return_value=_resp(_CATALOG)):
        hits = sd.discover(state="CA", limit=10)
    assert len(hits) == 3   # the bad row drops
    # Highest row count first.
    assert hits[0]["domain"] == "data.sfgov.org"
    assert hits[0]["rows"] == 500_000
    # All hits have a usable .json URL.
    assert all(h["json_url"].startswith("https://") for h in hits)


def test_discover_only_known_filters_to_provided_domains():
    known = {"data.sfgov.org"}
    with patch.object(sd.requests, "get", return_value=_resp(_CATALOG)):
        hits = sd.discover(only_known=True, known_domains=known)
    assert len(hits) == 1
    assert hits[0]["domain"] == "data.sfgov.org"


def test_discover_swallows_network_errors():
    with patch.object(sd.requests, "get",
                      side_effect=RuntimeError("network down")):
        assert sd.discover(state="CA") == []


# ─── sample_rows() ────────────────────────────────────────────────


def test_sample_rows_returns_payload():
    with patch.object(sd.requests, "get",
                      return_value=_resp([{"permit_id": "1", "city": "SF"}])):
        rows = sd.sample_rows("https://data.sfgov.org/resource/abcd-1234.json", limit=1)
    assert rows == [{"permit_id": "1", "city": "SF"}]


def test_sample_rows_returns_empty_on_error():
    with patch.object(sd.requests, "get", side_effect=RuntimeError("500")):
        assert sd.sample_rows("https://x", limit=1) == []
