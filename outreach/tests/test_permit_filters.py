"""
Tests for the quality / recency filters in `agents.permits_agent`.

These run as plain unit tests (no Django ORM, no network) so we can
iterate fast on the filter rules. The integration with the Django
pipeline is exercised separately in `test_sources_routing.py`.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from agents.permits_agent import (
    _is_enforcement_inspection,
    _is_future_inspection,
    _is_quality_permit,
    _is_recent,
    _parse_date,
)


# ─── _parse_date ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,expected_year",
    [
        ("2026-04-28", 2026),
        ("2026-04-28T12:00:00", 2026),
        ("2026-04-28 12:00:00", 2026),
        ("4/28/2026", 2026),
        ("9/8/2025", 2025),         # the format that surfaced the SJ bug
        ("12/31/24", 2024),
    ],
)
def test_parse_date_handles_common_formats(text, expected_year):
    parsed = _parse_date(text)
    assert parsed is not None
    assert parsed.year == expected_year


def test_parse_date_returns_none_for_garbage():
    assert _parse_date("") is None
    assert _parse_date("not a date") is None
    assert _parse_date("13/45/9999") is None


# ─── _is_recent ──────────────────────────────────────────────────────


def test_is_recent_returns_false_for_unparseable_date():
    """The pre-fix `_is_recent` returned True on parse failure. That
    behavior let years-old leads sneak through whenever the upstream
    served a non-ISO date format. We now drop unparseable dates."""
    assert _is_recent({"issued_date": "not a date"}) is False


def test_is_recent_returns_false_for_old_us_slashed_date():
    """Reproduces the SJ Telegram regression — `9/8/2025` is older than
    the default 3-month window relative to today (2026)."""
    assert _is_recent({"issued_date": "9/8/2025"}) is False


def test_is_recent_returns_true_for_recent_date():
    recent = (datetime.utcnow() - timedelta(days=10)).strftime("%m/%d/%Y")
    assert _is_recent({"issued_date": recent}) is True


def test_is_recent_returns_false_when_no_date():
    assert _is_recent({}) is False
    assert _is_recent({"issued_date": "", "filed_date": ""}) is False


# ─── _is_quality_permit ──────────────────────────────────────────────


def test_quality_drops_planning_only_san_jose_pattern():
    """Reproduces the lead from the operator's Telegram —
    'PLAN A (BEMP 100%) LOT 16' is a master-plan approval, not the
    actual building permit."""
    lead = {
        "permit_type": "PLAN A (BEMP 100%) LOT 16",
        "description": "New Construction",
    }
    ok, reason = _is_quality_permit(lead)
    assert ok is False
    assert "plan a" in reason


def test_quality_drops_code_investigation():
    lead = {
        "permit_type": "Code Investigation",
        "description": "Citizen complaint follow-up",
    }
    ok, reason = _is_quality_permit(lead)
    assert ok is False
    assert "code investigation" in reason


def test_quality_drops_fence_only():
    lead = {"permit_type": "Fence", "description": "6ft wood fence"}
    ok, _ = _is_quality_permit(lead)
    assert ok is False


def test_quality_drops_pool_only_and_landscape_only():
    assert _is_quality_permit(
        {"permit_type": "Pool Only", "description": ""}
    )[0] is False
    assert _is_quality_permit(
        {"permit_type": "Landscape Only", "description": ""}
    )[0] is False


def test_quality_passes_real_construction_permit():
    lead = {
        "permit_type": "additions alterations or repairs",
        "description": "ROUGH-IN INSULATION ATTIC R-38, walls R-15",
    }
    ok, reason = _is_quality_permit(lead)
    assert ok is True
    assert reason == ""


def test_quality_passes_new_wood_frame_construction():
    lead = {
        "permit_type": "new construction wood frame",
        "description": "New 3-story SFD",
    }
    ok, _ = _is_quality_permit(lead)
    assert ok is True


def test_quality_does_not_match_long_description_with_passing_phrase():
    """Description-level check is length-bounded so a real scope that
    happens to mention a bad phrase (e.g. 'addresses prior code
    compliance issue') still passes."""
    lead = {
        "permit_type": "additions alterations or repairs",
        "description": (
            "Full reroof with R-38 attic insulation. Addresses prior "
            "code compliance citation from 2019. Rough-in inspection "
            "scheduled for next month."
        ),
    }
    ok, _ = _is_quality_permit(lead)
    assert ok is True


# ─── inspection helpers ──────────────────────────────────────────────


def test_future_inspection_accepts_today_or_later():
    today = datetime.utcnow().strftime("%-m/%-d/%Y") if hasattr(
        datetime.utcnow(), "strftime"
    ) else datetime.utcnow().strftime("%m/%d/%Y")
    assert _is_future_inspection(
        {"best_visit": f"Wed {today}, 7:00 AM - 10:00 AM"}
    ) is True


def test_future_inspection_rejects_stale_2018_date():
    """Reproduces the operator's complaint — a 2018 date showing up as
    a 'next inspection' window in 2026."""
    assert _is_future_inspection(
        {"best_visit": "Wed 4/10/2018, 7:00 AM - 10:00 AM"}
    ) is False


def test_future_inspection_rejects_unparseable_string():
    assert _is_future_inspection({"best_visit": "TBD"}) is False
    assert _is_future_inspection({}) is False


def test_enforcement_inspection_detects_code_investigation():
    assert _is_enforcement_inspection({"type": "Code Investigation"}) is True
    assert _is_enforcement_inspection({"type": "Code Compliance Visit"}) is True
    assert _is_enforcement_inspection({"type": "No Entry"}) is True


def test_enforcement_inspection_passes_real_milestones():
    assert _is_enforcement_inspection({"type": "Rough-In Insulation"}) is False
    assert _is_enforcement_inspection({"type": "Final"}) is False
    assert _is_enforcement_inspection({"type": ""}) is False
