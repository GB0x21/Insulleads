"""
Tests for `utils.inspection_filters` — the shared helpers that gate
the "next inspection" Telegram block across every agent.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from utils.inspection_filters import (
    is_enforcement_inspection,
    is_future_inspection,
    is_useful_inspection,
    parse_date,
)


# ─── parse_date ───────────────────────────────────────────────────


@pytest.mark.parametrize("text,year", [
    ("2026-04-28", 2026),
    ("2026-04-28T12:00:00", 2026),
    ("4/28/2026", 2026),
    ("9/8/2025", 2025),
    ("12/31/24", 2024),
])
def test_parse_date_handles_common_formats(text, year):
    parsed = parse_date(text)
    assert parsed is not None
    assert parsed.year == year


def test_parse_date_returns_none_for_garbage():
    assert parse_date("") is None
    assert parse_date("not a date") is None


# ─── is_future_inspection ─────────────────────────────────────────


def test_future_inspection_accepts_today_or_later():
    today = datetime.utcnow().strftime("%m/%d/%Y")
    assert is_future_inspection(
        {"best_visit": f"Wed {today}, 7:00 AM - 10:00 AM"}
    ) is True


def test_future_inspection_rejects_2018_dates():
    """The exact regression that surfaced 'Visita la obra: Wed 4/10/2018'
    on the operator's Telegram for construction-agent leads."""
    assert is_future_inspection(
        {"best_visit": "Wed 4/10/2018, 7:00 AM - 10:00 AM"}
    ) is False


def test_future_inspection_rejects_unparseable():
    assert is_future_inspection({"best_visit": "TBD"}) is False
    assert is_future_inspection({}) is False


# ─── is_enforcement_inspection ────────────────────────────────────


def test_enforcement_detects_code_investigation():
    assert is_enforcement_inspection({"type": "Code Investigation"}) is True
    assert is_enforcement_inspection({"type": "Code Compliance"}) is True
    assert is_enforcement_inspection({"type": "Citizen Complaint"}) is True


def test_enforcement_passes_real_milestones():
    assert is_enforcement_inspection({"type": "Rough-In Insulation"}) is False
    assert is_enforcement_inspection({"type": "Final Building"}) is False


# ─── is_useful_inspection (one-shot) ──────────────────────────────


def test_useful_inspection_combines_both_checks():
    today = datetime.utcnow().strftime("%m/%d/%Y")
    # Future + non-enforcement → useful
    assert is_useful_inspection({
        "best_visit": f"Wed {today}, 9:00 AM",
        "type": "Framing",
    }) is True
    # Future BUT enforcement → not useful
    assert is_useful_inspection({
        "best_visit": f"Wed {today}",
        "type": "Code Investigation",
    }) is False
    # Past → not useful regardless of type
    assert is_useful_inspection({
        "best_visit": "Wed 4/10/2018",
        "type": "Framing",
    }) is False
    # None → not useful
    assert is_useful_inspection(None) is False
    assert is_useful_inspection({}) is False
