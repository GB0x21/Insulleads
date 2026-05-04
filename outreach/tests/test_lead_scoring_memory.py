"""
Tests para utils/lead_scoring.py — score_lead() y score_lead_with_memory() (Fase 2).

Unitarios: sin red, sin Django. Usa DB temporal para los tests con memoria.
"""
from __future__ import annotations

import os
import pytest


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test_scoring.db"))
    monkeypatch.setenv("MIN_OUTCOMES_FOR_SIGNAL", "2")
    import importlib, utils.lead_outcomes as lo, utils.memory as mem
    importlib.reload(lo)
    importlib.reload(mem)
    mem.init_memory_db()
    lo.init_outcomes_db()
    return lo


_BASE_LEAD = {
    "city": "Oakland",
    "address": "123 Main St",
    "description": "residential remodel",
    "permit_type": "additions alterations or repairs",
    "contractor": "Test GC Inc",
    "contact_phone": "510-555-1234",
    "contact_email": "gc@test.com",
    "value_float": 120000,
    "_agent_key": "permits",
}


# ── score_lead — base heuristic ───────────────────────────────────────


def test_score_lead_returns_required_keys():
    from utils.lead_scoring import score_lead
    result = score_lead(_BASE_LEAD)
    assert "score" in result
    assert "grade" in result
    assert "grade_emoji" in result
    assert "reasons" in result


def test_score_lead_range():
    from utils.lead_scoring import score_lead
    result = score_lead(_BASE_LEAD)
    assert 0 <= result["score"] <= 100


def test_score_lead_hot_has_fire_emoji():
    from utils.lead_scoring import score_lead
    lead = dict(_BASE_LEAD, value_float=600000, description="spray foam insulation attic")
    result = score_lead(lead)
    if result["score"] >= 90:
        assert result["grade_emoji"] == "🔥"


def test_score_lead_higher_value_gives_higher_score():
    from utils.lead_scoring import score_lead
    low  = score_lead(dict(_BASE_LEAD, value_float=10000))
    high = score_lead(dict(_BASE_LEAD, value_float=600000))
    assert high["score"] > low["score"]


def test_score_lead_insulation_keywords_boost():
    from utils.lead_scoring import score_lead
    no_kw  = score_lead(dict(_BASE_LEAD, description="fence repair"))
    has_kw = score_lead(dict(_BASE_LEAD, description="attic insulation spray foam"))
    assert has_kw["score"] >= no_kw["score"]


def test_score_lead_contact_quality_boost():
    from utils.lead_scoring import score_lead
    no_contact  = score_lead({**_BASE_LEAD, "contact_phone": "", "contact_email": ""})
    has_contact = score_lead(_BASE_LEAD)
    assert has_contact["score"] > no_contact["score"]


def test_score_lead_tier1_city_gets_more_pts():
    from utils.lead_scoring import score_lead
    sf    = score_lead(dict(_BASE_LEAD, city="San Francisco"))
    rural = score_lead(dict(_BASE_LEAD, city="Unknown Rural Town"))
    assert sf["score"] >= rural["score"]


# ── score_lead_with_memory ────────────────────────────────────────────


def test_score_with_memory_falls_back_gracefully_with_no_history(isolated_db):
    from utils.lead_scoring import score_lead, score_lead_with_memory
    base   = score_lead(_BASE_LEAD)
    memory = score_lead_with_memory(_BASE_LEAD)
    # Sin historial el score debe ser igual (bonus=0)
    assert memory["score"] == base["score"]


def test_score_with_memory_higher_for_won_contractor(isolated_db):
    lo = isolated_db
    from utils.lead_scoring import score_lead, score_lead_with_memory
    contractor = "Top Insulation Co"
    for i in range(3):
        lo.register_outcome(
            {**_BASE_LEAD, "id": f"w{i}", "contractor": contractor}, "won"
        )
    base   = score_lead(dict(_BASE_LEAD, contractor=contractor))
    memory = score_lead_with_memory(dict(_BASE_LEAD, contractor=contractor))
    assert memory["score"] >= base["score"]
    assert memory.get("memory_bonus", 0) > 0


def test_score_with_memory_lower_for_lost_contractor(isolated_db):
    lo = isolated_db
    from utils.lead_scoring import score_lead, score_lead_with_memory
    contractor = "Bad Contractor LLC"
    for i in range(4):
        lo.register_outcome(
            {**_BASE_LEAD, "id": f"l{i}", "contractor": contractor, "city": "Fremont"}, "lost"
        )
    lo.register_outcome(
        {**_BASE_LEAD, "id": "w0", "contractor": contractor, "city": "Fremont"}, "won"
    )
    base   = score_lead(dict(_BASE_LEAD, contractor=contractor, city="Fremont"))
    memory = score_lead_with_memory(dict(_BASE_LEAD, contractor=contractor, city="Fremont"))
    assert memory["score"] <= base["score"]


def test_score_with_memory_bonus_field_present(isolated_db):
    lo = isolated_db
    from utils.lead_scoring import score_lead_with_memory
    for i in range(3):
        lo.register_outcome({**_BASE_LEAD, "id": f"w{i}"}, "won")
    result = score_lead_with_memory(_BASE_LEAD)
    assert "memory_bonus" in result


def test_score_with_memory_clamped_to_100(isolated_db):
    lo = isolated_db
    from utils.lead_scoring import score_lead_with_memory
    for i in range(20):
        lo.register_outcome({**_BASE_LEAD, "id": f"w{i}"}, "won")
    result = score_lead_with_memory(_BASE_LEAD)
    assert result["score"] <= 100


def test_score_with_memory_clamped_to_0(isolated_db):
    lo = isolated_db
    from utils.lead_scoring import score_lead_with_memory
    for i in range(20):
        lo.register_outcome({**_BASE_LEAD, "id": f"l{i}", "value_float": 0}, "lost")
    cold_lead = {
        "city": "Unknown", "address": "", "description": "fence",
        "contractor": _BASE_LEAD["contractor"], "contact_phone": "",
        "contact_email": "", "value_float": 0, "_agent_key": "permits",
    }
    result = score_lead_with_memory(cold_lead)
    assert result["score"] >= 0


# ── format_score_line ─────────────────────────────────────────────────


def test_format_score_line_base():
    from utils.lead_scoring import format_score_line, score_lead
    scoring = score_lead(_BASE_LEAD)
    line = format_score_line(scoring)
    assert str(scoring["score"]) in line
    assert scoring["grade"] in line


def test_format_score_line_with_memory_bonus():
    from utils.lead_scoring import format_score_line
    scoring = {"score": 79, "grade": "WARM", "grade_emoji": "🟠",
               "reasons": ["test reason"], "memory_bonus": 15}
    line = format_score_line(scoring)
    assert "+mem:+15" in line
