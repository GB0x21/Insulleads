"""
Tests para utils/lead_outcomes.py — Historial de Conversiones (Fase 2).

Todos los tests son unitarios: sin red, sin Django ORM.
Cada test usa su propia DB temporal.
"""
from __future__ import annotations

import os
import pytest


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test_outcomes.db"))
    # Forzar MIN_OUTCOMES_FOR_SIGNAL=2 para que los tests no necesiten 5 registros
    monkeypatch.setenv("MIN_OUTCOMES_FOR_SIGNAL", "2")
    import importlib
    import utils.lead_outcomes as mod
    import utils.memory as mem
    importlib.reload(mod)
    importlib.reload(mem)
    mem.init_memory_db()
    mod.init_outcomes_db()
    return mod


_LEAD_WON = {
    "id": "lead_001",
    "city": "Oakland",
    "address": "123 Main St",
    "contractor": "ABC Insulation Inc",
    "contact_phone": "510-555-0001",
    "contact_email": "abc@example.com",
    "value_float": 150000,
    "_agent_key": "permits",
}

_LEAD_LOST = {
    "id": "lead_002",
    "city": "Fremont",
    "address": "456 Oak Ave",
    "contractor": "XYZ Builders",
    "contact_phone": "510-555-0002",
    "value_float": 80000,
    "_agent_key": "permits",
}


# ── init_outcomes_db ──────────────────────────────────────────────────


def test_init_creates_table(isolated_db, tmp_path):
    import sqlite3
    conn = sqlite3.connect(os.environ["DB_PATH"])
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "lead_outcomes" in tables
    conn.close()


# ── register_outcome ──────────────────────────────────────────────────


def test_register_outcome_inserts_won(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON, "won")
    stats = m.get_outcome_stats()
    assert stats["won"] == 1


def test_register_outcome_inserts_lost(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_LOST, "lost")
    stats = m.get_outcome_stats()
    assert stats["lost"] == 1


def test_register_outcome_inserts_replied(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON, "replied")
    stats = m.get_outcome_stats()
    assert stats["replied"] == 1


def test_register_outcome_ignores_duplicate(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON, "won")
    m.register_outcome(_LEAD_WON, "won")  # mismo id + outcome
    stats = m.get_outcome_stats()
    assert stats["won"] == 1  # no duplica


def test_register_outcome_rejects_invalid_outcome(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON, "invalid_outcome")
    stats = m.get_outcome_stats()
    assert stats["total"] == 0


# ── find_similar_outcomes ─────────────────────────────────────────────


def test_find_similar_returns_empty_when_few_outcomes(isolated_db, monkeypatch):
    m = isolated_db
    monkeypatch.setenv("MIN_OUTCOMES_FOR_SIGNAL", "10")
    import importlib; importlib.reload(m)
    m.register_outcome(_LEAD_WON, "won")
    similar = m.find_similar_outcomes(_LEAD_WON)
    assert similar == []


def test_find_similar_returns_results_with_enough_data(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON.copy() | {"id": "a1"}, "won")
    m.register_outcome(_LEAD_WON.copy() | {"id": "a2"}, "won")
    m.register_outcome(_LEAD_LOST.copy() | {"id": "a3"}, "lost")

    similar = m.find_similar_outcomes(_LEAD_WON)
    assert len(similar) >= 1
    assert all("outcome" in s for s in similar)
    assert all("similarity" in s for s in similar)


def test_find_similar_sorted_by_similarity(isolated_db):
    m = isolated_db
    for i in range(3):
        m.register_outcome(_LEAD_WON.copy() | {"id": f"w{i}"}, "won")
    similar = m.find_similar_outcomes(_LEAD_WON)
    sims = [s["similarity"] for s in similar]
    assert sims == sorted(sims, reverse=True)


# ── outcome_memory_bonus ──────────────────────────────────────────────


def test_bonus_zero_when_no_history(isolated_db, monkeypatch):
    m = isolated_db
    monkeypatch.setenv("MIN_OUTCOMES_FOR_SIGNAL", "100")
    import importlib; importlib.reload(m)
    bonus, reasons = m.outcome_memory_bonus(_LEAD_WON)
    assert bonus == 0
    assert reasons == []


def test_bonus_positive_for_known_winner_contractor(isolated_db):
    m = isolated_db
    # Registrar varios WON del mismo contratista
    for i in range(3):
        m.register_outcome(
            _LEAD_WON.copy() | {"id": f"w{i}", "contractor": "ABC Insulation Inc"},
            "won",
        )
    bonus, reasons = m.outcome_memory_bonus(
        {"city": "Oakland", "contractor": "ABC Insulation Inc", "value_float": 140000}
    )
    assert bonus > 0
    assert len(reasons) > 0


def test_bonus_negative_for_consistent_loser_contractor(isolated_db):
    m = isolated_db
    for i in range(4):
        m.register_outcome(
            _LEAD_LOST.copy() | {"id": f"l{i}", "contractor": "XYZ Builders"},
            "lost",
        )
    m.register_outcome(
        _LEAD_LOST.copy() | {"id": "w1", "contractor": "XYZ Builders"},
        "won",
    )
    bonus, reasons = m.outcome_memory_bonus(
        {"city": "Fremont", "contractor": "XYZ Builders", "value_float": 80000}
    )
    assert bonus < 0


def test_bonus_clamped_to_max(isolated_db):
    m = isolated_db
    for i in range(10):
        m.register_outcome(
            _LEAD_WON.copy() | {"id": f"w{i}"},
            "won",
        )
    bonus, _ = m.outcome_memory_bonus(_LEAD_WON)
    assert bonus <= m.MAX_MEMORY_BONUS


def test_penalty_clamped_to_min(isolated_db):
    m = isolated_db
    for i in range(10):
        m.register_outcome(
            _LEAD_LOST.copy() | {"id": f"l{i}"},
            "lost",
        )
    bonus, _ = m.outcome_memory_bonus(_LEAD_LOST)
    assert bonus >= -m.MAX_MEMORY_PENALTY


# ── get_outcome_stats ─────────────────────────────────────────────────


def test_get_outcome_stats_empty(isolated_db):
    m = isolated_db
    stats = m.get_outcome_stats()
    assert stats["total"] == 0
    assert stats["won"] == 0
    assert stats["lost"] == 0
    assert stats["replied"] == 0


def test_get_outcome_stats_counts(isolated_db):
    m = isolated_db
    m.register_outcome(_LEAD_WON.copy() | {"id": "w1"}, "won")
    m.register_outcome(_LEAD_WON.copy() | {"id": "w2"}, "won")
    m.register_outcome(_LEAD_LOST.copy() | {"id": "l1"}, "lost")
    stats = m.get_outcome_stats()
    assert stats["won"] == 2
    assert stats["lost"] == 1
    assert stats["total"] == 3
