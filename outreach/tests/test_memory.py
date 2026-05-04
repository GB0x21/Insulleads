"""
Tests para utils/memory.py — Motor de Memoria Persistente (Fase 2).

Todos los tests son unitarios: sin red, sin Django, sin Telegram.
Cada test crea su propia DB temporal para evitar interferencias.
"""
from __future__ import annotations

import os
import pytest


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Cada test usa su propia base de datos temporal."""
    db = str(tmp_path / "test_memory.db")
    monkeypatch.setenv("DB_PATH", db)
    # Reimportar módulo para que tome el nuevo DB_PATH
    import importlib
    import utils.memory as mem_mod
    importlib.reload(mem_mod)
    mem_mod.init_memory_db()
    return mem_mod


# ── init_memory_db ────────────────────────────────────────────────────


def test_init_creates_tables(isolated_db):
    import sqlite3, os
    db_path = os.environ["DB_PATH"]
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "memories" in tables
    assert "agent_summaries" in tables
    assert "memory_vectors" in tables
    conn.close()


# ── record_observation ────────────────────────────────────────────────


def test_record_observation_inserts_row(isolated_db):
    m = isolated_db
    m.record_observation("permits", "lead_found", "Oakland — 123 Main: remodel", {"city": "Oakland"})
    results = m.search_memories("Oakland", agent_key="permits")
    assert len(results) >= 1
    assert "Oakland" in results[0]["content"]


def test_record_observation_vector_stored(isolated_db, tmp_path):
    m = isolated_db
    import sqlite3
    m.record_observation("solar", "lead_found", "Berkeley solar install", {"city": "Berkeley"})
    conn = sqlite3.connect(os.environ["DB_PATH"])
    row = conn.execute("SELECT vector_blob FROM memory_vectors LIMIT 1").fetchone()
    conn.close()
    assert row is not None
    import json
    vec = json.loads(row[0])
    assert len(vec) == m.EMBED_DIM
    assert abs(sum(x**2 for x in vec) ** 0.5 - 1.0) < 0.01  # normalizado


def test_record_observation_deduplicates_identical_content(isolated_db):
    m = isolated_db
    import sqlite3
    content = "Same content repeated"
    m.record_observation("permits", "lead_found", content)
    m.record_observation("permits", "lead_found", content)
    conn = sqlite3.connect(os.environ["DB_PATH"])
    # memories puede tener duplicados (FTS lo permite), pero memory_vectors no
    vec_count = conn.execute("SELECT COUNT(*) FROM memory_vectors").fetchone()[0]
    conn.close()
    assert vec_count == 1


# ── search_memories ───────────────────────────────────────────────────


def test_search_returns_relevant_results(isolated_db):
    m = isolated_db
    m.record_observation("permits", "lead_found", "Oakland attic insulation remodel")
    m.record_observation("permits", "lead_found", "Berkeley solar panel install")
    m.record_observation("solar", "lead_found", "San Francisco solar PV system")

    results = m.search_memories("attic insulation", agent_key="permits")
    assert len(results) >= 1
    assert any("Oakland" in r["content"] for r in results)


def test_search_filters_by_agent(isolated_db):
    m = isolated_db
    m.record_observation("permits", "lead_found", "Oakland permit remodel")
    m.record_observation("solar", "lead_found", "Oakland solar install")

    results = m.search_memories("Oakland", agent_key="solar")
    assert all(r["agent_key"] == "solar" for r in results)


def test_search_returns_similarity_score(isolated_db):
    m = isolated_db
    m.record_observation("permits", "lead_sent", "HOT lead Oakland insulation")
    results = m.search_memories("insulation Oakland", agent_key="permits")
    assert len(results) >= 1
    assert "_similarity" in results[0]
    assert 0.0 <= results[0]["_similarity"] <= 1.0


def test_search_empty_returns_empty(isolated_db):
    m = isolated_db
    results = m.search_memories("query with no matches at all xyz123")
    assert isinstance(results, list)


# ── get_context ───────────────────────────────────────────────────────


def test_get_context_returns_string(isolated_db):
    m = isolated_db
    m.record_observation("permits", "lead_found", "Berkeley remodel project")
    ctx = m.get_context("permits", query="Berkeley")
    assert isinstance(ctx, str)


def test_get_context_respects_max_tokens(isolated_db):
    m = isolated_db
    for i in range(20):
        m.record_observation("permits", "lead_found", f"Oakland project {i} with long description text here")
    ctx = m.get_context("permits", query="Oakland", max_tokens=100)
    assert len(ctx) <= 100


def test_get_context_empty_for_new_agent(isolated_db):
    m = isolated_db
    ctx = m.get_context("unknown_agent")
    assert ctx == ""


# ── get_summary / update_summary ─────────────────────────────────────


def test_update_and_get_summary(isolated_db):
    m = isolated_db
    m.update_summary("permits", "Agente de permisos — Oakland más activa", obs_count=50)
    summary = m.get_summary("permits")
    assert "Oakland" in summary


def test_get_summary_empty_for_unknown_agent(isolated_db):
    m = isolated_db
    assert m.get_summary("nonexistent_agent") == ""


# ── _hash_embed ───────────────────────────────────────────────────────


def test_hash_embed_is_normalized(isolated_db):
    m = isolated_db
    vec = m._hash_embed("hello world insulation oakland")
    norm = sum(x**2 for x in vec) ** 0.5
    assert abs(norm - 1.0) < 1e-5


def test_hash_embed_is_deterministic(isolated_db):
    m = isolated_db
    a = m._hash_embed("test string")
    b = m._hash_embed("test string")
    assert a == b


def test_hash_embed_differs_for_different_inputs(isolated_db):
    m = isolated_db
    a = m._hash_embed("insulation attic")
    b = m._hash_embed("solar panel install")
    sim = m._cosine(a, b)
    assert sim < 0.99  # no son idénticos


def test_cosine_similarity_identical(isolated_db):
    m = isolated_db
    vec = m._hash_embed("test")
    assert abs(m._cosine(vec, vec) - 1.0) < 1e-5


def test_cosine_similarity_range(isolated_db):
    m = isolated_db
    a = m._hash_embed("Oakland insulation permit")
    b = m._hash_embed("Berkeley solar install")
    sim = m._cosine(a, b)
    assert -1.0 <= sim <= 1.0


# ── get_agent_patterns ────────────────────────────────────────────────


def test_get_agent_patterns_empty_for_new_agent(isolated_db):
    m = isolated_db
    patterns = m.get_agent_patterns("new_agent")
    assert patterns == {}


def test_get_agent_patterns_counts_cities(isolated_db):
    m = isolated_db
    for _ in range(3):
        m.record_observation("permits", "lead_found", "Oakland project", {"city": "Oakland", "value_float": 100000})
    m.record_observation("permits", "lead_found", "Berkeley project", {"city": "Berkeley", "value_float": 200000})

    patterns = m.get_agent_patterns("permits")
    assert patterns["top_cities"].get("Oakland", 0) == 3
    assert patterns["top_cities"].get("Berkeley", 0) == 1
    assert patterns["avg_value"] > 0
