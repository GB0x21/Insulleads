"""
Tests para utils/agent_metrics.py — Learning Loop Engine (Fase 1).

Unitarios: sin red, sin Django. Cada test usa DB temporal aislada.
"""
from __future__ import annotations

import os
import pytest


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test_metrics.db"))
    monkeypatch.setenv("CB_THRESHOLD", "3")
    monkeypatch.setenv("CB_BACKOFF_BASE_MIN", "15")
    monkeypatch.setenv("CB_BACKOFF_MAX_MIN", "240")
    monkeypatch.setenv("ADAPTIVE_WINDOW", "5")
    monkeypatch.setenv("PRODUCTIVE_THRESHOLD", "2.0")
    monkeypatch.setenv("QUIET_THRESHOLD", "0.5")
    import importlib
    import utils.agent_metrics as mod
    importlib.reload(mod)
    mod.init_metrics_db()
    return mod


# ── init_metrics_db ───────────────────────────────────────────────────


def test_init_creates_tables(isolated_db, tmp_path):
    import sqlite3
    conn = sqlite3.connect(os.environ["DB_PATH"])
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "agent_runs" in tables
    assert "agent_state" in tables
    conn.close()


# ── record_run ────────────────────────────────────────────────────────


def test_record_run_success(isolated_db):
    m = isolated_db
    m.record_run("permits", leads_found=10, leads_sent=3, duration_s=1.5)
    report = m.get_health_report()
    agent = next(a for a in report["agents"] if a["key"] == "permits")
    assert agent["total_runs"] == 1
    assert agent["total_sent"] == 3
    assert agent["fails_consec"] == 0


def test_record_run_error_increments_fails(isolated_db):
    m = isolated_db
    m.record_run("solar", leads_found=0, leads_sent=0, duration_s=0.5, error="timeout")
    report = m.get_health_report()
    agent = next(a for a in report["agents"] if a["key"] == "solar")
    assert agent["fails_consec"] == 1


def test_record_run_success_resets_fails(isolated_db):
    m = isolated_db
    m.record_run("permits", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    m.record_run("permits", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    m.record_run("permits", leads_found=5, leads_sent=2, duration_s=1.0)  # éxito
    report = m.get_health_report()
    agent = next(a for a in report["agents"] if a["key"] == "permits")
    assert agent["fails_consec"] == 0


# ── Circuit Breaker ───────────────────────────────────────────────────


def test_circuit_breaker_opens_after_threshold(isolated_db):
    m = isolated_db
    for _ in range(3):
        m.record_run("energy", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    assert m.is_circuit_open("energy") is True


def test_circuit_breaker_stays_closed_below_threshold(isolated_db):
    m = isolated_db
    for _ in range(2):
        m.record_run("flood", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    assert m.is_circuit_open("flood") is False


def test_circuit_breaker_closed_for_unknown_agent(isolated_db):
    m = isolated_db
    assert m.is_circuit_open("nonexistent") is False


def test_circuit_breaker_closes_after_expiry(isolated_db, monkeypatch):
    m = isolated_db
    for _ in range(3):
        m.record_run("rodents", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    assert m.is_circuit_open("rodents") is True

    # Forzar expiración: sobreescribir cb_open_until al pasado
    import sqlite3
    from datetime import datetime, timezone, timedelta
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    conn = sqlite3.connect(os.environ["DB_PATH"])
    conn.execute("UPDATE agent_state SET cb_open_until=? WHERE agent_key='rodents'", (past,))
    conn.commit()
    conn.close()

    assert m.is_circuit_open("rodents") is False


# ── Adaptive Intervals ────────────────────────────────────────────────


def test_get_adaptive_interval_returns_default_with_no_history(isolated_db):
    m = isolated_db
    interval = m.get_adaptive_interval("permits", default_interval=60)
    assert interval == 60


def test_set_base_interval_stores_value(isolated_db):
    m = isolated_db
    m.set_base_interval("permits", 60)
    interval = m.get_adaptive_interval("permits", default_interval=90)
    assert interval == 60  # usa el override, no el default


def test_productive_agent_gets_shorter_interval(isolated_db):
    m = isolated_db
    m.set_base_interval("permits", 60)
    # 5 ciclos con 3 leads enviados cada uno (avg=3 >= PRODUCTIVE_THRESHOLD=2)
    for _ in range(5):
        m.record_run("permits", leads_found=5, leads_sent=3, duration_s=1.0)
    interval = m.get_adaptive_interval("permits", 60)
    assert interval < 60


def test_quiet_agent_gets_longer_interval(isolated_db):
    m = isolated_db
    m.set_base_interval("solar", 60)
    # 5 ciclos con 0 leads (avg=0 < QUIET_THRESHOLD=0.5)
    for _ in range(5):
        m.record_run("solar", leads_found=2, leads_sent=0, duration_s=0.5)
    interval = m.get_adaptive_interval("solar", 60)
    assert interval > 60


# ── Health Report ─────────────────────────────────────────────────────


def test_health_report_structure(isolated_db):
    m = isolated_db
    m.record_run("permits", leads_found=10, leads_sent=3, duration_s=1.5)
    report = m.get_health_report()
    assert "agents" in report
    assert "total_sent_all" in report
    assert "open_cbs" in report


def test_health_report_total_sent(isolated_db):
    m = isolated_db
    m.record_run("permits", leads_found=10, leads_sent=5, duration_s=1.0)
    m.record_run("solar", leads_found=3, leads_sent=2, duration_s=0.5)
    report = m.get_health_report()
    assert report["total_sent_all"] == 7


def test_format_health_telegram_contains_key_info(isolated_db):
    m = isolated_db
    m.record_run("permits", leads_found=10, leads_sent=3, duration_s=1.5)
    for _ in range(3):
        m.record_run("solar", leads_found=0, leads_sent=0, duration_s=0.1, error="err")
    report = m.get_health_report()
    msg = m.format_health_telegram(report)
    assert "permits" in msg
    assert "solar" in msg
    assert "PAUSADO" in msg  # solar tiene CB abierto
