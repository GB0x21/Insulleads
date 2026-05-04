"""
utils/agent_metrics.py
━━━━━━━━━━━━━━━━━━━━━━
Learning Loop Engine — inspirado en Hermes Agent.

Cada ciclo de agente registra métricas en SQLite. Con el tiempo, el sistema:
  1. Ajusta intervalos adaptativamente (agentes productivos corren más seguido)
  2. Activa circuit breaker (agentes que fallan se pausan con backoff exponencial)
  3. Reporta health del sistema en Telegram

Tablas:
  agent_runs   — log de cada ejecución (leads, enviados, duración, error)
  agent_state  — estado actual por agente (circuit breaker, interval override)
"""

from __future__ import annotations

import os
import sqlite3
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/leads.db")


def _env_int(key: str, default: int) -> int:
    """Parse an env var as int, stripping inline shell comments (# ...)."""
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    """Parse an env var as float, stripping inline shell comments (# ...)."""
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return float(raw)
    except ValueError:
        return default


CB_THRESHOLD        = _env_int("CB_THRESHOLD", 3)
CB_BACKOFF_BASE_MIN = _env_int("CB_BACKOFF_BASE_MIN", 15)
CB_BACKOFF_MAX_MIN  = _env_int("CB_BACKOFF_MAX_MIN", 240)

ADAPTIVE_WINDOW      = _env_int("ADAPTIVE_WINDOW", 10)
PRODUCTIVE_THRESHOLD = _env_float("PRODUCTIVE_THRESHOLD", 2.0)
QUIET_THRESHOLD      = _env_float("QUIET_THRESHOLD", 0.5)


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_metrics_db():
    """Crea las tablas de métricas si no existen."""
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS agent_runs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_key    TEXT    NOT NULL,
                run_at       TEXT    NOT NULL,
                leads_found  INTEGER DEFAULT 0,
                leads_sent   INTEGER DEFAULT 0,
                duration_s   REAL    DEFAULT 0,
                error        TEXT    DEFAULT NULL
            );

            CREATE TABLE IF NOT EXISTS agent_state (
                agent_key        TEXT PRIMARY KEY,
                consecutive_fails INTEGER DEFAULT 0,
                cb_open_until    TEXT    DEFAULT NULL,
                interval_override INTEGER DEFAULT NULL,
                total_runs       INTEGER DEFAULT 0,
                total_sent       INTEGER DEFAULT 0,
                last_run_at      TEXT    DEFAULT NULL
            );
        """)
        c.commit()
    logger.info("[metrics] Tablas de métricas inicializadas")


def record_run(
    agent_key: str,
    leads_found: int,
    leads_sent: int,
    duration_s: float,
    error: Optional[str] = None,
):
    """Registra el resultado de un ciclo del agente y actualiza su estado."""
    now_iso = datetime.now(timezone.utc).isoformat()

    with _conn() as c:
        # Log del ciclo
        c.execute(
            """INSERT INTO agent_runs
               (agent_key, run_at, leads_found, leads_sent, duration_s, error)
               VALUES (?,?,?,?,?,?)""",
            (agent_key, now_iso, leads_found, leads_sent, duration_s, error),
        )

        # Actualizar estado del agente
        if error:
            # Incrementar fallos consecutivos
            c.execute(
                """INSERT INTO agent_state
                   (agent_key, consecutive_fails, total_runs, last_run_at)
                   VALUES (?,1,1,?)
                   ON CONFLICT(agent_key) DO UPDATE SET
                     consecutive_fails = consecutive_fails + 1,
                     total_runs        = total_runs + 1,
                     last_run_at       = excluded.last_run_at""",
                (agent_key, now_iso),
            )
        else:
            c.execute(
                """INSERT INTO agent_state
                   (agent_key, consecutive_fails, total_runs, total_sent, last_run_at)
                   VALUES (?,0,1,?,?)
                   ON CONFLICT(agent_key) DO UPDATE SET
                     consecutive_fails = 0,
                     total_runs        = total_runs + 1,
                     total_sent        = total_sent + excluded.total_sent,
                     last_run_at       = excluded.last_run_at""",
                (agent_key, leads_sent, now_iso),
            )
        c.commit()

    _update_circuit_breaker(agent_key)
    _update_adaptive_interval(agent_key)


def _update_circuit_breaker(agent_key: str):
    """Abre el circuit breaker si hay demasiados fallos consecutivos."""
    with _conn() as c:
        row = c.execute(
            "SELECT consecutive_fails FROM agent_state WHERE agent_key=?",
            (agent_key,),
        ).fetchone()

        if not row:
            return

        fails = row[0]
        if fails >= CB_THRESHOLD:
            # Backoff exponencial: 15m, 30m, 60m... hasta 240m
            backoff_min = min(
                CB_BACKOFF_BASE_MIN * (2 ** (fails - CB_THRESHOLD)),
                CB_BACKOFF_MAX_MIN,
            )
            open_until = (
                datetime.now(timezone.utc) + timedelta(minutes=backoff_min)
            ).isoformat()
            c.execute(
                "UPDATE agent_state SET cb_open_until=? WHERE agent_key=?",
                (open_until, agent_key),
            )
            c.commit()
            logger.warning(
                f"[{agent_key}] Circuit breaker ABIERTO — "
                f"{fails} fallos consecutivos, pausa {backoff_min:.0f}min"
            )


def _update_adaptive_interval(agent_key: str):
    """Ajusta el intervalo del agente según productividad reciente."""
    with _conn() as c:
        rows = c.execute(
            """SELECT leads_sent FROM agent_runs
               WHERE agent_key=? AND error IS NULL
               ORDER BY id DESC LIMIT ?""",
            (agent_key, ADAPTIVE_WINDOW),
        ).fetchall()

    if len(rows) < 3:
        return  # Historial insuficiente

    avg_sent = sum(r[0] for r in rows) / len(rows)
    with _conn() as c:
        current = c.execute(
            "SELECT interval_override FROM agent_state WHERE agent_key=?",
            (agent_key,),
        ).fetchone()

    base_interval = None  # Se usará el default del registro si no hay override
    current_override = current[0] if current else None

    if avg_sent >= PRODUCTIVE_THRESHOLD:
        # Agente productivo → reducir intervalo 30% (más frecuente)
        if current_override is None:
            return  # Sin override base no podemos calcular
        new_interval = max(int(current_override * 0.7), 15)
        logger.info(f"[{agent_key}] Productivo (avg={avg_sent:.1f} leads) → intervalo {new_interval}min")
    elif avg_sent < QUIET_THRESHOLD:
        # Agente silencioso → aumentar intervalo 50% (menos frecuente)
        if current_override is None:
            return
        new_interval = min(int(current_override * 1.5), 1440)
        logger.info(f"[{agent_key}] Silencioso (avg={avg_sent:.1f} leads) → intervalo {new_interval}min")
    else:
        return

    with _conn() as c:
        c.execute(
            "UPDATE agent_state SET interval_override=? WHERE agent_key=?",
            (new_interval, agent_key),
        )
        c.commit()


def is_circuit_open(agent_key: str) -> bool:
    """Retorna True si el circuit breaker está abierto (agente en pausa)."""
    with _conn() as c:
        row = c.execute(
            "SELECT cb_open_until FROM agent_state WHERE agent_key=?",
            (agent_key,),
        ).fetchone()

    if not row or not row[0]:
        return False

    open_until = datetime.fromisoformat(row[0])
    if datetime.now(timezone.utc) < open_until:
        return True

    # Circuit breaker expiró → cerrarlo
    with _conn() as c:
        c.execute(
            "UPDATE agent_state SET cb_open_until=NULL WHERE agent_key=?",
            (agent_key,),
        )
        c.commit()
    logger.info(f"[{agent_key}] Circuit breaker cerrado — reanudando")
    return False


def get_adaptive_interval(agent_key: str, default_interval: int) -> int:
    """Retorna el intervalo efectivo para el agente (override o default)."""
    with _conn() as c:
        row = c.execute(
            "SELECT interval_override FROM agent_state WHERE agent_key=?",
            (agent_key,),
        ).fetchone()

    if row and row[0]:
        return row[0]
    return default_interval


def set_base_interval(agent_key: str, interval: int):
    """Registra el intervalo base del agente para que el adaptativo pueda calcularse."""
    with _conn() as c:
        c.execute(
            """INSERT INTO agent_state (agent_key, interval_override)
               VALUES (?,?)
               ON CONFLICT(agent_key) DO UPDATE SET
                 interval_override = COALESCE(agent_state.interval_override, excluded.interval_override)""",
            (agent_key, interval),
        )
        c.commit()


def get_health_report() -> dict:
    """
    Genera un reporte de salud de todos los agentes.
    Retorna dict con stats por agente y estado del sistema.
    """
    with _conn() as c:
        states = c.execute(
            """SELECT agent_key, consecutive_fails, cb_open_until,
                      interval_override, total_runs, total_sent, last_run_at
               FROM agent_state ORDER BY agent_key"""
        ).fetchall()

        recent_errors = c.execute(
            """SELECT agent_key, COUNT(*) as cnt FROM agent_runs
               WHERE error IS NOT NULL
               AND run_at >= datetime('now','-24 hours')
               GROUP BY agent_key"""
        ).fetchall()

    error_map = {row[0]: row[1] for row in recent_errors}

    agents = []
    for row in states:
        agent_key, fails, cb_until, interval, total_runs, total_sent, last_run = row
        cb_open = False
        if cb_until:
            cb_open = datetime.fromisoformat(cb_until) > datetime.now(timezone.utc)

        agents.append({
            "key":          agent_key,
            "total_runs":   total_runs or 0,
            "total_sent":   total_sent or 0,
            "fails_consec": fails or 0,
            "cb_open":      cb_open,
            "interval":     interval,
            "errors_24h":   error_map.get(agent_key, 0),
            "last_run":     last_run,
        })

    total_sent_all = sum(a["total_sent"] for a in agents)
    open_cbs       = sum(1 for a in agents if a["cb_open"])

    return {
        "agents":          agents,
        "total_sent_all":  total_sent_all,
        "open_cbs":        open_cbs,
        "generated_at":    datetime.now().isoformat(),
    }


def format_health_telegram(report: dict) -> str:
    """Formatea el health report para Telegram."""
    lines = [
        "📊 *HEALTH REPORT — Insulleads Agents*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        f"📨 Total leads enviados: *{report['total_sent_all']:,}*",
        "",
    ]

    if report["open_cbs"] > 0:
        lines.append(f"⚠️ Circuit breakers abiertos: *{report['open_cbs']}*")
        lines.append("")

    for a in report["agents"]:
        if a["cb_open"]:
            status = "🔴 PAUSADO"
        elif a["fails_consec"] > 0:
            status = f"🟡 {a['fails_consec']} fallos"
        else:
            status = "🟢 OK"

        errors_str = f" | ❌ {a['errors_24h']}err/24h" if a["errors_24h"] else ""
        lines.append(
            f"▸ *{a['key']}* — {status} | "
            f"✉️ {a['total_sent']:,}{errors_str}"
        )

    return "\n".join(lines)
