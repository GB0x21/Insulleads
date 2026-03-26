"""
utils/db.py — SQLite para deduplicación de leads
Nunca se envía el mismo lead dos veces.
"""

import os
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/leads.db")


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    """Crea la tabla si no existe."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_leads (
                agent_key  TEXT NOT NULL,
                lead_id    TEXT NOT NULL,
                sent_at    TEXT NOT NULL,
                PRIMARY KEY (agent_key, lead_id)
            )
        """)
        conn.commit()
    logger.info(f"Base de datos inicializada: {DB_PATH}")


def is_sent(agent_key: str, lead_id: str) -> bool:
    """Retorna True si el lead ya fue enviado."""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM sent_leads WHERE agent_key=? AND lead_id=?",
            (agent_key, lead_id),
        ).fetchone()
    return row is not None


def mark_sent(agent_key: str, lead_id: str):
    """Registra el lead como enviado."""
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sent_leads (agent_key, lead_id, sent_at) VALUES (?,?,?)",
            (agent_key, lead_id, datetime.utcnow().isoformat()),
        )
        conn.commit()


def get_stats() -> dict:
    """Retorna conteo de leads enviados por agente."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT agent_key, COUNT(*) FROM sent_leads GROUP BY agent_key"
        ).fetchall()
    return {row[0]: row[1] for row in rows}
