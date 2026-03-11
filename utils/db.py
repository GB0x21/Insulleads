"""
utils/db.py — SQLite helper para evitar leads duplicados
y guardar historial de todo lo enviado a Telegram.
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "leads.db")


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sent_leads (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            agent       TEXT NOT NULL,
            external_id TEXT NOT NULL,
            address     TEXT,
            details     TEXT,
            sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(agent, external_id)
        );

        CREATE TABLE IF NOT EXISTS agent_runs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            agent      TEXT NOT NULL,
            ran_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            new_leads  INTEGER DEFAULT 0,
            error      TEXT
        );
    """)
    conn.commit()
    conn.close()


def is_already_sent(agent: str, external_id: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM sent_leads WHERE agent=? AND external_id=?",
        (agent, external_id)
    ).fetchone()
    conn.close()
    return row is not None


def mark_as_sent(agent: str, external_id: str, address: str = "", details: str = ""):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO sent_leads (agent, external_id, address, details) VALUES (?,?,?,?)",
            (agent, external_id, address, details)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # ya existe
    finally:
        conn.close()


def log_run(agent: str, new_leads: int, error: str = None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO agent_runs (agent, new_leads, error) VALUES (?,?,?)",
        (agent, new_leads, error)
    )
    conn.commit()
    conn.close()


def get_stats():
    conn = get_conn()
    rows = conn.execute("""
        SELECT agent,
               COUNT(*) as total,
               MAX(sent_at) as last_lead
        FROM sent_leads
        GROUP BY agent
        ORDER BY total DESC
    """).fetchall()
    conn.close()
    return rows
