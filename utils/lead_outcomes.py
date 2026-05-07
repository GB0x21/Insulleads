"""
utils/lead_outcomes.py
━━━━━━━━━━━━━━━━━━━━━━
Historial de Conversiones — Feedback Loop del CRM a los Agentes.

claude-mem mantiene memoria entre sesiones para inyectar contexto futuro.
Aquí implementamos el equivalente para leads: cuando un lead se marca como
WON/LOST/REPLIED en el CRM, esa información retroalimenta el scoring.

Flujo:
  1. CRM (crm/views.py) llama a register_outcome() al hacer label_lead
  2. register_outcome() guarda el outcome con features del lead en SQLite
  3. score_lead_with_memory() consulta outcomes similares antes de puntuar
  4. Si hay WON similares → bonus al score; si hay LOST → penalty

Tabla: lead_outcomes
  Guarda features clave del lead + outcome (won/lost/replied) + valor real
  para que el sistema pueda aprender qué características predicen conversión.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from utils.memory import _hash_embed, _cosine

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/leads.db")


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return float(raw)
    except ValueError:
        return default


MIN_OUTCOMES_FOR_SIGNAL = _env_int("MIN_OUTCOMES_FOR_SIGNAL", 5)
SIMILARITY_THRESHOLD    = _env_float("OUTCOME_SIMILARITY_THRESHOLD", 0.6)
# Máximo bonus/penalty de memoria en puntos (de 0-100)
MAX_MEMORY_BONUS        = _env_int("MAX_MEMORY_BONUS", 15)
MAX_MEMORY_PENALTY      = _env_int("MAX_MEMORY_PENALTY", 10)

OUTCOME_WON     = "won"
OUTCOME_LOST    = "lost"
OUTCOME_REPLIED = "replied"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS lead_outcomes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         TEXT    NOT NULL,
    agent_key       TEXT    NOT NULL,
    outcome         TEXT    NOT NULL,   -- 'won','lost','replied'
    city            TEXT    DEFAULT '',
    project_type    TEXT    DEFAULT '',
    contractor      TEXT    DEFAULT '',
    value_float     REAL    DEFAULT 0,
    has_phone       INTEGER DEFAULT 0,
    has_email       INTEGER DEFAULT 0,
    content_key     TEXT    NOT NULL,   -- SHA1 del texto del lead (dedup)
    vector_blob     TEXT    NOT NULL,   -- JSON list (64-dim hash embedding)
    raw_features    TEXT    DEFAULT '{}',
    registered_at   TEXT    NOT NULL,
    UNIQUE(lead_id, outcome)
);

CREATE INDEX IF NOT EXISTS idx_outcomes_agent
    ON lead_outcomes(agent_key);
CREATE INDEX IF NOT EXISTS idx_outcomes_city
    ON lead_outcomes(city);
CREATE INDEX IF NOT EXISTS idx_outcomes_outcome
    ON lead_outcomes(outcome);
"""


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_outcomes_db():
    """Crea las tablas de outcomes si no existen."""
    with _conn() as c:
        c.executescript(_SCHEMA)
        c.commit()
    logger.info("[outcomes] Tablas de outcomes inicializadas")


def _lead_to_text(lead: dict) -> str:
    """Convierte un lead dict en texto para embedding."""
    parts = [
        lead.get("city", ""),
        lead.get("address", ""),
        lead.get("description", "") or lead.get("permit_type", ""),
        lead.get("contractor", ""),
        lead.get("owner", ""),
    ]
    return " ".join(p for p in parts if p).lower()


def register_outcome(lead: dict, outcome: str):
    """
    Registra el resultado de un lead en el historial de outcomes.

    Llamar desde:
      - crm/views.py cuando el usuario hace label_lead (WON/LOST/REPLIED)
      - agents directamente si pueden detectar conversión

    lead: dict con campos del lead (id, city, address, contractor, etc.)
    outcome: 'won', 'lost', 'replied'
    """
    if outcome not in (OUTCOME_WON, OUTCOME_LOST, OUTCOME_REPLIED):
        logger.warning(f"[outcomes] Outcome inválido: {outcome}")
        return

    lead_id   = str(lead.get("id", ""))
    agent_key = lead.get("_agent_key", lead.get("agent_key", "unknown"))
    city      = lead.get("city", "")
    ptype     = lead.get("permit_type") or lead.get("project_type") or lead.get("description", "")[:50]
    contractor = lead.get("contractor") or lead.get("contact_name", "")
    value     = float(lead.get("value_float", 0) or 0)
    has_phone = int(bool(lead.get("contact_phone") or lead.get("phone")))
    has_email = int(bool(lead.get("contact_email")))

    text     = _lead_to_text(lead)
    ckey     = hashlib.sha1(text.encode()).hexdigest()[:16]
    vector   = _hash_embed(text)
    raw_feats = json.dumps({
        k: lead.get(k)
        for k in ["city", "address", "permit_type", "value_float",
                  "contractor", "contact_phone", "contact_email",
                  "description", "_agent_key"]
    })

    now = datetime.now(timezone.utc).isoformat()

    with _conn() as c:
        c.execute(
            """INSERT OR IGNORE INTO lead_outcomes
               (lead_id, agent_key, outcome, city, project_type, contractor,
                value_float, has_phone, has_email, content_key, vector_blob,
                raw_features, registered_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (lead_id, agent_key, outcome, city, ptype, contractor,
             value, has_phone, has_email, ckey, json.dumps(vector),
             raw_feats, now),
        )
        c.commit()

    logger.info(
        f"[outcomes] Registrado: [{outcome.upper()}] {city} — {contractor or 'sin contratista'}"
    )


def find_similar_outcomes(
    lead: dict,
    limit: int = 10,
    min_similarity: float = SIMILARITY_THRESHOLD,
) -> list[dict]:
    """
    Busca outcomes de leads similares al dado usando similitud vectorial.

    Retorna lista de outcomes ordenados por similitud, cada uno con:
      outcome, city, contractor, value_float, similarity
    """
    with _conn() as c:
        count = c.execute("SELECT COUNT(*) FROM lead_outcomes").fetchone()[0]

    if count < MIN_OUTCOMES_FOR_SIGNAL:
        return []

    text       = _lead_to_text(lead)
    query_vec  = _hash_embed(text)

    with _conn() as c:
        rows = c.execute(
            """SELECT outcome, city, project_type, contractor,
                      value_float, has_phone, has_email, vector_blob
               FROM lead_outcomes
               ORDER BY id DESC LIMIT 500"""
        ).fetchall()

    scored = []
    for row in rows:
        try:
            vec = json.loads(row["vector_blob"])
        except (json.JSONDecodeError, TypeError):
            continue
        sim = _cosine(query_vec, vec)
        if sim >= min_similarity:
            scored.append({
                "outcome":      row["outcome"],
                "city":         row["city"],
                "contractor":   row["contractor"],
                "value_float":  row["value_float"],
                "similarity":   round(sim, 3),
            })

    scored.sort(key=lambda x: -x["similarity"])
    return scored[:limit]


def outcome_memory_bonus(lead: dict) -> tuple[int, list[str]]:
    """
    Calcula un bonus/penalty para el score del lead basado en historial.

    Retorna:
      (bonus: int, reasons: list[str])
      bonus puede ser negativo (penalty)

    Lógica:
      - Si el contratista exacto tiene WON previos → bonus máximo
      - Si la ciudad tiene ratio WON alto → bonus moderado
      - Si leads similares resultan en WON más que LOST → bonus proporcional
      - Caso opuesto → penalty
    """
    reasons = []
    total_bonus = 0

    contractor = (lead.get("contractor") or lead.get("contact_name", "")).lower().strip()
    city       = (lead.get("city", "")).lower().strip()

    with _conn() as c:
        count = c.execute("SELECT COUNT(*) FROM lead_outcomes").fetchone()[0]

    if count < MIN_OUTCOMES_FOR_SIGNAL:
        return 0, []

    # ── Bonus 1: Contratista conocido con WON previo ────────────────────
    if contractor:
        with _conn() as c:
            contractor_rows = c.execute(
                """SELECT outcome, COUNT(*) as cnt FROM lead_outcomes
                   WHERE lower(contractor) = ?
                   GROUP BY outcome""",
                (contractor,),
            ).fetchall()

        if contractor_rows:
            c_outcomes = {r["outcome"]: r["cnt"] for r in contractor_rows}
            won_c  = c_outcomes.get(OUTCOME_WON, 0) + c_outcomes.get(OUTCOME_REPLIED, 0)
            lost_c = c_outcomes.get(OUTCOME_LOST, 0)

            if won_c > 0 and lost_c == 0:
                total_bonus += MAX_MEMORY_BONUS
                reasons.append(f"Contratista '{contractor}' con WON previo (memoria)")
            elif won_c > lost_c:
                pts = int(MAX_MEMORY_BONUS * won_c / (won_c + lost_c))
                total_bonus += pts
                reasons.append(f"Contratista con buen historial ({won_c}W/{lost_c}L)")
            elif lost_c > won_c * 2:
                total_bonus -= int(MAX_MEMORY_PENALTY * 0.5)
                reasons.append(f"Contratista con historial negativo ({won_c}W/{lost_c}L)")

    # ── Bonus 2: Ciudad con buen ratio de conversión ────────────────────
    if city:
        with _conn() as c:
            city_rows = c.execute(
                """SELECT outcome, COUNT(*) as cnt FROM lead_outcomes
                   WHERE lower(city) = ?
                   GROUP BY outcome""",
                (city,),
            ).fetchall()

        if city_rows:
            c_out = {r["outcome"]: r["cnt"] for r in city_rows}
            won_city  = c_out.get(OUTCOME_WON, 0) + c_out.get(OUTCOME_REPLIED, 0)
            lost_city = c_out.get(OUTCOME_LOST, 0)
            total_city = won_city + lost_city

            if total_city >= 3:
                ratio = won_city / total_city
                if ratio >= 0.6:
                    pts = int(8 * ratio)
                    total_bonus += pts
                    reasons.append(f"Ciudad {city} con ratio WON {ratio:.0%} (memoria)")
                elif ratio <= 0.25:
                    total_bonus -= 5
                    reasons.append(f"Ciudad {city} con bajo ratio WON (memoria)")

    # ── Bonus 3: Similitud vectorial con outcomes históricos ─────────────
    similar = find_similar_outcomes(lead, limit=5)
    if similar:
        won_sim  = sum(1 for s in similar if s["outcome"] in (OUTCOME_WON, OUTCOME_REPLIED))
        lost_sim = sum(1 for s in similar if s["outcome"] == OUTCOME_LOST)
        total_sim = won_sim + lost_sim

        if total_sim >= 2:
            ratio = won_sim / total_sim
            if ratio >= 0.6:
                pts = min(int(6 * ratio), 6)
                total_bonus += pts
                reasons.append(f"Leads similares con {won_sim}/{total_sim} WON (memoria)")
            elif ratio <= 0.25:
                total_bonus -= 4
                reasons.append(f"Leads similares con bajo ratio WON (memoria)")

    # Clamp al rango permitido
    total_bonus = max(-MAX_MEMORY_PENALTY, min(MAX_MEMORY_BONUS, total_bonus))
    return total_bonus, reasons


def get_outcome_stats() -> dict:
    """
    Retorna estadísticas globales de outcomes para el health report.
    """
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM lead_outcomes").fetchone()[0]
        by_outcome = c.execute(
            "SELECT outcome, COUNT(*) as cnt FROM lead_outcomes GROUP BY outcome"
        ).fetchall()
        by_agent = c.execute(
            """SELECT agent_key, outcome, COUNT(*) as cnt
               FROM lead_outcomes GROUP BY agent_key, outcome"""
        ).fetchall()
        top_cities = c.execute(
            """SELECT city, outcome, COUNT(*) as cnt
               FROM lead_outcomes WHERE outcome='won'
               GROUP BY city ORDER BY cnt DESC LIMIT 5"""
        ).fetchall()

    outcomes_map = {r["outcome"]: r["cnt"] for r in by_outcome}

    return {
        "total":      total,
        "won":        outcomes_map.get("won", 0),
        "lost":       outcomes_map.get("lost", 0),
        "replied":    outcomes_map.get("replied", 0),
        "by_agent":   [dict(r) for r in by_agent],
        "top_cities": [dict(r) for r in top_cities],
    }
