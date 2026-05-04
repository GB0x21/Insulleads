"""
utils/memory.py
━━━━━━━━━━━━━━━
Motor de Memoria Persistente — inspirado en claude-mem.

claude-mem guarda observaciones de sesión en SQLite + Chroma y las inyecta
en sesiones futuras. Aquí adaptamos ese mismo patrón para los agentes de
Insulleads: cada agente acumula "observaciones" de lo que aprende en cada
ciclo, y esa memoria enriquece las decisiones de scoring y routing.

Arquitectura (paralela a claude-mem):
  ┌──────────────────────────────────────────────────────────┐
  │  claude-mem              │  utils/memory.py              │
  │──────────────────────────│───────────────────────────────│
  │  Sessions + observations │  memories table (FTS5)        │
  │  Chroma vector search    │  Hash embeddings (sin deps)   │
  │  Session summaries       │  agent_summaries table        │
  │  Context injection       │  get_context(agent, query)    │
  │  Progressive disclosure  │  search → get_full (3-layer)  │
  └──────────────────────────────────────────────────────────┘

Tablas SQLite:
  memories        — observaciones individuales con FTS5
  agent_summaries — resúmenes comprimidos por agente
  memory_vectors  — embeddings hash para similitud vectorial
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/leads.db")

# Cuántas observaciones mantener antes de comprimir (similar al window de claude-mem)
MEMORY_WINDOW     = int(os.getenv("MEMORY_WINDOW", "200"))
# Cuántas observaciones usar para generar un resumen comprimido
COMPRESS_TRIGGER  = int(os.getenv("MEMORY_COMPRESS_TRIGGER", "50"))
# Dimensión del hash embedding
EMBED_DIM         = 64


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_key   TEXT    NOT NULL,
    obs_type    TEXT    NOT NULL,      -- 'lead_found','lead_sent','pattern','insight'
    content     TEXT    NOT NULL,      -- texto legible de la observación
    metadata    TEXT    DEFAULT '{}',  -- JSON con datos del lead/contexto
    created_at  TEXT    NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
    content,
    agent_key UNINDEXED,
    obs_type  UNINDEXED,
    content='memories',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
    INSERT INTO memories_fts(rowid, content, agent_key, obs_type)
    VALUES (new.id, new.content, new.agent_key, new.obs_type);
END;

CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
    INSERT INTO memories_fts(memories_fts, rowid, content, agent_key, obs_type)
    VALUES ('delete', old.id, old.content, old.agent_key, old.obs_type);
END;

CREATE TABLE IF NOT EXISTS agent_summaries (
    agent_key   TEXT PRIMARY KEY,
    summary     TEXT NOT NULL,
    obs_count   INTEGER DEFAULT 0,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_vectors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_key   TEXT NOT NULL,
    obs_type    TEXT NOT NULL,
    content_key TEXT NOT NULL,         -- hash(content) para dedup
    vector_blob BLOB NOT NULL,         -- pickled numpy array (64-dim)
    metadata    TEXT DEFAULT '{}',
    created_at  TEXT NOT NULL,
    UNIQUE(content_key)
);
"""


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_memory_db():
    """Crea las tablas de memoria si no existen."""
    with _conn() as c:
        c.executescript(_SCHEMA)
        c.commit()
    logger.info("[memory] Tablas de memoria inicializadas")


# ── Hash embedding (sin dependencias externas) ────────────────────────────────

def _hash_embed(text: str, dim: int = EMBED_DIM) -> list[float]:
    """
    Embedding determinístico por feature hashing.
    Mismo algoritmo que outreach/ml/embeddings.py pero en 64 dims y
    retorna lista (evita numpy como dep obligatoria).
    """
    vec = [0.0] * dim
    for token in text.lower().split():
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 16) % 2 == 0 else -1.0
        vec[idx] += sign

    norm = sum(x * x for x in vec) ** 0.5
    if norm > 0:
        vec = [x / norm for x in vec]
    return vec


def _cosine(a: list[float], b: list[float]) -> float:
    dot   = sum(x * y for x, y in zip(a, b))
    na    = sum(x * x for x in a) ** 0.5
    nb    = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _content_key(content: str) -> str:
    return hashlib.sha1(content.encode()).hexdigest()[:16]


# ── Core API ──────────────────────────────────────────────────────────────────

def record_observation(
    agent_key: str,
    obs_type: str,
    content: str,
    metadata: Optional[dict] = None,
):
    """
    Registra una observación en la memoria del agente.

    obs_type:
      'lead_found'  — un lead fue encontrado (con o sin contacto)
      'lead_sent'   — un lead fue enviado a Telegram
      'pattern'     — un patrón notable detectado (hot zone, tipo de proyecto)
      'insight'     — aprendizaje derivado del scoring / feedback
    """
    now    = datetime.now(timezone.utc).isoformat()
    meta   = json.dumps(metadata or {})
    ckey   = _content_key(content)
    vector = _hash_embed(content)

    with _conn() as c:
        c.execute(
            """INSERT INTO memories (agent_key, obs_type, content, metadata, created_at)
               VALUES (?,?,?,?,?)""",
            (agent_key, obs_type, content, meta, now),
        )

        # Upsert vector (ignora duplicados por content_key)
        c.execute(
            """INSERT OR IGNORE INTO memory_vectors
               (agent_key, obs_type, content_key, vector_blob, metadata, created_at)
               VALUES (?,?,?,?,?,?)""",
            (agent_key, obs_type, ckey, json.dumps(vector), meta, now),
        )
        c.commit()

    # Comprimir si el agente acumuló muchas observaciones
    _maybe_compress(agent_key)


def search_memories(
    query: str,
    agent_key: Optional[str] = None,
    obs_type: Optional[str] = None,
    limit: int = 10,
    min_score: float = 0.0,
) -> list[dict]:
    """
    Búsqueda híbrida: FTS5 (texto) + cosine similarity (vector).
    Retorna lista de observaciones ordenadas por relevancia.

    Sigue el patrón de 3 capas de claude-mem:
      Layer 1: FTS5 rápido → IDs + snippets
      Layer 2: Re-rank por similitud vectorial
      Layer 3: Resultado final con metadata completa
    """
    # Capa 1: FTS5 full-text search
    fts_filter = ""
    params: list = [query]
    if agent_key:
        fts_filter += " AND m.agent_key = ?"
        params.append(agent_key)
    if obs_type:
        fts_filter += " AND m.obs_type = ?"
        params.append(obs_type)
    params.append(limit * 3)  # traer más para re-rankear

    with _conn() as c:
        rows = c.execute(
            f"""SELECT m.id, m.agent_key, m.obs_type, m.content, m.metadata, m.created_at
                FROM memories m
                JOIN memories_fts fts ON fts.rowid = m.id
                WHERE memories_fts MATCH ?
                {fts_filter}
                ORDER BY rank
                LIMIT ?""",
            params,
        ).fetchall()

    if not rows:
        # Fallback: últimas observaciones del agente si no hay resultados FTS
        fallback_params: list = []
        fallback_filter = ""
        if agent_key:
            fallback_filter = "WHERE agent_key = ?"
            fallback_params.append(agent_key)
        with _conn() as c:
            rows = c.execute(
                f"SELECT id, agent_key, obs_type, content, metadata, created_at "
                f"FROM memories {fallback_filter} ORDER BY id DESC LIMIT ?",
                fallback_params + [limit],
            ).fetchall()

    if not rows:
        return []

    # Capa 2: Re-rank por similitud vectorial
    query_vec = _hash_embed(query)
    scored = []
    for row in rows:
        content_vec = _hash_embed(row["content"])
        sim = _cosine(query_vec, content_vec)
        scored.append((sim, dict(row)))

    scored.sort(key=lambda x: -x[0])

    # Capa 3: Devolver los mejores con score
    result = []
    for sim, row in scored[:limit]:
        if sim < min_score:
            continue
        try:
            row["metadata"] = json.loads(row.get("metadata") or "{}")
        except (json.JSONDecodeError, TypeError):
            row["metadata"] = {}
        row["_similarity"] = round(sim, 3)
        result.append(row)

    return result


def get_context(agent_key: str, query: str = "", max_tokens: int = 500) -> str:
    """
    Genera contexto inyectable para el agente — análogo al context injection
    de claude-mem. Combina el resumen del agente con observaciones relevantes.

    max_tokens: límite aproximado de caracteres (claude-mem usa token budget).
    """
    parts = []

    # Resumen del agente (equivalent a la session summary de claude-mem)
    summary = get_summary(agent_key)
    if summary:
        parts.append(f"[Memoria del agente {agent_key}]\n{summary}")

    # Observaciones relevantes si hay query
    if query:
        memories = search_memories(query, agent_key=agent_key, limit=5)
        if memories:
            obs_lines = []
            for m in memories:
                obs_lines.append(f"  • [{m['obs_type']}] {m['content'][:120]}")
            parts.append("Observaciones relevantes:\n" + "\n".join(obs_lines))

    context = "\n\n".join(parts)
    return context[:max_tokens] if context else ""


def get_summary(agent_key: str) -> str:
    """Retorna el resumen comprimido del agente."""
    with _conn() as c:
        row = c.execute(
            "SELECT summary FROM agent_summaries WHERE agent_key=?",
            (agent_key,),
        ).fetchone()
    return row["summary"] if row else ""


def update_summary(agent_key: str, summary: str, obs_count: int = 0):
    """Actualiza el resumen del agente."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute(
            """INSERT INTO agent_summaries (agent_key, summary, obs_count, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(agent_key) DO UPDATE SET
                 summary    = excluded.summary,
                 obs_count  = obs_count + excluded.obs_count,
                 updated_at = excluded.updated_at""",
            (agent_key, summary, obs_count, now),
        )
        c.commit()


def _maybe_compress(agent_key: str):
    """
    Comprime observaciones antiguas en un resumen cuando hay demasiadas.
    Análogo a la compresión de contexto de claude-mem (Endless Mode).
    Sin LLM: generamos un resumen estadístico determinístico.
    """
    with _conn() as c:
        count = c.execute(
            "SELECT COUNT(*) FROM memories WHERE agent_key=?",
            (agent_key,),
        ).fetchone()[0]

    if count < COMPRESS_TRIGGER:
        return

    # Ya tenemos suficiente historial — generar resumen estadístico
    _build_statistical_summary(agent_key)

    # Mantener solo las últimas MEMORY_WINDOW observaciones
    with _conn() as c:
        oldest_keep = c.execute(
            """SELECT id FROM memories WHERE agent_key=?
               ORDER BY id DESC LIMIT 1 OFFSET ?""",
            (agent_key, MEMORY_WINDOW),
        ).fetchone()

        if oldest_keep:
            c.execute(
                "DELETE FROM memories WHERE agent_key=? AND id < ?",
                (agent_key, oldest_keep["id"]),
            )
            c.commit()
            logger.info(
                f"[memory] {agent_key}: comprimido a {MEMORY_WINDOW} observaciones"
            )


def _build_statistical_summary(agent_key: str):
    """
    Genera un resumen estadístico de las observaciones del agente.
    No requiere LLM — usa agregaciones de los metadatos.
    """
    with _conn() as c:
        rows = c.execute(
            """SELECT obs_type, content, metadata FROM memories
               WHERE agent_key=? ORDER BY id DESC LIMIT 200""",
            (agent_key,),
        ).fetchall()

    if not rows:
        return

    # Conteos por tipo
    type_counts: dict[str, int] = {}
    cities: dict[str, int] = {}
    project_types: dict[str, int] = {}
    total_value = 0.0
    value_count = 0

    for row in rows:
        obs_type = row["obs_type"]
        type_counts[obs_type] = type_counts.get(obs_type, 0) + 1
        try:
            meta = json.loads(row["metadata"] or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}

        city = meta.get("city", "")
        if city:
            cities[city] = cities.get(city, 0) + 1

        ptype = meta.get("permit_type") or meta.get("project_type", "")
        if ptype:
            project_types[ptype] = project_types.get(ptype, 0) + 1

        val = meta.get("value_float", 0)
        if val and isinstance(val, (int, float)) and val > 0:
            total_value += val
            value_count += 1

    # Top cities y project types
    top_cities    = sorted(cities.items(), key=lambda x: -x[1])[:5]
    top_ptypes    = sorted(project_types.items(), key=lambda x: -x[1])[:5]
    avg_value     = total_value / value_count if value_count else 0

    summary_parts = [
        f"Agente {agent_key} — {len(rows)} observaciones recientes",
        f"Leads encontrados: {type_counts.get('lead_found', 0)} | "
        f"Enviados: {type_counts.get('lead_sent', 0)}",
    ]
    if top_cities:
        cities_str = ", ".join(f"{c} ({n})" for c, n in top_cities)
        summary_parts.append(f"Ciudades más activas: {cities_str}")
    if top_ptypes:
        types_str = ", ".join(f"{t} ({n})" for t, n in top_ptypes[:3])
        summary_parts.append(f"Tipos de proyecto: {types_str}")
    if avg_value > 0:
        summary_parts.append(f"Valor promedio de proyecto: ${avg_value:,.0f}")

    summary = " | ".join(summary_parts)
    update_summary(agent_key, summary, obs_count=len(rows))
    logger.info(f"[memory] Resumen actualizado para {agent_key}: {summary[:80]}...")


# ── Utilidades de contexto para scoring ───────────────────────────────────────

def get_agent_patterns(agent_key: str) -> dict:
    """
    Extrae patrones aprendidos por el agente desde su memoria.
    Usado para enriquecer el scoring con conocimiento histórico.

    Retorna:
      top_cities:    {city: count} — ciudades más activas
      top_sources:   {obs_type: count}
      avg_value:     float — valor promedio de proyectos
      sent_ratio:    float — ratio leads_sent / leads_found
    """
    with _conn() as c:
        rows = c.execute(
            """SELECT obs_type, metadata FROM memories
               WHERE agent_key=? ORDER BY id DESC LIMIT 100""",
            (agent_key,),
        ).fetchall()

    if not rows:
        return {}

    cities: dict[str, int] = {}
    total_found = total_sent = 0
    values: list[float] = []

    for row in rows:
        if row["obs_type"] == "lead_found":
            total_found += 1
        elif row["obs_type"] == "lead_sent":
            total_sent += 1
        try:
            meta = json.loads(row["metadata"] or "{}")
        except (json.JSONDecodeError, TypeError):
            meta = {}
        city = meta.get("city", "")
        if city:
            cities[city] = cities.get(city, 0) + 1
        val = meta.get("value_float", 0)
        if val and isinstance(val, (int, float)) and val > 0:
            values.append(float(val))

    return {
        "top_cities":  dict(sorted(cities.items(), key=lambda x: -x[1])[:10]),
        "avg_value":   sum(values) / len(values) if values else 0.0,
        "sent_ratio":  total_sent / total_found if total_found else 0.0,
        "total_found": total_found,
        "total_sent":  total_sent,
    }
