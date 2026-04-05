"""
utils/dedup.py
━━━━━━━━━━━━━━
Deduplicación Cross-Agent — consolida leads de múltiples agentes
que refieren a la MISMA propiedad en un solo lead enriquecido.

Problema: la misma dirección "123 Main St, SF" puede aparecer como:
  - Permiso de construcción (permits_agent)
  - Inspección de framing (construction_agent)
  - Reporte de roedores (rodents_agent)
  - Instalación solar (solar_agent)
  - Venta reciente (realestate_agent)

Solución: normalizar dirección → agrupar → fusionar datos → enviar
UN solo lead con TODA la información de todos los agentes.

Flujo:
  1. Cada agente reporta sus leads al DeduplicationEngine
  2. El engine normaliza direcciones y agrupa por propiedad
  3. Si hay match cross-agent → fusiona en "super lead"
  4. El super lead tiene score boosteado (más señales = mejor lead)
  5. Solo se notifica UNA vez por propiedad consolidada
"""

import os
import re
import logging
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "data/leads.db")

# Ventana de dedup: leads de la misma propiedad dentro de N días se consolidan
DEDUP_WINDOW_DAYS = int(os.getenv("DEDUP_WINDOW_DAYS", "30"))


# ── Normalización de direcciones ─────────────────────────────────────

_STREET_ABBR = {
    "street": "st", "avenue": "ave", "boulevard": "blvd", "drive": "dr",
    "road": "rd", "lane": "ln", "place": "pl", "court": "ct",
    "circle": "cir", "terrace": "ter", "way": "wy", "parkway": "pkwy",
    "highway": "hwy", "north": "n", "south": "s", "east": "e", "west": "w",
    "apartment": "apt", "suite": "ste", "unit": "unit", "floor": "fl",
    "building": "bldg", "number": "#",
}

_CITY_ALIASES = {
    "sf": "san francisco", "sj": "san jose", "oak": "oakland",
    "berk": "berkeley", "rich": "richmond", "sv": "sunnyvale",
    "sc": "santa clara", "alameda county": "oakland",
    "santa clara county": "san jose",
}


def normalize_address(address: str, city: str = "") -> str:
    """
    Normaliza una dirección para comparación.
    '123 Main Street, San Francisco' → '123 main st san francisco'
    """
    if not address:
        return ""

    text = address.lower().strip()
    # Remover caracteres especiales excepto números y letras
    text = re.sub(r"[,\.\#\-\/\\]", " ", text)
    # Reemplazar abreviaturas
    words = text.split()
    normalized = []
    for w in words:
        normalized.append(_STREET_ABBR.get(w, w))
    text = " ".join(normalized)
    # Colapsar espacios
    text = re.sub(r"\s+", " ", text).strip()

    # Agregar ciudad normalizada si no está en la dirección
    if city:
        city_norm = city.lower().strip()
        city_norm = _CITY_ALIASES.get(city_norm, city_norm)
        if city_norm not in text:
            text = f"{text} {city_norm}"

    return text


def _address_key(address: str, city: str = "") -> str:
    """Genera una clave única para una dirección normalizada."""
    norm = normalize_address(address, city)
    # Extraer solo número + calle (ignorar apt/suite/city)
    match = re.match(r"(\d+\s+\w+(?:\s+\w+)?)", norm)
    if match:
        return match.group(1)
    return norm[:50]  # Fallback: primeros 50 chars


# ── Motor de Deduplicación ───────────────────────────────────────────

class DeduplicationEngine:
    """
    Motor de deduplicación cross-agent.
    Consolida leads de múltiples agentes en super-leads.
    """

    def __init__(self):
        self._property_map: dict[str, list] = defaultdict(list)
        self._consolidated: dict[str, dict] = {}
        self._init_db()

    def _init_db(self):
        """Crea tabla de propiedades consolidadas."""
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS consolidated_leads (
                        address_key    TEXT PRIMARY KEY,
                        address        TEXT NOT NULL,
                        city           TEXT NOT NULL,
                        agent_sources  TEXT NOT NULL,
                        first_seen     TEXT NOT NULL,
                        last_updated   TEXT NOT NULL,
                        lead_data      TEXT NOT NULL,
                        notified       INTEGER DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS property_signals (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        address_key    TEXT NOT NULL,
                        agent_key      TEXT NOT NULL,
                        signal_type    TEXT NOT NULL,
                        signal_data    TEXT,
                        detected_at    TEXT NOT NULL,
                        UNIQUE(address_key, agent_key, signal_type)
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.debug(f"[Dedup] DB init: {e}")

    def _get_conn(self) -> sqlite3.Connection:
        import os
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        return sqlite3.connect(DB_PATH)

    def register_lead(self, lead: dict, agent_key: str) -> dict:
        """
        Registra un lead de un agente. Si la misma propiedad ya fue
        reportada por otro agente, consolida la información.

        Retorna el lead consolidado (puede tener más datos que el original).
        """
        address = lead.get("address", "")
        city = lead.get("city", "")
        if not address:
            return lead

        addr_key = _address_key(address, city)

        # Registrar señal en DB
        self._record_signal(addr_key, agent_key, lead)

        # Buscar señales previas de otros agentes
        other_signals = self._get_signals(addr_key, exclude_agent=agent_key)

        if not other_signals:
            # No hay cross-match, pero persiste el lead en consolidated_leads
            self._persist_consolidated(addr_key, lead, [agent_key])
            return lead  # Retornar original

        # ── Consolidar: fusionar datos de todos los agentes ──────
        consolidated = dict(lead)  # Copiar lead original

        source_agents = [agent_key]
        for signal in other_signals:
            source_agents.append(signal["agent_key"])
            signal_data = signal.get("signal_data_parsed", {})

            # Fusionar campos que el lead original no tiene
            for key in ["contractor", "owner", "contact_phone", "contact_email",
                        "value_float", "year_built", "property_age", "lic_number",
                        "insulation_need", "assessed_value", "energy_score",
                        "solar_potential", "pest_type", "severity"]:
                if not consolidated.get(key) and signal_data.get(key):
                    consolidated[key] = signal_data[key]

            # Valor: tomar el mayor
            if signal_data.get("value_float", 0) > consolidated.get("value_float", 0):
                consolidated["value_float"] = signal_data["value_float"]

        # Agregar metadata de consolidación
        unique_agents = sorted(set(source_agents))
        consolidated["_cross_agent_sources"] = unique_agents
        consolidated["_cross_agent_count"] = len(unique_agents)
        consolidated["_is_consolidated"] = True

        # Descripción de señales
        signal_descriptions = []
        agent_labels = {
            "permits": "🏗️ Permiso", "solar": "☀️ Solar",
            "rodents": "🐀 Plaga", "construction": "🚧 Construcción",
            "realestate": "🏠 Venta", "energy": "⚡ Energía",
            "flood": "🌊 Inundación", "places": "📍 Negocio",
            "yelp": "⭐ Yelp", "deconstruction": "🔨 Deconstrucción",
        }
        for ag in unique_agents:
            signal_descriptions.append(agent_labels.get(ag, ag))
        consolidated["_signal_summary"] = " + ".join(signal_descriptions)

        # Score boost por múltiples señales
        cross_boost = (len(unique_agents) - 1) * 10  # +10 por cada agente adicional
        if consolidated.get("_scoring"):
            consolidated["_scoring"]["score"] = min(
                consolidated["_scoring"]["score"] + cross_boost, 100
            )
            consolidated["_scoring"]["reasons"].insert(
                0, f"🔗 {len(unique_agents)} señales cruzadas"
            )

        logger.info(
            f"[Dedup] Consolidado: {address} ({city}) — "
            f"{len(unique_agents)} agentes: {', '.join(unique_agents)}"
        )

        # Persistir lead consolidado en DB
        self._persist_consolidated(addr_key, consolidated, unique_agents)

        return consolidated

    def _persist_consolidated(self, addr_key: str, lead: dict, agents: list):
        """Persiste lead consolidado en la tabla consolidated_leads."""
        import json
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO consolidated_leads
                    (address_key, address, city, agent_sources, first_seen, last_updated, lead_data, notified)
                    VALUES (?, ?, ?, ?, COALESCE(
                        (SELECT first_seen FROM consolidated_leads WHERE address_key = ?),
                        ?
                    ), ?, ?, 0)
                """, (
                    addr_key,
                    lead.get("address", ""),
                    lead.get("city", ""),
                    ",".join(agents),
                    addr_key,
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                    json.dumps(lead, default=str),
                ))
                conn.commit()
        except Exception as e:
            logger.debug(f"[Dedup] Persist consolidated error: {e}")

    def _record_signal(self, address_key: str, agent_key: str, lead: dict):
        """Registra una señal de un agente para una propiedad."""
        import json
        # Extraer datos relevantes para persistir
        signal_data = {
            k: lead.get(k) for k in [
                "contractor", "owner", "contact_phone", "contact_email",
                "value_float", "year_built", "property_age", "lic_number",
                "insulation_need", "assessed_value", "energy_score",
                "solar_potential", "pest_type", "severity", "phase",
                "description",
            ] if lead.get(k)
        }

        signal_type = lead.get("_agent_key", agent_key)

        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO property_signals
                    (address_key, agent_key, signal_type, signal_data, detected_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    address_key, agent_key, signal_type,
                    json.dumps(signal_data, default=str),
                    datetime.utcnow().isoformat(),
                ))
                conn.commit()
        except Exception as e:
            logger.debug(f"[Dedup] Record signal error: {e}")

    def _get_signals(self, address_key: str,
                     exclude_agent: str = "") -> list:
        """Obtiene señales previas de otros agentes para una propiedad."""
        import json
        cutoff = (datetime.utcnow() - timedelta(days=DEDUP_WINDOW_DAYS)).isoformat()

        try:
            with self._get_conn() as conn:
                rows = conn.execute("""
                    SELECT agent_key, signal_type, signal_data, detected_at
                    FROM property_signals
                    WHERE address_key = ? AND agent_key != ? AND detected_at >= ?
                """, (address_key, exclude_agent, cutoff)).fetchall()

            signals = []
            for row in rows:
                signal = {
                    "agent_key":   row[0],
                    "signal_type": row[1],
                    "detected_at": row[3],
                }
                try:
                    signal["signal_data_parsed"] = json.loads(row[2]) if row[2] else {}
                except (json.JSONDecodeError, TypeError):
                    signal["signal_data_parsed"] = {}
                signals.append(signal)
            return signals

        except Exception as e:
            logger.debug(f"[Dedup] Get signals error: {e}")
            return []

    def get_multi_signal_properties(self, min_signals: int = 2) -> list:
        """
        Retorna propiedades con señales de múltiples agentes.
        Útil para reportes de hot leads consolidados.
        """
        try:
            with self._get_conn() as conn:
                cutoff = (datetime.utcnow() - timedelta(days=DEDUP_WINDOW_DAYS)).isoformat()
                rows = conn.execute("""
                    SELECT address_key, COUNT(DISTINCT agent_key) as agent_count,
                           GROUP_CONCAT(DISTINCT agent_key) as agents
                    FROM property_signals
                    WHERE detected_at >= ?
                    GROUP BY address_key
                    HAVING agent_count >= ?
                    ORDER BY agent_count DESC
                """, (cutoff, min_signals)).fetchall()

            return [
                {"address_key": r[0], "agent_count": r[1], "agents": r[2].split(",")}
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"[Dedup] Multi-signal query error: {e}")
            return []


# ── Singleton global ─────────────────────────────────────────────────
_engine: DeduplicationEngine | None = None

def get_dedup_engine() -> DeduplicationEngine:
    global _engine
    if _engine is None:
        _engine = DeduplicationEngine()
    return _engine
