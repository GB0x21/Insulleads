#!/usr/bin/env python3
"""
utils/crm_sync.py — Sync leads from Insulleads SQLite → Krayin CRM

Reads consolidated_leads where crm_synced=0, creates Person + Lead
in Krayin CRM via direct MySQL insertion, then marks crm_synced=1.

Designed to run every 5 minutes via systemd timer or cron:
    */5 * * * * /home/insulleads/Insulleads/venv/bin/python /home/insulleads/Insulleads/utils/crm_sync.py

Requires:
    DB_PATH                — SQLite path (default: data/leads.db)

MySQL connection reads from Krayin's .env automatically.

Configuration file (created by crm_setup.py or diagnose.sh):
    data/crm_config.json   — pipeline_id, source_ids, agent_to_source mapping
"""

import os
import sys
import json
import sqlite3
import logging
import subprocess
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CRM-Sync] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "data/leads.db")
BATCH_SIZE = int(os.getenv("CRM_SYNC_BATCH", "50"))

# Krayin CRM directory (for reading MySQL credentials from its .env)
CRM_DIR = os.getenv("CRM_DIR", "/home/insulleads/krayin-crm")

# ── Load CRM config (created by crm_setup.py) ──────────────────
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "crm_config.json"
)

# Agent key → source name mapping
AGENT_TO_SOURCE = {
    "permits":        "Permisos de Construccion",
    "solar":          "Solar",
    "rodents":        "Plagas/Roedores",
    "flood":          "Inundacion",
    "construction":   "Construccion Activa",
    "realestate":     "Inmobiliaria",
    "energy":         "Energia",
    "places":         "Google Places",
    "yelp":           "Yelp",
    "deconstruction": "Demolicion",
}


def _load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Config no encontrado: {CONFIG_PATH}")
        logger.error("Ejecuta primero: python utils/crm_setup.py")
        return {}
    with open(CONFIG_PATH) as f:
        return json.load(f)


def _read_krayin_env() -> dict:
    """Read MySQL credentials from Krayin's .env file."""
    env_path = os.path.join(CRM_DIR, ".env")
    creds = {
        "host": "127.0.0.1",
        "port": "3306",
        "database": "krayin_crm",
        "user": "krayin",
        "password": "",
    }
    if not os.path.exists(env_path):
        logger.warning(f"Krayin .env no encontrado: {env_path}")
        return creds

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            if key == "DB_HOST":
                creds["host"] = val
            elif key == "DB_PORT":
                creds["port"] = val
            elif key == "DB_DATABASE":
                creds["database"] = val
            elif key == "DB_USERNAME":
                creds["user"] = val
            elif key == "DB_PASSWORD":
                creds["password"] = val
    return creds


def _mysql_query(creds: dict, query: str, params: tuple = ()) -> list[dict]:
    """Execute a MySQL query using the mysql CLI and return results as dicts."""
    # For simple queries, use mysql CLI to avoid PyMySQL dependency
    cmd = [
        "mysql",
        f"-u{creds['user']}",
        f"-p{creds['password']}",
        f"-h{creds['host']}",
        f"-P{creds['port']}",
        creds["database"],
        "-e", query,
        "-sN",  # silent, no headers
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            logger.debug(f"MySQL error: {result.stderr[:200]}")
            return []
        rows = []
        for line in result.stdout.strip().split("\n"):
            if line:
                rows.append(line.split("\t"))
        return rows
    except Exception as e:
        logger.error(f"MySQL exec error: {e}")
        return []


def _mysql_insert(creds: dict, query: str) -> int | None:
    """Execute a MySQL INSERT and return the last insert ID."""
    full_query = f"{query}; SELECT LAST_INSERT_ID();"
    cmd = [
        "mysql",
        f"-u{creds['user']}",
        f"-p{creds['password']}",
        f"-h{creds['host']}",
        f"-P{creds['port']}",
        creds["database"],
        "-e", full_query,
        "-sN",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            logger.debug(f"MySQL insert error: {result.stderr[:200]}")
            return None
        lines = result.stdout.strip().split("\n")
        if lines:
            last_id = lines[-1].strip()
            if last_id.isdigit():
                return int(last_id)
        return None
    except Exception as e:
        logger.error(f"MySQL insert error: {e}")
        return None


def _escape(val: str) -> str:
    """MySQL string escaping for CLI insertion."""
    if val is None:
        return ""
    return (
        val.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "")
        .replace("\x00", "")
        .replace("\x1a", "")
    )


class CRMSync:
    """Synchronize leads from SQLite to Krayin CRM via direct MySQL."""

    def __init__(self):
        self.config = _load_config()
        self.pipeline_id = self.config.get("pipeline_id")
        self.source_ids = self.config.get("source_ids", {})
        self.agent_to_source = self.config.get("agent_to_source", AGENT_TO_SOURCE)
        self.type_ids = self.config.get("type_ids", {})
        self._stages = {}  # code → stage_id
        self._person_cache = {}  # name → person_id
        self.creds = _read_krayin_env()

    def _test_mysql(self) -> bool:
        """Test MySQL connectivity."""
        rows = _mysql_query(self.creds, "SELECT 1")
        if rows:
            logger.info("Conectado a MySQL (Krayin CRM)")
            return True
        logger.error("No se puede conectar a MySQL")
        return False

    def _load_stages(self):
        """Load pipeline stages from MySQL."""
        if self._stages:
            return
        rows = _mysql_query(
            self.creds,
            f"SELECT id, code FROM lead_pipeline_stages WHERE lead_pipeline_id={self.pipeline_id}"
        )
        for row in rows:
            if len(row) >= 2:
                self._stages[row[1]] = int(row[0])
        if self._stages:
            logger.info(f"  Stages cargados: {list(self._stages.keys())}")

    def _resolve_stage_id(self, score: int) -> int | None:
        """All new leads go to 'Nuevo' stage. Score is stored in description."""
        self._load_stages()
        return self._stages.get("nuevo")

    def _resolve_source_id(self, agent_sources: str) -> int | None:
        """Map agent_sources string to source_id."""
        primary_agent = agent_sources.split(",")[0].strip()
        source_name = self.agent_to_source.get(primary_agent)
        if source_name:
            return self.source_ids.get(source_name)
        return None

    def _detect_lead_type(self, lead_data: dict) -> str:
        """Detect lead type from data."""
        desc = (lead_data.get("description") or "").lower()
        permit = (lead_data.get("permit_type") or "").lower()
        combined = f"{desc} {permit}"

        if any(w in combined for w in ["commercial", "comercial", "office", "retail", "store"]):
            return "Comercial"
        if any(w in combined for w in ["multi", "duplex", "triplex", "apartment", "units"]):
            return "Multifamiliar"
        if any(w in combined for w in ["industrial", "warehouse", "factory", "manufacturing"]):
            return "Industrial"
        return "Residencial"

    def _find_or_create_person(self, lead_data: dict) -> int | None:
        """Find or create a Person in Krayin via MySQL."""
        name = (
            lead_data.get("contractor")
            or lead_data.get("owner")
            or "Propietario Desconocido"
        )[:100]

        # Check cache
        if name in self._person_cache:
            return self._person_cache[name]

        # Check if person already exists
        rows = _mysql_query(
            self.creds,
            f"SELECT id FROM persons WHERE name='{_escape(name)}' LIMIT 1"
        )
        if rows and rows[0]:
            pid = int(rows[0][0])
            self._person_cache[name] = pid
            return pid

        # Create person
        now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
        pid = _mysql_insert(
            self.creds,
            f"INSERT INTO persons (name, created_at, updated_at) VALUES ('{_escape(name)}', '{now}', '{now}')"
        )
        if pid:
            self._person_cache[name] = pid

            # Add email if available
            email = lead_data.get("contact_email")
            if email:
                _mysql_insert(
                    self.creds,
                    f"INSERT INTO person_emails (person_id, value, label) VALUES ({pid}, '{_escape(email)}', 'work')"
                )

            # Add phone if available
            phone = lead_data.get("contact_phone")
            if phone:
                _mysql_insert(
                    self.creds,
                    f"INSERT INTO person_phones (person_id, value, label) VALUES ({pid}, '{_escape(phone)}', 'work')"
                )

        return pid

    def _create_lead(self, address_key: str, address: str, city: str,
                     agent_sources: str, lead_data: dict) -> int | None:
        """Create a Lead in Krayin CRM via MySQL. Returns krayin_lead_id or None."""
        title = f"{address} — {city}"
        if len(title) > 120:
            title = title[:120]

        # Build description
        description_parts = []
        if lead_data.get("description"):
            description_parts.append(lead_data["description"][:500])
        if lead_data.get("permit_type"):
            description_parts.append(f"Tipo permiso: {lead_data['permit_type']}")
        if lead_data.get("contractor"):
            description_parts.append(f"Contratista: {lead_data['contractor']}")
        if lead_data.get("contact_phone"):
            description_parts.append(f"Tel: {lead_data['contact_phone']}")
        if lead_data.get("contact_email"):
            description_parts.append(f"Email: {lead_data['contact_email']}")

        # Source agents
        source_labels = []
        for ag in agent_sources.split(","):
            ag = ag.strip()
            source_labels.append(AGENT_TO_SOURCE.get(ag, ag))
        description_parts.append(f"Fuentes: {', '.join(source_labels)}")

        # Scoring info
        scoring = lead_data.get("_scoring", {})
        score = scoring.get("score", 0) if scoring else 0
        if scoring:
            grade = scoring.get("grade", "")
            description_parts.append(f"Score: {score}/100 ({grade})")
            reasons = scoring.get("reasons", [])
            if reasons:
                description_parts.append("Razones: " + ", ".join(reasons[:5]))

        # Cross-agent signals
        cross_count = lead_data.get("_cross_agent_count", 0)
        if cross_count and cross_count > 1:
            description_parts.append(f"Senales cruzadas: {cross_count} agentes")

        description = "\n".join(description_parts) if description_parts else ""

        # Map value
        lead_value = lead_data.get("value_float") or lead_data.get("assessed_value") or 0
        try:
            lead_value = float(lead_value)
        except (ValueError, TypeError):
            lead_value = 0

        # Resolve IDs
        source_id = self._resolve_source_id(agent_sources)
        stage_id = self._resolve_stage_id(score)
        person_id = self._find_or_create_person(lead_data)

        # Detect lead type
        lead_type_name = self._detect_lead_type(lead_data)
        lead_type_id = self.type_ids.get(lead_type_name) or self.type_ids.get("Residencial")

        # Expected close date
        close_days = 60 if score < 50 else (30 if score < 80 else 14)
        now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
        close_date = (datetime.now(tz=None) + timedelta(days=close_days)).strftime("%Y-%m-%d")

        # Resolve admin user_id (owner of the lead)
        if not hasattr(self, '_admin_user_id'):
            rows = _mysql_query(self.creds, "SELECT id FROM users ORDER BY id LIMIT 1")
            self._admin_user_id = int(rows[0][0]) if rows and rows[0] else 1

        # Build INSERT
        fields = ["title", "description", "lead_value", "status",
                   "lead_pipeline_id", "expected_close_date", "user_id",
                   "created_at", "updated_at"]
        values = [
            f"'{_escape(title)}'",
            f"'{_escape(description)}'",
            str(lead_value),
            "1",
            str(self.pipeline_id),
            f"'{close_date}'",
            str(self._admin_user_id),
            f"'{now}'",
            f"'{now}'",
        ]

        if stage_id:
            fields.append("lead_pipeline_stage_id")
            values.append(str(stage_id))
        if source_id:
            fields.append("lead_source_id")
            values.append(str(source_id))
        if person_id:
            fields.append("person_id")
            values.append(str(person_id))
        if lead_type_id:
            fields.append("lead_type_id")
            values.append(str(lead_type_id))

        query = f"INSERT INTO leads ({', '.join(fields)}) VALUES ({', '.join(values)})"
        krayin_id = _mysql_insert(self.creds, query)

        if krayin_id:
            logger.info(f"  [OK] Lead #{krayin_id}: {title} ({lead_type_name}, score={score})")
        else:
            logger.warning(f"  [WARN] Lead fallido: {title}")

        return krayin_id

    def sync(self):
        """Main sync loop — read unsynced leads and push to CRM."""
        logger.info("Iniciando sincronizacion...")

        if not self.config:
            logger.error("Sin configuracion. Ejecuta: python utils/crm_setup.py")
            return

        if not self.pipeline_id:
            logger.error("pipeline_id no configurado")
            return

        if not self._test_mysql():
            return

        # Read unsynced leads from SQLite
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row

            # Migration: add krayin_lead_id column if missing
            try:
                conn.execute("SELECT krayin_lead_id FROM consolidated_leads LIMIT 1")
            except sqlite3.OperationalError:
                conn.execute(
                    "ALTER TABLE consolidated_leads ADD COLUMN krayin_lead_id INTEGER"
                )
                conn.commit()

            cursor = conn.execute("""
                SELECT address_key, address, city, agent_sources, lead_data
                FROM consolidated_leads
                WHERE crm_synced = 0
                ORDER BY last_updated DESC
                LIMIT ?
            """, (BATCH_SIZE,))
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Error leyendo SQLite: {e}")
            return

        if not rows:
            logger.info("No hay leads nuevos para sincronizar")
            conn.close()
            return

        logger.info(f"Sincronizando {len(rows)} leads...")

        synced = 0
        failed = 0

        for row in rows:
            try:
                lead_data = json.loads(row["lead_data"])
            except (json.JSONDecodeError, TypeError):
                lead_data = {}

            krayin_id = self._create_lead(
                address_key=row["address_key"],
                address=row["address"],
                city=row["city"],
                agent_sources=row["agent_sources"],
                lead_data=lead_data,
            )

            if krayin_id:
                try:
                    conn.execute(
                        "UPDATE consolidated_leads SET crm_synced = 1, krayin_lead_id = ? WHERE address_key = ?",
                        (krayin_id, row["address_key"])
                    )
                    conn.commit()
                    synced += 1
                except Exception as e:
                    logger.error(f"Error marcando synced: {e}")
                    failed += 1
            else:
                failed += 1

        conn.close()

        logger.info(f"Sincronizacion completa: {synced} OK, {failed} fallidos")


def main():
    sync = CRMSync()
    sync.sync()


if __name__ == "__main__":
    main()
