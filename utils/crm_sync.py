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

def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default

# ── Configuration ───────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "data/leads.db")
BATCH_SIZE = _env_int("CRM_SYNC_BATCH", 600)

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
    "accela":         "Accela / IVARA Permisos",
    "dins":           "Cal Fire DINS",
    "hud_multifamily": "HUD Multifamiliar",
    "shovels":        "Shovels.ai Permisos",
    "thermal":        "Anomalia Termica",
    "email_prospect": "Email Prospect",
    "web_crawler":    "Web Crawler",
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
            logger.warning(f"MySQL query error: {result.stderr[:200]}")
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
            logger.warning(f"MySQL insert error: {result.stderr[:200]}")
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

    # Agent key → display name for lead sources
    SOURCE_NAMES = {
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
        "accela":         "Accela / IVARA Permisos",
        "dins":           "Cal Fire DINS",
        "hud_multifamily": "HUD Multifamiliar",
        "shovels":        "Shovels.ai Permisos",
        "thermal":        "Anomalia Termica",
        "email_prospect": "Email Prospect",
        "web_crawler":    "Web Crawler",
    }

    def __init__(self):
        self.config = _load_config()
        self.creds = _read_krayin_env()
        self._stages = {}
        self._person_cache = {}
        self._source_cache = {}  # source_name → id
        self.pipeline_id = None
        self.type_ids = {}

    def is_configured(self) -> bool:
        """Chequea si el CRM está configurado (MySQL + credenciales)."""
        return bool(self.creds.get("host") and self.creds.get("user"))

    def _test_mysql(self) -> bool:
        """Test MySQL connectivity."""
        rows = _mysql_query(self.creds, "SELECT 1")
        if rows:
            logger.info("Conectado a MySQL (Krayin CRM)")
            return True
        logger.error("No se puede conectar a MySQL")
        return False

    def _bootstrap(self) -> bool:
        """Load pipeline, sources, and types from MySQL (not config file)."""
        if not self._test_mysql():
            return False

        # Load pipeline
        rows = _mysql_query(self.creds, "SELECT id FROM lead_pipelines WHERE is_default = 1 LIMIT 1")
        if not rows or not rows[0]:
            rows = _mysql_query(self.creds, "SELECT id FROM lead_pipelines ORDER BY id LIMIT 1")
        self.pipeline_id = int(rows[0][0]) if rows and rows[0] else None
        if not self.pipeline_id:
            logger.error("No se encontro pipeline")
            return False

        # Load or create sources
        for agent_key, source_name in self.SOURCE_NAMES.items():
            rows = _mysql_query(
                self.creds,
                f"SELECT id FROM lead_sources WHERE name = '{_escape(source_name)}' LIMIT 1"
            )
            if rows and rows[0]:
                self._source_cache[agent_key] = int(rows[0][0])
            else:
                now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
                sid = _mysql_insert(
                    self.creds,
                    f"INSERT INTO lead_sources (name, created_at, updated_at) "
                    f"VALUES ('{_escape(source_name)}', '{now}', '{now}')"
                )
                if sid:
                    self._source_cache[agent_key] = sid
                    logger.info(f"  Source creado: {source_name} (ID={sid})")

        logger.info(f"  Sources cargados: {len(self._source_cache)}")

        # Load or create types
        REQUIRED_TYPES = ["Residencial", "Comercial", "Multifamiliar", "Industrial"]
        rows = _mysql_query(self.creds, "SELECT id, name FROM lead_types")
        for row in rows:
            if len(row) >= 2:
                self.type_ids[row[1]] = int(row[0])
        for tname in REQUIRED_TYPES:
            if tname not in self.type_ids:
                now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
                tid = _mysql_insert(
                    self.creds,
                    f"INSERT INTO lead_types (name, created_at, updated_at) "
                    f"VALUES ('{_escape(tname)}', '{now}', '{now}')"
                )
                if tid:
                    self.type_ids[tname] = tid
                    logger.info(f"  Type creado: {tname} (ID={tid})")
        logger.info(f"  Types cargados: {list(self.type_ids.keys())}")

        # Load or create tags for score grades
        self._tag_cache = {}  # tag_name → tag_id
        TAG_NAMES = ["HOT", "WARM", "MEDIUM", "COOL", "COLD"]
        for tag_name in TAG_NAMES:
            rows = _mysql_query(
                self.creds,
                f"SELECT id FROM tags WHERE name = '{_escape(tag_name)}' LIMIT 1"
            )
            if rows and rows[0]:
                self._tag_cache[tag_name] = int(rows[0][0])
            else:
                now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
                tid = _mysql_insert(
                    self.creds,
                    f"INSERT INTO tags (name, color, user_id, created_at, updated_at) "
                    f"VALUES ('{_escape(tag_name)}', '', 1, '{now}', '{now}')"
                )
                if tid:
                    self._tag_cache[tag_name] = tid
                    logger.info(f"  Tag creado: {tag_name} (ID={tid})")
        # Also create source-type tags
        for agent_key, source_name in self.SOURCE_NAMES.items():
            rows = _mysql_query(
                self.creds,
                f"SELECT id FROM tags WHERE name = '{_escape(source_name)}' LIMIT 1"
            )
            if rows and rows[0]:
                self._tag_cache[source_name] = int(rows[0][0])
            else:
                now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
                tid = _mysql_insert(
                    self.creds,
                    f"INSERT INTO tags (name, color, user_id, created_at, updated_at) "
                    f"VALUES ('{_escape(source_name)}', '', 1, '{now}', '{now}')"
                )
                if tid:
                    self._tag_cache[source_name] = tid
                    logger.info(f"  Tag creado: {source_name} (ID={tid})")
        logger.info(f"  Tags cargados: {len(self._tag_cache)}")

        return True

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

    def _get_default_person_id(self) -> int:
        """Get or create a default person for leads without a specific contact."""
        if hasattr(self, '_default_person_id') and self._default_person_id:
            return self._default_person_id
        # Check if default person exists
        rows = _mysql_query(
            self.creds,
            "SELECT id FROM persons WHERE name='Propietario' LIMIT 1"
        )
        if rows and rows[0]:
            self._default_person_id = int(rows[0][0])
            return self._default_person_id
        # Create default person (emails is NOT NULL json column)
        now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
        pid = _mysql_insert(
            self.creds,
            f"INSERT INTO persons (name, emails, contact_numbers, created_at, updated_at) "
            f"VALUES ('Propietario', '[]', '[]', '{now}', '{now}')"
        )
        self._default_person_id = pid or 1
        return self._default_person_id

    def _get_default_source_id(self) -> int:
        """Get the first available lead source as fallback."""
        if hasattr(self, '_default_source_id') and self._default_source_id:
            return self._default_source_id
        rows = _mysql_query(self.creds, "SELECT id FROM lead_sources ORDER BY id LIMIT 1")
        self._default_source_id = int(rows[0][0]) if rows and rows[0] else 1
        return self._default_source_id

    def _resolve_source_id(self, agent_sources: str) -> int | None:
        """Map agent_sources string to source_id from MySQL cache."""
        primary_agent = agent_sources.split(",")[0].strip().lower()
        return self._source_cache.get(primary_agent)

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

    def _find_or_create_person(self, lead_data: dict, source_label: str = "") -> int | None:
        """Find or create a Person in Krayin via MySQL.

        Uses contractor/owner name if available, otherwise uses the
        source label (e.g. 'Solar', 'Demolicion') as the person name
        so the Kanban card shows the lead type.
        """
        name = (
            lead_data.get("contractor")
            or lead_data.get("owner")
            or lead_data.get("owner_name")
            or lead_data.get("business_name")
            or lead_data.get("buyer")
            or source_label
            or "Lead"
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

        # Build emails JSON array (required NOT NULL column)
        email = lead_data.get("contact_email")
        if email:
            emails_json = f'[{{"value": "{_escape(email)}", "label": "work"}}]'
        else:
            emails_json = "[]"

        # Build contact_numbers JSON array — check multiple phone sources
        phone = (
            lead_data.get("contact_phone")
            or lead_data.get("phone")
        )
        if phone:
            phones_json = f'[{{"value": "{_escape(str(phone))}", "label": "work"}}]'
        else:
            phones_json = "[]"

        # Create person with required JSON columns
        now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")
        pid = _mysql_insert(
            self.creds,
            f"INSERT INTO persons (name, emails, contact_numbers, created_at, updated_at) "
            f"VALUES ('{_escape(name)}', '{emails_json}', '{phones_json}', '{now}', '{now}')"
        )
        if pid:
            self._person_cache[name] = pid
            logger.info(f"  Persona creada: {name} (ID={pid})")
        else:
            logger.warning(f"  No se pudo crear persona: {name}")

        return pid

    def _build_description(self, lead_data: dict, agent_sources: str) -> tuple[str, int]:
        """Build a rich description from all available lead_data fields.

        Returns (description_text, score).
        """
        sections = []
        scoring = lead_data.get("_scoring", {}) or {}
        score = scoring.get("score", 0)

        # ── Scoring section ──────────────────────────────────
        grade = scoring.get("grade", "")
        grade_emoji = scoring.get("grade_emoji", "")
        if score or grade:
            sections.append(
                f"{grade_emoji} SCORE: {score}/100 — {grade}"
            )
        reasons = scoring.get("reasons", [])
        if reasons:
            sections.append("Razones: " + " | ".join(reasons[:8]))

        # ── Source / cross-agent info ────────────────────────
        source_labels = []
        for ag in agent_sources.split(","):
            ag = ag.strip()
            source_labels.append(AGENT_TO_SOURCE.get(ag, ag))
        sections.append(f"Fuentes: {', '.join(source_labels)}")

        cross_count = lead_data.get("_cross_agent_count", 0)
        if cross_count and cross_count > 1:
            summary = lead_data.get("_signal_summary", "")
            line = f"Senales cruzadas: {cross_count} agentes"
            if summary:
                line += f" ({summary})"
            sections.append(line)

        sections.append("")  # blank line separator

        # ── General description ──────────────────────────────
        if lead_data.get("description"):
            sections.append(lead_data["description"][:600])

        # ── Contact info ─────────────────────────────────────
        contact_lines = []
        if lead_data.get("contractor"):
            contact_lines.append(f"Contratista: {lead_data['contractor']}")
        if lead_data.get("owner") or lead_data.get("owner_name"):
            contact_lines.append(f"Propietario: {lead_data.get('owner') or lead_data.get('owner_name')}")
        if lead_data.get("contact_name"):
            contact_lines.append(f"Contacto: {lead_data['contact_name']}")
        if lead_data.get("contact_phone"):
            contact_lines.append(f"Tel: {lead_data['contact_phone']}")
        if lead_data.get("contact_email"):
            contact_lines.append(f"Email: {lead_data['contact_email']}")
        if lead_data.get("contact_source"):
            contact_lines.append(f"Fuente contacto: {lead_data['contact_source']}")
        if contact_lines:
            sections.append("--- CONTACTO ---")
            sections.extend(contact_lines)
            sections.append("")

        # ── Permits data ─────────────────────────────────────
        permit_lines = []
        if lead_data.get("permit_type"):
            permit_lines.append(f"Tipo permiso: {lead_data['permit_type']}")
        if lead_data.get("filed_date"):
            permit_lines.append(f"Fecha solicitud: {lead_data['filed_date']}")
        if lead_data.get("issued_date"):
            permit_lines.append(f"Fecha emision: {lead_data['issued_date']}")
        if lead_data.get("lic_number"):
            permit_lines.append(f"Licencia: {lead_data['lic_number']}")
        if lead_data.get("permit_url"):
            permit_lines.append(f"URL: {lead_data['permit_url']}")
        if permit_lines:
            sections.append("--- PERMISO ---")
            sections.extend(permit_lines)
            sections.append("")

        # ── Construction phase data ──────────────────────────
        construction_lines = []
        if lead_data.get("phase"):
            construction_lines.append(f"Fase: {lead_data['phase']}")
        if lead_data.get("inspector"):
            construction_lines.append(f"Inspector: {lead_data['inspector']}")
        if lead_data.get("result"):
            construction_lines.append(f"Resultado: {lead_data['result']}")
        if lead_data.get("timing"):
            construction_lines.append(f"Timing: {lead_data['timing']}")
        if lead_data.get("action"):
            construction_lines.append(f"Accion: {lead_data['action']}")
        if construction_lines:
            sections.append("--- CONSTRUCCION ---")
            sections.extend(construction_lines)
            sections.append("")

        # ── Solar data ───────────────────────────────────────
        solar_lines = []
        if lead_data.get("solar_potential"):
            solar_lines.append(f"Potencial solar: {lead_data['solar_potential']}")
        if lead_data.get("system_size_kw"):
            solar_lines.append(f"Tamano sistema: {lead_data['system_size_kw']} kW")
        if lead_data.get("max_panels"):
            solar_lines.append(f"Max paneles: {lead_data['max_panels']}")
        if lead_data.get("annual_kwh"):
            solar_lines.append(f"Produccion anual: {lead_data['annual_kwh']} kWh")
        if lead_data.get("annual_savings"):
            solar_lines.append(f"Ahorro anual: ${lead_data['annual_savings']}")
        if lead_data.get("roof_sqft"):
            solar_lines.append(f"Techo: {lead_data['roof_sqft']} sqft")
        if lead_data.get("ghi_annual"):
            solar_lines.append(f"GHI anual: {lead_data['ghi_annual']}")
        if lead_data.get("carbon_offset"):
            solar_lines.append(f"Offset CO2: {lead_data['carbon_offset']}")
        if lead_data.get("utility_name"):
            solar_lines.append(f"Utilidad: {lead_data['utility_name']}")
        if lead_data.get("utility_rate"):
            solar_lines.append(f"Tarifa: ${lead_data['utility_rate']}/kWh")
        if solar_lines:
            sections.append("--- SOLAR ---")
            sections.extend(solar_lines)
            sections.append("")

        # ── Rodents / pest data ──────────────────────────────
        pest_lines = []
        if lead_data.get("pest_type"):
            pest_lines.append(f"Tipo plaga: {lead_data.get('pest_emoji', '')} {lead_data['pest_type']}")
        if lead_data.get("severity"):
            pest_lines.append(f"Severidad: {lead_data['severity']}")
        if lead_data.get("damage_type"):
            pest_lines.append(f"Tipo dano: {lead_data['damage_type']}")
        if lead_data.get("insulation_need"):
            pest_lines.append(f"Necesidad aislamiento: {lead_data['insulation_need']}")
        if lead_data.get("neighborhood"):
            pest_lines.append(f"Barrio: {lead_data['neighborhood']}")
        if pest_lines:
            sections.append("--- PLAGAS ---")
            sections.extend(pest_lines)
            sections.append("")

        # ── Energy data ──────────────────────────────────────
        energy_lines = []
        if lead_data.get("energy_score"):
            energy_lines.append(f"Score energetico: {lead_data['energy_score']}")
        if lead_data.get("efficiency"):
            energy_lines.append(f"Eficiencia: {lead_data['efficiency']}")
        if lead_data.get("eui"):
            energy_lines.append(f"EUI: {lead_data['eui']}")
        if lead_data.get("emissions"):
            energy_lines.append(f"Emisiones: {lead_data['emissions']}")
        if lead_data.get("building_type"):
            energy_lines.append(f"Tipo edificio: {lead_data['building_type']}")
        if lead_data.get("building_name"):
            energy_lines.append(f"Edificio: {lead_data['building_name']}")
        if energy_lines:
            sections.append("--- ENERGIA ---")
            sections.extend(energy_lines)
            sections.append("")

        # ── Deconstruction data ──────────────────────────────
        decon_lines = []
        if lead_data.get("decon_type"):
            decon_lines.append(f"Tipo demolicion: {lead_data.get('decon_emoji', '')} {lead_data['decon_type']}")
        if lead_data.get("decon_priority"):
            decon_lines.append(f"Prioridad: {lead_data['decon_priority']}")
        if lead_data.get("opportunity"):
            decon_lines.append(f"Oportunidad: {lead_data['opportunity']}")
        if decon_lines:
            sections.append("--- DEMOLICION ---")
            sections.extend(decon_lines)
            sections.append("")

        # ── Real estate data ─────────────────────────────────
        re_lines = []
        if lead_data.get("buyer"):
            re_lines.append(f"Comprador: {lead_data['buyer']}")
        if lead_data.get("seller"):
            re_lines.append(f"Vendedor: {lead_data['seller']}")
        if lead_data.get("insulation_priority"):
            re_lines.append(f"Prioridad aislamiento: {lead_data['insulation_priority']}")
        if re_lines:
            sections.append("--- INMOBILIARIA ---")
            sections.extend(re_lines)
            sections.append("")

        # ── Business data (Places/Yelp) ──────────────────────
        biz_lines = []
        if lead_data.get("business_name"):
            biz_lines.append(f"Negocio: {lead_data['business_name']}")
        if lead_data.get("rating"):
            stars = f"{lead_data['rating']}/5"
            reviews = lead_data.get("total_reviews") or lead_data.get("review_count")
            if reviews:
                stars += f" ({reviews} resenas)"
            biz_lines.append(f"Rating: {stars}")
        if lead_data.get("category") or lead_data.get("categories_str"):
            biz_lines.append(f"Categoria: {lead_data.get('categories_str') or lead_data.get('category')}")
        if lead_data.get("phone"):
            biz_lines.append(f"Tel negocio: {lead_data['phone']}")
        if lead_data.get("website"):
            biz_lines.append(f"Web: {lead_data['website']}")
        if lead_data.get("yelp_url"):
            biz_lines.append(f"Yelp: {lead_data['yelp_url']}")
        if lead_data.get("search_keyword"):
            biz_lines.append(f"Busqueda: {lead_data['search_keyword']}")
        if biz_lines:
            sections.append("--- NEGOCIO ---")
            sections.extend(biz_lines)
            sections.append("")

        # ── Flood / weather data ─────────────────────────────
        flood_lines = []
        if lead_data.get("event"):
            flood_lines.append(f"Evento: {lead_data['event']}")
        if lead_data.get("headline"):
            flood_lines.append(f"Titulo: {lead_data['headline']}")
        if lead_data.get("urgency"):
            flood_lines.append(f"Urgencia: {lead_data['urgency']}")
        if lead_data.get("areas"):
            areas = lead_data["areas"]
            if isinstance(areas, list):
                areas = ", ".join(areas)
            flood_lines.append(f"Areas: {areas}")
        if flood_lines:
            sections.append("--- INUNDACION ---")
            sections.extend(flood_lines)
            sections.append("")

        # ── Property details ─────────────────────────────────
        prop_lines = []
        if lead_data.get("property_type"):
            prop_lines.append(f"Tipo propiedad: {lead_data['property_type']}")
        if lead_data.get("sqft"):
            prop_lines.append(f"Superficie: {lead_data['sqft']} sqft")
        if lead_data.get("year_built"):
            prop_lines.append(f"Ano construccion: {lead_data['year_built']}")
        if lead_data.get("property_age"):
            prop_lines.append(f"Antiguedad: {lead_data['property_age']} anos")
        if lead_data.get("assessed_value"):
            prop_lines.append(f"Valor tasado: ${lead_data['assessed_value']:,.0f}" if isinstance(lead_data.get("assessed_value"), (int, float)) else f"Valor tasado: {lead_data['assessed_value']}")
        if lead_data.get("zip_code"):
            prop_lines.append(f"Codigo postal: {lead_data['zip_code']}")
        if lead_data.get("lat") and lead_data.get("lon"):
            prop_lines.append(f"Coordenadas: {lead_data['lat']}, {lead_data['lon']}")
        if prop_lines:
            sections.append("--- PROPIEDAD ---")
            sections.extend(prop_lines)
            sections.append("")

        # ── Dates ────────────────────────────────────────────
        date_val = lead_data.get("date") or lead_data.get("filed_date")
        if date_val:
            sections.append(f"Fecha registro: {date_val}")
        if lead_data.get("source"):
            sections.append(f"Fuente datos: {lead_data['source']}")

        return "\n".join(sections), score

    def _create_lead(self, address_key: str, address: str, city: str,
                     agent_sources: str, lead_data: dict) -> int | None:
        """Create a Lead in Krayin CRM via MySQL. Returns krayin_lead_id or None."""
        title = f"{address} — {city}"
        if len(title) > 120:
            title = title[:120]

        # Build rich description with all available data
        description, score = self._build_description(lead_data, agent_sources)

        # Map value
        lead_value = lead_data.get("value_float") or lead_data.get("assessed_value") or 0
        try:
            lead_value = float(lead_value)
        except (ValueError, TypeError):
            lead_value = 0

        # Resolve IDs (always required for Kanban rendering)
        source_id = self._resolve_source_id(agent_sources) or self._get_default_source_id()
        stage_id = self._resolve_stage_id(score) or 1

        # Resolve source label for person name fallback
        primary_agent = agent_sources.split(",")[0].strip().lower()
        source_label = self.SOURCE_NAMES.get(primary_agent, "Lead")
        person_id = self._find_or_create_person(lead_data, source_label) or self._get_default_person_id()

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

        # Build INSERT — all relationship fields are required for Kanban display
        fields = ["title", "description", "lead_value", "status",
                   "lead_pipeline_id", "lead_pipeline_stage_id",
                   "lead_source_id", "person_id", "lead_type_id",
                   "expected_close_date", "user_id",
                   "created_at", "updated_at"]
        values = [
            f"'{_escape(title)}'",
            f"'{_escape(description)}'",
            str(lead_value),
            "1",
            str(self.pipeline_id),
            str(stage_id),
            str(source_id),
            str(person_id),
            str(lead_type_id or 1),
            f"'{close_date}'",
            str(self._admin_user_id),
            f"'{now}'",
            f"'{now}'",
        ]

        query = f"INSERT INTO leads ({', '.join(fields)}) VALUES ({', '.join(values)})"
        krayin_id = _mysql_insert(self.creds, query)

        if krayin_id:
            logger.info(f"  [OK] Lead #{krayin_id}: {title} ({lead_type_name}, score={score})")
            # Assign tags: score grade + source types
            self._assign_tags(krayin_id, lead_data, agent_sources)
        else:
            logger.warning(f"  [WARN] Lead fallido: {title}")

        return krayin_id

    def _assign_tags(self, lead_id: int, lead_data: dict, agent_sources: str):
        """Assign score-grade and source-type tags to a lead."""
        scoring = lead_data.get("_scoring", {}) or {}
        grade = scoring.get("grade", "")
        now = datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")

        tag_ids = []

        # Score grade tag (HOT, WARM, MEDIUM, COOL, COLD)
        if grade and grade in self._tag_cache:
            tag_ids.append(self._tag_cache[grade])

        # Source type tags (one per agent source)
        for ag in agent_sources.split(","):
            ag = ag.strip().lower()
            source_name = self.SOURCE_NAMES.get(ag)
            if source_name and source_name in self._tag_cache:
                tag_ids.append(self._tag_cache[source_name])

        # Insert lead_tags entries (Krayin uses per-entity pivot tables, not a generic taggables)
        for tag_id in tag_ids:
            _mysql_insert(
                self.creds,
                f"INSERT IGNORE INTO lead_tags (tag_id, lead_id) VALUES ({tag_id}, {lead_id})"
            )

    def sync(self) -> int:
        """Main sync loop — read unsynced leads and push to CRM. Returns count of synced leads."""
        logger.info("Iniciando sincronizacion...")

        if not self._bootstrap():
            return 0

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
                ORDER BY json_extract(lead_data, '$._scoring.score') DESC,
                         last_updated DESC
                LIMIT ?
            """, (BATCH_SIZE,))
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Error leyendo SQLite: {e}")
            return 0

        if not rows:
            # Debug: chequear cuántos leads hay sin sincronizar
            try:
                debug_cursor = conn.execute(
                    "SELECT COUNT(*) FROM consolidated_leads WHERE crm_synced = 0"
                )
                count_unsync = debug_cursor.fetchone()[0]
                if count_unsync > 0:
                    logger.info(f"[crm_sync] {count_unsync} leads con crm_synced=0 aún en DB")
            except Exception:
                pass
            logger.info("No hay leads nuevos para sincronizar")
            conn.close()
            return 0

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
        return synced


def main():
    sync = CRMSync()
    sync.sync()


if __name__ == "__main__":
    main()
