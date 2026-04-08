#!/usr/bin/env python3
"""
utils/crm_sync.py — Sync leads from Insulleads SQLite → Krayin CRM

Reads consolidated_leads where crm_synced=0, creates Person + Lead
in Krayin CRM via REST API, then marks crm_synced=1.

Designed to run every 5 minutes via systemd timer or cron:
    */5 * * * * /home/insulleads/Insulleads/venv/bin/python /home/insulleads/Insulleads/utils/crm_sync.py

Requires:
    KRAYIN_URL             — Base URL (e.g. http://localhost/crm)
    KRAYIN_ADMIN_EMAIL     — Admin email
    KRAYIN_ADMIN_PASSWORD  — Admin password
    DB_PATH                — SQLite path (default: data/leads.db)

Configuration file (created by crm_setup.py):
    data/crm_config.json   — pipeline_id, source_ids, agent_to_source mapping
"""

import os
import sys
import json
import sqlite3
import logging
import time
import requests
from datetime import datetime
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
KRAYIN_URL = os.getenv("KRAYIN_URL", "http://localhost/crm")
ADMIN_EMAIL = os.getenv("KRAYIN_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("KRAYIN_ADMIN_PASSWORD", "admin123")
DB_PATH = os.getenv("DB_PATH", "data/leads.db")
BATCH_SIZE = int(os.getenv("CRM_SYNC_BATCH", "50"))

API_BASE = f"{KRAYIN_URL.rstrip('/')}/api/v1"

# ── Load CRM config (created by crm_setup.py) ──────────────────
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "crm_config.json"
)


def _load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Config no encontrado: {CONFIG_PATH}")
        logger.error("Ejecuta primero: python utils/crm_setup.py")
        return {}
    with open(CONFIG_PATH) as f:
        return json.load(f)


class CRMSync:
    """Synchronize leads from SQLite to Krayin CRM."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.token = None
        self.config = _load_config()
        self.pipeline_id = self.config.get("pipeline_id")
        self.source_ids = self.config.get("source_ids", {})
        self.agent_to_source = self.config.get("agent_to_source", {})
        self.type_ids = self.config.get("type_ids", {})
        self._person_cache = {}  # email/phone → person_id
        self._stages = {}  # code → stage_id

    def authenticate(self) -> bool:
        """Get Sanctum bearer token."""
        try:
            resp = self.session.post(
                f"{API_BASE}/admin/login",
                json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                self.token = data.get("token") or data.get("data", {}).get("token")
                if self.token:
                    self.session.headers["Authorization"] = f"Bearer {self.token}"
                    return True
            logger.error(f"Login fallido: {resp.status_code}")
            return False
        except requests.ConnectionError:
            logger.error(f"No se puede conectar a {API_BASE}")
            return False
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return False

    def _load_stages(self):
        """Load all pipeline stages into a code→id map."""
        if self._stages:
            return

        try:
            resp = self.session.get(
                f"{API_BASE}/settings/pipelines/{self.pipeline_id}",
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                for stage in data.get("stages", []):
                    code = stage.get("code", "")
                    self._stages[code] = stage["id"]
        except Exception as e:
            logger.debug(f"Error loading stages: {e}")

    def _resolve_stage_id(self, score: int, contacted: bool = False) -> int | None:
        """Map lead score + status to the appropriate pipeline stage."""
        self._load_stages()

        if contacted:
            return self._stages.get("contactado") or self._stages.get("nuevo")

        # Score-based stage assignment
        if score >= 80:
            return self._stages.get("calificado") or self._stages.get("nuevo")
        elif score >= 50:
            return self._stages.get("contactado") or self._stages.get("nuevo")
        else:
            return self._stages.get("nuevo")

    def _resolve_source_id(self, agent_sources: str) -> int | None:
        """Map agent_sources string to Krayin source_id."""
        primary_agent = agent_sources.split(",")[0].strip()
        source_name = self.agent_to_source.get(primary_agent)
        if source_name:
            return self.source_ids.get(source_name)
        return None

    def _find_or_create_person(self, lead_data: dict) -> int | None:
        """Find or create a Person (contractor/owner) in Krayin."""
        name = (
            lead_data.get("contractor")
            or lead_data.get("owner")
            or "Propietario Desconocido"
        )
        email = lead_data.get("contact_email")
        phone = lead_data.get("contact_phone")

        # Check cache
        cache_key = email or phone or name
        if cache_key in self._person_cache:
            return self._person_cache[cache_key]

        # Build person payload
        payload = {"name": name[:100]}
        if email:
            payload["emails"] = [{"value": email, "label": "work"}]
        if phone:
            payload["contact_numbers"] = [{"value": phone, "label": "work"}]

        try:
            resp = self.session.post(
                f"{API_BASE}/contacts/persons",
                json=payload,
                timeout=15,
            )
            if resp.status_code in (200, 201):
                person_id = resp.json().get("data", {}).get("id")
                if person_id:
                    self._person_cache[cache_key] = person_id
                    return person_id
            elif resp.status_code == 422:
                # Person might already exist — try to search
                logger.debug(f"Person 422: {resp.text[:200]}")
        except Exception as e:
            logger.debug(f"Person creation error: {e}")

        return None

    def _detect_lead_type(self, lead_data: dict, agent_sources: str) -> str:
        """Detect lead type from data and source agents."""
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

    def _create_lead(self, address_key: str, address: str, city: str,
                     agent_sources: str, lead_data: dict) -> int | None:
        """Create a Lead in Krayin CRM. Returns krayin_lead_id or None."""
        title = f"{address} — {city}"
        if len(title) > 120:
            title = title[:120]

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
        from utils.crm_setup import AGENT_TO_SOURCE
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
        lead_type_name = self._detect_lead_type(lead_data, agent_sources)
        lead_type_id = self.type_ids.get(lead_type_name) or self.type_ids.get("Residencial")

        # Build lead payload
        payload = {
            "title": title,
            "description": description,
            "lead_value": lead_value,
            "status": 1,
            "lead_pipeline_id": self.pipeline_id,
        }

        if stage_id:
            payload["lead_pipeline_stage_id"] = stage_id
        if source_id:
            payload["lead_source_id"] = source_id
        if person_id:
            payload["person_id"] = person_id
        if lead_type_id:
            payload["lead_type_id"] = lead_type_id

        # Expected close date: score-based (higher score = sooner close)
        from datetime import timedelta
        close_days = 60 if score < 50 else (30 if score < 80 else 14)
        payload["expected_close_date"] = (
            datetime.utcnow() + timedelta(days=close_days)
        ).strftime("%Y-%m-%d")

        try:
            resp = self.session.post(
                f"{API_BASE}/leads",
                json=payload,
                timeout=15,
            )
            if resp.status_code in (200, 201):
                krayin_id = resp.json().get("data", {}).get("id")
                logger.info(f"  [OK] Lead creado: {title} (krayin_id={krayin_id}, stage={lead_type_name})")
                return krayin_id
            else:
                logger.warning(
                    f"  [WARN] Lead fallido ({resp.status_code}): {title} — {resp.text[:200]}"
                )
                return None
        except Exception as e:
            logger.error(f"  [ERROR] Lead creation: {e}")
            return None

    def sync(self):
        """Main sync loop — read unsynced leads and push to CRM."""
        logger.info("Iniciando sincronizacion...")

        if not self.config:
            logger.error("Sin configuracion. Ejecuta: python utils/crm_setup.py")
            return

        if not self.pipeline_id:
            logger.error("pipeline_id no configurado. Ejecuta: python utils/crm_setup.py")
            return

        if not self.authenticate():
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

            # Rate limit: don't overwhelm the API
            time.sleep(0.2)

        conn.close()

        logger.info(f"Sincronizacion completa: {synced} OK, {failed} fallidos")


def main():
    sync = CRMSync()
    sync.sync()


if __name__ == "__main__":
    main()
