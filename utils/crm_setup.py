#!/usr/bin/env python3
"""
utils/crm_setup.py — One-time Krayin CRM configuration

Sets up the pipeline, stages, lead sources, and lead types for
the Insulleads insulation sales workflow.

Usage:
    python utils/crm_setup.py          # Run setup
    python utils/crm_setup.py --check  # Verify configuration

Requires env vars:
    KRAYIN_URL          — Base URL of Krayin CRM (e.g. http://localhost/crm)
    KRAYIN_ADMIN_EMAIL  — Admin email (default: admin@example.com)
    KRAYIN_ADMIN_PASSWORD — Admin password (default: admin123)
"""

import os
import sys
import json
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────
KRAYIN_URL = os.getenv("KRAYIN_URL", "http://localhost")
ADMIN_EMAIL = os.getenv("KRAYIN_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("KRAYIN_ADMIN_PASSWORD", "admin123")

API_BASE = f"{KRAYIN_URL.rstrip('/')}/api/v1"

# ── Pipeline stages for insulation sales ────────────────────────
PIPELINE_NAME = "Insulacion Bay Area"
PIPELINE_STAGES = [
    {"name": "Nuevo",      "code": "nuevo",     "sort_order": 1},
    {"name": "Contactado", "code": "contactado", "sort_order": 2},
    {"name": "Calificado", "code": "calificado", "sort_order": 3},
    {"name": "Propuesta",  "code": "propuesta",  "sort_order": 4},
    {"name": "Ganado",     "code": "won",        "sort_order": 5},
    {"name": "Perdido",    "code": "lost",       "sort_order": 6},
]

# ── Lead sources (one per agent) ───────────────────────────────
LEAD_SOURCES = [
    "Permisos de Construccion",
    "Solar",
    "Plagas/Roedores",
    "Inundacion",
    "Construccion Activa",
    "Inmobiliaria",
    "Energia",
    "Google Places",
    "Yelp",
    "Demolicion",
    "Accela / IVARA Permisos",
    "Cal Fire DINS",
    "HUD Multifamiliar",
    "Shovels.ai Permisos",
    "Anomalia Termica",
    "Email Prospect",
    "Web Crawler",
]

# Map agent_key → source name for sync bridge
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

# ── Lead types ──────────────────────────────────────────────────
LEAD_TYPES = [
    "Residencial",
    "Comercial",
    "Multifamiliar",
    "Industrial",
]


class KrayinSetup:
    """Configure Krayin CRM for Insulleads integration."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.token = None

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
                    logger.info("  [OK] Autenticado con Krayin CRM")
                    return True
            logger.error(f"  [ERROR] Login fallido: {resp.status_code} {resp.text[:200]}")
            return False
        except requests.ConnectionError:
            logger.error(f"  [ERROR] No se puede conectar a {API_BASE}")
            return False
        except Exception as e:
            logger.error(f"  [ERROR] Auth error: {e}")
            return False

    def setup_pipeline(self) -> int | None:
        """Create insulation sales pipeline with stages."""
        logger.info("\n── Configurando Pipeline ──")

        # Check if pipeline already exists
        resp = self.session.get(f"{API_BASE}/settings/pipelines", timeout=15)
        if resp.status_code == 200:
            pipelines = resp.json().get("data", [])
            for p in pipelines:
                if p.get("name") == PIPELINE_NAME:
                    logger.info(f"  [OK] Pipeline '{PIPELINE_NAME}' ya existe (id={p['id']})")
                    return p["id"]

        # Create pipeline with stages
        payload = {
            "name": PIPELINE_NAME,
            "is_default": 1,
            "rotten_days": 30,
            "stages": [
                {
                    "name": s["name"],
                    "code": s["code"],
                    "sort_order": s["sort_order"],
                }
                for s in PIPELINE_STAGES
            ],
        }

        resp = self.session.post(
            f"{API_BASE}/settings/pipelines",
            json=payload,
            timeout=15,
        )

        if resp.status_code in (200, 201):
            data = resp.json().get("data", {})
            pipeline_id = data.get("id")
            logger.info(f"  [OK] Pipeline '{PIPELINE_NAME}' creado (id={pipeline_id})")
            for stage in PIPELINE_STAGES:
                logger.info(f"       ▸ {stage['name']} ({stage['code']})")
            return pipeline_id
        else:
            logger.error(f"  [ERROR] No se pudo crear pipeline: {resp.status_code} {resp.text[:300]}")
            return None

    def setup_sources(self) -> dict:
        """Create lead sources for each agent."""
        logger.info("\n── Configurando Lead Sources ──")

        # Get existing sources
        existing = {}
        resp = self.session.get(f"{API_BASE}/settings/sources", timeout=15)
        if resp.status_code == 200:
            for s in resp.json().get("data", []):
                existing[s.get("name")] = s.get("id")

        source_ids = {}
        for source_name in LEAD_SOURCES:
            if source_name in existing:
                source_ids[source_name] = existing[source_name]
                logger.info(f"  [OK] Source '{source_name}' ya existe (id={existing[source_name]})")
                continue

            resp = self.session.post(
                f"{API_BASE}/settings/sources",
                json={"name": source_name},
                timeout=15,
            )

            if resp.status_code in (200, 201):
                data = resp.json().get("data", {})
                source_ids[source_name] = data.get("id")
                logger.info(f"  [OK] Source '{source_name}' creado (id={data.get('id')})")
            else:
                logger.warning(f"  [WARN] No se pudo crear source '{source_name}': {resp.status_code}")

        return source_ids

    def setup_types(self) -> dict:
        """Create lead types."""
        logger.info("\n── Configurando Lead Types ──")

        # Get existing types
        existing = {}
        resp = self.session.get(f"{API_BASE}/settings/types", timeout=15)
        if resp.status_code == 200:
            for t in resp.json().get("data", []):
                existing[t.get("name")] = t.get("id")

        type_ids = {}
        for type_name in LEAD_TYPES:
            if type_name in existing:
                type_ids[type_name] = existing[type_name]
                logger.info(f"  [OK] Type '{type_name}' ya existe (id={existing[type_name]})")
                continue

            resp = self.session.post(
                f"{API_BASE}/settings/types",
                json={"name": type_name},
                timeout=15,
            )

            if resp.status_code in (200, 201):
                data = resp.json().get("data", {})
                type_ids[type_name] = data.get("id")
                logger.info(f"  [OK] Type '{type_name}' creado (id={data.get('id')})")
            else:
                logger.warning(f"  [WARN] No se pudo crear type '{type_name}': {resp.status_code}")

        return type_ids

    def check(self) -> bool:
        """Verify CRM configuration is correct."""
        logger.info("\n══ Verificando configuracion CRM ══\n")

        if not self.authenticate():
            return False

        ok = True

        # Check pipeline
        resp = self.session.get(f"{API_BASE}/settings/pipelines", timeout=15)
        if resp.status_code == 200:
            pipelines = [p["name"] for p in resp.json().get("data", [])]
            if PIPELINE_NAME in pipelines:
                logger.info(f"  [OK] Pipeline: {PIPELINE_NAME}")
            else:
                logger.error(f"  [FAIL] Pipeline '{PIPELINE_NAME}' no encontrado")
                ok = False
        else:
            logger.error(f"  [FAIL] No se puede obtener pipelines: {resp.status_code}")
            ok = False

        # Check sources
        resp = self.session.get(f"{API_BASE}/settings/sources", timeout=15)
        if resp.status_code == 200:
            sources = [s["name"] for s in resp.json().get("data", [])]
            for name in LEAD_SOURCES:
                if name in sources:
                    logger.info(f"  [OK] Source: {name}")
                else:
                    logger.error(f"  [FAIL] Source '{name}' no encontrado")
                    ok = False

        # Check types
        resp = self.session.get(f"{API_BASE}/settings/types", timeout=15)
        if resp.status_code == 200:
            types = [t["name"] for t in resp.json().get("data", [])]
            for name in LEAD_TYPES:
                if name in types:
                    logger.info(f"  [OK] Type: {name}")
                else:
                    logger.error(f"  [FAIL] Type '{name}' no encontrado")
                    ok = False

        logger.info(f"\n{'  VERIFICACION EXITOSA' if ok else '  HAY ERRORES — ejecuta: python utils/crm_setup.py'}")
        return ok

    def run(self):
        """Run full CRM setup."""
        logger.info("══════════════════════════════════════════════════")
        logger.info("  Krayin CRM — Setup para Insulleads")
        logger.info("══════════════════════════════════════════════════")
        logger.info(f"  URL: {KRAYIN_URL}")
        logger.info(f"  API: {API_BASE}")

        if not self.authenticate():
            sys.exit(1)

        pipeline_id = self.setup_pipeline()
        source_ids = self.setup_sources()
        type_ids = self.setup_types()

        # Save configuration for sync bridge
        config = {
            "pipeline_id": pipeline_id,
            "source_ids": source_ids,
            "type_ids": type_ids,
            "agent_to_source": AGENT_TO_SOURCE,
        }

        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "crm_config.json"
        )
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        logger.info(f"\n  [OK] Configuracion guardada en {config_path}")
        logger.info("\n══════════════════════════════════════════════════")
        logger.info("  SETUP COMPLETO")
        logger.info("══════════════════════════════════════════════════")
        logger.info(f"\n  Pipeline: {PIPELINE_NAME} ({len(PIPELINE_STAGES)} etapas)")
        logger.info(f"  Sources:  {len(source_ids)} fuentes de leads")
        logger.info(f"  Types:    {len(type_ids)} tipos de lead")
        logger.info(f"\n  CRM login: {KRAYIN_URL.rstrip('/')}/admin/login")
        logger.info(f"  Email:     {ADMIN_EMAIL}")
        logger.info("")


if __name__ == "__main__":
    setup = KrayinSetup()
    if "--check" in sys.argv:
        ok = setup.check()
        sys.exit(0 if ok else 1)
    else:
        setup.run()
