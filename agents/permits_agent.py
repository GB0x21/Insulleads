"""
agents/permits_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 1 — PERMISOS DE CONSTRUCCIÓN

Fuentes:
  • SF DataSF  (San Francisco)
  • San Jose Open Data
  • Oakland Open Data
  • Contra Costa County

Filtra por: ADU, addition, remodel, new construction
→ Contactar al contratista o propietario antes que la competencia.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead


# Tipos de permiso que indican proyecto de construcción relevante
PERMIT_KEYWORDS = [
    "addition", "adu", "accessory dwelling", "remodel",
    "new construction", "tenant improvement", "renovation",
    "garage conversion", "basement", "crawl space"
]


class PermitsAgent(BaseAgent):
    name      = "🏗️ Permisos de Construcción"
    emoji     = "🏗️"
    agent_key = "permits"

    def fetch_leads(self) -> list[dict]:
        leads = []
        leads += self._fetch_sf()
        leads += self._fetch_san_jose()
        leads += self._fetch_oakland()
        return leads

    # ── San Francisco ─────────────────────────────────────────────
    def _fetch_sf(self) -> list[dict]:
        """
        API: SF Open Data — Building Permits
        Docs: https://data.sfgov.org/resource/i98e-djp9.json
        """
        since = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 100,
            "$where": f"filed_date >= '{since}'",
            "$order": "filed_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            ptype = (item.get("permit_type_definition", "") or "").lower()

            if not any(kw in desc or kw in ptype for kw in PERMIT_KEYWORDS):
                continue

            leads.append({
                "id":          f"sf_{item.get('permit_number', '')}",
                "city":        "San Francisco",
                "address":     self._sf_address(item),
                "permit_type": item.get("permit_type_definition", "N/A"),
                "description": item.get("description", "N/A")[:120],
                "status":      item.get("status", "N/A"),
                "filed_date":  item.get("filed_date", "")[:10],
                "contractor":  item.get("contractor_company_name", "No indicado"),
                "owner":       item.get("owner_name", "No indicado"),
                "estimated_cost": f"${item.get('estimated_cost', '0'):,}" if item.get("estimated_cost") else "N/A",
                "source_url":  f"https://sfdbi.org/permit/{item.get('permit_number', '')}",
            })
        return leads

    def _sf_address(self, item: dict) -> str:
        parts = [
            item.get("street_number", ""),
            item.get("street_name", ""),
            item.get("street_suffix", ""),
        ]
        return " ".join(p for p in parts if p).title()

    # ── San José ─────────────────────────────────────────────────
    def _fetch_san_jose(self) -> list[dict]:
        """
        API: San Jose Open Data — Building Permits
        Docs: https://data.sanjoseca.gov/resource/5e7j-kygj.json
        """
        since = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {
            "$limit": 100,
            "$where": f"application_date >= '{since}'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            desc = (item.get("work_description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue

            leads.append({
                "id":          f"sj_{item.get('permit_number', '')}",
                "city":        "San José",
                "address":     item.get("address", "N/A").title(),
                "permit_type": item.get("permit_type", "N/A"),
                "description": item.get("work_description", "N/A")[:120],
                "status":      item.get("status", "N/A"),
                "filed_date":  item.get("application_date", "")[:10],
                "contractor":  item.get("contractor_name", "No indicado"),
                "owner":       item.get("owner_name", "No indicado"),
                "estimated_cost": item.get("job_value", "N/A"),
                "source_url":  "https://www.sanjoseca.gov/your-government/departments-offices/planning-building-code-enforcement",
            })
        return leads

    # ── Oakland ──────────────────────────────────────────────────
    def _fetch_oakland(self) -> list[dict]:
        """
        API: Oakland Open Data — Building Permits
        Docs: https://data.oaklandca.gov/resource/p8h3-ngmm.json
        """
        since = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.oaklandca.gov/resource/p8h3-ngmm.json"
        params = {
            "$limit": 100,
            "$where": f"application_date >= '{since}'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue

            leads.append({
                "id":          f"oak_{item.get('permit_number', '')}",
                "city":        "Oakland",
                "address":     item.get("address", "N/A").title(),
                "permit_type": item.get("permit_type", "N/A"),
                "description": item.get("description", "N/A")[:120],
                "status":      item.get("status", "N/A"),
                "filed_date":  item.get("application_date", "")[:10],
                "contractor":  item.get("contractor_name", "No indicado"),
                "owner":       item.get("owner_name", "No indicado"),
                "estimated_cost": item.get("valuation", "N/A"),
                "source_url":  "https://aca.accela.com/OAKLAND",
            })
        return leads

    # ── Telegram notify ──────────────────────────────────────────
    def notify(self, lead: dict):
        send_lead(
            agent_name="Permisos de Construcción",
            emoji="🏗️",
            title=f"{lead['city']} — {lead['address']}",
            fields={
                "Tipo de Permiso": lead["permit_type"],
                "Descripción":     lead["description"],
                "Estado":          lead["status"],
                "Fecha Solicitud": lead["filed_date"],
                "Contratista":     lead["contractor"],
                "Propietario":     lead["owner"],
                "Valor Estimado":  lead["estimated_cost"],
                "Ver Permiso":     lead["source_url"],
            },
            cta="💡 Contacta al contratista y ofrece insulación para el proyecto"
        )
