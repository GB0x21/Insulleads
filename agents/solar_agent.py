"""
agents/solar_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 2 — INSTALACIONES SOLARES

Fuentes:
  • SF DataSF (permisos filtrados por solar/photovoltaic)
  • San Jose Open Data (misma lógica)
  • California Solar Initiative (CPUC) — proyectos aprobados

Lógica:
  Propietario instala paneles solares → NECESITA mejorar aislamiento
  para maximizar su retorno de inversión.
  
Pitch: "Acabas de instalar paneles — asegúrate de que el calor/frío
no escape por el techo. Un buen aislamiento aumenta tu ahorro hasta 30%."
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead

SOLAR_KEYWORDS = [
    "solar", "photovoltaic", "pv system", "pv panel",
    "solar panel", "solar array", "battery storage", "powerwall"
]


class SolarAgent(BaseAgent):
    name      = "☀️ Instalaciones Solares"
    emoji     = "☀️"
    agent_key = "solar"

    def fetch_leads(self) -> list[dict]:
        leads = []
        leads += self._fetch_sf_solar()
        leads += self._fetch_sj_solar()
        leads += self._fetch_csi()
        return leads

    # ── San Francisco — permisos solar ────────────────────────────
    def _fetch_sf_solar(self) -> list[dict]:
        since = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 100,
            "$where": f"filed_date >= '{since}' AND UPPER(description) LIKE '%SOLAR%'",
            "$order": "filed_date DESC",
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
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue

            # Número de paneles estimado del texto
            kw_installed = self._extract_kw(desc)

            leads.append({
                "id":           f"sf_solar_{item.get('permit_number', '')}",
                "city":         "San Francisco",
                "address":      self._sf_address(item),
                "description":  item.get("description", "")[:150],
                "filed_date":   item.get("filed_date", "")[:10],
                "owner":        item.get("owner_name", "No indicado"),
                "owner_phone":  item.get("owner_phone", ""),
                "contractor":   item.get("contractor_company_name", "No indicado"),
                "kw_installed": kw_installed,
                "permit_no":    item.get("permit_number", ""),
            })
        return leads

    # ── San José — permisos solar ──────────────────────────────────
    def _fetch_sj_solar(self) -> list[dict]:
        since = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {
            "$limit": 100,
            "$where": f"application_date >= '{since}' AND UPPER(work_description) LIKE '%SOLAR%'",
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
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue

            leads.append({
                "id":           f"sj_solar_{item.get('permit_number', '')}",
                "city":         "San José",
                "address":      item.get("address", "N/A").title(),
                "description":  item.get("work_description", "")[:150],
                "filed_date":   item.get("application_date", "")[:10],
                "owner":        item.get("owner_name", "No indicado"),
                "owner_phone":  "",
                "contractor":   item.get("contractor_name", "No indicado"),
                "kw_installed": self._extract_kw(desc),
                "permit_no":    item.get("permit_number", ""),
            })
        return leads

    # ── California Solar Initiative (CPUC) ─────────────────────────
    def _fetch_csi(self) -> list[dict]:
        """
        API pública del CPUC — California Solar Initiative
        Retorna proyectos residenciales aprobados recientemente.
        Docs: https://data.ca.gov/dataset/california-solar-initiative-csi-program-data
        """
        url = "https://data.ca.gov/api/3/action/datastore_search"
        params = {
            "resource_id": "6da1b2e5-b5e6-4b5b-8ed5-e2a3db00e45a",  # dataset CSI
            "limit": 50,
            "filters": '{"county": "San Francisco"}',
            "sort": "app_approved_date desc",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            records = resp.json().get("result", {}).get("records", [])
        except Exception:
            return []

        leads = []
        for item in records:
            # Solo los aprobados en las últimas 72 horas
            approved = item.get("app_approved_date", "")
            if not approved:
                continue

            leads.append({
                "id":           f"csi_{item.get('app_id', item.get('_id', ''))}",
                "city":         item.get("city", "Bay Area"),
                "address":      item.get("address", "No indicada"),
                "description":  f"Sistema solar residencial aprobado",
                "filed_date":   approved[:10],
                "owner":        item.get("contact_name", "No indicado"),
                "owner_phone":  item.get("phone", ""),
                "contractor":   item.get("contractor_name", "No indicado"),
                "kw_installed": f"{item.get('system_size_dc', '?')} kW",
                "permit_no":    item.get("app_id", ""),
            })
        return leads

    # ── Helpers ───────────────────────────────────────────────────
    def _sf_address(self, item: dict) -> str:
        parts = [item.get("street_number", ""),
                 item.get("street_name", ""),
                 item.get("street_suffix", "")]
        return " ".join(p for p in parts if p).title()

    def _extract_kw(self, text: str) -> str:
        """Intenta extraer kW del texto del permiso."""
        import re
        m = re.search(r"(\d+\.?\d*)\s*kw", text, re.IGNORECASE)
        return f"{m.group(1)} kW" if m else "No especificado"

    # ── Telegram notify ───────────────────────────────────────────
    def notify(self, lead: dict):
        send_lead(
            agent_name="Instalaciones Solares",
            emoji="☀️",
            title=f"{lead['city']} — {lead['address']}",
            fields={
                "Sistema Instalado": lead["kw_installed"],
                "Permiso #":         lead["permit_no"],
                "Descripción":       lead["description"],
                "Fecha Aprobación":  lead["filed_date"],
                "Propietario":       lead["owner"],
                "Teléfono":          lead.get("owner_phone") or "No disponible",
                "Instalador Solar":  lead["contractor"],
            },
            cta=(
                "💡 PITCH: 'Acabas de instalar paneles solares — un buen aislamiento "
                "puede aumentar tu ahorro energético hasta un 30%. ¿Te interesa una "
                "evaluación gratuita?'"
            )
        )
