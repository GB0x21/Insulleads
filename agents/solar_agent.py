"""
agents/solar_agent.py  v2.0
Ventana: 3 meses — con enriquecimiento de contacto completo
"""
import re
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import enrich_lead, contact_score_label

SOLAR_KEYWORDS = ["solar", "photovoltaic", "pv system", "pv panel", "solar panel", "solar array", "battery storage", "powerwall"]
LOOKBACK_DAYS = 90


class SolarAgent(BaseAgent):
    name      = "Instalaciones Solares"
    emoji     = "☀️"
    agent_key = "solar"

    def fetch_leads(self) -> list:
        leads = []
        leads += self._fetch_sf_solar()
        leads += self._fetch_sj_solar()
        leads += self._fetch_oakland_solar()
        return leads

    def _fetch_sf_solar(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 1000,
            "$where": f"filed_date >= '{since}' AND (UPPER(description) LIKE '%SOLAR%' OR UPPER(description) LIKE '%PHOTOVOLTAIC%' OR UPPER(description) LIKE '%PV%')",
            "$order": "filed_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue
            address = self._sf_address(item)
            kw_installed = self._extract_kw(desc)
            lead = {
                "id":               "sf_solar_" + item.get("permit_number", ""),
                "city":             "San Francisco",
                "address":          address,
                "description":      (item.get("description", "") or "")[:150],
                "filed_date":       (item.get("filed_date", "") or "")[:10],
                "owner":            item.get("owner_name", ""),
                "owner_phone":      item.get("owner_phone", ""),
                "contractor":       item.get("contractor_company_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license_number", ""),
                "kw_installed":     kw_installed,
                "permit_no":        item.get("permit_number", ""),
                "estimated_cost":   "$" + f"{float(item.get('estimated_cost') or 0):,.0f}" if item.get("estimated_cost") else "N/A",
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _fetch_sj_solar(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {
            "$limit": 1000,
            "$where": f"application_date >= '{since}' AND UPPER(work_description) LIKE '%SOLAR%'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            desc = (item.get("work_description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue
            address = (item.get("address", "") or "N/A").title()
            lead = {
                "id": "sj_solar_" + item.get("permit_number", ""),
                "city": "San Jose",
                "address": address,
                "description": desc[:150],
                "filed_date": (item.get("application_date", "") or "")[:10],
                "owner": item.get("owner_name", ""),
                "owner_phone": item.get("owner_phone", ""),
                "contractor": item.get("contractor_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license", ""),
                "kw_installed": self._extract_kw(desc),
                "permit_no": item.get("permit_number", ""),
                "estimated_cost": item.get("job_value", "N/A"),
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _fetch_oakland_solar(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.oaklandca.gov/resource/p8h3-ngmm.json"
        params = {
            "$limit": 1000,
            "$where": f"application_date >= '{since}' AND UPPER(description) LIKE '%SOLAR%'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue
            address = (item.get("address", "") or "N/A").title()
            lead = {
                "id": "oak_solar_" + item.get("permit_number", ""),
                "city": "Oakland",
                "address": address,
                "description": desc[:150],
                "filed_date": (item.get("application_date", "") or "")[:10],
                "owner": item.get("owner_name", ""),
                "owner_phone": item.get("owner_phone", ""),
                "contractor": item.get("contractor_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license", ""),
                "kw_installed": self._extract_kw(desc),
                "permit_no": item.get("permit_number", ""),
                "estimated_cost": item.get("valuation", "N/A"),
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _sf_address(self, item: dict) -> str:
        parts = [item.get("street_number",""), item.get("street_name",""), item.get("street_suffix","")]
        return " ".join(p for p in parts if p).title()

    def _extract_kw(self, text: str) -> str:
        m = re.search(r"(\d+\.?\d*)\s*kw", text, re.IGNORECASE)
        return (m.group(1) + " kW") if m else "No especificado"

    def notify(self, lead: dict):
        score_label = contact_score_label(lead.get("contact_score", 0))
        send_lead(
            agent_name="Instalaciones Solares",
            emoji="☀️",
            title=lead["city"] + " — " + lead["address"],
            fields={
                "Sistema Solar":       lead["kw_installed"],
                "Permiso #":           lead["permit_no"],
                "Descripcion":         lead["description"],
                "Fecha Aprobacion":    lead["filed_date"],
                "Costo Estimado":      lead.get("estimated_cost", "N/A"),
                "Propietario":         lead.get("owner") or "No encontrado",
                "Dir. Postal Owner":   lead.get("owner_mail_addr") or "No disponible",
                "Tel. Propietario":    lead.get("owner_phone") or "No disponible",
                "Instalador Solar":    lead.get("contractor") or "No indicado",
                "Tel. Instalador":     lead.get("contractor_phone") or "No disponible",
                "Dir. Instalador":     lead.get("contractor_addr") or "No disponible",
                "Lic. CSLB":           lead.get("contractor_license") or "N/A",
                "Ver en Maps":         lead.get("maps_url") or "No disponible",
                "Info Contacto":       score_label,
            },
            cta="PITCH: Acabas de instalar paneles solares — un buen aislamiento puede aumentar tu ahorro energetico hasta un 30%. Evaluacion GRATUITA?"
        )
