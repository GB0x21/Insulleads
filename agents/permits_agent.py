"""
agents/permits_agent.py  v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 1 — PERMISOS DE CONSTRUCCIÓN

Ventana: últimos 3 meses
Fuentes: SF DataSF · San Jose Open Data · Oakland Open Data
Enriquecimiento: owner (assessor) · contractor (CSLB) · maps
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import requests
from datetime import datetime, timedelta
from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import enrich_lead, contact_score_label

PERMIT_KEYWORDS = [
    "addition", "adu", "accessory dwelling", "remodel",
    "new construction", "tenant improvement", "renovation",
    "garage conversion", "basement", "crawl space",
    "new building", "alteration", "dwelling"
]

# 3 meses de ventana
LOOKBACK_DAYS = 90


class PermitsAgent(BaseAgent):
    name      = "Permisos de Construccion"
    emoji     = "🏗️"
    agent_key = "permits"

    def fetch_leads(self) -> list:
        leads = []
        leads += self._fetch_sf()
        leads += self._fetch_san_jose()
        leads += self._fetch_oakland()
        return leads

    def _fetch_sf(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 1000,
            "$where": f"filed_date >= '{since}'",
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
            desc  = (item.get("description", "") or "").lower()
            ptype = (item.get("permit_type_definition", "") or "").lower()
            if not any(kw in desc or kw in ptype for kw in PERMIT_KEYWORDS):
                continue
            address = self._sf_address(item)
            lead = {
                "id":               "sf_" + item.get("permit_number", ""),
                "city":             "San Francisco",
                "address":          address,
                "permit_type":      item.get("permit_type_definition", "N/A"),
                "description":      (item.get("description", "") or "")[:150],
                "status":           item.get("status", "N/A"),
                "filed_date":       (item.get("filed_date", "") or "")[:10],
                "owner":            item.get("owner_name", ""),
                "owner_phone":      item.get("owner_phone", ""),
                "contractor":       item.get("contractor_company_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license_number", ""),
                "estimated_cost":   "$" + f"{float(item.get('estimated_cost') or 0):,.0f}" if item.get("estimated_cost") else "N/A",
                "permit_no":        item.get("permit_number", ""),
                "source_url":       "https://sfdbi.org/permit/" + item.get("permit_number", ""),
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _fetch_san_jose(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {"$limit": 1000, "$where": f"application_date >= '{since}'", "$order": "application_date DESC"}
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            desc = (item.get("work_description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue
            address = (item.get("address", "") or "N/A").title()
            lead = {
                "id": "sj_" + item.get("permit_number", ""),
                "city": "San Jose",
                "address": address,
                "permit_type": item.get("permit_type", "N/A"),
                "description": (item.get("work_description", "") or "")[:150],
                "status": item.get("status", "N/A"),
                "filed_date": (item.get("application_date", "") or "")[:10],
                "owner": item.get("owner_name", ""),
                "owner_phone": item.get("owner_phone", ""),
                "contractor": item.get("contractor_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license", ""),
                "estimated_cost": item.get("job_value", "N/A"),
                "permit_no": item.get("permit_number", ""),
                "source_url": "https://www.sanjoseca.gov/your-government/departments-offices/planning-building-code-enforcement",
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _fetch_oakland(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        url = "https://data.oaklandca.gov/resource/p8h3-ngmm.json"
        params = {"$limit": 1000, "$where": f"application_date >= '{since}'", "$order": "application_date DESC"}
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception:
            return []
        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue
            address = (item.get("address", "") or "N/A").title()
            lead = {
                "id": "oak_" + item.get("permit_number", ""),
                "city": "Oakland",
                "address": address,
                "permit_type": item.get("permit_type", "N/A"),
                "description": (item.get("description", "") or "")[:150],
                "status": item.get("status", "N/A"),
                "filed_date": (item.get("application_date", "") or "")[:10],
                "owner": item.get("owner_name", ""),
                "owner_phone": item.get("owner_phone", ""),
                "contractor": item.get("contractor_name", ""),
                "contractor_phone": item.get("contractor_phone", ""),
                "contractor_license": item.get("contractor_license", ""),
                "estimated_cost": item.get("valuation", "N/A"),
                "permit_no": item.get("permit_number", ""),
                "source_url": "https://aca.accela.com/OAKLAND",
            }
            lead = enrich_lead(lead)
            leads.append(lead)
        return leads

    def _sf_address(self, item: dict) -> str:
        parts = [item.get("street_number", ""), item.get("street_name", ""), item.get("street_suffix", "")]
        return " ".join(p for p in parts if p).title()

    def notify(self, lead: dict):
        score_label = contact_score_label(lead.get("contact_score", 0))
        send_lead(
            agent_name="Permisos de Construccion",
            emoji="🏗️",
            title=lead["city"] + " — " + lead["address"],
            fields={
                "Tipo de Permiso":     lead["permit_type"],
                "Descripcion":         lead["description"],
                "Estado":              lead["status"],
                "Fecha Solicitud":     lead["filed_date"],
                "Valor Estimado":      lead["estimated_cost"],
                "Permiso #":           lead["permit_no"],
                "Propietario":         lead.get("owner") or "No encontrado",
                "Dir. Postal Owner":   lead.get("owner_mail_addr") or "No disponible",
                "Tel. Propietario":    lead.get("owner_phone") or "No disponible",
                "Contratista":         lead.get("contractor") or "No indicado",
                "Tel. Contratista":    lead.get("contractor_phone") or "No disponible",
                "Dir. Contratista":    lead.get("contractor_addr") or "No disponible",
                "Lic. CSLB":           lead.get("contractor_license") or "N/A",
                "Estado Licencia":     lead.get("contractor_status") or "N/A",
                "Ver en Maps":         lead.get("maps_url") or "No disponible",
                "Permiso Oficial":     lead["source_url"],
                "Info Contacto":       score_label,
            },
            cta="Contacta al contratista o propietario — proyecto activo necesita insulacion"
        )
