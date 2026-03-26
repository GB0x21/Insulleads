"""
agents/permits_agent.py
━━━━━━━━━━━━━━━━━━━━━━
🏗️ Permisos de Construcción — Bay Area Completa
Ciudades : SF · Oakland · San Jose · Berkeley · Sunnyvale
           Santa Clara · Richmond · Fremont · Hayward

Enriquecimiento de contacto GC (en orden):
  1º → CSVs locales en contacts/  (fuzzy match por nombre)
  2º → CSLB web                   (fallback si no hay match)
"""

import os
import re
import time
import logging
import requests
from bs4 import BeautifulSoup

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
#  FUENTES OPEN DATA — BAY AREA
# ─────────────────────────────────────────────────────────────────

PERMIT_SOURCES = [
    {
        "city": "San Francisco",
        "url":  "https://data.sfgov.org/resource/i98e-djp9.json",
        "params": {
            "$limit": 50,
            "$order": "filed_date DESC",
            "$where": (
                "status IN('issued','complete') AND "
                "permit_type_definition IN("
                "'additions alterations or repairs',"
                "'new construction wood frame',"
                "'otc additions',"
                "'accessory dwelling units',"
                "'new construction - wood frame')"
            ),
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "street_number",
            "address2":    "street_name",
            "permit_type": "permit_type_definition",
            "description": "description",
            "status":      "status",
            "filed_date":  "filed_date",
            "issued_date": "issued_date",
            "contractor":  "contractor_company_name",
            "lic_number":  "contractor_license",
            "owner":       "owner",
            "value":       "estimated_cost",
            "url_tpl":     "https://sfdbi.org/permit/{permit_number}",
        },
    },
    {
        "city": "Oakland",
        "url":  "https://data.oaklandca.gov/resource/p8h7-gzqg.json",
        "params": {
            "$limit": 50,
            "$order": "applied_date DESC",
            "$where": "status IN('ISSUED','FINALED')",
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "site_address",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "description",
            "status":      "status",
            "filed_date":  "applied_date",
            "issued_date": "issue_date",
            "contractor":  "primary_contractor",
            "lic_number":  "contractor_lic_number",
            "owner":       "owner_name",
            "value":       "valuation",
            "url_tpl":     "https://www.oaklandca.gov/resources/planning-and-building-permit-search",
        },
    },
    {
        "city": "San Jose",
        "url":  "https://data.sanjoseca.gov/resource/4yft-3k4m.json",
        "params": {
            "$limit": 50,
            "$order": "applicationdate DESC",
            "$where": "status IN('ISSUED','COMPLETE')",
        },
        "field_map": {
            "id":          "permitno",
            "address":     "address",
            "address2":    None,
            "permit_type": "permittype",
            "description": "description",
            "status":      "status",
            "filed_date":  "applicationdate",
            "issued_date": "issuedate",
            "contractor":  "contractorname",
            "lic_number":  "contractorlicno",
            "owner":       "ownername",
            "value":       "valuation",
            "url_tpl":     "https://portal.sanjoseca.gov/permittracker/details/{permitno}",
        },
    },
    {
        "city": "Berkeley",
        "url":  "https://data.cityofberkeley.info/resource/cqze-unm8.json",
        "params": {
            "$limit": 50,
            "$order": "date_issued DESC",
            "$where": "permit_status IN('ISSUED','FINALED')",
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "location_address",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "permit_description",
            "status":      "permit_status",
            "filed_date":  "date_filed",
            "issued_date": "date_issued",
            "contractor":  "contractor_name",
            "lic_number":  "contractor_license",
            "owner":       "property_owner",
            "value":       "project_valuation",
            "url_tpl":     "https://permits.cityofberkeley.info/eTRAKiT/",
        },
    },
    {
        "city": "Sunnyvale",
        "url":  "https://data.sunnyvale.ca.gov/resource/7xm5-teup.json",
        "params": {
            "$limit": 50,
            "$order": "issued_date DESC",
            "$where": "permit_status='Issued'",
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "address",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "description",
            "status":      "permit_status",
            "filed_date":  "application_date",
            "issued_date": "issued_date",
            "contractor":  "contractor_name",
            "lic_number":  "contractor_license_number",
            "owner":       "property_owner",
            "value":       "project_value",
            "url_tpl":     "https://sunapps.sunnyvale.ca.gov/pds/",
        },
        "_skip_if_no_data": True,
    },
    {
        "city": "Santa Clara",
        "url":  "https://data.santa-clara.ca.gov/resource/building-permits.json",
        "params": {
            "$limit": 50,
            "$order": "issue_date DESC",
            "$where": "status='Issued'",
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "address",
            "address2":    None,
            "permit_type": "type",
            "description": "description",
            "status":      "status",
            "filed_date":  "application_date",
            "issued_date": "issue_date",
            "contractor":  "contractor",
            "lic_number":  "license_number",
            "owner":       "owner",
            "value":       "value",
            "url_tpl":     "https://www.santaclaraca.gov/government/departments/community-development/building-division",
        },
        "_skip_if_no_data": True,
    },
    {
        "city": "Richmond",
        "url":  "https://data.ci.richmond.ca.us/resource/permits.json",
        "params": {
            "$limit": 50,
            "$order": "date_issued DESC",
            "$where": "status='ISSUED'",
        },
        "field_map": {
            "id":          "permit_number",
            "address":     "site_address",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "work_description",
            "status":      "status",
            "filed_date":  "application_date",
            "issued_date": "date_issued",
            "contractor":  "contractor_name",
            "lic_number":  "contractor_license",
            "owner":       "owner_name",
            "value":       "declared_valuation",
            "url_tpl":     "https://www.ci.richmond.ca.us/1357/Building-Permits",
        },
        "_skip_if_no_data": True,
    },
    {
        "city": "Fremont",
        "url":  "https://www.fremont.gov/CivicAlerts.aspx",
        "params": {},
        "field_map": {
            "id":          "permit_number",
            "address":     "address",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "description",
            "status":      "status",
            "filed_date":  "application_date",
            "issued_date": "issue_date",
            "contractor":  "contractor_name",
            "lic_number":  "contractor_license",
            "owner":       "owner",
            "value":       "valuation",
            "url_tpl":     "https://www.fremont.gov/government/departments/building-services",
        },
        "_skip_if_no_data": True,
    },
    {
        "city": "Hayward",
        "url":  "https://hayward.permitportal.us/api/permits",
        "params": {"status": "Issued", "type": "Building", "limit": 30},
        "field_map": {
            "id":          "permit_number",
            "address":     "location",
            "address2":    None,
            "permit_type": "permit_type",
            "description": "description",
            "status":      "status",
            "filed_date":  "applied",
            "issued_date": "issued",
            "contractor":  "contractor",
            "lic_number":  "lic_no",
            "owner":       "owner",
            "value":       "valuation",
            "url_tpl":     "https://hayward.permitportal.us/permit/{permit_number}",
        },
        "_skip_if_no_data": True,
    },
]

INSULATION_KEYWORDS = [
    "insulation", "insulate", "adu", "accessory dwelling",
    "addition", "remodel", "renovation", "attic", "crawl",
    "energy", "retrofit", "new construction", "garage conversion",
    "dwelling", "residential", "hvac", "weatherization",
]

# ─────────────────────────────────────────────────────────────────
#  CSLB FALLBACK
# ─────────────────────────────────────────────────────────────────

_CSLB_URL = "https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
_CSLB_HDR = {
    "User-Agent": "Mozilla/5.0 (compatible; InsulTechs-LeadBot/1.0)",
    "Accept":     "text/html,application/xhtml+xml",
}


def _cslb_lookup(license_number: str = None, company_name: str = None) -> dict:
    result = {}
    try:
        s = requests.Session()
        s.headers.update(_CSLB_HDR)
        r = s.get(_CSLB_URL, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        hidden = {t.get("name", ""): t.get("value", "")
                  for t in soup.find_all("input", {"type": "hidden"})}

        if license_number and re.match(r"^\d+$", str(license_number).strip()):
            val, typ = str(license_number).strip(), "License"
        elif company_name:
            val, typ = company_name.strip()[:50], "Business"
        else:
            return result

        payload = {
            **hidden,
            "ctl00$ContentPlaceHolder1$RadioButtonList1": typ,
            "ctl00$ContentPlaceHolder1$TextBox1":         val,
            "ctl00$ContentPlaceHolder1$Button1":          "Submit",
        }
        r2 = s.post(_CSLB_URL, data=payload, timeout=10)
        r2.raise_for_status()
        soup2 = BeautifulSoup(r2.text, "html.parser")

        table = (soup2.find("table", {"id": re.compile(r"Grid|Results|License", re.I)})
                 or soup2.find("table"))
        if table:
            for row in table.find_all("tr")[1:2]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 3:
                    result = {
                        "phone":       cells[3] if len(cells) > 3 else "",
                        "cslb_name":   cells[1] if len(cells) > 1 else "",
                        "cslb_city":   cells[2] if len(cells) > 2 else "",
                        "cslb_status": cells[4] if len(cells) > 4 else "",
                    }
                    break
        if not result.get("phone"):
            tag = soup2.find(string=re.compile(r"\(\d{3}\)\s*\d{3}-\d{4}"))
            if tag:
                result["phone"] = tag.strip()
    except Exception as e:
        logger.debug(f"CSLB lookup error: {e}")
    return result


# ─────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────

def _normalize_permit(raw: dict, field_map: dict, city: str) -> dict:
    get = lambda k: raw.get(field_map.get(k) or "", "") or ""
    address = get("address")
    if field_map.get("address2") and raw.get(field_map["address2"]):
        address = f"{address} {raw[field_map['address2']]}".strip()
    permit_id  = get("id")
    url_tpl    = field_map.get("url_tpl", "")
    permit_url = url_tpl.format(**{k: raw.get(v, "") for k, v in field_map.items() if v})
    return {
        "id":          f"{city}_{permit_id}",
        "city":        city,
        "address":     address,
        "permit_type": get("permit_type"),
        "description": get("description"),
        "status":      get("status"),
        "filed_date":  get("filed_date")[:10] if get("filed_date") else "",
        "issued_date": get("issued_date")[:10] if get("issued_date") else "",
        "contractor":  get("contractor"),
        "lic_number":  get("lic_number"),
        "owner":       get("owner"),
        "value":       get("value"),
        "permit_url":  permit_url,
    }


def _is_relevant(lead: dict) -> bool:
    haystack = (
        (lead.get("description") or "") + " " +
        (lead.get("permit_type")  or "")
    ).lower()
    return any(kw in haystack for kw in INSULATION_KEYWORDS)


# ─────────────────────────────────────────────────────────────────
#  AGENTE
# ─────────────────────────────────────────────────────────────────

class PermitsAgent(BaseAgent):
    name      = "🏗️ Permisos de Construcción — Bay Area"
    emoji     = "🏗️"
    agent_key = "permits"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts: list = load_all_contacts()
        self._cache:    dict = {}

    def _enrich_gc(self, lead: dict) -> dict:
        contractor = (lead.get("contractor") or "").strip()
        lic        = (lead.get("lic_number")  or "").strip()
        cache_key  = lic or contractor
        if not cache_key:
            return {}
        if cache_key in self._cache:
            return self._cache[cache_key]

        enrichment = {}

        # 1. CSV locales
        match = lookup_contact(contractor, self._contacts)
        if match:
            enrichment = {
                "contact_phone":  match.get("phone", ""),
                "contact_email":  match.get("email", ""),
                "contact_source": f"CSV ({match['source']})",
                "contact_name":   match["raw_name"],
            }

        # 2. CSLB fallback
        if not enrichment or (not enrichment.get("contact_phone") and not enrichment.get("contact_email")):
            time.sleep(0.5)
            cslb = _cslb_lookup(
                license_number=lic,
                company_name=contractor if not lic else None,
            )
            if cslb:
                enrichment = {
                    "contact_phone":  cslb.get("phone", ""),
                    "contact_email":  "",
                    "contact_source": "CSLB",
                    "contact_name":   cslb.get("cslb_name", ""),
                    "cslb_city":      cslb.get("cslb_city", ""),
                    "cslb_status":    cslb.get("cslb_status", ""),
                }

        self._cache[cache_key] = enrichment
        return enrichment

    def fetch_leads(self) -> list:
        all_leads = []
        for source in PERMIT_SOURCES:
            city         = source["city"]
            skip_on_fail = source.get("_skip_if_no_data", False)
            try:
                resp = requests.get(
                    source["url"],
                    params=source["params"],
                    timeout=15,
                    headers={"Accept": "application/json"},
                )
                resp.raise_for_status()
                records = resp.json()
                if not isinstance(records, list):
                    continue
                city_n = 0
                for raw in records:
                    lead = _normalize_permit(raw, source["field_map"], city)
                    if _is_relevant(lead):
                        lead.update(self._enrich_gc(lead))
                        all_leads.append(lead)
                        city_n += 1
                logger.info(f"[{city}] {len(records)} registros → {city_n} leads relevantes")
            except Exception as e:
                if skip_on_fail:
                    logger.debug(f"[{city}] API no disponible, omitido: {e}")
                else:
                    logger.error(f"[{city}] Error: {e}")
        return all_leads

    def notify(self, lead: dict):
        phone  = lead.get("contact_phone") or "No disponible"
        email  = lead.get("contact_email") or "—"
        source = lead.get("contact_source", "")
        phone_str = f"{phone}  _(via {source})_" if source else phone

        fields = {
            "📍 Ciudad":           lead.get("city"),
            "🔖 Tipo de Permiso":  lead.get("permit_type"),
            "📝 Descripción":      (lead.get("description") or "")[:200],
            "📊 Estado":           lead.get("status"),
            "📅 Fecha Solicitud":  lead.get("filed_date"),
            "📅 Fecha Emisión":    lead.get("issued_date"),
            "👷 Contratista (GC)": lead.get("contractor") or "No especificado",
            "🪪 Licencia CSLB":    lead.get("lic_number") or "—",
            "📞 Teléfono GC":      phone_str,
            "✉️  Email GC":        email,
            "👤 Propietario":      lead.get("owner") or "—",
            "💰 Valor Estimado":   (
                f"${float(lead['value']):,.0f}" if lead.get("value") else "—"
            ),
        }
        if lead.get("contact_source") == "CSLB":
            if lead.get("cslb_city"):
                fields["🏢 Ciudad GC (CSLB)"] = lead["cslb_city"]
            if lead.get("cslb_status"):
                fields["✅ Estado Licencia"]   = lead["cslb_status"]

        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields,
            url=lead.get("permit_url"),
            cta="📲 Contacta al GC y ofrece insulación para el proyecto",
        )
