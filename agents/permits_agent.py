"""
agents/permits_agent.py  v5
━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏗️ Permisos de Construcción — Bay Area Completa

FIXES v5:
  ✅ Filtro valor > MIN_PERMIT_VALUE (default $50,000)
  ✅ Filtro fecha: solo últimos PERMIT_MONTHS meses (default 3)
  ✅ SJ CKAN: fecha filtrada server-side via SQL en la query
"""

import os
import re
import time
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

PARALLEL_CITIES   = int(os.getenv("PARALLEL_CITIES", "6"))
MIN_PERMIT_VALUE  = float(os.getenv("MIN_PERMIT_VALUE", "50000"))   # $50K mínimo
PERMIT_MONTHS     = int(os.getenv("PERMIT_MONTHS", "3"))             # últimos 3 meses


def _cutoff_date() -> str:
    """Fecha ISO de hace PERMIT_MONTHS meses para filtrar en las queries."""
    cutoff = datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)
    return cutoff.strftime("%Y-%m-%dT00:00:00")


def _cutoff_date_sj() -> str:
    """Formato para San Jose CKAN (YYYY-MM-DD)."""
    cutoff = datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)
    return cutoff.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────
#  FUENTES OPEN DATA
# ─────────────────────────────────────────────────────────────────

def _build_sources() -> list:
    """Construye las fuentes con fechas de corte calculadas en runtime."""
    cutoff     = _cutoff_date()
    cutoff_sj  = _cutoff_date_sj()

    return [
        # ── San Francisco ─────────────────────────────────────────
        {
            "city": "San Francisco", "engine": "socrata",
            "url":  "https://data.sfgov.org/resource/i98e-djp9.json",
            "params": {
                "$limit": 200, "$order": "issued_date DESC",
                "$where": (
                    f"status IN('issued','complete') "
                    f"AND issued_date >= '{cutoff}' "
                    f"AND estimated_cost >= {MIN_PERMIT_VALUE} "
                    f"AND permit_type_definition IN("
                    f"'additions alterations or repairs',"
                    f"'new construction wood frame',"
                    f"'otc additions','accessory dwelling units',"
                    f"'new construction - wood frame')"
                ),
            },
            "field_map": {
                "id":"permit_number","address":"street_number","address2":"street_name",
                "permit_type":"permit_type_definition","description":"description",
                "status":"status","filed_date":"filed_date","issued_date":"issued_date",
                "contractor":"contractor_company_name","lic_number":"contractor_license",
                "owner":"owner","value":"estimated_cost",
                "url_tpl":"https://sfdbi.org/permit/{permit_number}",
            },
        },
        # ── San Jose ──────────────────────────────────────────────
        {
            "city": "San Jose", "engine": "ckan_sql",
            "url":  "https://data.sanjoseca.gov/api/3/action/datastore_search_sql",
            "params": {
                "sql": (
                    f"SELECT * FROM \"761b7ae8-3be1-4ad6-923d-c7af6404a904\" "
                    f"WHERE \"ISSUEDATE\" >= '{cutoff_sj}' "
                    f"AND CAST(REPLACE(REPLACE(\"PERMITVALUATION\", ',', ''), '$', '') AS FLOAT) >= {MIN_PERMIT_VALUE} "
                    f"ORDER BY \"ISSUEDATE\" DESC LIMIT 200"
                )
            },
            "field_map": {
                "id":"FOLDERNUMBER","address":"gx_location","address2":None,
                "permit_type":"FOLDERNAME","description":"WORKDESCRIPTION",
                "status":"Status","filed_date":None,"issued_date":"ISSUEDATE",
                "contractor":"CONTRACTOR","lic_number":None,
                "owner":"OWNERNAME","value":"PERMITVALUATION",
                "url_tpl":"https://www.sjpermits.org/",
            },
        },
        # ── Sunnyvale ─────────────────────────────────────────────
        {
            "city": "Sunnyvale", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.sunnyvale.ca.gov/resource/7xm5-teup.json",
            "params": {
                "$limit":200,"$order":"issued_date DESC",
                "$where": f"permit_status='Issued' AND issued_date >= '{cutoff}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"permit_status",
                "filed_date":"application_date","issued_date":"issued_date",
                "contractor":"contractor_name","lic_number":"contractor_license_number",
                "owner":"property_owner","value":"project_value",
                "url_tpl":"https://sunapps.sunnyvale.ca.gov/pds/",
            },
        },
        # ── Santa Clara ───────────────────────────────────────────
        {
            "city": "Santa Clara", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.santa-clara.ca.gov/resource/building-permits.json",
            "params": {
                "$limit":200,"$order":"issue_date DESC",
                "$where": f"status='Issued' AND issue_date >= '{cutoff}'",
            },
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issue_date",
                "contractor":"contractor","lic_number":"license_number",
                "owner":"owner","value":"value",
                "url_tpl":"https://www.santaclaraca.gov/government/departments/community-development/building-division",
            },
        },
        # ── Richmond ──────────────────────────────────────────────
        {
            "city": "Richmond", "engine": "socrata", "_skip_if_no_data": True,
            "url":  "https://data.ci.richmond.ca.us/resource/permits.json",
            "params": {
                "$limit":200,"$order":"date_issued DESC",
                "$where": f"status='ISSUED' AND date_issued >= '{cutoff}'",
            },
            "field_map": {
                "id":"permit_number","address":"site_address","address2":None,
                "permit_type":"permit_type","description":"work_description","status":"status",
                "filed_date":"application_date","issued_date":"date_issued",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner_name","value":"declared_valuation",
                "url_tpl":"https://www.ci.richmond.ca.us/1357/Building-Permits",
            },
        },
        # ── Fremont ───────────────────────────────────────────────
        {
            "city": "Fremont", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://www.fremont.gov/CivicAlerts.aspx", "params": {},
            "field_map": {
                "id":"permit_number","address":"address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"application_date","issued_date":"issue_date",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://www.fremont.gov/government/departments/building-services",
            },
        },
        # ── Hayward ───────────────────────────────────────────────
        {
            "city": "Hayward", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://hayward.permitportal.us/api/permits",
            "params": {"status":"Issued","type":"Building","limit":200},
            "field_map": {
                "id":"permit_number","address":"location","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"applied","issued_date":"issued",
                "contractor":"contractor","lic_number":"lic_no",
                "owner":"owner","value":"valuation",
                "url_tpl":"https://hayward.permitportal.us/permit/{permit_number}",
            },
        },
        # ── Oakland — Accela (sin API pública) ────────────────────
        {
            "city": "Oakland", "engine": "socrata", "_skip_if_no_data": True,
            "url": "https://data.oaklandca.gov/resource/p8h7-gzqg.json",
            "params": {"$limit":1},
            "field_map": {
                "id":"permit_number","address":"site_address","address2":None,
                "permit_type":"permit_type","description":"description","status":"status",
                "filed_date":"applied_date","issued_date":"issue_date",
                "contractor":"primary_contractor","lic_number":"contractor_lic_number",
                "owner":"owner_name","value":"valuation",
                "url_tpl":"https://aca-prod.accela.com/OAKLAND/",
            },
        },
        # ── Berkeley — requiere Socrata token ─────────────────────
        {
            "city": "Berkeley", "engine": "socrata",
            "_skip_if_no_data": True, "_requires_token": True,
            "url": "https://data.cityofberkeley.info/resource/cqze-unm8.json",
            "params": {
                "$limit":200,"$order":"date_issued DESC",
                "$where": f"permit_status IN('ISSUED','FINALED') AND date_issued >= '{cutoff}'",
            },
            "field_map": {
                "id":"permit_number","address":"location_address","address2":None,
                "permit_type":"permit_type","description":"permit_description","status":"permit_status",
                "filed_date":"date_filed","issued_date":"date_issued",
                "contractor":"contractor_name","lic_number":"contractor_license",
                "owner":"property_owner","value":"project_valuation",
                "url_tpl":"https://permits.cityofberkeley.info/eTRAKiT/",
            },
        },
    ]


INSULATION_KEYWORDS = [
    "insulation","insulate","adu","accessory dwelling","addition","remodel",
    "renovation","attic","crawl","energy","retrofit","new construction",
    "garage conversion","dwelling","residential","hvac","weatherization",
]

# ── CSLB fallback ──────────────────────────────────────────────────
_CSLB_URL = "https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
_CSLB_HDR = {"User-Agent": "Mozilla/5.0 (compatible; InsulTechs-LeadBot/1.0)"}

def _cslb_lookup(license_number=None, company_name=None) -> dict:
    result = {}
    try:
        s = requests.Session()
        s.headers.update(_CSLB_HDR)
        r = s.get(_CSLB_URL, timeout=10)
        r.raise_for_status()
        hidden = {t.get("name",""): t.get("value","")
                  for t in BeautifulSoup(r.text,"html.parser").find_all("input",{"type":"hidden"})}
        if license_number and re.match(r"^\d+$", str(license_number).strip()):
            val, typ = str(license_number).strip(), "License"
        elif company_name:
            val, typ = company_name.strip()[:50], "Business"
        else:
            return result
        payload = {**hidden,
                   "ctl00$ContentPlaceHolder1$RadioButtonList1": typ,
                   "ctl00$ContentPlaceHolder1$TextBox1": val,
                   "ctl00$ContentPlaceHolder1$Button1": "Submit"}
        r2 = s.post(_CSLB_URL, data=payload, timeout=10)
        r2.raise_for_status()
        soup2 = BeautifulSoup(r2.text, "html.parser")
        table = soup2.find("table",{"id": re.compile(r"Grid|Results|License",re.I)}) or soup2.find("table")
        if table:
            for row in table.find_all("tr")[1:2]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) >= 3:
                    result = {"phone": cells[3] if len(cells)>3 else "",
                              "cslb_name": cells[1] if len(cells)>1 else "",
                              "cslb_city": cells[2] if len(cells)>2 else "",
                              "cslb_status": cells[4] if len(cells)>4 else ""}
    except Exception as e:
        logger.debug(f"CSLB error: {e}")
    return result


# ── Parsers ────────────────────────────────────────────────────────
def _fetch_socrata(source: dict) -> list:
    headers = {"Accept": "application/json"}
    token = os.getenv("SOCRATA_APP_TOKEN", "")
    if token:
        headers["X-App-Token"] = token
    resp = requests.get(source["url"], params=source["params"], timeout=15, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_ckan_sql(source: dict) -> list:
    """CKAN datastore_search_sql — soporta WHERE y ORDER BY server-side."""
    resp = requests.get(source["url"], params=source["params"], timeout=30,
                        headers={"Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"CKAN SQL error: {data.get('error')}")
    return data.get("result", {}).get("records", [])


def _fetch_source(source: dict) -> tuple:
    city = source["city"]
    try:
        engine = source.get("engine", "socrata")
        if engine == "ckan_sql":
            records = _fetch_ckan_sql(source)
        else:
            records = _fetch_socrata(source)
        return (city, records, None)
    except Exception as e:
        return (city, [], e)


# ── Normalización ──────────────────────────────────────────────────
def _parse_value(raw_value) -> float:
    """Convierte string de valor a float, limpiando $, comas, etc."""
    if not raw_value:
        return 0.0
    try:
        cleaned = re.sub(r"[^\d.]", "", str(raw_value))
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def _normalize_permit(raw: dict, field_map: dict, city: str) -> dict:
    get = lambda k: raw.get(field_map.get(k) or "", "") or ""
    address = get("address")
    if field_map.get("address2") and raw.get(field_map["address2"]):
        address = f"{address} {raw[field_map['address2']]}".strip()
    raw_vals = {v: raw.get(v,"") for k,v in field_map.items() if v and k!="url_tpl"}
    try:
        permit_url = field_map.get("url_tpl","").format(**raw_vals)
    except KeyError:
        permit_url = field_map.get("url_tpl","")
    return {
        "id":          f"{city}_{get('id')}",
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
        "value_float": _parse_value(get("value")),
        "permit_url":  permit_url,
    }


def _is_relevant(lead: dict) -> bool:
    """Filtra por keyword de insulación Y valor mínimo."""
    # Filtro de valor
    if lead["value_float"] < MIN_PERMIT_VALUE:
        return False
    # Filtro de keyword
    haystack = ((lead.get("description") or "") + " " + (lead.get("permit_type") or "")).lower()
    return any(kw in haystack for kw in INSULATION_KEYWORDS)


def _is_recent(lead: dict) -> bool:
    """Verifica que el lead esté dentro del rango de meses configurado."""
    date_str = lead.get("issued_date") or lead.get("filed_date") or ""
    if not date_str:
        return True   # si no hay fecha, no filtramos
    try:
        issued = datetime.strptime(date_str[:10], "%Y-%m-%d")
        cutoff = datetime.utcnow() - timedelta(days=30 * PERMIT_MONTHS)
        return issued >= cutoff
    except Exception:
        return True


# ── AGENTE ─────────────────────────────────────────────────────────
class PermitsAgent(BaseAgent):
    name      = "🏗️ Permisos de Construcción — Bay Area"
    emoji     = "🏗️"
    agent_key = "permits"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts    = load_all_contacts()
        self._cslb_cache  = {}

    def _enrich_gc(self, lead: dict) -> dict:
        contractor = (lead.get("contractor") or "").strip()
        lic        = (lead.get("lic_number")  or "").strip()
        cache_key  = lic or contractor
        if not cache_key:
            return {}
        if cache_key in self._cslb_cache:
            return self._cslb_cache[cache_key]
        enrichment = {}
        match = lookup_contact(contractor, self._contacts)
        if match:
            enrichment = {
                "contact_phone":  match.get("phone",""),
                "contact_email":  match.get("email",""),
                "contact_source": f"CSV ({match['source']})",
                "contact_name":   match["raw_name"],
            }
        if not enrichment or (not enrichment.get("contact_phone") and not enrichment.get("contact_email")):
            time.sleep(0.3)
            cslb = _cslb_lookup(license_number=lic, company_name=contractor if not lic else None)
            if cslb:
                enrichment = {
                    "contact_phone":  cslb.get("phone",""),
                    "contact_email":  "",
                    "contact_source": "CSLB",
                    "contact_name":   cslb.get("cslb_name",""),
                    "cslb_city":      cslb.get("cslb_city",""),
                    "cslb_status":    cslb.get("cslb_status",""),
                }
        self._cslb_cache[cache_key] = enrichment
        return enrichment

    def fetch_leads(self) -> list:
        all_leads = []
        sources = _build_sources()
        active  = [s for s in sources
                   if not (s.get("_requires_token") and not os.getenv("SOCRATA_APP_TOKEN"))]

        with ThreadPoolExecutor(max_workers=PARALLEL_CITIES) as executor:
            futures = {executor.submit(_fetch_source, s): s for s in active}
            for fut in as_completed(futures):
                source       = futures[fut]
                city         = source["city"]
                skip_on_fail = source.get("_skip_if_no_data", False)
                _, records, error = fut.result()

                if error:
                    if skip_on_fail:
                        logger.debug(f"[{city}] Omitido: {error}")
                    else:
                        logger.error(f"[{city}] Error: {error}")
                    continue

                city_n = 0
                for raw in records:
                    lead = _normalize_permit(raw, source["field_map"], city)
                    if _is_relevant(lead) and _is_recent(lead):
                        lead.update(self._enrich_gc(lead))
                        all_leads.append(lead)
                        city_n += 1

                logger.info(
                    f"[{city}] {len(records)} registros → "
                    f"{city_n} leads (>${MIN_PERMIT_VALUE/1000:.0f}K, "
                    f"últimos {PERMIT_MONTHS} meses)"
                )

        return all_leads

    def notify(self, lead: dict):
        phone  = lead.get("contact_phone") or "No disponible"
        email  = lead.get("contact_email") or "—"
        source = lead.get("contact_source","")
        value  = lead.get("value_float", 0)

        fields = {
            "📍 Ciudad":           lead.get("city"),
            "🔖 Tipo de Permiso":  lead.get("permit_type"),
            "📝 Descripción":      (lead.get("description") or "")[:200],
            "📊 Estado":           lead.get("status"),
            "📅 Fecha Emisión":    lead.get("issued_date"),
            "👷 Contratista (GC)": lead.get("contractor") or "No especificado",
            "🪪 Licencia CSLB":    lead.get("lic_number") or "—",
            "📞 Teléfono GC":      f"{phone}  _(via {source})_" if source else phone,
            "✉️  Email GC":        email,
            "👤 Propietario":      lead.get("owner") or "—",
            "💰 Valor Estimado":   f"${value:,.0f}" if value else "—",
        }
        if lead.get("contact_source") == "CSLB":
            if lead.get("cslb_city"):
                fields["🏢 Ciudad GC (CSLB)"] = lead["cslb_city"]
            if lead.get("cslb_status"):
                fields["✅ Estado Licencia"]   = lead["cslb_status"]

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields, url=lead.get("permit_url"),
            cta="📲 Contacta al GC y ofrece insulación para el proyecto",
        )
