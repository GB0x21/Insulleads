"""
agents/energy_agent.py
━━━━━━━━━━━━━━━━━━━━━━
⚡ Eficiencia Energética — Bay Area

Fuentes gratuitas:
  1. DOE Better Buildings (Open Data) — auditorías energéticas
  2. BayREN (Bay Area Regional Energy Network) — datos de rebates
  3. SF Energy Benchmarking (Socrata) — edificios con bajo rendimiento
  4. ENERGY STAR Portfolio Manager — edificios certificados (o no)

Lógica: Edificios con alta emisión / bajo rating energético = oportunidad
de vender insulación como mejora de eficiencia.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact
from utils.lead_scoring import score_lead, format_score_line
from utils.notifications import notify_multichannel

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT   = int(os.getenv("SOURCE_TIMEOUT", "45"))
ENERGY_MONTHS    = int(os.getenv("ENERGY_MONTHS", "6"))
PARALLEL_ENERGY  = int(os.getenv("PARALLEL_ENERGY", "4"))


def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * ENERGY_MONTHS)).strftime("%Y-%m-%dT00:00:00")


ENERGY_SOURCES = [
    # ── SF Energy Benchmarking — edificios comerciales ───────────
    # Dataset público: rendimiento energético de edificios >10k sqft
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/j2j3-acqj.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "latest_benchmark_year DESC",
            "$where": (
                "energy_star_score IS NOT NULL "
                "AND energy_star_score < 50"
            ),
        },
        "field_map": {
            "id":           "building_id",
            "address":      "building_address",
            "name":         "building_name",
            "type":         "primary_property_type",
            "sqft":         "floor_area",
            "year_built":   "year_built",
            "energy_score": "energy_star_score",
            "emissions":    "total_ghg_emissions_metric_tons_co2e",
            "eui":          "weather_normalized_site_eui_kbtu_ft",
        },
    },
    # ── SF Solar Permits with Energy Audit ───────────────────────
    # Propiedades que pidieron auditoría energética
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/i98e-djp9.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 50,
            "$order": "filed_date DESC",
            "$where": (
                "filed_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%ENERGY AUDIT%' "
                "OR UPPER(description) LIKE '%ENERGY RETROFIT%' "
                "OR UPPER(description) LIKE '%WEATHERIZATION%' "
                "OR UPPER(description) LIKE '%TITLE 24%' "
                "OR UPPER(description) LIKE '%ENERGY COMPLIANCE%')"
            ),
        },
        "field_map": {
            "id":        "permit_number",
            "address":   "street_number",
            "address2":  "street_name",
            "name":      "description",
            "type":      "permit_type_definition",
            "year_built": None,
            "energy_score": None,
            "owner":     "owner",
        },
        "_is_permit": True,
    },
    # ── Oakland Energy Benchmarking ─────────────────────────────
    {
        "city":    "Oakland",
        "engine":  "socrata",
        "url":     "https://data.oaklandca.gov/resource/energy-benchmarking.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "year DESC",
            "$where": (
                "energy_star_score IS NOT NULL "
                "AND energy_star_score < 50"
            ),
        },
        "field_map": {
            "id":           "building_id",
            "address":      "address",
            "name":         "building_name",
            "type":         "property_type",
            "sqft":         "floor_area",
            "year_built":   "year_built",
            "energy_score": "energy_star_score",
            "emissions":    "total_ghg_emissions",
            "eui":          "site_eui",
        },
    },
    # ── San Jose Energy Permits — CKAN ──────────────────────────
    {
        "city":    "San Jose",
        "engine":  "socrata",
        "url":     "https://data.sanjoseca.gov/resource/energy-benchmarking.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$where": (
                "energy_star_score IS NOT NULL "
                "AND energy_star_score < 50"
            ),
        },
        "field_map": {
            "id":           "building_id",
            "address":      "address",
            "name":         "building_name",
            "type":         "property_type",
            "sqft":         "floor_area",
            "year_built":   "year_built",
            "energy_score": "energy_star_score",
            "emissions":    "total_ghg_emissions",
            "eui":          "site_eui",
        },
    },
    # ── Berkeley Energy Permits ─────────────────────────────────
    {
        "city":    "Berkeley",
        "engine":  "socrata",
        "url":     "https://data.cityofberkeley.info/resource/k92i-t48y.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 50,
            "$order": "issue_date DESC",
            "$where": (
                "issue_date >= '{cutoff_iso}' "
                "AND (UPPER(project_description) LIKE '%ENERGY AUDIT%' "
                "OR UPPER(project_description) LIKE '%WEATHERIZATION%' "
                "OR UPPER(project_description) LIKE '%TITLE 24%' "
                "OR UPPER(project_description) LIKE '%ENERGY RETROFIT%')"
            ),
        },
        "field_map": {
            "id":        "record_number",
            "address":   "address",
            "name":      "project_description",
            "type":      "record_type",
            "year_built": None,
            "energy_score": None,
            "owner":     "owner",
        },
        "_is_permit": True,
    },
    # ── Contra Costa County Energy Permits ──────────────────────
    {
        "city":    "Contra Costa County",
        "engine":  "socrata",
        "url":     "https://data.contracosta.gov/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 50,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%ENERGY AUDIT%' "
                "OR UPPER(description) LIKE '%WEATHERIZATION%' "
                "OR UPPER(description) LIKE '%TITLE 24%' "
                "OR UPPER(description) LIKE '%ENERGY RETROFIT%' "
                "OR UPPER(description) LIKE '%INSULATION%')"
            ),
        },
        "field_map": {
            "id":        "permit_number",
            "address":   "address",
            "name":      "description",
            "type":      "permit_type",
            "year_built": None,
            "energy_score": None,
            "owner":     "owner",
        },
        "_is_permit": True,
    },
    # ── Alameda County Energy Permits ───────────────────────────
    {
        "city":    "Alameda County",
        "engine":  "socrata",
        "url":     "https://data.acgov.org/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 50,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%ENERGY AUDIT%' "
                "OR UPPER(description) LIKE '%WEATHERIZATION%' "
                "OR UPPER(description) LIKE '%TITLE 24%' "
                "OR UPPER(description) LIKE '%ENERGY RETROFIT%' "
                "OR UPPER(description) LIKE '%INSULATION%')"
            ),
        },
        "field_map": {
            "id":        "permit_number",
            "address":   "address",
            "name":      "description",
            "type":      "permit_type",
            "year_built": None,
            "energy_score": None,
            "owner":     "owner",
        },
        "_is_permit": True,
    },
    # ── Solano County Energy Permits ────────────────────────────
    {
        "city":    "Solano County",
        "engine":  "socrata",
        "url":     "https://data.solanocounty.com/resource/building-permits.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 50,
            "$order": "issued_date DESC",
            "$where": (
                "issued_date >= '{cutoff_iso}' "
                "AND (UPPER(description) LIKE '%ENERGY AUDIT%' "
                "OR UPPER(description) LIKE '%WEATHERIZATION%' "
                "OR UPPER(description) LIKE '%TITLE 24%' "
                "OR UPPER(description) LIKE '%ENERGY RETROFIT%' "
                "OR UPPER(description) LIKE '%INSULATION%')"
            ),
        },
        "field_map": {
            "id":        "permit_number",
            "address":   "address",
            "name":      "description",
            "type":      "permit_type",
            "year_built": None,
            "energy_score": None,
            "owner":     "owner",
        },
        "_is_permit": True,
    },
]


def _fetch_socrata(source: dict) -> list:
    cutoff_iso = _cutoff_iso()
    params = {
        k: v.replace("{cutoff_iso}", cutoff_iso) if isinstance(v, str) else v
        for k, v in source["params"].items()
    }
    token = os.getenv("SOCRATA_APP_TOKEN", "")
    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token
    resp = requests.get(source["url"], params=params,
                        timeout=source.get("timeout", 30), headers=headers)
    if resp.status_code == 400:
        logger.warning(f"[Energy/{source['city']}] 400 Bad Request")
        return []
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_source(source: dict) -> tuple[str, list]:
    records = _fetch_socrata(source)
    return source["city"], records


class EnergyAgent(BaseAgent):
    name      = "⚡ Eficiencia Energética — Bay Area"
    emoji     = "⚡"
    agent_key = "energy"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        leads = []

        with ThreadPoolExecutor(max_workers=PARALLEL_ENERGY) as executor:
            futures = {
                executor.submit(_fetch_source, src): src
                for src in ENERGY_SOURCES
            }
            for fut in as_completed(futures):
                src = futures[fut]
                try:
                    city, records = fut.result()
                    fm = src["field_map"]
                    get = lambda r, k, _fm=fm: r.get(_fm.get(k) or "", "") or ""

                    for raw in records:
                        addr = get(raw, "address")
                        if fm.get("address2") and raw.get(fm["address2"]):
                            addr = f"{addr} {raw[fm['address2']]}".strip()

                        energy_score = 0
                        try:
                            energy_score = int(float(get(raw, "energy_score") or 0))
                        except (ValueError, TypeError):
                            pass

                        year_built = 0
                        try:
                            year_built = int(get(raw, "year_built") or 0)
                        except (ValueError, TypeError):
                            pass

                        eui = 0
                        try:
                            eui = float(get(raw, "eui") or 0)
                        except (ValueError, TypeError):
                            pass

                        # Rating de eficiencia
                        if energy_score > 0:
                            if energy_score < 25:
                                efficiency = "🔴 MUY BAJA"
                            elif energy_score < 50:
                                efficiency = "🟡 BAJA"
                            elif energy_score < 75:
                                efficiency = "🟢 MEDIA"
                            else:
                                efficiency = "⭐ ALTA"
                        else:
                            efficiency = "📋 Auditoría solicitada"

                        lead = {
                            "id":            f"{city}_energy_{get(raw,'id')}",
                            "city":          city,
                            "address":       addr,
                            "building_name": get(raw, "name"),
                            "building_type": get(raw, "type"),
                            "description":   get(raw, "name") or get(raw, "type"),
                            "year_built":    year_built if year_built > 1900 else None,
                            "energy_score":  energy_score,
                            "efficiency":    efficiency,
                            "eui":           eui,
                            "sqft":          get(raw, "sqft"),
                            "emissions":     get(raw, "emissions"),
                            "owner":         get(raw, "owner") or "",
                            "_agent_key":    "energy",
                        }

                        # Enriquecer contacto
                        search_name = lead.get("building_name") or lead.get("owner")
                        if search_name:
                            match = lookup_contact(search_name, self._contacts)
                            if match:
                                lead["contact_phone"]  = match.get("phone", "")
                                lead["contact_email"]  = match.get("email", "")
                                lead["contact_source"] = f"CSV ({match['source']})"

                        # Lead scoring
                        scoring = score_lead(lead)
                        lead["_scoring"] = scoring

                        leads.append(lead)

                    logger.info(f"[Energy/{city}] {len(records)} edificios procesados")

                except Exception as e:
                    logger.error(f"[Energy/{src['city']}] Error: {e}")

        # Ordenar por energy_score ascendente (peor eficiencia primero)
        leads.sort(key=lambda l: l.get("energy_score", 100))
        return leads

    def notify(self, lead: dict):
        scoring = lead.get("_scoring", {})
        score_line = format_score_line(scoring) if scoring else ""

        fields = {
            "📍 Ciudad":            lead.get("city"),
            "🏢 Edificio":         lead.get("building_name") or "—",
            "🏗️ Tipo":             lead.get("building_type") or "—",
        }

        if lead.get("year_built"):
            age = datetime.now().year - lead["year_built"]
            fields["📅 Antigüedad"] = f"{age} años (construido {lead['year_built']})"

        fields["⚡ Eficiencia"] = lead.get("efficiency", "—")

        if lead.get("energy_score"):
            fields["📊 Energy Star Score"] = f"{lead['energy_score']}/100"

        if lead.get("eui"):
            fields["🔥 EUI"] = f"{lead['eui']:.1f} kBTU/ft²"

        if lead.get("sqft"):
            fields["📐 Superficie"] = f"{lead['sqft']} sqft"

        if lead.get("emissions"):
            fields["🌍 Emisiones CO₂"] = f"{lead['emissions']} ton/año"

        if lead.get("owner"):
            fields["👤 Propietario"] = lead["owner"]

        if lead.get("contact_phone"):
            fields["📞 Teléfono"] = lead["contact_phone"]
        if lead.get("contact_email"):
            fields["✉️  Email"] = lead["contact_email"]

        if score_line:
            fields["🎯 Lead Score"] = score_line

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta="⚡ Baja eficiencia = oportunidad de insulación. ¡Contacta al propietario!",
        )

        notify_multichannel(lead, scoring)
