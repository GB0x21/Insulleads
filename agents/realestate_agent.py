"""
agents/realestate_agent.py
━━━━━━━━━━━━━━━━━━━━━━━━━━
🏠 Propiedades Recién Vendidas — Bay Area

Fuentes gratuitas:
  1. SF Assessor-Recorder (Socrata) — transacciones de propiedad
  2. San Jose Property Sales (CKAN)
  3. Oakland/Alameda County sales (Socrata)

Lógica: Casa recién vendida + antigüedad > 20 años = nuevo propietario
que probablemente renovará, incluyendo insulación.
"""

import os
import re
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

SOURCE_TIMEOUT  = int(os.getenv("SOURCE_TIMEOUT", "45"))
SALE_MONTHS     = int(os.getenv("SALE_MONTHS", "2"))
MIN_SALE_PRICE  = float(os.getenv("MIN_SALE_PRICE", "400000"))
PARALLEL_RE     = int(os.getenv("PARALLEL_REALESTATE", "4"))


def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * SALE_MONTHS)).strftime("%Y-%m-%dT00:00:00")

def _parse_value(v) -> float:
    try:
        return float(re.sub(r"[^\d.]", "", str(v) or "") or "0")
    except Exception:
        return 0.0


REALESTATE_SOURCES = [
    # ── SF Assessor — Ventas de propiedades ──────────────────────
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/5gah-bvex.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "recording_date DESC",
            "$where": (
                "recording_date >= '{cutoff_iso}' "
                "AND document_type = 'DEED' "
                "AND consideration_amount > 400000"
            ),
        },
        "field_map": {
            "id":       "document_id",
            "address":  "property_address",
            "date":     "recording_date",
            "price":    "consideration_amount",
            "buyer":    "grantee",
            "seller":   "grantor",
            "doc_type": "document_type",
        },
    },
    # ── Alameda County (Oakland, Berkeley, Fremont, Hayward) ─────
    {
        "city":    "Alameda County",
        "engine":  "socrata",
        "url":     "https://data.acgov.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 400000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "situs_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "doc_type": "document_type",
            "year_built": "year_built",
        },
        "_skip_if_no_data": True,
    },
    # ── Santa Clara County (San Jose, Sunnyvale, Santa Clara) ────
    {
        "city":    "Santa Clara County",
        "engine":  "socrata",
        "url":     "https://data.sccgov.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_amount > 400000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_amount",
            "buyer":    "buyer",
            "seller":   "seller",
            "year_built": "year_built",
        },
        "_skip_if_no_data": True,
    },
    # ── Contra Costa County ─────────────────────────────────────
    {
        "city":    "Contra Costa County",
        "engine":  "socrata",
        "url":     "https://data.contracosta.gov/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 400000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── San Mateo County ────────────────────────────────────────
    {
        "city":    "San Mateo County",
        "engine":  "socrata",
        "url":     "https://data.smcgov.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 400000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── Solano County ───────────────────────────────────────────
    {
        "city":    "Solano County",
        "engine":  "socrata",
        "url":     "https://data.solanocounty.com/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 300000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── Marin County ────────────────────────────────────────────
    {
        "city":    "Marin County",
        "engine":  "socrata",
        "url":     "https://data.marincounty.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 400000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── Napa County ─────────────────────────────────────────────
    {
        "city":    "Napa County",
        "engine":  "socrata",
        "url":     "https://data.countyofnapa.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 300000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── Sonoma County ───────────────────────────────────────────
    {
        "city":    "Sonoma County",
        "engine":  "socrata",
        "url":     "https://data.sonomacounty.ca.gov/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 300000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
    },
    # ── San Joaquin County ──────────────────────────────────────
    {
        "city":    "San Joaquin County",
        "engine":  "socrata",
        "url":     "https://data.sjgov.org/resource/property-sales.json",
        "timeout": SOURCE_TIMEOUT,
        "_skip_if_no_data": True,
        "params": {
            "$limit": 100,
            "$order": "sale_date DESC",
            "$where": (
                "sale_date >= '{cutoff_iso}' "
                "AND sale_price > 250000"
            ),
        },
        "field_map": {
            "id":       "apn",
            "address":  "property_address",
            "date":     "sale_date",
            "price":    "sale_price",
            "buyer":    "buyer_name",
            "seller":   "seller_name",
            "year_built": "year_built",
        },
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
        logger.warning(f"[RealEstate/{source['city']}] 400 — dataset puede no existir")
        return []
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_source(source: dict) -> tuple[str, list]:
    records = _fetch_socrata(source)
    return source["city"], records


class RealEstateAgent(BaseAgent):
    name      = "🏠 Propiedades Recién Vendidas — Bay Area"
    emoji     = "🏠"
    agent_key = "realestate"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        leads = []

        with ThreadPoolExecutor(max_workers=PARALLEL_RE) as executor:
            futures = {
                executor.submit(_fetch_source, src): src
                for src in REALESTATE_SOURCES
            }
            for fut in as_completed(futures):
                src = futures[fut]
                try:
                    city, records = fut.result()
                    fm = src["field_map"]
                    get = lambda r, k, _fm=fm: r.get(_fm.get(k) or "", "") or ""

                    for raw in records:
                        price = _parse_value(get(raw, "price"))
                        if price < MIN_SALE_PRICE:
                            continue

                        year_built = 0
                        if fm.get("year_built"):
                            try:
                                year_built = int(get(raw, "year_built") or 0)
                            except (ValueError, TypeError):
                                pass

                        # Calcular antigüedad — casas viejas = mejor lead
                        age = (datetime.now().year - year_built) if year_built > 1900 else 0
                        insulation_priority = (
                            "ALTA" if age > 40 else
                            "MEDIA" if age > 20 else
                            "BAJA" if age > 0 else "DESCONOCIDA"
                        )

                        lead = {
                            "id":          f"{city}_{get(raw,'id')}",
                            "city":        city,
                            "address":     get(raw, "address"),
                            "description": f"Venta de propiedad — {get(raw, 'doc_type') or 'DEED'}",
                            "date":        get(raw, "date")[:10] if get(raw, "date") else "",
                            "value":       str(price),
                            "value_float": price,
                            "buyer":       get(raw, "buyer"),
                            "seller":      get(raw, "seller"),
                            "year_built":  year_built if year_built > 1900 else None,
                            "property_age": age if age > 0 else None,
                            "insulation_priority": insulation_priority,
                            "_agent_key":  "realestate",
                        }

                        # Buscar contacto del comprador en CSV
                        if lead["buyer"]:
                            match = lookup_contact(lead["buyer"], self._contacts)
                            if match:
                                lead["contact_phone"]  = match.get("phone", "")
                                lead["contact_email"]  = match.get("email", "")
                                lead["contact_source"] = f"CSV ({match['source']})"

                        # Lead scoring
                        scoring = score_lead(lead)
                        lead["_scoring"] = scoring

                        leads.append(lead)

                    logger.info(f"[RealEstate/{city}] {len(records)} ventas encontradas")

                except Exception as e:
                    if not src.get("_skip_if_no_data"):
                        logger.error(f"[RealEstate/{src['city']}] Error: {e}")
                    else:
                        logger.debug(f"[RealEstate/{src['city']}] {e}")

        # Ordenar por precio (mayor = mejor oportunidad)
        leads.sort(key=lambda l: l.get("value_float", 0), reverse=True)
        return leads

    def notify(self, lead: dict):
        scoring = lead.get("_scoring", {})
        score_line = format_score_line(scoring) if scoring else ""

        price = lead.get("value_float", 0)
        age = lead.get("property_age")
        priority = lead.get("insulation_priority", "")

        fields = {
            "📍 Zona":               lead.get("city"),
            "📅 Fecha Venta":        lead.get("date"),
            "💰 Precio":             f"${price:,.0f}" if price else "—",
            "👤 Comprador":          lead.get("buyer") or "—",
            "👥 Vendedor":           lead.get("seller") or "—",
        }

        if age:
            fields["🏗️ Antigüedad"] = f"{age} años (construida {lead['year_built']})"

        fields["🔥 Prioridad Insulación"] = priority

        if lead.get("contact_phone"):
            src = lead.get("contact_source", "")
            fields["📞 Teléfono"] = (
                f"{lead['contact_phone']}  _(via {src})_" if src
                else lead["contact_phone"]
            )
        if lead.get("contact_email"):
            fields["✉️  Email"] = lead["contact_email"]

        if score_line:
            fields["📊 Lead Score"] = score_line

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            cta="🏠 Nuevo propietario = oportunidad de insulación. ¡Contacta antes de la renovación!",
        )

        # Notificación multi-canal para leads calientes
        notify_multichannel(lead, scoring)
