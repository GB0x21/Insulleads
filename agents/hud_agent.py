"""
agents/hud_agent.py
━━━━━━━━━━━━━━━━━━━
🏘️ HUD multifamily / LIHTC properties — California

Source: HUD's open ArcGIS FeatureServer hosting the LIHTC (Low-Income
Housing Tax Credit) dataset and the Multifamily Properties — Assisted
dataset. Both are public, no auth.

Why this matters for insulation lead-gen:
  - LIHTC and HUD-assisted multifamily properties have funded
    rehabilitation cycles every 15-30 years; a single conversation
    with the property owner / sponsor = one large insulation job
    spanning 50-500 units, vs the 1-unit jobs from SFR permits.
  - LIHTC `placed_in_service_year` ≥ ~25 years ago is a strong signal
    that the property is in the rehab window today.
  - HUD's data ships with the sponsor / owner contact org, so the
    enrichment cascade has a real entity to look up.

Endpoints (HUD's ArcGIS Open Data, env-overridable):

  HUD_LIHTC_URL         FeatureServer for LIHTC properties
  HUD_MULTIFAMILY_URL   FeatureServer for HUD-assisted multifamily

Filters (env-overridable):
  HUD_STATE             two-letter state filter, default 'CA'
  HUD_MIN_UNITS         drop properties with fewer units (default 8)
  HUD_REHAB_WINDOW_YRS  consider LIHTC placed-in-service ≥ N yrs ago a
                        rehab candidate (default 20)
  HUD_MAX_RECORDS       per-layer cap (default 2000)

Soft-fails when HUD is unreachable / a layer URL changes.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)


HUD_LIHTC_URL = os.getenv(
    "HUD_LIHTC_URL",
    "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/LIHTC/FeatureServer/0",
)
HUD_MULTIFAMILY_URL = os.getenv(
    "HUD_MULTIFAMILY_URL",
    "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/Multifamily_Properties_Assisted/FeatureServer/0",
)
HUD_STATE = os.getenv("HUD_STATE", "CA").upper()
HUD_MIN_UNITS = int(os.getenv("HUD_MIN_UNITS", "8"))
HUD_REHAB_WINDOW = int(os.getenv("HUD_REHAB_WINDOW_YRS", "20"))
HUD_MAX_RECORDS = int(os.getenv("HUD_MAX_RECORDS", "2000"))
HUD_TIMEOUT = int(os.getenv("HUD_TIMEOUT_S", "60"))


# ─── Field plucking ───────────────────────────────────────────────


def _first(attrs: dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = attrs.get(k)
        if v not in (None, "", 0):
            return str(v).strip()
    return ""


def _int(attrs: dict[str, Any], *keys: str) -> int:
    for k in keys:
        v = attrs.get(k)
        if v in (None, "", 0):
            continue
        try:
            return int(float(v))
        except (TypeError, ValueError):
            continue
    return 0


# ─── Normalization ────────────────────────────────────────────────


def _normalize_lihtc(attrs: dict[str, Any], geom: dict[str, Any]) -> dict | None:
    state = _first(attrs, "PROJ_ST", "STATE", "St", "State_Code").upper()
    if HUD_STATE and state != HUD_STATE:
        return None

    name = _first(attrs, "PROJECT", "PROJECT_NAME", "Project_Name", "PROJ_NAME")
    address = _first(attrs, "PROJ_ADD", "PROJECT_ADDRESS", "Address", "Site_Address")
    city = _first(attrs, "PROJ_CTY", "CITY", "City", "Project_City")
    if not (name or address) or not city:
        return None

    units = _int(attrs, "N_UNITS", "TOTAL_UNITS", "UNITS", "Total_Units")
    if HUD_MIN_UNITS and units and units < HUD_MIN_UNITS:
        return None

    placed = _int(attrs, "YR_PIS", "PLACED_IN_SERVICE", "Placed_In_Service_Year",
                  "YEAR_PUT_INTO_SERVICE")
    sponsor = _first(attrs, "PROJ_SPONSOR", "SPONSOR", "Owner", "Owner_Name",
                     "Project_Sponsor")
    object_id = attrs.get("OBJECTID") or attrs.get("ObjectID") or attrs.get("FID")

    age = (datetime.utcnow().year - placed) if placed else 0
    in_rehab_window = placed and age >= HUD_REHAB_WINDOW

    return {
        "id": f"hud_lihtc_{object_id}",
        "address": address or name,
        "city": city,
        "state": state,
        "permit_type": "LIHTC multifamily",
        "description": (
            f"{name or 'LIHTC property'} — {units or '?'} units"
            + (f", placed in service {placed}" if placed else "")
            + (f" ({age} yrs — rehab window)" if in_rehab_window else "")
            + "."
        ),
        "issued_date": str(placed) if placed else "",
        "value_float": float(units * 50_000) if units else 0.0,
        "contractor": "",
        "owner": sponsor,
        "lic_number": "",
        "units": units,
        "placed_in_service": placed,
        "rehab_candidate": bool(in_rehab_window),
        "permit_url": HUD_LIHTC_URL,
        "_agent_key": "hud_multifamily",
    }


def _normalize_assisted(attrs: dict[str, Any], geom: dict[str, Any]) -> dict | None:
    state = _first(attrs, "STATE", "STATE_CODE", "State", "St").upper()
    if HUD_STATE and state != HUD_STATE:
        return None

    name = _first(attrs, "PROPERTY_NAME_TEXT", "PROPERTY_NAME", "Property_Name", "NAME")
    address = _first(attrs, "STD_ADDR", "ADDRESS", "Address", "PROPERTY_STREET_ADDRESS")
    city = _first(attrs, "STD_CITY", "CITY", "City", "PROPERTY_CITY")
    if not (name or address) or not city:
        return None

    units = _int(attrs, "TOTAL_ASSISTED_UNIT_COUNT", "TOTAL_UNITS", "UNITS",
                 "ASSISTED_UNITS")
    if HUD_MIN_UNITS and units and units < HUD_MIN_UNITS:
        return None

    owner = _first(attrs, "OWNER_ORG_NAME", "OWNER_NAME", "Owner_Name",
                   "OWNER_COMPANY_NAME")
    program = _first(attrs, "ASSISTANCE_PROGRAM", "PROGRAM_LABEL", "Program")
    object_id = attrs.get("OBJECTID") or attrs.get("ObjectID") or attrs.get("FID")

    return {
        "id": f"hud_mf_{object_id}",
        "address": address or name,
        "city": city,
        "state": state,
        "permit_type": "HUD-assisted multifamily",
        "description": (
            f"{name or 'HUD-assisted property'} — {units or '?'} units"
            + (f" ({program})" if program else "")
            + "."
        ),
        "issued_date": "",
        "value_float": float(units * 50_000) if units else 0.0,
        "contractor": "",
        "owner": owner,
        "lic_number": "",
        "units": units,
        "program": program,
        "permit_url": HUD_MULTIFAMILY_URL,
        "_agent_key": "hud_multifamily",
    }


# ─── HTTP fetch ──────────────────────────────────────────────────


def _fetch(url: str, where: str = "1=1") -> list[dict]:
    params = {
        "where": where,
        "outFields": "*",
        "f": "json",
        "resultRecordCount": str(HUD_MAX_RECORDS),
        "returnGeometry": "false",
    }
    resp = requests.get(
        url + "/query",
        params=params,
        timeout=HUD_TIMEOUT,
        headers={"User-Agent": "Insulleads/hud"},
    )
    resp.raise_for_status()
    data = resp.json()
    feats = data.get("features", [])
    return feats if isinstance(feats, list) else []


# ─── Agent ────────────────────────────────────────────────────────


class HUDMultifamilyAgent(BaseAgent):
    name = "🏘️ HUD Multifamily / LIHTC"
    emoji = "🏘️"
    agent_key = "hud_multifamily"

    def fetch_leads(self) -> list:
        leads: list[dict] = []

        # LIHTC — server-side state filter shaves payload.
        lihtc_where = f"PROJ_ST = '{HUD_STATE}'" if HUD_STATE else "1=1"
        try:
            features = _fetch(HUD_LIHTC_URL, where=lihtc_where)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[hud-lihtc] fetch failed: %s", exc)
            features = []
        kept = 0
        for feat in features:
            lead = _normalize_lihtc(feat.get("attributes") or {},
                                    feat.get("geometry") or {})
            if lead:
                leads.append(lead)
                kept += 1
        logger.info("[hud-lihtc] %d features → %d leads (state=%s, ≥%d units)",
                    len(features), kept, HUD_STATE, HUD_MIN_UNITS)

        # HUD-assisted multifamily.
        assisted_where = f"STATE = '{HUD_STATE}'" if HUD_STATE else "1=1"
        try:
            features = _fetch(HUD_MULTIFAMILY_URL, where=assisted_where)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[hud-mf] fetch failed: %s", exc)
            features = []
        kept = 0
        for feat in features:
            lead = _normalize_assisted(feat.get("attributes") or {},
                                       feat.get("geometry") or {})
            if lead:
                leads.append(lead)
                kept += 1
        logger.info("[hud-mf] %d features → %d leads", len(features), kept)

        # Sort so rehab-window LIHTC properties bubble up first.
        leads.sort(
            key=lambda l: (
                0 if l.get("rehab_candidate") else 1,
                -int(l.get("units") or 0),
            )
        )
        return leads

    def notify(self, lead: dict):
        units = lead.get("units") or 0
        fields = {
            "📍 Ciudad":       lead.get("city"),
            "🏘️  Programa":    lead.get("permit_type") or "—",
            "🏠 Unidades":     str(units) if units else "—",
            "👤 Sponsor":      lead.get("owner") or "—",
            "📝 Nota":         (lead.get("description") or "")[:200],
        }
        if lead.get("placed_in_service"):
            fields["📅 Placed in Service"] = str(lead["placed_in_service"])
        if lead.get("rehab_candidate"):
            fields["🛠️  Rehab Candidate"] = "Sí (en ventana de rehab)"
        cta = (
            "🛠️ Multifamiliar HUD/LIHTC. Una conversación = decenas/cientos "
            "de unidades. Contactar al sponsor para ofrecer insulación."
        )
        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields,
            url=lead.get("permit_url"),
            cta=cta,
        )
