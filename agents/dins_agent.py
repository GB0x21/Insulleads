"""
agents/dins_agent.py
━━━━━━━━━━━━━━━━━━━━
🔥 Cal Fire DINS — post-wildfire reconstruction leads (California)

Source: Cal Fire Damage Inspection (DINS) program. After every major
wildfire CAL FIRE inspectors door-to-door each parcel inside the fire
perimeter and record:

  - Address, parcel + GPS coordinates
  - Structure type (SFR / MFR / commercial / outbuilding)
  - Damage % bucket (Affected / Minor / Major / Destroyed)
  - Incident name + start date

Datasets are published as ArcGIS REST FeatureServer layers — one per
fire year, plus the running 'DINS_(year)_Public_View' that's the
canonical place. URLs are listed at
https://www.fire.ca.gov/incidents/data and indexed at
https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services.

Why this is gold for insulation lead-gen:
  - Every "Destroyed" / "Major Damage" structure in CA gets rebuilt
    (insurance pays). The address universe is the entire wildfire
    rebuild market — Camp Fire, North Complex, Glass, Dixie, Mosquito,
    Park, etc.
  - Insulation is required by Title 24 on every rebuild, so signal
    intent ≈ 100% per address.
  - The natural follow-up is to cross-reference each DINS address
    against the permits agent (same property → active rebuild permit
    → contractor name). That happens automatically if both agents are
    enabled — `outreach/utils/dedup` consolidates by address.

Env knobs:
  DINS_LAYERS           comma-separated ArcGIS FeatureServer query URLs
                        (defaults to the most-recent fire years; update
                        as Cal Fire publishes new layers)
  DINS_DAMAGE_MIN       'destroyed' | 'major' | 'minor' | 'affected'
                        (default: 'major' — keep ≥26% damage)
  DINS_MAX_RECORDS      max records per layer per fetch (default 1000)
  DINS_TIMEOUT_S        request timeout (default 45)

Soft-fails when ArcGIS is unreachable / a layer URL has been retired —
returns the leads it could fetch and logs the rest.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests

from agents.base import BaseAgent
from utils.telegram import send_lead

logger = logging.getLogger(__name__)


# ─── Configuration ────────────────────────────────────────────────


# Cal Fire publishes one DINS layer per fire year on the
# `services3.arcgis.com/T4QMspbfLg3qTGWY` host. URLs change as years
# roll, so they're env-overridable. Defaults point at recent years
# that are most likely to still be in active rebuild.
_DEFAULT_LAYERS = ",".join([
    # 2025 fires (Park, Borel, Coffee Pot, …)
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/DINS_2025_Public_View/FeatureServer/0",
    # 2024
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/DINS_2024_Public_View/FeatureServer/0",
    # 2023
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/DINS_2023_Public_View/FeatureServer/0",
])

DINS_LAYERS = [u.strip() for u in os.getenv("DINS_LAYERS", _DEFAULT_LAYERS).split(",") if u.strip()]
DINS_TIMEOUT = int(os.getenv("DINS_TIMEOUT_S", "45"))
DINS_MAX = int(os.getenv("DINS_MAX_RECORDS", "1000"))

# Damage bucket ordering — used to filter `>= DINS_DAMAGE_MIN`. Keys
# match the strings Cal Fire uses in the `DAMAGE` field (case-insensitive
# substring match).
_DAMAGE_RANK = {
    "no damage":  0,
    "affected":   1,
    "minor":      2,
    "major":      3,
    "destroyed":  4,
    "inaccessible": 0,  # treat as unknown/skip
}
_DAMAGE_MIN = os.getenv("DINS_DAMAGE_MIN", "major").lower()


def _damage_value(text: str) -> int:
    if not text:
        return 0
    t = text.lower()
    for key, val in _DAMAGE_RANK.items():
        if key in t:
            return val
    return 0


def _damage_threshold() -> int:
    return _DAMAGE_RANK.get(_DAMAGE_MIN, 3)


# ─── Lead normalization ──────────────────────────────────────────


def _join_address(attrs: dict[str, Any]) -> str:
    """Build the street address from ArcGIS attribute fields. Cal Fire
    has shipped the column names slightly differently across years
    (e.g. `STREETNAME` vs `Street_Name` vs `STREET`) so we try a few
    candidates per slot."""
    def _first(*keys: str) -> str:
        for k in keys:
            v = attrs.get(k)
            if v not in (None, "", "0"):
                return str(v).strip()
        return ""

    number = _first("STREETNUMBER", "Street_Number", "STREETNUM", "Streetno")
    name   = _first("STREETNAME", "Street_Name", "STREETNM", "Streetname")
    sfx    = _first("STREETTYPE", "Street_Type", "STREETSUFFIX", "Streetsfx")
    parts = [p for p in (number, name, sfx) if p]
    return " ".join(parts).strip()


def _city(attrs: dict[str, Any]) -> str:
    for k in ("CITY", "City", "Incident_City", "Closest_City"):
        v = attrs.get(k)
        if v:
            return str(v).strip()
    return ""


def _county(attrs: dict[str, Any]) -> str:
    for k in ("COUNTY", "County", "INCIDENT_COUNTY", "Incident_County"):
        v = attrs.get(k)
        if v:
            return str(v).strip()
    return ""


def _incident(attrs: dict[str, Any]) -> str:
    for k in ("INCIDENTNAME", "Incident_Name", "INCIDENT", "Fire_Name"):
        v = attrs.get(k)
        if v:
            return str(v).strip()
    return ""


def _structure_type(attrs: dict[str, Any]) -> str:
    for k in ("STRUCTURETYPE", "Structure_Category", "STRUCTUREC", "Structure_Type"):
        v = attrs.get(k)
        if v:
            return str(v).strip()
    return ""


def _damage(attrs: dict[str, Any]) -> str:
    for k in ("DAMAGE", "Damage", "Damage_Status"):
        v = attrs.get(k)
        if v:
            return str(v).strip()
    return ""


def _normalize(feature: dict[str, Any], layer_url: str) -> dict | None:
    attrs = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    address = _join_address(attrs)
    city = _city(attrs)
    if not address or not city:
        # Cal Fire sometimes ships rows with no street — those are
        # outbuildings on rural parcels and can't drive outreach.
        return None

    damage = _damage(attrs)
    if _damage_value(damage) < _damage_threshold():
        return None

    incident = _incident(attrs)
    county = _county(attrs)
    structure = _structure_type(attrs)
    object_id = attrs.get("OBJECTID") or attrs.get("ObjectID") or attrs.get("FID")

    lat = geom.get("y") or attrs.get("Latitude") or attrs.get("LAT")
    lon = geom.get("x") or attrs.get("Longitude") or attrs.get("LON")

    return {
        "id": f"dins_{layer_url.rsplit('/', 3)[-3]}_{object_id}",
        "address": address,
        "city": city,
        "county": county,
        "incident": incident,
        "damage": damage,
        "structure_type": structure,
        "permit_type": f"Wildfire rebuild — {damage}",
        "description": (
            f"{structure or 'Structure'} damaged in {incident or 'wildfire'} "
            f"({damage}). Active rebuild permit likely on this parcel."
        ),
        "issued_date": "",   # the rebuild permit (if any) lives in permits_agent
        "value_float": 0.0,  # unknown until we cross-reference permits
        "contractor": "",    # not in DINS; enrichment cascade will try to fill
        "owner": "",
        "lic_number": "",
        "latitude": lat,
        "longitude": lon,
        "permit_url": layer_url,
        "_agent_key": "dins",
    }


# ─── HTTP fetch ──────────────────────────────────────────────────


def _fetch_layer(layer_url: str) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": str(DINS_MAX),
        "returnGeometry": "true",
        "outSR": "4326",
    }
    resp = requests.get(
        layer_url + "/query",
        params=params,
        timeout=DINS_TIMEOUT,
        headers={"User-Agent": "Insulleads/dins"},
    )
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    if not isinstance(features, list):
        return []
    return features


# ─── Agent ────────────────────────────────────────────────────────


class DINSAgent(BaseAgent):
    name = "🔥 Cal Fire DINS — Post-Wildfire Rebuilds"
    emoji = "🔥"
    agent_key = "dins"

    def fetch_leads(self) -> list:
        if not DINS_LAYERS:
            logger.info("[dins] DINS_LAYERS empty — skipping")
            return []

        all_leads: list[dict] = []
        for url in DINS_LAYERS:
            try:
                features = _fetch_layer(url)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[dins] %s failed: %s", url, exc)
                continue
            kept = 0
            for feat in features:
                lead = _normalize(feat, url)
                if lead:
                    all_leads.append(lead)
                    kept += 1
            logger.info(
                "[dins] %s → %d features → %d leads (≥%s damage)",
                url.rsplit("/", 3)[-3], len(features), kept, _DAMAGE_MIN,
            )

        logger.info("[dins] %d post-fire rebuild leads total", len(all_leads))
        return all_leads

    def notify(self, lead: dict):
        fields = {
            "📍 Ciudad":       lead.get("city"),
            "🗺️  Condado":     lead.get("county") or "—",
            "🔥 Incendio":     lead.get("incident") or "—",
            "🏚️  Daño":        lead.get("damage") or "—",
            "🏠 Estructura":   lead.get("structure_type") or "—",
            "📝 Nota":         (lead.get("description") or "")[:200],
        }
        cta = (
            "🛠️ Casa siendo reconstruida tras incendio. Title 24 obliga "
            "insulación nueva — contactar permits para identificar al GC."
        )
        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"{lead.get('city')} — {lead.get('address')}",
            fields=fields,
            url=lead.get("permit_url"),
            cta=cta,
        )
