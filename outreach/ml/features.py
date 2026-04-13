"""
Tabular feature extraction for the LightGBM qualifier.

Given a ``Lead`` row, returns a flat dict of ~21 numeric / categorical
features that a gradient-boosted tree can learn from directly. Feature
names are stable so the same extractor can be used at train-time and
inference-time (critical — feature-order drift is the #1 way to silently
break a tree model).

This module is deliberately decoupled from ``outreach.llm`` and from
any vendor SDK: it only reads fields that live on the Lead model, plus
the legacy ``utils.lead_scoring`` components for parity with the
heuristic.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from outreach.models import Lead


# Stable feature order — used as LightGBM column order and in
# feature_importance reports.
FEATURE_ORDER: list[str] = [
    # 1-7 — mirror of the 7 heuristic components (pre-computed points).
    "project_value_pts",
    "project_type_pts",
    "contact_quality_pts",
    "recency_pts",
    "geography_pts",
    "source_type_pts",
    "insulation_signal_pts",
    # 8-13 — raw lead attributes
    "project_value",
    "days_old",
    "has_phone",
    "has_email",
    "has_contractor",
    "has_address",
    # 14-18 — shape / geography
    "title_length",
    "description_length",
    "latitude",
    "longitude",
    "thermal_anomaly_k",
    # 19-21 — cross-model features
    "qualification_score_gp",
    "lead_score_heuristic",
    "source_kind_hash",
]

CATEGORICAL_FEATURES: list[str] = ["source_kind_hash"]


# ─── Heuristic decomposition ─────────────────────────────────────────
# We want the 7 points values that `utils.lead_scoring.score_lead`
# computes internally, but exposed as individual features (the heuristic
# collapses them into a single 0-100 number). Re-derive them here with
# the same weights so LightGBM can learn non-linear combinations.

from utils.lead_scoring import (  # noqa: E402
    _HIGH_INTENT_KEYWORDS,
    _LOW_INTENT_KEYWORDS,
    _MEDIUM_INTENT_KEYWORDS,
)


_CITY_TIER: dict[str, int] = {
    "san francisco": 10, "oakland": 9, "berkeley": 9,
    "richmond": 8, "san jose": 7, "hayward": 7,
    "alameda": 8, "emeryville": 8, "vallejo": 8,
    "concord": 7, "martinez": 7, "pittsburg": 7, "antioch": 7,
    "daly city": 7, "albany": 7, "el cerrito": 7, "san leandro": 7,
    "fremont": 6, "walnut creek": 6, "pleasant hill": 6,
    "fairfield": 6, "napa": 6, "san rafael": 6, "san mateo": 6,
    "san bruno": 6, "hercules": 6, "pinole": 6, "castro valley": 6,
    "san lorenzo": 6, "south san francisco": 6,
}

_SOURCE_TIER: dict[str, int] = {
    "permits": 10, "construction": 10, "deconstruction": 9,
    "realestate": 9, "solar": 8, "energy": 7,
    "rodents": 6, "places": 5, "yelp": 4, "flood": 5,
    "thermal": 10,
}


def _project_value_pts(value: float) -> int:
    if value >= 500_000:
        return 20
    if value >= 200_000:
        return 15
    if value >= 100_000:
        return 12
    if value >= 50_000:
        return 8
    if value > 0:
        return 4
    return 0


def _project_type_pts(text: str) -> int:
    text = text.lower()
    if any(kw in text for kw in _HIGH_INTENT_KEYWORDS):
        return 15
    if any(kw in text for kw in _MEDIUM_INTENT_KEYWORDS):
        return 10
    if any(kw in text for kw in _LOW_INTENT_KEYWORDS):
        return 3
    return 5


def _contact_quality_pts(
    phone: bool, email: bool, company: bool, name: bool
) -> int:
    pts = 0
    if phone:
        pts += 8
    if email:
        pts += 6
    if company:
        pts += 4
    if name:
        pts += 2
    return min(pts, 20)


def _recency_pts(days_old: int) -> int:
    if days_old <= 7:
        return 15
    if days_old <= 14:
        return 12
    if days_old <= 30:
        return 9
    if days_old <= 60:
        return 5
    return 2


def _geography_pts(city: str) -> int:
    return _CITY_TIER.get(city.lower().strip(), 4)


def _source_type_pts(kind: str) -> int:
    return _SOURCE_TIER.get((kind or "").lower(), 5)


# ─── Public API ─────────────────────────────────────────────────────
def extract_features(lead: "Lead") -> dict[str, Any]:
    """Return a feature dict keyed by ``FEATURE_ORDER``.

    All values are native Python scalars so the caller can feed them
    straight into a ``pandas.DataFrame`` or a plain ``dict``-of-lists.
    """
    project_value = float(lead.project_value or 0)
    desc = f"{lead.title or ''} {lead.description or ''} {lead.project_type or ''}"

    days_old = 0
    if lead.created_at:
        delta = datetime.now(timezone.utc) - lead.created_at
        days_old = max(0, delta.days)

    has_phone = bool(lead.contact_phone)
    has_email = bool(lead.contact_email)
    has_contractor = bool(lead.contact_company)
    has_address = bool(lead.address)

    kind = getattr(lead.source, "kind", "") or ""

    feats: dict[str, Any] = {
        "project_value_pts": _project_value_pts(project_value),
        "project_type_pts": _project_type_pts(desc),
        "contact_quality_pts": _contact_quality_pts(
            has_phone, has_email, has_contractor, bool(lead.contact_name)
        ),
        "recency_pts": _recency_pts(days_old),
        "geography_pts": _geography_pts(lead.city or ""),
        "source_type_pts": _source_type_pts(kind),
        "insulation_signal_pts": _insulation_signal_pts(lead),
        "project_value": project_value,
        "days_old": days_old,
        "has_phone": int(has_phone),
        "has_email": int(has_email),
        "has_contractor": int(has_contractor),
        "has_address": int(has_address),
        "title_length": len(lead.title or ""),
        "description_length": len(lead.description or ""),
        "latitude": float(lead.latitude) if lead.latitude is not None else 0.0,
        "longitude": float(lead.longitude) if lead.longitude is not None else 0.0,
        "thermal_anomaly_k": float(getattr(lead, "thermal_anomaly_k", 0.0) or 0.0),
        "qualification_score_gp": float(lead.qualification_score or 0.0),
        "lead_score_heuristic": int(lead.lead_score or 0),
        # LightGBM handles hashed categoricals better than raw strings.
        "source_kind_hash": hash(kind) % 10_000,
    }
    return feats


def _insulation_signal_pts(lead: "Lead") -> int:
    """Mirror of the insulation_signal branch in utils.lead_scoring."""
    raw = lead.raw or {}
    score = 0
    pest = str(raw.get("pest_type", "")).lower()
    if pest in ("rodent", "termite"):
        score += 8
    elif pest == "wildlife":
        score += 6
    if raw.get("solar_potential"):
        score += 4
    energy = raw.get("energy_score")
    if isinstance(energy, (int, float)) and energy < 50:
        score += 6
    return min(score, 10)


def to_row(feats: dict[str, Any]) -> list[Any]:
    """Project a feature dict onto the stable column order."""
    return [feats[name] for name in FEATURE_ORDER]
