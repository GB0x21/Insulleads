"""
Lead-reachability scoring.

`contact_quality_score(lead)` returns 0-4 based on how many independent
channels we have to reach the contractor. Used by the outreach loop and
the legacy permits agent to prioritize sending leads we can actually
act on, and surfaced in the Telegram message so the operator sees the
reachability at a glance.

The function deliberately accepts both a Django ``Lead`` instance (the
pipeline path) and a plain dict (the legacy ``agents/`` path) — duck
typing keeps both call sites identical.

Channels and weights:

  +1  contact_phone
  +1  contact_email
  +1  website        (top-level on the dict path; under
                      ``Lead.enrichment_log['website']`` on the Django path)
  +1  linkedin       (same dual lookup as website)

The weights are intentionally flat (1 each). A separate "qualification"
axis already captures conversion likelihood — quality only measures
"can we reach them", which is binary per channel.
"""
from __future__ import annotations

from typing import Any


def _read(lead: Any, key: str, *, json_field: str = "enrichment_log") -> str:
    """Read a lead field that may live as a model attribute, a top-level
    dict key, or inside a JSON sidecar (`Lead.enrichment_log`)."""
    # Model attribute wins (Django Lead path).
    val = getattr(lead, key, None)
    if val:
        return str(val)
    # Plain dict path.
    if isinstance(lead, dict):
        val = lead.get(key)
        if val:
            return str(val)
        log = lead.get(json_field)
    else:
        log = getattr(lead, json_field, None)
    if isinstance(log, dict):
        val = log.get(key)
        if val:
            return str(val)
    return ""


def contact_quality_score(lead: Any) -> int:
    """0-4 reachability score: +1 each for phone, email, website, linkedin."""
    score = 0
    if _read(lead, "contact_phone"):
        score += 1
    if _read(lead, "contact_email"):
        score += 1
    if _read(lead, "website"):
        score += 1
    if _read(lead, "linkedin"):
        score += 1
    return score


def contact_quality_label(score: int) -> str:
    """Human-readable label used in the Telegram message footer."""
    return f"{max(0, min(4, score))}/4"
