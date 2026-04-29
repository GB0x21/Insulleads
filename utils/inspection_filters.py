"""
utils/inspection_filters.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Shared helpers that decide whether a "next inspection" block produced
by ``utils/inspection_schedule.py`` is worth showing to the operator.

Two filters, both used by every agent that renders an inspection
window in its Telegram card (`permits_agent`, `construction_agent`,
…). Pre-v9 each agent had its own local copy and only `permits_agent`
applied them — that's why operators kept seeing 4/10/2018 "Code
Investigation" rows from `construction_agent`.

  * :func:`is_future_inspection` — True when ``visit['best_visit']``
    parses to today-or-later.
  * :func:`is_enforcement_inspection` — True when ``visit['type']``
    matches an enforcement/non-construction phrase
    (Code Investigation, Code Compliance, …).

The date parser is multi-format because the upstream agencies serve
ISO, ISO-with-T, US-slashed (``m/d/Y``), and 2-digit-year variants.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any


# Inspection types that mean an enforcement / non-construction visit.
_ENFORCEMENT_INSPECTION_TYPES = (
    "code investigation",
    "code compliance",
    "code enforcement",
    "complaint",
    "no entry",
    "courtesy",
    "no action",
)


def parse_date(text: str) -> datetime | None:
    """Best-effort multi-format date parser. Tries ISO (with/without
    time), US-slashed (`m/d/Y` or `m/d/y`), and ISO-with-T."""
    if not text:
        return None
    text = text.strip()
    candidates = (text[:10], text[:19], text)
    fmts = (
        "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y", "%-m/%-d/%Y", "%m/%d/%y", "%-m/%-d/%y",
        "%d/%m/%Y", "%-d/%-m/%Y",
    )
    for cand in candidates:
        for fmt in fmts:
            try:
                return datetime.strptime(cand, fmt)
            except (ValueError, TypeError):
                continue
    return None


def is_enforcement_inspection(visit: dict[str, Any]) -> bool:
    itype = (visit.get("type") or "").lower()
    return any(p in itype for p in _ENFORCEMENT_INSPECTION_TYPES)


def is_future_inspection(visit: dict[str, Any]) -> bool:
    """True iff ``visit['best_visit']`` parses to a date that's today
    or later. The helper produces strings like
    ``Wed 4/10/2018, 7:00 AM - 10:00 AM`` — we extract the date token
    and run it through :func:`parse_date`. Returns False when no date
    can be parsed (defensive: don't show stale-or-unparseable blocks)."""
    txt = (visit.get("best_visit") or "").strip()
    if not txt:
        return False
    # Try slashed-date first, then ISO.
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", txt)
    if not m:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", txt)
        if not m:
            return False
    parsed = parse_date(m.group(1))
    if not parsed:
        return False
    return parsed.date() >= datetime.utcnow().date()


def is_useful_inspection(visit: dict[str, Any] | None) -> bool:
    """One-shot `should I show this block?` helper. False when the
    visit is missing, stale, or an enforcement-type visit."""
    if not visit:
        return False
    return is_future_inspection(visit) and not is_enforcement_inspection(visit)
