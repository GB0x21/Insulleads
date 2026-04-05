"""
utils/inspection_schedule.py — Lookup upcoming inspections for permits

Queries city inspection calendars to find when the GC/superintendent
will be on-site. This info helps sales reps visit in person during
scheduled inspections for higher close rates.

Supported cities:
- San Francisco (Socrata: biys-ruxt.json) — full inspection schedules
- San Jose (CKAN) — permit activity dates
- Other cities — estimates based on permit phase/issuance
"""

import os
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT = int(os.getenv("SOURCE_TIMEOUT", "30"))

# ── Socrata/CKAN inspection endpoints by city ─────────────────────
INSPECTION_ENDPOINTS = {
    "San Francisco": {
        "engine": "socrata",
        "url": "https://data.sfgov.org/resource/biys-ruxt.json",
        "permit_field": "permit_number",
        "address_field": "block",
        "date_field": "inspection_date",
        "type_field": "inspection_type_description",
        "status_field": "inspection_status",
    },
    "San Jose": {
        "engine": "ckan",
        "url": "https://data.sanjoseca.gov/api/3/action/datastore_search",
        "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904",
        "permit_field": "FOLDERNUMBER",
        "address_field": "gx_location",
        "date_field": "ISSUEDATE",
        "type_field": "WORKDESCRIPTION",
        "status_field": "Status",
    },
}

# ── Typical inspection timeline from permit issuance ──────────────
# Used to estimate visit times when no inspection API is available
PHASE_TIMELINE_DAYS = {
    "foundation":  7,    # Foundation inspection ~1 week after permit
    "framing":     21,   # Framing inspection ~3 weeks
    "rough_mep":   28,   # Rough MEP ~4 weeks
    "insulation":  35,   # Insulation inspection ~5 weeks
    "drywall":     42,   # Drywall/close ~6 weeks
    "final":       60,   # Final inspection ~8 weeks
}

# Best times to visit job sites (GC is typically present)
BEST_VISIT_HOURS = "7:00 AM - 10:00 AM"


def lookup_inspections(permit_id: str = None, address: str = None,
                       city: str = None) -> list:
    """
    Look up upcoming inspections for a permit or address.

    Returns list of dicts with:
      - date: inspection date (ISO string)
      - type: inspection type (e.g., "FRAMING", "ROUGH PLUMBING")
      - status: scheduled/completed/failed
      - best_visit: recommended visit time string

    Tries city-specific API first, falls back to estimation.
    """
    if not city:
        return []

    endpoint = INSPECTION_ENDPOINTS.get(city)
    if endpoint:
        try:
            if endpoint["engine"] == "socrata":
                return _lookup_socrata(endpoint, permit_id, address)
            elif endpoint["engine"] == "ckan":
                return _lookup_ckan(endpoint, permit_id, address)
        except Exception as e:
            logger.debug(f"[InspectionSchedule] API error for {city}: {e}")

    return []


def _lookup_socrata(endpoint: dict, permit_id: str = None,
                    address: str = None) -> list:
    """Query Socrata inspection endpoint."""
    today = datetime.utcnow().strftime("%Y-%m-%dT00:00:00")

    where_parts = [f"inspection_date >= '{today}'"]
    if permit_id:
        where_parts.append(f"{endpoint['permit_field']} = '{permit_id}'")
    elif address:
        # Fuzzy match on address
        addr_clean = address.split(",")[0].strip().upper()[:30]
        where_parts.append(f"UPPER({endpoint['address_field']}) LIKE '%{addr_clean}%'")
    else:
        return []

    params = {
        "$where": " AND ".join(where_parts),
        "$order": f"{endpoint['date_field']} ASC",
        "$limit": 10,
    }

    token = os.getenv("SOCRATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    resp = requests.get(endpoint["url"], params=params,
                        headers=headers, timeout=SOURCE_TIMEOUT)

    if resp.status_code != 200:
        return []

    results = []
    for record in resp.json():
        insp_date = record.get(endpoint["date_field"], "")
        insp_type = record.get(endpoint["type_field"], "")
        insp_status = record.get(endpoint["status_field"], "")

        # Parse date for display
        date_display = _format_date(insp_date)
        day_of_week = _get_day_of_week(insp_date)

        results.append({
            "date": insp_date,
            "date_display": date_display,
            "day_of_week": day_of_week,
            "type": insp_type,
            "status": insp_status,
            "best_visit": f"{day_of_week} {date_display}, {BEST_VISIT_HOURS}",
            "source": "city_calendar",
        })

    return results


def _lookup_ckan(endpoint: dict, permit_id: str = None,
                 address: str = None) -> list:
    """Query CKAN inspection endpoint."""
    filters = {}
    if permit_id:
        filters[endpoint["permit_field"]] = permit_id

    params = {
        "resource_id": endpoint["resource_id"],
        "limit": 10,
    }
    if filters:
        import json
        params["filters"] = json.dumps(filters)

    resp = requests.get(endpoint["url"], params=params, timeout=SOURCE_TIMEOUT)

    if resp.status_code != 200:
        return []

    data = resp.json()
    records = data.get("result", {}).get("records", [])

    results = []
    for record in records:
        insp_date = record.get(endpoint["date_field"], "")
        insp_type = record.get(endpoint["type_field"], "")
        insp_status = record.get(endpoint["status_field"], "")

        date_display = _format_date(insp_date)
        day_of_week = _get_day_of_week(insp_date)

        results.append({
            "date": insp_date,
            "date_display": date_display,
            "day_of_week": day_of_week,
            "type": insp_type,
            "status": insp_status,
            "best_visit": f"{day_of_week} {date_display}, {BEST_VISIT_HOURS}",
            "source": "city_calendar",
        })

    return results


def estimate_inspection_dates(issued_date: str, phase: str = None,
                              city: str = None) -> list:
    """
    Estimate upcoming inspection dates based on permit issuance date
    and current construction phase.

    Used when no inspection API is available for the city.
    """
    try:
        # Parse issued date
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                base_date = datetime.strptime(issued_date[:19], fmt)
                break
            except ValueError:
                continue
        else:
            return []
    except Exception:
        return []

    now = datetime.utcnow()
    results = []

    for phase_name, days_offset in PHASE_TIMELINE_DAYS.items():
        est_date = base_date + timedelta(days=days_offset)

        # Only include future or very recent dates (last 3 days)
        if est_date >= now - timedelta(days=3):
            date_display = _format_date(est_date.isoformat())
            day_of_week = _get_day_of_week(est_date.isoformat())

            phase_labels = {
                "foundation": "Cimentacion",
                "framing": "Estructura (Framing)",
                "rough_mep": "MEP Rough-In",
                "insulation": "Insulacion",
                "drywall": "Drywall/Cierre",
                "final": "Inspeccion Final",
            }

            results.append({
                "date": est_date.isoformat(),
                "date_display": date_display,
                "day_of_week": day_of_week,
                "type": phase_labels.get(phase_name, phase_name),
                "status": "estimated",
                "best_visit": f"{day_of_week} {date_display}, {BEST_VISIT_HOURS}",
                "source": "estimated",
            })

    return results


def get_next_visit_window(lead: dict) -> dict | None:
    """
    Get the best upcoming visit window for a lead.
    Combines API lookups with estimation.

    Returns dict with:
      - date: ISO date
      - date_display: human-readable date
      - day_of_week: e.g., "Lunes"
      - type: inspection type
      - best_visit: full recommendation string
      - source: "city_calendar" or "estimated"
    """
    city = lead.get("city", "")
    permit_id = lead.get("permit_id")
    address = lead.get("address", "")
    issued_date = lead.get("issued_date") or lead.get("date")
    phase = lead.get("phase")

    # Try API lookup first
    inspections = lookup_inspections(
        permit_id=permit_id, address=address, city=city
    )

    if inspections:
        # Filter to future/scheduled inspections
        now_iso = datetime.utcnow().isoformat()
        upcoming = [i for i in inspections
                    if i.get("date", "") >= now_iso
                    and i.get("status", "").upper() not in ("COMPLETED", "APPROVED", "PASS")]

        if upcoming:
            return upcoming[0]  # Next upcoming inspection

    # Fall back to estimation
    if issued_date:
        estimates = estimate_inspection_dates(issued_date, phase, city)
        if estimates:
            return estimates[0]  # Next estimated inspection

    return None


def format_visit_info(visit: dict) -> str:
    """Format visit window info for Telegram notification."""
    if not visit:
        return ""

    source_label = ""
    if visit.get("source") == "city_calendar":
        source_label = "📅 Calendario oficial"
    else:
        source_label = "📅 Estimado"

    insp_type = visit.get("type", "")
    best_time = visit.get("best_visit", "")

    lines = [f"🗓️ *MEJOR MOMENTO PARA VISITAR:*"]
    lines.append(f"▸ {best_time}")
    if insp_type:
        lines.append(f"▸ Inspeccion: {insp_type}")
    lines.append(f"▸ Fuente: {source_label}")
    lines.append(f"▸ Tip: El GC/superintendente estara en sitio")

    return "\n".join(lines)


# ── Internal helpers ──────────────────────────────────────────────

def _format_date(date_str: str) -> str:
    """Format ISO date to human-readable."""
    if not date_str:
        return ""
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str[:19], fmt)
                return dt.strftime("%d/%m/%Y")
            except ValueError:
                continue
    except Exception:
        pass
    return date_str[:10]


def _get_day_of_week(date_str: str) -> str:
    """Get Spanish day of week from ISO date."""
    if not date_str:
        return ""
    try:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str[:19], fmt)
                days = {
                    0: "Lunes", 1: "Martes", 2: "Miercoles",
                    3: "Jueves", 4: "Viernes", 5: "Sabado", 6: "Domingo",
                }
                return days[dt.weekday()]
            except ValueError:
                continue
    except Exception:
        pass
    return ""
