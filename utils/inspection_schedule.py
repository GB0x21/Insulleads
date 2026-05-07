"""
utils/inspection_schedule.py — Lookup upcoming inspections for permits

Queries city inspection calendars to find when the GC/superintendent
will be on-site. This info helps sales reps visit in person during
scheduled inspections for higher close rates.

Coverage:
- San Francisco (Socrata: biys-ruxt.json) — real inspection schedules
- San Jose (CKAN) — permit activity dates
- Oakland, Berkeley, Sunnyvale, Santa Clara, Richmond, Palo Alto,
  Mountain View, Redwood City, Daly City, San Mateo, Concord,
  Walnut Creek, Alameda, Vallejo — city-specific Socrata endpoints
- Contra Costa, Alameda, San Mateo, Solano, Marin, Napa, Sonoma,
  San Joaquin counties — county-level Socrata endpoints
- ALL other cities — estimation based on permit phase/issuance date
"""

import os
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, str(default)).split("#")[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default

SOURCE_TIMEOUT = _env_int("SOURCE_TIMEOUT", 30)

# biys-ruxt fue retirado por SF DBI (404 desde 2026). SF usa estimación.

# ── San Jose: CKAN inspection endpoint ──────────────────────────
SJ_INSPECTION_ENDPOINT = {
    "engine": "ckan",
    "url": "https://data.sanjoseca.gov/api/3/action/datastore_search",
    "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904",
    "permit_field": "FOLDERNUMBER",
    "address_field": "gx_location",
    "date_field": "ISSUEDATE",
    "type_field": "WORKDESCRIPTION",
    "status_field": "Status",
}

# ── Socrata permit endpoints (city-level) ───────────────────────
# These return permit data (not inspection-specific), so we extract
# issued_date and estimate inspection timelines from it.
CITY_PERMIT_ENDPOINTS = {
    "Oakland": {
        "url": "https://data.oaklandca.gov/resource/p8h7-gzqg.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Berkeley": {
        "url": "https://data.cityofberkeley.info/resource/cqze-unm8.json",
        "permit_field": "record_number",
        "address_field": "address",
        "date_field": "issue_date",
        "type_field": "project_description",
        "status_field": "record_status",
    },
    "Sunnyvale": {
        "url": "https://data.sunnyvale.ca.gov/resource/7xm5-teup.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "permit_status",
    },
    "Santa Clara": {
        "url": "https://data.santa-clara.ca.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issue_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Richmond": {
        "url": "https://data.ci.richmond.ca.us/resource/permits.json",
        "permit_field": "permit_number",
        "address_field": "site_address",
        "date_field": "date_issued",
        "type_field": "work_description",
        "status_field": "status",
    },
    "Palo Alto": {
        "url": "https://data.cityofpaloalto.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Mountain View": {
        "url": "https://data.mountainview.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Redwood City": {
        "url": "https://data.redwoodcity.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Daly City": {
        "url": "https://data.dalycity.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "San Mateo": {
        "url": "https://data.cityofsanmateo.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Concord": {
        "url": "https://data.concordca.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Walnut Creek": {
        "url": "https://data.walnut-creek.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Alameda": {
        "url": "https://data.alamedaca.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
    "Vallejo": {
        "url": "https://data.cityofvallejo.net/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
    },
}

# ── County-level Socrata endpoints ──────────────────────────────
COUNTY_PERMIT_ENDPOINTS = {
    "Contra Costa County": {
        "url": "https://data.contracosta.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Pleasant Hill", "Martinez", "Clayton", "Pittsburg",
            "Lafayette", "Orinda", "Antioch", "Moraga", "Danville",
            "Hercules", "Pinole", "Oakley", "Brentwood",
        ],
    },
    "Alameda County": {
        "url": "https://data.acgov.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Dublin", "San Leandro", "Pleasanton", "Livermore",
            "Newark", "Albany", "Piedmont",
        ],
    },
    "San Mateo County": {
        "url": "https://data.smcgov.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "East Palo Alto", "Menlo Park", "Pacifica", "Hillsborough",
        ],
    },
    "Solano County": {
        "url": "https://data.solanocounty.com/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Benicia", "Fairfield", "Vacaville",
        ],
    },
    "Marin County": {
        "url": "https://data.marincounty.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Novato", "San Rafael",
        ],
    },
    "Napa County": {
        "url": "https://data.countyofnapa.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Napa",
        ],
    },
    "Sonoma County": {
        "url": "https://data.sonomacounty.ca.gov/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Sonoma", "Petaluma",
        ],
    },
    "San Joaquin County": {
        "url": "https://data.sjgov.org/resource/building-permits.json",
        "permit_field": "permit_number",
        "address_field": "address",
        "date_field": "issued_date",
        "type_field": "description",
        "status_field": "status",
        "cities": [
            "Santa Cruz",
        ],
    },
}

# ── City → county mapping (built at import time) ───────────────
CITY_TO_COUNTY = {}
for _county_name, _county_cfg in COUNTY_PERMIT_ENDPOINTS.items():
    for _city in _county_cfg["cities"]:
        CITY_TO_COUNTY[_city] = _county_name

# ── Cities that have specific endpoints (city-level or SF/SJ) ──
# Other cities: Campbell, Fremont, Gilroy, Hayward, Los Altos,
# Los Gatos, Milpitas, Morgan Hill, Saratoga → estimation only
# (no open-data Socrata/CKAN portal available)

# ── Typical inspection timeline from permit issuance ────────────
# Solo estimamos fases útiles para un subcontratista de insulación.
# La estimación se suma a la fecha de emisión del permiso.
PHASE_TIMELINE_DAYS = {
    "framing":   21,
    "rough_mep": 35,
}

BEST_VISIT_HOURS = "7:00 AM - 10:00 AM"


# ═══════════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════════

def lookup_inspections(permit_id: str = None, address: str = None,
                       city: str = None) -> list:
    """
    Look up upcoming inspections for a permit or address.
    Tries in order:
      1. SF real inspection API (biys-ruxt)
      2. San Jose CKAN
      3. City-specific Socrata permit endpoint → estimate from issued_date
      4. County-level Socrata permit endpoint → estimate from issued_date
    """
    if not city:
        return []

    # 1. San Jose — CKAN
    if city == "San Jose":
        try:
            return _lookup_ckan(SJ_INSPECTION_ENDPOINT, permit_id, address)
        except Exception as e:
            logger.debug(f"[InspectionSchedule] SJ CKAN error: {e}")

    # 3. City-specific Socrata permit endpoint
    city_ep = CITY_PERMIT_ENDPOINTS.get(city)
    if city_ep:
        try:
            return _lookup_socrata_permits(city_ep, permit_id, address, city)
        except Exception as e:
            logger.debug(f"[InspectionSchedule] {city} API error: {e}")

    # 4. County-level endpoint
    county_name = CITY_TO_COUNTY.get(city)
    if county_name:
        county_ep = COUNTY_PERMIT_ENDPOINTS[county_name]
        try:
            return _lookup_socrata_permits(
                county_ep, permit_id, address, city
            )
        except Exception as e:
            logger.debug(f"[InspectionSchedule] {county_name} API error: {e}")

    return []


def estimate_inspection_dates(issued_date: str, phase: str = None,
                              city: str = None) -> list:
    """
    Estimate upcoming inspection dates based on permit issuance date
    and current construction phase.
    """
    try:
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

    phase_labels = {
        "framing":   "Estructura — Framing",
        "rough_mep": "MEP Rough-In",
    }

    for phase_name, days_offset in PHASE_TIMELINE_DAYS.items():
        est_date = base_date + timedelta(days=days_offset)

        # Solo fechas estrictamente futuras (mínimo mañana)
        if est_date < now + timedelta(days=1):
            continue

        date_display = _format_date(est_date.isoformat())
        day_of_week = _get_day_of_week(est_date.isoformat())

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
        now_iso = datetime.utcnow().isoformat()
        upcoming = [i for i in inspections
                    if i.get("date", "") >= now_iso
                    and i.get("status", "").upper() not in
                    ("COMPLETED", "APPROVED", "PASS")]

        if upcoming:
            return upcoming[0]

    # Fall back to estimation
    if issued_date:
        estimates = estimate_inspection_dates(issued_date, phase, city)
        if estimates:
            return estimates[0]

    return None


def format_visit_info(visit: dict) -> str:
    """Format visit window info for Telegram notification."""
    if not visit:
        return ""

    source_label = ""
    if visit.get("source") == "city_calendar":
        source_label = "📅 Calendario oficial"
    elif visit.get("source") == "permit_data":
        source_label = "📅 Basado en permiso emitido"
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


# ═══════════════════════════════════════════════════════════════════
#  INTERNAL — Socrata real inspections (SF only)
# ═══════════════════════════════════════════════════════════════════

def _lookup_socrata_inspections(endpoint: dict, permit_id: str = None,
                                address: str = None) -> list:
    """Query Socrata inspection endpoint (SF biys-ruxt — has actual dates)."""
    today = datetime.utcnow().strftime("%Y-%m-%dT00:00:00")

    where_parts = [f"inspection_date >= '{today}'"]
    if permit_id:
        where_parts.append(f"{endpoint['permit_field']} = '{permit_id}'")
    elif address:
        addr_clean = address.split(",")[0].strip().upper()[:30]
        where_parts.append(
            f"UPPER({endpoint['address_field']}) LIKE '%{addr_clean}%'"
        )
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


# ═══════════════════════════════════════════════════════════════════
#  INTERNAL — CKAN (San Jose)
# ═══════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════
#  INTERNAL — Socrata permit endpoints (all other cities)
# ═══════════════════════════════════════════════════════════════════

def _lookup_socrata_permits(endpoint: dict, permit_id: str = None,
                            address: str = None, city: str = None) -> list:
    """
    Query a Socrata permit endpoint to find the permit's issued_date,
    then estimate inspection dates from that.

    This covers cities that have open-data permit portals but no
    dedicated inspection calendar API.
    """
    where_parts = []
    if permit_id:
        where_parts.append(
            f"{endpoint['permit_field']} = '{permit_id}'"
        )
    elif address:
        addr_clean = address.split(",")[0].strip().upper()[:30]
        where_parts.append(
            f"UPPER({endpoint['address_field']}) LIKE '%{addr_clean}%'"
        )
    else:
        return []

    params = {
        "$where": " AND ".join(where_parts),
        "$order": f"{endpoint['date_field']} DESC",
        "$limit": 5,
    }

    token = os.getenv("SOCRATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    resp = requests.get(endpoint["url"], params=params,
                        headers=headers, timeout=SOURCE_TIMEOUT)
    if resp.status_code != 200:
        return []

    records = resp.json()
    if not records:
        return []

    # Take the most recent permit's issued_date and estimate inspections
    record = records[0]
    issued_date = record.get(endpoint["date_field"], "")
    permit_desc = record.get(endpoint["type_field"], "")

    if not issued_date:
        return []

    # Estimate inspection timeline from the permit issuance date
    estimates = estimate_inspection_dates(issued_date, phase=None, city=city)

    # Enrich with permit data source info
    for est in estimates:
        est["source"] = "permit_data"
        est["permit_description"] = permit_desc

    return estimates


# ═══════════════════════════════════════════════════════════════════
#  INTERNAL — Helpers
# ═══════════════════════════════════════════════════════════════════

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
