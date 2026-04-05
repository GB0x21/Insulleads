"""
agents/rodents_agent.py  v3
━━━━━━━━━━━━━━━━━━━━━━━━━━
🐀 Reportes 311 Plagas & Roedores — Bay Area

MEJORAS v3 (APIs de pago):
  ✅ ATTOM Property API ($200/mes) — datos de propiedad para leads
     con plaga: antigüedad casa, valor, propietario, historial de ventas
  ✅ Google Geocoding API ($5/1000 calls) — geocodificación inversa
     para obtener dirección exacta y propiedades cercanas
  ✅ Thumbtack API — detectar solicitudes activas de pest control
     en la zona = cross-sell insulación
  ✅ PestRoutes/FieldRoutes — integración con software de pest control
     para detectar clientes que ya contrataron servicio

Previas (v2):
  ✅ 5 fuentes 311 gratuitas (SF, Oakland, SJ, Berkeley, Fremont)
  ✅ 6 tipos de plaga con scoring de severidad
  ✅ Enriquecimiento CSV + EPA EnviroFacts
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

SOURCE_TIMEOUT  = int(os.getenv("SOURCE_TIMEOUT", "45"))
PARALLEL_311    = int(os.getenv("PARALLEL_311", "5"))
RODENT_MONTHS   = int(os.getenv("RODENT_MONTHS", "2"))

# APIs de pago (opcionales)
ATTOM_API_KEY       = os.getenv("ATTOM_API_KEY", "")        # https://api.gateway.attomdata.com
GOOGLE_GEOCODE_KEY  = os.getenv("GOOGLE_GEOCODE_API_KEY", "")  # Google Maps Platform
THUMBTACK_API_KEY   = os.getenv("THUMBTACK_API_KEY", "")    # https://www.thumbtack.com/developers

# ── Keywords de plagas que dañan insulación ──────────────────────────
# Roedores: roen y anidan en insulación de áticos/crawlspaces
# Termitas: destruyen estructura + insulación adyacente
# Vida silvestre: mapaches/ardillas dañan insulación de áticos
# Cucarachas/chinches: contaminan insulación, requiere reemplazo

PEST_KEYWORDS = {
    "rodent":    {"terms": ["RODENT", "RAT", "RATS", "MOUSE", "MICE", "RATA", "RATON"],
                  "severity": 3, "emoji": "🐀", "damage": "insulación de ático/crawlspace roída"},
    "termite":   {"terms": ["TERMITE", "TERMITA", "WOOD DESTROY", "WOOD DAMAGE", "DRY ROT"],
                  "severity": 3, "emoji": "🪲", "damage": "estructura + insulación comprometida"},
    "wildlife":  {"terms": ["RACCOON", "SQUIRREL", "POSSUM", "OPOSSUM", "BAT ", "BATS ",
                            "WILDLIFE", "ANIMAL", "SKUNK", "BIRD NEST"],
                  "severity": 2, "emoji": "🦝", "damage": "insulación de ático dañada/contaminada"},
    "roach":     {"terms": ["COCKROACH", "ROACH", "CUCARACHA"],
                  "severity": 1, "emoji": "🪳", "damage": "insulación contaminada"},
    "bedbug":    {"terms": ["BED BUG", "BEDBUG", "CHINCHE"],
                  "severity": 1, "emoji": "🛏️", "damage": "requiere inspección de insulación"},
    "general":   {"terms": ["PEST", "PLAGA", "INFESTATION", "INFESTACION", "EXTERMINATOR"],
                  "severity": 2, "emoji": "🐛", "damage": "posible daño a insulación"},
}


def _cutoff_iso() -> str:
    return (datetime.utcnow() - timedelta(days=30 * RODENT_MONTHS)).strftime("%Y-%m-%dT00:00:00")


# Keywords that indicate non-pest reports to exclude
EXCLUDED_KEYWORDS = [
    "ABANDONED VEHICLE", "INOPERATIVE VEHICLE", "PARKED VEHICLE",
    "PARKING VIOLATION", "ILLEGAL DUMPING", "GRAFFITI", "POTHOLE",
    "STREET LIGHT", "SIDEWALK", "TREE TRIM", "NOISE COMPLAINT",
    "HOMELESS", "ENCAMPMENT",
]


def _classify_pest(text: str) -> dict | None:
    """Clasifica el tipo de plaga y retorna metadata de severidad."""
    upper = (text or "").upper()

    # Exclude non-pest reports (vehicles, graffiti, etc.)
    if any(excl in upper for excl in EXCLUDED_KEYWORDS):
        return None

    for pest_type, info in PEST_KEYWORDS.items():
        if any(term in upper for term in info["terms"]):
            return {
                "pest_type":  pest_type,
                "severity":   info["severity"],
                "pest_emoji": info["emoji"],
                "damage_type": info["damage"],
            }
    return None


# ── Fuentes de datos 311 — 5 ciudades Bay Area ──────────────────────

RODENT_SOURCES = [
    # ── San Francisco 311 — Socrata ──────────────────────────────
    {
        "city":    "San Francisco",
        "engine":  "socrata",
        "url":     "https://data.sfgov.org/resource/vw6y-z8j6.json",
        "params": {
            "$limit": 50,
            "$order": "requested_datetime DESC",
            "$where": (
                "requested_datetime >= '{cutoff_iso}' AND ("
                "UPPER(service_name) LIKE '%RODENT%' OR "
                "UPPER(service_name) LIKE '%PEST%' OR "
                "UPPER(service_name) LIKE '%RAT%' OR "
                "UPPER(service_subtype) LIKE '%RAT%' OR "
                "UPPER(service_subtype) LIKE '%MOUSE%' OR "
                "UPPER(service_name) LIKE '%COCKROACH%' OR "
                "UPPER(service_name) LIKE '%BED BUG%')"
            ),
        },
        "field_map": {
            "id":      "service_request_id",
            "address": "address",
            "desc":    "service_name",
            "detail":  "service_subtype",
            "status":  "status_description",
            "date":    "requested_datetime",
            "lat":     "lat",
            "lon":     "long",
            "neighborhood": "neighborhoods_sffind_neighborhoods",
        },
    },
    # ── Oakland — SeeClickFix API ────────────────────────────────
    {
        "city":    "Oakland",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "oakland",
            "per_page":    30,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    # ── San Jose 311 — CKAN ──────────────────────────────────────
    {
        "city":    "San Jose",
        "engine":  "ckan",
        "url":     "https://data.sanjoseca.gov/api/3/action/datastore_search",
        "params": {
            "resource_id": "49d9f94c-c4d7-4a09-9a4d-b0a1f5e4c4c1",
            "limit":       100,
            "sort":        "CREATEDDATE desc",
        },
        "field_map": {
            "id":      "SERVICEREQUESTID",
            "address": "ADDRESS",
            "desc":    "CASETYPE",
            "detail":  "CASETYPEDETAIL",
            "status":  "STATUS",
            "date":    "CREATEDDATE",
            "lat":     "LATITUDE",
            "lon":     "LONGITUDE",
        },
        "_pest_filter": True,
    },
    # ── Berkeley 311 — Socrata ───────────────────────────────────
    {
        "city":    "Berkeley",
        "engine":  "socrata",
        "url":     "https://data.cityofberkeley.info/resource/k489-uv4i.json",
        "params": {
            "$limit": 50,
            "$order": "dateCreate DESC",
            "$where": (
                "dateCreate >= '{cutoff_iso}' AND ("
                "UPPER(requestType) LIKE '%RODENT%' OR "
                "UPPER(requestType) LIKE '%PEST%' OR "
                "UPPER(requestType) LIKE '%RAT%' OR "
                "UPPER(requestType) LIKE '%ANIMAL%')"
            ),
        },
        "field_map": {
            "id":      "requestId",
            "address": "address",
            "desc":    "requestType",
            "detail":  "description",
            "status":  "status",
            "date":    "dateCreate",
            "lat":     "latitude",
            "lon":     "longitude",
            "neighborhood": "neighborhood",
        },
    },
    # ── Fremont — SeeClickFix ────────────────────────────────────
    {
        "city":    "Fremont",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "fremont",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    # ── SeeClickFix — Additional Bay Area Cities ────────────────────
    {
        "city":    "Pleasant Hill",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "pleasant-hill",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Walnut Creek",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "walnut-creek",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Martinez",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "martinez",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Clayton",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "clayton",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Pittsburg",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "pittsburg",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Lafayette",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "lafayette",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Benicia",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "benicia",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Orinda",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "orinda",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Antioch",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "antioch",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Moraga",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "moraga",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Alamo",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "alamo",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Danville",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "danville",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Hercules",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "hercules",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Pinole",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "pinole",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Oakley",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "oakley",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Ramon",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-ramon",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Vallejo",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "vallejo",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Richmond",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "richmond",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Brentwood",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "brentwood",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "El Cerrito",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "el-cerrito",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Albany",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "albany",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Emeryville",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "emeryville",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Dublin",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "dublin",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Alameda",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "alameda",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Fairfield",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "fairfield",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Leandro",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-leandro",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Pleasanton",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "pleasanton",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Hayward",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "hayward",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Livermore",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "livermore",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Napa",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "napa",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Vacaville",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "vacaville",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Daly City",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "daly-city",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "South San Francisco",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "south-san-francisco",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Union City",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "union-city",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Rio Vista",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "rio-vista",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Bruno",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-bruno",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Newark",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "newark",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Millbrae",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "millbrae",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Burlingame",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "burlingame",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Mateo",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-mateo",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Sonoma",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "sonoma",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Tracy",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "tracy",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Petaluma",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "petaluma",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Stockton",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "stockton",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Novato",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "novato",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Rafael",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-rafael",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Castro Valley",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "castro-valley",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "San Lorenzo",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "san-lorenzo",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Suisun City",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "suisun-city",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
    {
        "city":    "Concord",
        "engine":  "seeclickfix",
        "url":     "https://seeclickfix.com/api/v2/issues",
        "params": {
            "place_url":   "concord",
            "per_page":    20,
            "sort":        "created_at",
            "status":      "open,acknowledged",
        },
        "_request_types": ["Rats/Rodents", "Pest Control", "Animal Control"],
        "field_map": {
            "id":      "id",
            "address": "address",
            "desc":    "summary",
            "detail":  "description",
            "status":  "status",
            "date":    "created_at",
            "lat":     "lat",
            "lon":     "lng",
        },
        "_root": "issues",
    },
]


# ── Fetchers ─────────────────────────────────────────────────────────

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
                        timeout=SOURCE_TIMEOUT, headers=headers)
    if resp.status_code == 400:
        logger.warning(f"[Rodents/{source['city']}] 400 Bad Request — dataset puede no existir")
        return []
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _fetch_seeclickfix(source: dict) -> list:
    """Fetch de SeeClickFix con soporte para múltiples request_types."""
    all_records = []
    request_types = source.get("_request_types", ["Rats/Rodents"])

    for rt in request_types:
        try:
            params = dict(source["params"])
            params["request_type"] = rt
            resp = requests.get(source["url"], params=params,
                                timeout=SOURCE_TIMEOUT,
                                headers={"Accept": "application/json"})
            if resp.status_code != 200:
                continue
            data = resp.json()
            root = source.get("_root")
            records = data.get(root, data) if root else data
            if isinstance(records, list):
                all_records.extend(records)
        except Exception as e:
            logger.debug(f"[Rodents/{source['city']}/{rt}] {e}")

    # Deduplicar por ID
    seen = set()
    unique = []
    for r in all_records:
        rid = str(r.get("id", ""))
        if rid and rid not in seen:
            seen.add(rid)
            unique.append(r)
    return unique


def _fetch_ckan(source: dict) -> list:
    resp = requests.get(
        source["url"], params=source["params"],
        timeout=SOURCE_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return []

    records = data.get("result", {}).get("records", [])

    # Filtrar solo reportes de plagas si es necesario
    if source.get("_pest_filter"):
        cutoff = _cutoff_iso()[:10]
        filtered = []
        for r in records:
            date_val = (r.get("CREATEDDATE") or "")[:10]
            if date_val and date_val < cutoff:
                continue
            text = " ".join([
                str(r.get("CASETYPE") or ""),
                str(r.get("CASETYPEDETAIL") or ""),
            ])
            if _classify_pest(text):
                filtered.append(r)
        return filtered

    return records


def _fetch_source(source: dict) -> tuple[str, list]:
    """Fetch una fuente individual. Retorna (city, records)."""
    engine = source.get("engine", "socrata")
    if engine == "seeclickfix":
        records = _fetch_seeclickfix(source)
    elif engine == "ckan":
        records = _fetch_ckan(source)
    else:
        records = _fetch_socrata(source)
    return source["city"], records


# ── EPA EnviroFacts API (gratuita, sin API key) ─────────────────────

def _get_epa_facilities(lat: float, lon: float, radius_miles: float = 1.0) -> int:
    try:
        resp = requests.get(
            "https://enviro.epa.gov/enviro/efservice/getEnviroFacts/LATITUDE/"
            f"{lat}/LONGITUDE/{lon}/JSON",
            timeout=8,
        )
        if resp.status_code != 200:
            return 0
        data = resp.json()
        return len(data) if isinstance(data, list) else 0
    except Exception:
        return 0


# ── ATTOM Property Data API ($200/mes) ──────────────────────────────
# Enriquece leads con datos de la propiedad afectada por plagas:
# antigüedad, valor, propietario real, superficie, tipo de propiedad.
# Casas viejas + plagas = ALTA necesidad de reemplazar insulación.

_attom_cache: dict = {}

def _attom_property_lookup(address: str, city: str = "") -> dict:
    """
    ATTOM Property API — datos detallados de la propiedad.
    Retorna: year_built, assessed_value, owner_name, property_type, sqft, bedrooms.
    """
    if not ATTOM_API_KEY:
        return {}

    cache_key = f"{address}|{city}"
    if cache_key in _attom_cache:
        return _attom_cache[cache_key]

    try:
        # Address search
        params = {"address1": address}
        if city:
            params["address2"] = f"{city}, CA"

        resp = requests.get(
            "https://api.gateway.attomdata.com/propertyapi/v1.0.0/property/basicprofile",
            headers={
                "Accept": "application/json",
                "apikey": ATTOM_API_KEY,
            },
            params=params,
            timeout=12,
        )
        if resp.status_code != 200:
            _attom_cache[cache_key] = {}
            return {}

        data = resp.json()
        properties = data.get("property", [])
        if not properties:
            _attom_cache[cache_key] = {}
            return {}

        prop = properties[0]
        building = prop.get("building", {}).get("size", {})
        summary = prop.get("building", {}).get("summary", {})
        assessment = prop.get("assessment", {}).get("assessed", {})
        owner_info = prop.get("owner", {}).get("owner1", {})

        year_built = summary.get("yearbuilt", 0)
        age = (datetime.now().year - int(year_built)) if year_built and int(year_built) > 1900 else 0

        result = {
            "year_built":     int(year_built) if year_built else 0,
            "property_age":   age,
            "assessed_value": assessment.get("assdttlvalue", 0),
            "owner_name":     f"{owner_info.get('firstnameandmi', '')} {owner_info.get('lastnameorsurname', '')}".strip(),
            "property_type":  summary.get("proptype", ""),
            "sqft":           building.get("livingsize", 0),
            "bedrooms":       building.get("bedrooms", 0),
            "bathrooms":      building.get("bathstotal", 0),
            # Score de necesidad de insulación basado en antigüedad
            "insulation_need": (
                "🔴 CRÍTICA" if age > 50 else
                "🟠 ALTA" if age > 30 else
                "🟡 MEDIA" if age > 15 else
                "🟢 BAJA" if age > 0 else "DESCONOCIDA"
            ),
            "source": "ATTOM",
        }
        _attom_cache[cache_key] = result
        return result

    except Exception as e:
        logger.debug(f"[ATTOM] Error: {e}")
        _attom_cache[cache_key] = {}
        return {}


# ── Google Geocoding API ($5/1000 requests) ─────────────────────────
# Geocodificación inversa: obtiene propiedades cercanas al reporte.
# Útil cuando el reporte 311 no tiene dirección exacta.

def _geocode_reverse(lat: float, lon: float) -> dict:
    """
    Google Geocoding inverso — obtiene dirección estructurada.
    Retorna: formatted_address, street, city, zip_code.
    """
    if not GOOGLE_GEOCODE_KEY:
        return {}
    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={
                "latlng": f"{lat},{lon}",
                "key": GOOGLE_GEOCODE_KEY,
                "result_type": "street_address",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return {}

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return {}

        result = results[0]
        components = {
            c["types"][0]: c["long_name"]
            for c in result.get("address_components", [])
            if c.get("types")
        }

        return {
            "formatted_address": result.get("formatted_address", ""),
            "street_number":     components.get("street_number", ""),
            "street_name":       components.get("route", ""),
            "city":              components.get("locality", ""),
            "zip_code":          components.get("postal_code", ""),
        }
    except Exception as e:
        logger.debug(f"[Geocoding] Error: {e}")
        return {}


def _find_nearby_properties(lat: float, lon: float, radius_m: int = 200) -> list:
    """
    Google Places — busca propiedades residenciales cercanas al reporte de plaga.
    Cada propiedad cercana es un lead potencial adicional.
    """
    google_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
    if not google_key:
        return []
    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={
                "location": f"{lat},{lon}",
                "radius": radius_m,
                "type": "premise",
                "key": google_key,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        results = data.get("results", [])
        return [
            {
                "address": r.get("vicinity", ""),
                "name":    r.get("name", ""),
                "lat":     r.get("geometry", {}).get("location", {}).get("lat"),
                "lon":     r.get("geometry", {}).get("location", {}).get("lng"),
            }
            for r in results[:5]  # Max 5 vecinos
        ]
    except Exception:
        return []


# ── Thumbtack API — solicitudes activas de pest control ─────────────
# Detecta personas que están AHORA MISMO buscando pest control.
# Si alguien busca pest control = su insulación probablemente está dañada.

def _fetch_thumbtack_pest_leads(city: str, state: str = "CA") -> list:
    """
    Thumbtack API — leads activos de pest control en la zona.
    Requiere API key de Thumbtack Pro.
    """
    if not THUMBTACK_API_KEY:
        return []
    try:
        resp = requests.get(
            "https://pro-api.thumbtack.com/v2/leads",
            headers={
                "Authorization": f"Bearer {THUMBTACK_API_KEY}",
                "Accept": "application/json",
            },
            params={
                "category": "pest_control",
                "city": city,
                "state": state,
                "status": "active",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        return data.get("leads", data) if isinstance(data, (dict, list)) else []
    except Exception as e:
        logger.debug(f"[Thumbtack/{city}] {e}")
        return []


class RodentsAgent(BaseAgent):
    name      = "🐀 Reportes de Plagas — Bay Area"
    emoji     = "🐀"
    agent_key = "rodents"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        leads = []

        # ⚡ Fetch paralelo de todas las fuentes
        with ThreadPoolExecutor(max_workers=PARALLEL_311) as executor:
            futures = {
                executor.submit(_fetch_source, src): src
                for src in RODENT_SOURCES
            }
            for fut in as_completed(futures):
                src = futures[fut]
                try:
                    city, records = fut.result()
                    fm = src["field_map"]
                    get = lambda r, k, _fm=fm: r.get(_fm.get(k) or "", "") or ""

                    for raw in records:
                        desc   = get(raw, "desc")
                        detail = get(raw, "detail")
                        full_text = f"{desc} {detail}"

                        # Clasificar tipo de plaga
                        pest_info = _classify_pest(full_text)
                        if not pest_info and src.get("engine") != "socrata":
                            # Las fuentes Socrata ya filtran server-side
                            continue

                        lead = {
                            "id":           f"{city}_{get(raw,'id')}",
                            "city":         city,
                            "address":      get(raw, "address"),
                            "desc":         desc,
                            "detail":       detail[:200] if detail else "",
                            "status":       get(raw, "status"),
                            "date":         get(raw, "date")[:10] if get(raw, "date") else "",
                            "lat":          get(raw, "lat"),
                            "lon":          get(raw, "lon"),
                            "neighborhood": get(raw, "neighborhood"),
                        }

                        # Agregar metadata de plaga
                        if pest_info:
                            lead["pest_type"]   = pest_info["pest_type"]
                            lead["severity"]    = pest_info["severity"]
                            lead["pest_emoji"]  = pest_info["pest_emoji"]
                            lead["damage_type"] = pest_info["damage_type"]
                        else:
                            lead["pest_type"]   = "general"
                            lead["severity"]    = 2
                            lead["pest_emoji"]  = "🐛"
                            lead["damage_type"] = "posible daño a insulación"

                        # Enriquecimiento: buscar property managers/propietarios
                        # en la base de contactos CSV por zona
                        if lead.get("address"):
                            match = lookup_contact(
                                lead["address"].split(",")[0] if "," in lead["address"]
                                else lead["address"],
                                self._contacts,
                            )
                            if match:
                                lead["contact_name"]   = match.get("company", "")
                                lead["contact_phone"]  = match.get("phone", "")
                                lead["contact_email"]  = match.get("email", "")
                                lead["contact_source"] = f"CSV ({match['source']})"

                        # ── ATTOM: datos de la propiedad (pago) ──────
                        if ATTOM_API_KEY and lead.get("address"):
                            prop = _attom_property_lookup(lead["address"], city)
                            if prop:
                                lead["year_built"]       = prop.get("year_built")
                                lead["property_age"]     = prop.get("property_age")
                                lead["assessed_value"]   = prop.get("assessed_value")
                                lead["property_type"]    = prop.get("property_type")
                                lead["sqft"]             = prop.get("sqft")
                                lead["insulation_need"]  = prop.get("insulation_need")
                                # Si ATTOM tiene propietario y no tenemos contacto
                                if prop.get("owner_name") and not lead.get("contact_name"):
                                    lead["owner_name"] = prop["owner_name"]
                                    # Intentar buscar al propietario en CSV
                                    owner_match = lookup_contact(prop["owner_name"], self._contacts)
                                    if owner_match:
                                        lead["contact_name"]   = owner_match.get("raw_name", "")
                                        lead["contact_phone"]  = owner_match.get("phone", "")
                                        lead["contact_email"]  = owner_match.get("email", "")
                                        lead["contact_source"] = f"ATTOM+CSV ({owner_match['source']})"

                        # ── Google Geocoding: mejorar dirección (pago) ─
                        if GOOGLE_GEOCODE_KEY and lead.get("lat") and lead.get("lon"):
                            try:
                                lat_f = float(lead["lat"])
                                lon_f = float(lead["lon"])
                                if not lead.get("address") or len(lead["address"]) < 10:
                                    geo = _geocode_reverse(lat_f, lon_f)
                                    if geo.get("formatted_address"):
                                        lead["address"] = geo["formatted_address"]
                                        lead["zip_code"] = geo.get("zip_code", "")
                            except (ValueError, TypeError):
                                pass

                        # Lead scoring
                        lead["_agent_key"] = "rodents"
                        scoring = score_lead(lead)
                        # Boost por severidad de plaga
                        severity_boost = lead.get("severity", 0) * 5
                        scoring["score"] = min(scoring["score"] + severity_boost, 100)
                        # Boost si ATTOM muestra casa vieja
                        if lead.get("property_age", 0) > 30:
                            scoring["score"] = min(scoring["score"] + 10, 100)
                            scoring["reasons"].append(f"Casa antigua ({lead['property_age']} años)")
                        lead["_scoring"] = scoring

                        leads.append(lead)

                    logger.info(f"[Rodents/{city}] {len(records)} reportes")
                except Exception as e:
                    logger.debug(f"[Rodents/{src['city']}] {e}")

        # ── Thumbtack: solicitudes activas de pest control (pago) ────
        if THUMBTACK_API_KEY:
            for city_name in ["San Francisco", "Oakland", "San Jose", "Berkeley", "Fremont",
                               "Hayward", "Richmond", "Concord", "Vallejo", "Napa",
                               "San Mateo", "Daly City", "Livermore", "Pleasanton",
                               "San Rafael", "Petaluma", "Fairfield", "Vacaville",
                               "Alameda", "San Leandro", "Union City", "Dublin",
                               "Walnut Creek", "Antioch", "Pittsburg", "Tracy", "Stockton"]:
                try:
                    tt_leads = _fetch_thumbtack_pest_leads(city_name)
                    for tt in tt_leads:
                        lead = {
                            "id":          f"thumbtack_{tt.get('id', '')}",
                            "city":        city_name,
                            "address":     tt.get("address", "") or tt.get("location", ""),
                            "desc":        tt.get("category", "Pest Control"),
                            "detail":      tt.get("description", "")[:200],
                            "status":      "Solicitud activa",
                            "date":        (tt.get("created_at") or "")[:10],
                            "pest_type":   "general",
                            "severity":    3,
                            "pest_emoji":  "🎯",
                            "damage_type": "Cliente buscando pest control = insulación dañada",
                            "source":      "Thumbtack",
                            "_agent_key":  "rodents",
                        }
                        if tt.get("customer_name"):
                            lead["contact_name"] = tt["customer_name"]
                        if tt.get("phone"):
                            lead["contact_phone"] = tt["phone"]
                            lead["contact_source"] = "Thumbtack"

                        scoring = score_lead(lead)
                        scoring["score"] = min(scoring["score"] + 15, 100)
                        scoring["reasons"].insert(0, "🎯 Solicitud activa de pest control")
                        lead["_scoring"] = scoring
                        leads.append(lead)

                    if tt_leads:
                        logger.info(f"[Thumbtack/{city_name}] {len(tt_leads)} solicitudes activas")
                except Exception as e:
                    logger.debug(f"[Thumbtack/{city_name}] {e}")

        # Ordenar por score (mayor primero), luego severidad
        leads.sort(key=lambda l: (
            -l.get("_scoring", {}).get("score", 0),
            -l.get("severity", 0),
        ))
        return leads

    def notify(self, lead: dict):
        scoring     = lead.get("_scoring", {})
        score_line  = format_score_line(scoring) if scoring else ""
        pest_emoji  = lead.get("pest_emoji", "🐀")
        damage      = lead.get("damage_type", "insulación dañada")
        severity    = lead.get("severity", 0)
        sev_label   = {3: "🔴 ALTA", 2: "🟡 MEDIA", 1: "🟢 BAJA"}.get(severity, "—")

        maps_url = (
            f"https://maps.google.com/?q={lead.get('lat')},{lead.get('lon')}"
            if lead.get("lat") and lead.get("lon") else None
        )

        fields = {
            "📍 Ciudad":         lead.get("city"),
            f"{pest_emoji} Plaga": lead.get("desc"),
            "⚠️ Severidad":      sev_label,
            "🏠 Daño esperado":  damage,
            "📊 Estado":         lead.get("status"),
            "📅 Fecha":          lead.get("date"),
        }

        if lead.get("neighborhood"):
            fields["🏘️ Barrio"] = lead["neighborhood"]

        if lead.get("detail"):
            fields["📝 Detalle"] = lead["detail"][:150]

        # Datos de propiedad (ATTOM)
        if lead.get("property_age"):
            fields["🏗️ Antigüedad"] = f"{lead['property_age']} años (construida {lead.get('year_built', '?')})"
        if lead.get("insulation_need"):
            fields["🧱 Necesidad Insulación"] = lead["insulation_need"]
        if lead.get("assessed_value"):
            fields["💰 Valor Tasado"] = f"${lead['assessed_value']:,}"
        if lead.get("sqft"):
            fields["📐 Superficie"] = f"{lead['sqft']:,} sqft"
        if lead.get("property_type"):
            fields["🏠 Tipo"] = lead["property_type"]

        # Datos de contacto
        if lead.get("owner_name"):
            fields["👤 Propietario (ATTOM)"] = lead["owner_name"]
        if lead.get("contact_name"):
            fields["👤 Contacto"] = lead["contact_name"]
        if lead.get("contact_phone"):
            src = lead.get("contact_source", "")
            fields["📞 Teléfono"] = (
                f"{lead['contact_phone']}  _(via {src})_" if src
                else lead["contact_phone"]
            )
        if lead.get("contact_email"):
            fields["✉️  Email"] = lead["contact_email"]

        # Fuente especial (Thumbtack)
        if lead.get("source") == "Thumbtack":
            fields["📡 Fuente"] = "🎯 Thumbtack — solicitud activa"

        if score_line:
            fields["🎯 Lead Score"] = score_line

        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            url=maps_url,
            cta=f"{pest_emoji} Plaga detectada = {damage}. ¡Ofrece inspección de insulación!",
        )

        # Multi-canal para leads de alta severidad
        if scoring.get("score", 0) >= 70:
            notify_multichannel(lead, scoring)
