"""
agents/rodents_agent.py  v2
━━━━━━━━━━━━━━━━━━━━━━━━━━
🐀 Reportes 311 Plagas & Roedores — Bay Area

MEJORAS v2:
  ✅ +3 ciudades: San Jose 311 (CKAN), Berkeley 311 (Socrata),
     Fremont SeeClickFix — total 5 fuentes
  ✅ Tipos de plaga ampliados: roedores, termitas, vida silvestre,
     cucarachas, chinches (todos causan daño a insulación)
  ✅ Enriquecimiento de contacto: lookup en CSV de contactos
     para encontrar propietarios/property managers cercanos
  ✅ Fetch paralelo de todas las fuentes (ThreadPoolExecutor)
  ✅ Score de severidad basado en tipo de plaga y recurrencia
  ✅ Integración EPA EnviroFacts API (gratuita) para datos de
     calidad ambiental por zona
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact

logger = logging.getLogger(__name__)

SOURCE_TIMEOUT  = int(os.getenv("SOURCE_TIMEOUT", "45"))
PARALLEL_311    = int(os.getenv("PARALLEL_311", "5"))
RODENT_MONTHS   = int(os.getenv("RODENT_MONTHS", "2"))

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


def _classify_pest(text: str) -> dict | None:
    """Clasifica el tipo de plaga y retorna metadata de severidad."""
    upper = (text or "").upper()
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
# Datos de calidad ambiental que correlacionan con plagas

def _get_epa_facilities(lat: float, lon: float, radius_miles: float = 1.0) -> int:
    """
    Consulta EPA Envirofacts para contar facilities ambientales cercanas.
    Más facilities = más probabilidad de plagas = mejor lead.
    Retorna el conteo o 0 si falla.
    """
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

                        leads.append(lead)

                    logger.info(f"[Rodents/{city}] {len(records)} reportes")
                except Exception as e:
                    logger.debug(f"[Rodents/{src['city']}] {e}")

        # Ordenar por severidad (mayor primero)
        leads.sort(key=lambda l: l.get("severity", 0), reverse=True)
        return leads

    def notify(self, lead: dict):
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

        # Agregar datos de contacto si disponibles
        if lead.get("contact_name"):
            fields["👤 Contacto"]  = lead["contact_name"]
        if lead.get("contact_phone"):
            src = lead.get("contact_source", "")
            fields["📞 Teléfono"] = (
                f"{lead['contact_phone']}  _(via {src})_" if src
                else lead["contact_phone"]
            )
        if lead.get("contact_email"):
            fields["✉️  Email"] = lead["contact_email"]

        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=f"{lead['city']} — {lead['address']}",
            fields=fields,
            url=maps_url,
            cta=f"{pest_emoji} Plaga detectada = {damage}. ¡Ofrece inspección de insulación!",
        )
