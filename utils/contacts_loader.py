"""
utils/contacts_loader.py  v3
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Carga y unifica todos los .csv de contacts/

⚡ OPTIMIZACIONES v3:
  1. Cache singleton de módulo  — carga 1x por proceso (sin I/O extra)
  2. Dict de exactos norm_name  — O(1) para matches perfectos
  3. Índice invertido de tokens — pre-filtra de 52k → ~20 candidatos
     ANTES de correr SequenceMatcher → elimina 99%+ del trabajo de CPU
"""

import os
import re
import csv
import logging
import unicodedata
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

CONTACTS_DIR    = os.getenv("CONTACTS_DIR", "contacts")
FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "0.72"))

# ── Cache singleton ───────────────────────────────────────────────
_CONTACTS_CACHE: list | None = None
_EXACT_INDEX:    dict        = {}   # norm_name → contacto   O(1)
_TOKEN_INDEX:    dict        = {}   # token     → [contactos] pre-filtro

# ── Stopwords: tokens que no diferencian empresas ─────────────────
_STOPWORDS = {
    "inc","llc","corp","co","company","construction","contractor",
    "builders","building","general","services","group","the","and",
    "de","la","los","el","del","enterprise","enterprises","ltd",
}

# ── Keywords de detección de columnas ────────────────────────────
_NAME_KEYS  = {"nombre","name","company","empresa","contractor","contratista","business","gc","razon","razonsocial","businessname","companyname","contractorname","nombreempresa","licensee","licenseename"}
_PHONE_KEYS = {"numero","number","phone","telefono","tel","celular","mobile","cell","phonenumber","telephone","movil","contactnumber","num","cel","businessphone","primaryphone"}
_EMAIL_KEYS = {"email","correo","mail","emailaddress","correoe","correoelectronico","businessemail","primaryemail"}


def _col_type(header: str) -> str | None:
    h = re.sub(r"[^a-z0-9]", "", header.lower())
    if h in _NAME_KEYS:  return "name"
    if h in _PHONE_KEYS: return "phone"
    if h in _EMAIL_KEYS: return "email"
    for k in _NAME_KEYS:
        if k in h or h in k: return "name"
    for k in _PHONE_KEYS:
        if k in h or h in k: return "phone"
    for k in _EMAIL_KEYS:
        if k in h or h in k: return "email"
    return None


def _detect_columns(headers: list) -> dict:
    mapping = {}
    for i, h in enumerate(headers):
        t = _col_type(h)
        if t and t not in mapping:
            mapping[t] = i
    return mapping


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def _sig_tokens(norm: str) -> list:
    """Tokens significativos (>=3 chars, no stopword) para el índice."""
    return [t for t in norm.split() if len(t) >= 3 and t not in _STOPWORDS]


_PHONE_RE = re.compile(r"[\d\+\-\(\)\s\.]{7,20}")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

def _clean_phone(v: str) -> str:
    v = v.strip()
    return v if _PHONE_RE.fullmatch(v) else ""

def _clean_email(v: str) -> str:
    v = v.strip().lower()
    return v if _EMAIL_RE.fullmatch(v) else ""


def _load_single_csv(path: Path) -> list:
    records = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            sample = f.read(4096)
        delimiter = ";" if sample.count(";") > sample.count(",") else ","
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader  = csv.reader(f, delimiter=delimiter)
            headers = next(reader, None)
            if not headers:
                return records
            col_map = _detect_columns(headers)
            if "name" not in col_map:
                logger.warning(f"[{path.name}] Sin columna nombre — omitido")
                return records
            if "phone" not in col_map and "email" not in col_map:
                logger.warning(f"[{path.name}] Sin tel ni email — omitido")
                return records
            ni = col_map["name"]
            pi = col_map.get("phone")
            ei = col_map.get("email")
            for row in reader:
                if not row:
                    continue
                raw_name = row[ni].strip() if ni < len(row) else ""
                if not raw_name:
                    continue
                phone = _clean_phone(row[pi]) if pi is not None and pi < len(row) else ""
                email = _clean_email(row[ei]) if ei is not None and ei < len(row) else ""
                if not phone and not email:
                    continue
                records.append({
                    "raw_name":  raw_name,
                    "norm_name": normalize_name(raw_name),
                    "phone":     phone,
                    "email":     email,
                    "source":    path.name,
                })
        t = "✓" if "phone" in col_map else "—"
        e = "✓" if "email" in col_map else "—"
        logger.info(f"[{path.name}] {len(records):,} contactos  (tel={t}, email={e})")
    except Exception as ex:
        logger.error(f"[{path.name}] Error: {ex}")
    return records


def _build_indexes(contacts: list):
    """Construye _EXACT_INDEX y _TOKEN_INDEX en memoria."""
    global _EXACT_INDEX, _TOKEN_INDEX
    exact  = {}
    token  = defaultdict(list)
    for c in contacts:
        norm = c["norm_name"]
        if norm not in exact:
            exact[norm] = c
        for tok in _sig_tokens(norm):
            token[tok].append(c)
    _EXACT_INDEX = exact
    _TOKEN_INDEX = dict(token)
    logger.info(f"⚡ Índices listos: {len(exact):,} exactos | {len(_TOKEN_INDEX):,} tokens únicos")


def load_all_contacts(contacts_dir: str = None) -> list:
    """Carga CSVs una sola vez y construye índices. Siguientes llamadas = 0ms."""
    global _CONTACTS_CACHE
    if _CONTACTS_CACHE is not None:
        return _CONTACTS_CACHE

    folder = Path(contacts_dir or CONTACTS_DIR)
    if not folder.exists():
        logger.warning(f"Carpeta '{folder}' no existe.")
        _CONTACTS_CACHE = []
        return _CONTACTS_CACHE

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No hay .csv en '{folder}'")
        _CONTACTS_CACHE = []
        return _CONTACTS_CACHE

    logger.info(f"Cargando {len(csv_files)} CSV(s) desde '{folder}'...")
    raw_all = []
    for f in csv_files:
        raw_all.extend(_load_single_csv(f))

    merged = {}
    for rec in raw_all:
        key = rec["norm_name"]
        if key not in merged:
            merged[key] = rec.copy()
        else:
            if not merged[key]["phone"] and rec["phone"]:
                merged[key]["phone"]  = rec["phone"]
                merged[key]["source"] = rec["source"]
            if not merged[key]["email"] and rec["email"]:
                merged[key]["email"] = rec["email"]

    _CONTACTS_CACHE = list(merged.values())
    logger.info(f"✅ {len(_CONTACTS_CACHE):,} contactos unificados de {len(raw_all):,} registros")
    _build_indexes(_CONTACTS_CACHE)
    return _CONTACTS_CACHE


def lookup_contact(contractor_name: str, contacts: list) -> dict | None:
    """
    Búsqueda de GC — 3 niveles, optimizado para 50k+ contactos:

    Nivel 1: O(1)  — dict exacto normalizado
    Nivel 2: O(k)  — token pre-filter (k = candidatos con token en común, ~5-30)
                     + SequenceMatcher SOLO sobre esos candidatos
    Nivel 3: O(n)  — fallback lineal (solo nombres sin tokens significativos)

    Resultado: lookup típico < 1ms en lugar de ~200ms con scan lineal de 52k
    """
    if not contractor_name:
        return None
    query = normalize_name(contractor_name)
    if not query:
        return None

    # Nivel 1 — exacto O(1)
    if query in _EXACT_INDEX:
        return _EXACT_INDEX[query]

    # Nivel 2 — token pre-filter
    query_tokens = _sig_tokens(query)
    candidates: dict = {}   # norm_name → contacto, sin duplicados
    for tok in query_tokens:
        for c in _TOKEN_INDEX.get(tok, []):
            candidates[c["norm_name"]] = c

    if candidates:
        best_score, best_contact = 0.0, None
        for norm, c in candidates.items():
            if norm == query:
                return c
            score = (0.95 if (query in norm or norm in query)
                     else SequenceMatcher(None, query, norm).ratio())
            if score > best_score:
                best_score, best_contact = score, c
        if best_score >= FUZZY_THRESHOLD:
            logger.debug(
                f"Match ({best_score:.2f}): '{contractor_name}' "
                f"→ '{best_contact['raw_name']}' [{best_contact['source']}]"
            )
            return best_contact

    # Nivel 3 — fallback lineal (solo si sin candidatos de tokens)
    if not candidates:
        best_score, best_contact = 0.0, None
        for c in contacts:
            norm  = c["norm_name"]
            score = (0.95 if (query in norm or norm in query)
                     else SequenceMatcher(None, query, norm).ratio())
            if score > best_score:
                best_score, best_contact = score, c
        if best_score >= FUZZY_THRESHOLD:
            return best_contact

    return None
