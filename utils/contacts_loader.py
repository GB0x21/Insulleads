"""
utils/contacts_loader.py
━━━━━━━━━━━━━━━━━━━━━━━━
Carga y unifica todos los .csv de contacts/
Detecta automáticamente columnas de nombre, teléfono y email
sin importar el idioma ni el nombre exacto del header.

Formatos soportados (ejemplos):
  Nombre, Numero
  Name, Phone
  Company, Email
  Business Name, Phone Number, Email Address
  GC, Tel, Correo
  contractor_name, mobile, email
  ... y cualquier combinación.
"""

import os
import re
import csv
import logging
import unicodedata
from pathlib import Path
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

CONTACTS_DIR   = os.getenv("CONTACTS_DIR", "contacts")
FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "0.72"))

# ── Keywords para auto-detectar columnas ─────────────────────────
_NAME_KEYS  = {
    "nombre", "name", "company", "empresa", "contractor", "contratista",
    "business", "gc", "razon", "razonsocial", "businessname", "companyname",
    "contractorname", "nombreempresa",
}
_PHONE_KEYS = {
    "numero", "number", "phone", "telefono", "tel", "celular", "mobile",
    "cell", "phonenumber", "telephone", "movil", "contactnumber", "num", "cel",
}
_EMAIL_KEYS = {
    "email", "correo", "mail", "emailaddress", "correoe", "correoelectronico",
}


def _col_type(header: str) -> str | None:
    h = re.sub(r"[^a-z0-9]", "", header.lower())  # strip a todo
    if h in _NAME_KEYS:  return "name"
    if h in _PHONE_KEYS: return "phone"
    if h in _EMAIL_KEYS: return "email"
    # substring fallback
    for k in _NAME_KEYS:
        if k in h or h in k: return "name"
    for k in _PHONE_KEYS:
        if k in h or h in k: return "phone"
    for k in _EMAIL_KEYS:
        if k in h or h in k: return "email"
    return None


def _detect_columns(headers: list[str]) -> dict:
    mapping = {}
    for i, h in enumerate(headers):
        t = _col_type(h)
        if t and t not in mapping:
            mapping[t] = i
    return mapping


# ── Normalización ─────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", name).strip()


# ── Validación ────────────────────────────────────────────────────

_PHONE_RE = re.compile(r"[\d\+\-\(\)\s\.]{7,20}")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _clean_phone(v: str) -> str:
    v = v.strip()
    return v if _PHONE_RE.fullmatch(v) else ""


def _clean_email(v: str) -> str:
    v = v.strip().lower()
    return v if _EMAIL_RE.fullmatch(v) else ""


# ── Carga de un CSV ───────────────────────────────────────────────

def _load_single_csv(path: Path) -> list[dict]:
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
                logger.warning(
                    f"[{path.name}] Sin columna de nombre detectada "
                    f"(headers: {headers}) — archivo omitido"
                )
                return records
            if "phone" not in col_map and "email" not in col_map:
                logger.warning(
                    f"[{path.name}] Sin columna de teléfono ni email — archivo omitido"
                )
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

        tel_sym   = "✓" if "phone" in col_map else "—"
        email_sym = "✓" if "email" in col_map else "—"
        logger.info(
            f"[{path.name}] {len(records):,} contactos  "
            f"(tel={tel_sym}, email={email_sym})"
        )
    except Exception as e:
        logger.error(f"[{path.name}] Error al leer: {e}")
    return records


# ── Carga completa de contacts/ ───────────────────────────────────

def load_all_contacts(contacts_dir: str = None) -> list[dict]:
    """
    Lee todos los .csv de contacts_dir y los unifica.
    Si el mismo GC aparece en varios archivos se fusionan los datos.
    """
    folder = Path(contacts_dir or CONTACTS_DIR)
    if not folder.exists():
        logger.warning(
            f"Carpeta '{folder}' no existe. "
            "Crea contacts/ y agrega tus CSVs ahí."
        )
        return []

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No hay .csv en '{folder}'")
        return []

    logger.info(f"Cargando {len(csv_files)} CSV(s) desde '{folder}'...")

    raw_all: list[dict] = []
    for f in csv_files:
        raw_all.extend(_load_single_csv(f))

    # Fusionar duplicados: primer teléfono y primer email encontrado ganan
    merged: dict[str, dict] = {}
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

    contacts = list(merged.values())
    logger.info(
        f"Contactos unificados: {len(contacts):,} "
        f"(de {len(raw_all):,} registros en {len(csv_files)} archivo(s))"
    )
    return contacts


# ── Búsqueda fuzzy ────────────────────────────────────────────────

def lookup_contact(contractor_name: str, contacts: list[dict]) -> dict | None:
    """
    Busca el GC con 3 niveles de tolerancia:
      1. Match exacto normalizado
      2. Substring (uno contiene al otro)  → score 0.95
      3. Fuzzy SequenceMatcher             → score >= FUZZY_THRESHOLD
    """
    if not contractor_name or not contacts:
        return None

    query = normalize_name(contractor_name)
    if not query:
        return None

    best_score   = 0.0
    best_contact = None

    for c in contacts:
        norm = c["norm_name"]
        if norm == query:
            return c
        score = 0.95 if (query in norm or norm in query) else SequenceMatcher(None, query, norm).ratio()
        if score > best_score:
            best_score, best_contact = score, c

    if best_score >= FUZZY_THRESHOLD:
        logger.debug(
            f"Match ({best_score:.2f}): '{contractor_name}' "
            f"→ '{best_contact['raw_name']}' [{best_contact['source']}]"
        )
        return best_contact
    return None
