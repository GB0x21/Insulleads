"""
utils/cslb_bulk.py
━━━━━━━━━━━━━━━━━━
On-disk index of the California Contractors State License Board (CSLB)
public dataset.

The CSLB ships a full extract of every active license (license number,
business name, address, classification(s), business phone) via their
Public Records Act portal:

    https://www.cslb.ca.gov/About_Us/Library/Data_Requests/

The extract is free; CSLB emails you a download link within ~2 business
days. Save the file as ``data/cslb_bulk.csv`` (or set
``CSLB_BULK_PATH``) and the rest of the project picks it up
automatically: ``permits_agent._cslb_lookup`` tries this index first
and falls back to the legacy scraper when there's no hit.

Why bother:
  * The legacy scraper hits the ASP.NET form once per contractor; the
    bulk file gives a < 1 ms lookup per contractor instead of ~2 s.
  * No rate limit. No ViewState. No HTML shape regressions.
  * Phone numbers are present for the whole license universe — the
    scraper sometimes returns blanks because of CSLB layout changes.

The file is parsed once, cached in memory, and indexed by both license
number AND a normalized business-name token bag so company-name lookups
benefit too.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BULK_PATH = os.getenv("CSLB_BULK_PATH", "data/cslb_bulk.csv")

# Module-level singletons — cheap on a fresh process, free on subsequent calls.
_BY_LICENSE: dict[str, dict[str, Any]] | None = None
_BY_NORM_NAME: dict[str, dict[str, Any]] | None = None
_LOADED_FROM: str | None = None


# ─── Header detection ───────────────────────────────────────────────
# CSLB has tweaked the column names a few times over the years. We
# fuzzy-match each candidate against the CSV header set so the parser
# survives small renames (e.g. "BusinessPhone" → "Business_Phone").

_LICENSE_KEYS = ("licensenumber", "licensenum", "licno", "license", "lic")
_NAME_KEYS = ("businessname", "compname", "company", "doingbusinessas", "dba")
_PHONE_KEYS = ("businessphone", "phone", "telephone", "tel", "phonenumber")
_CITY_KEYS = ("businesscity", "city", "mailcity", "homecity")
_STATUS_KEYS = ("primarystatus", "licensestatus", "status", "primarystatusdesc")
_CLASS_KEYS = ("classifications", "classification", "class", "classcode")


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (h or "").lower())


def _detect(headers: list[str]) -> dict[str, int]:
    """Map field-name → column index. Missing fields get omitted; the
    parser then skips that record only if `license` is missing."""
    norm = {_norm_header(h): i for i, h in enumerate(headers)}
    out: dict[str, int] = {}
    for field, candidates in (
        ("license", _LICENSE_KEYS),
        ("name", _NAME_KEYS),
        ("phone", _PHONE_KEYS),
        ("city", _CITY_KEYS),
        ("status", _STATUS_KEYS),
        ("classes", _CLASS_KEYS),
    ):
        for cand in candidates:
            for h, idx in norm.items():
                if cand == h or (cand in h and len(cand) >= 4):
                    out[field] = idx
                    break
            if field in out:
                break
    return out


# ─── Name normalization (for company-name lookup) ───────────────────


_STOPWORDS = {
    "inc", "llc", "corp", "co", "company", "construction", "contractor",
    "builders", "building", "general", "services", "group", "the", "and",
    "ltd", "enterprises", "enterprise",
}


def _normalize_name(name: str) -> str:
    """Lowercased, accent-stripped, alphanum-and-spaces, single-spaced."""
    if not name:
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def _name_key(name: str) -> str:
    """Drop legal-entity stopwords so 'Acme Builders Inc' and
    'Acme Builders LLC' map to the same key."""
    norm = _normalize_name(name)
    tokens = [t for t in norm.split() if t and t not in _STOPWORDS]
    return " ".join(tokens)


# ─── Loader ─────────────────────────────────────────────────────────


def _load(path: Path) -> tuple[dict[str, dict], dict[str, dict]]:
    by_license: dict[str, dict] = {}
    by_name: dict[str, dict] = {}

    with path.open("r", encoding="utf-8-sig", newline="", errors="replace") as fh:
        sample = fh.read(8192)
        fh.seek(0)
        delimiter = "," if sample.count(",") >= sample.count("\t") else "\t"
        reader = csv.reader(fh, delimiter=delimiter)
        headers = next(reader, None)
        if not headers:
            return by_license, by_name
        cols = _detect(headers)
        if "license" not in cols:
            logger.warning("[cslb_bulk] no license column in %s", path)
            return by_license, by_name

        for row in reader:
            if not row or cols["license"] >= len(row):
                continue
            lic = (row[cols["license"]] or "").strip()
            if not lic:
                continue

            def _val(field: str) -> str:
                idx = cols.get(field)
                if idx is None or idx >= len(row):
                    return ""
                return (row[idx] or "").strip()

            rec = {
                "license_number": lic,
                "business_name": _val("name"),
                "phone": _val("phone"),
                "city": _val("city"),
                "status": _val("status"),
                "classes": _val("classes"),
            }
            by_license[lic] = rec
            name_key = _name_key(rec["business_name"])
            if name_key and name_key not in by_name:
                by_name[name_key] = rec

    return by_license, by_name


def _ensure_loaded() -> bool:
    """Lazy-load the index. Returns True if we have data, False if the
    file is missing / unreadable (caller falls back to the scraper)."""
    global _BY_LICENSE, _BY_NORM_NAME, _LOADED_FROM
    if _BY_LICENSE is not None:
        return bool(_BY_LICENSE)

    path = Path(DEFAULT_BULK_PATH)
    if not path.exists():
        _BY_LICENSE = {}
        _BY_NORM_NAME = {}
        return False

    try:
        by_lic, by_name = _load(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[cslb_bulk] failed to load %s: %s", path, exc)
        _BY_LICENSE = {}
        _BY_NORM_NAME = {}
        return False

    _BY_LICENSE = by_lic
    _BY_NORM_NAME = by_name
    _LOADED_FROM = str(path)
    logger.info(
        "[cslb_bulk] loaded %s: %d licenses, %d unique names",
        path, len(by_lic), len(by_name),
    )
    return bool(by_lic)


# ─── Public API ─────────────────────────────────────────────────────


def is_available() -> bool:
    """True iff the bulk file is present and parsed at least one row."""
    return _ensure_loaded()


def info() -> dict[str, Any]:
    """Used by management commands and tests to introspect the index."""
    _ensure_loaded()
    return {
        "loaded_from": _LOADED_FROM,
        "licenses": len(_BY_LICENSE or {}),
        "name_keys": len(_BY_NORM_NAME or {}),
        "available": bool(_BY_LICENSE),
    }


def lookup(
    license_number: str | None = None, company_name: str | None = None
) -> dict[str, Any]:
    """Look up a contractor in the bulk index.

    Returns a dict shaped like the legacy scraper's output so the
    `permits_agent._cslb_lookup` cascade can swap backends without
    reshaping the data:

        {
          "phone": str, "cslb_name": str,
          "cslb_city": str, "cslb_status": str,
          "license_number": str, "classes": str,
        }

    Empty dict on miss / when the index isn't loaded.
    """
    if not _ensure_loaded():
        return {}

    rec = None
    if license_number:
        lic = re.sub(r"\D", "", str(license_number))
        rec = (_BY_LICENSE or {}).get(lic)

    if rec is None and company_name:
        key = _name_key(company_name)
        if key:
            rec = (_BY_NORM_NAME or {}).get(key)

    if not rec:
        return {}

    return {
        "phone": rec.get("phone", ""),
        "cslb_name": rec.get("business_name", ""),
        "cslb_city": rec.get("city", ""),
        "cslb_status": rec.get("status", ""),
        "license_number": rec.get("license_number", ""),
        "classes": rec.get("classes", ""),
    }


def reset_cache() -> None:
    """Test hook — drop the in-memory index so the next lookup re-loads."""
    global _BY_LICENSE, _BY_NORM_NAME, _LOADED_FROM
    _BY_LICENSE = None
    _BY_NORM_NAME = None
    _LOADED_FROM = None
