"""
Tests for `utils.cslb_bulk`.

We feed the loader a tiny CSV via tmp_path so the test runs anywhere
(no network, no need for the actual CSLB extract). The integration
with `permits_agent._enrich_gc` is exercised in test_permits_enrichment
by patching the lookup helpers directly.
"""
from __future__ import annotations

import pytest

from utils import cslb_bulk


_CSV = """LicenseNumber,BusinessName,BusinessPhone,BusinessCity,PrimaryStatus,Classifications
1098765,SPEED CONSTRUCTION INC,4087541800,SAN JOSE,CLEAR,B
123456,Acme Builders Inc,4155551212,OAKLAND,CLEAR,B C-2
999111,Beta Roofing LLC,,RICHMOND,SUSPENDED,C-39
"""


@pytest.fixture(autouse=True)
def _reset_cache():
    cslb_bulk.reset_cache()
    yield
    cslb_bulk.reset_cache()


def _write(tmp_path, csv: str = _CSV):
    p = tmp_path / "cslb_bulk.csv"
    p.write_text(csv, encoding="utf-8")
    return p


# ─── info / availability ─────────────────────────────────────────────


def test_index_unavailable_when_file_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH",
                        str(tmp_path / "missing.csv"))
    assert cslb_bulk.is_available() is False
    info = cslb_bulk.info()
    assert info["licenses"] == 0


def test_info_reports_loaded_counts(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    info = cslb_bulk.info()
    assert info["available"] is True
    assert info["licenses"] == 3
    assert info["name_keys"] == 3


# ─── lookup by license number ────────────────────────────────────────


def test_lookup_by_license_returns_phone_and_metadata(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    rec = cslb_bulk.lookup(license_number="1098765")
    assert rec["phone"] == "4087541800"
    assert rec["cslb_name"] == "SPEED CONSTRUCTION INC"
    assert rec["cslb_city"] == "SAN JOSE"
    assert rec["cslb_status"] == "CLEAR"
    assert rec["classes"] == "B"


def test_lookup_strips_non_digits_from_license(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    # Operator might pass "Lic. 1098765" or "#1098765"
    assert cslb_bulk.lookup(license_number="#1098765")["cslb_name"] == \
        "SPEED CONSTRUCTION INC"


# ─── lookup by company name (with stopword stripping) ───────────────


def test_lookup_by_name_drops_legal_entity_suffix(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    # Stored as "Acme Builders Inc"; lookup with "Acme Builders LLC"
    # should hit because Inc/LLC are stopworded.
    rec = cslb_bulk.lookup(company_name="Acme Builders LLC")
    assert rec["license_number"] == "123456"


def test_lookup_misses_when_name_has_no_overlap(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    assert cslb_bulk.lookup(company_name="Totally Different Corp") == {}


# ─── empty queries ───────────────────────────────────────────────────


def test_empty_query_returns_empty(monkeypatch, tmp_path):
    p = _write(tmp_path)
    monkeypatch.setattr(cslb_bulk, "DEFAULT_BULK_PATH", str(p))
    assert cslb_bulk.lookup() == {}
    assert cslb_bulk.lookup(license_number="", company_name="") == {}
