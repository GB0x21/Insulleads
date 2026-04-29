"""
Tests for the 5-tier contact-enrichment cascade in
``agents.permits_agent.PermitsAgent._enrich_gc``.

Every tier is mocked independently so each case isolates exactly which
backend should fire. The behavioural change under test is the move
from "short-circuit at first phone/email" to "fill missing fields"
across the whole cascade.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

import agents.permits_agent as pa


# ─── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def agent(monkeypatch):
    """Build a real PermitsAgent with the contacts loader stubbed so
    the test doesn't read filesystem CSVs."""
    monkeypatch.setattr(pa, "load_all_contacts", lambda: [])
    a = pa.PermitsAgent.__new__(pa.PermitsAgent)
    a._contacts = []
    a._cslb_cache = {}
    return a


def _lead(**overrides):
    base = dict(
        id="SF_001",
        city="San Francisco",
        contractor="Acme Builders Inc",
        lic_number="123456",
        owner="Jane Owner LLC",
        permit_type="additions alterations or repairs",
        description="Reroof + R-38 attic insulation",
        value_float=120_000,
        issued_date="2026-04-01",
    )
    base.update(overrides)
    return base


# ─── Tier 1 alone — CSV ──────────────────────────────────────────────


def test_csv_match_phone_only_does_not_short_circuit_cslb(agent, monkeypatch):
    """Pre-fix bug: when CSV produced a phone, the cascade returned
    immediately and we lost the license number / status. The fix tries
    CSLB even when CSV gave us a phone."""
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: {
        "raw_name": "Acme Builders Inc",
        "phone": "555-1212",
        "email": "",
        "source": "B_CONTACTS_GC.csv",
    })
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {
        "phone": "555-9999",  # CSLB also has a phone (we should NOT overwrite)
        "cslb_name": "Acme Builders Inc",
        "cslb_city": "Oakland",
        "cslb_status": "Active",
    })
    # No external APIs / LLM
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", False)
    with patch("utils.contact_enrichment.enrich_contact", return_value={}):
        out = agent._enrich_gc(_lead())

    # CSV phone preserved (we don't overwrite earlier tiers)
    assert out["contact_phone"] == "555-1212"
    # CSLB still consulted — license status / city now in payload
    assert out["cslb_city"] == "Oakland"
    assert out["cslb_status"] == "Active"
    # Source string lists both tiers
    assert "CSV" in out["contact_source"]
    assert "CSLB" in out["contact_source"]


# ─── Tier 3 — Hunter.io fills missing email ──────────────────────────


def test_hunter_fills_missing_email(agent, monkeypatch):
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: {
        "raw_name": "Acme Builders Inc",
        "phone": "555-1212",
        "email": "",
        "source": "csv1.csv",
    })
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", False)
    with patch("utils.contact_enrichment.enrich_contact", return_value={
        "email": "ops@acme.example",
        "phone": "",
        "position": "Owner",
        "linkedin_url": "https://linkedin.com/company/acme",
        "source": "Hunter.io",
    }):
        out = agent._enrich_gc(_lead())

    assert out["contact_email"] == "ops@acme.example"
    assert out["position"] == "Owner"
    assert out["linkedin"] == "https://linkedin.com/company/acme"
    assert "Hunter.io" in out["contact_source"]
    # Phone from CSV still preserved
    assert out["contact_phone"] == "555-1212"


def test_hunter_skipped_when_email_already_filled(agent, monkeypatch):
    """If CSV already gave us an email, don't burn a Hunter.io credit."""
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: {
        "raw_name": "Acme Builders Inc",
        "phone": "555-1212",
        "email": "info@acme.example",
        "source": "csv1.csv",
    })
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", False)
    with patch("utils.contact_enrichment.enrich_contact") as ext:
        ext.return_value = {}
        agent._enrich_gc(_lead())
    assert ext.call_count == 0


# ─── Tier 5 — LLM web_search fills website ──────────────────────────


def test_llm_tier_fills_website_and_license_when_still_missing(agent, monkeypatch):
    """When everything except website/license is filled, the LLM
    fills those without overwriting earlier-tier fields."""
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: {
        "raw_name": "Acme Builders Inc",
        "phone": "555-1212",
        "email": "info@acme.example",
        "source": "csv1.csv",
    })
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", True)

    class _StubAdapter:
        name = "anthropic"
        calls = 0

        def enrich_contact(self, _stub_lead):
            type(self).calls += 1
            return {
                "website": "https://acmebuilders.example",
                "cslb_license": "987654",
                "contact_name": "Jane Doe",
            }

    with patch("utils.contact_enrichment.enrich_contact", return_value={}):
        monkeypatch.setattr("outreach.llm.get_adapter", lambda: _StubAdapter())
        out = agent._enrich_gc(_lead(lic_number=""))

    assert _StubAdapter.calls == 1
    assert out["website"] == "https://acmebuilders.example"
    assert out["lic_number"] == "987654"  # original was empty, LLM filled it
    # contact_name was already set by the CSV tier — fill-missing rule
    # keeps the earlier value, the LLM doesn't overwrite.
    assert out["contact_name"] == "Acme Builders Inc"
    assert "llm:anthropic" in out["contact_source"]


def test_llm_tier_skipped_when_disabled_via_env(agent, monkeypatch):
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: None)
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", False)
    with patch("utils.contact_enrichment.enrich_contact", return_value={}), \
         patch("outreach.llm.get_adapter") as ga:
        agent._enrich_gc(_lead())
    assert ga.call_count == 0


def test_llm_tier_skipped_when_adapter_is_noop(agent, monkeypatch):
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: None)
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", True)

    class _Noop:
        name = "noop"
        enrich_calls = 0

        def enrich_contact(self, _l):
            type(self).enrich_calls += 1
            return {}

    noop = _Noop()
    with patch("utils.contact_enrichment.enrich_contact", return_value={}):
        monkeypatch.setattr("outreach.llm.get_adapter", lambda: noop)
        agent._enrich_gc(_lead())
    assert _Noop.enrich_calls == 0


# ─── Cache + early returns ───────────────────────────────────────────


def test_no_search_name_no_lookup(agent, monkeypatch):
    """Lead with no contractor and no owner → nothing to enrich."""
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: None)
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    out = agent._enrich_gc(_lead(contractor="", owner="", lic_number=""))
    assert out == {}


def test_cache_hit_avoids_recall(agent, monkeypatch):
    monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: {
        "raw_name": "Acme Builders Inc",
        "phone": "555-1212",
        "email": "",
        "source": "csv1.csv",
    })
    monkeypatch.setattr(pa, "_cslb_lookup", lambda **_kw: {})
    monkeypatch.setattr(pa, "_LLM_ENRICH_ENABLED", False)
    with patch("utils.contact_enrichment.enrich_contact", return_value={}):
        out1 = agent._enrich_gc(_lead())
        # Mutating cached result through a re-call should hit the cache,
        # not re-run any tier — patching lookup_contact to None confirms.
        monkeypatch.setattr(pa, "lookup_contact", lambda *_a, **_k: None)
        out2 = agent._enrich_gc(_lead())
    assert out1 == out2
    assert out2["contact_phone"] == "555-1212"


# ─── _merge_source ───────────────────────────────────────────────────


def test_merge_source_appends_without_dupes():
    assert pa._merge_source(None, "CSV") == "CSV"
    assert pa._merge_source("CSV", "CSLB") == "CSV + CSLB"
    assert pa._merge_source("CSV + CSLB", "CSLB") == "CSV + CSLB"
    assert pa._merge_source("CSV", "") == "CSV"


# ─── _LegacyLeadStub ─────────────────────────────────────────────────


def test_legacy_lead_stub_exposes_attrs_the_llm_enricher_reads():
    lead = _lead()
    stub = pa._LegacyLeadStub(lead, "Acme Builders Inc", {})
    # All attrs `outreach.llm.prompts.lead_payload` reads:
    for attr in ("title", "address", "city", "project_type",
                 "project_value", "description", "contact_company",
                 "contact_name", "contact_phone", "contact_email",
                 "lead_score"):
        assert hasattr(stub, attr), f"missing {attr}"
    assert stub.source.kind == "permits"
    assert stub.contact_company == "Acme Builders Inc"
    assert stub.project_value == 120_000.0
