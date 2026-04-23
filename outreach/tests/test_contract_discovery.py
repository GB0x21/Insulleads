"""
Tests for contract-discovery agents + the discover_contracts/analyze_contract
task handlers.

Covers:
- SAM.gov HTTP response → BidCandidate normalization (pure fn, no Django).
- discover_contracts handler end-to-end with a patched agent: asserts Lead
  + Contract rows are created, ANALYZE_CONTRACT task is enqueued, and the
  source timestamps are updated. Uses pytest-django's ``db`` fixture.
- analyze_contract handler end-to-end with a fake LLM adapter that returns
  a canned scorecard/redlines dict.
"""
from __future__ import annotations

from datetime import datetime, timezone as _tz
from decimal import Decimal
from pathlib import Path

import pytest

from agents.contract_discovery import BidCandidate
from agents.contract_discovery import base as disc_base
from agents.contract_discovery.sam_gov import _to_candidate


# ─── SAM.gov normalization (pure) ────────────────────────────────────
def test_sam_gov_to_candidate_maps_fields():
    item = {
        "noticeId": "N-123",
        "title": "Insulation services at Site X",
        "fullParentPathName": "VA / Veterans Affairs",
        "uiLink": "https://sam.gov/opp/N-123/view",
        "responseDeadLine": "2026-05-15T17:00:00-05:00",
        "description": "Install R-30 batt insulation.",
        "placeOfPerformance": {"city": {"name": "Austin"}},
        "resourceLinks": ["https://example.com/solicitation.pdf"],
        "award": {"amount": "125000"},
    }
    cand = _to_candidate(item)
    assert cand.external_id == "N-123"
    assert cand.provider == "sam_gov"
    assert cand.title.startswith("Insulation")
    assert cand.agency.startswith("VA")
    assert cand.source_url.endswith("/view")
    assert cand.file_urls == ["https://example.com/solicitation.pdf"]
    assert cand.estimated_value_usd == Decimal("125000")
    assert cand.deadline is not None
    assert cand.deadline.tzinfo is not None
    assert cand.city == "Austin"


# ─── Factory dispatch ────────────────────────────────────────────────
def test_get_discovery_agent_dispatches_known_providers():
    assert disc_base.get_discovery_agent("sam_gov").provider == "sam_gov"
    assert disc_base.get_discovery_agent("web_search").provider == "web_search"
    assert disc_base.get_discovery_agent("state_bids").provider == "state_bids"
    assert disc_base.get_discovery_agent("construction").provider == "construction"


def test_get_discovery_agent_rejects_unknown_provider():
    with pytest.raises(ValueError, match="Unknown contract-discovery provider"):
        disc_base.get_discovery_agent("nope")


# ─── discover_contracts handler (needs DB) ───────────────────────────
@pytest.fixture
def contract_source(db):
    from outreach.models import Campaign, Source

    camp = Campaign.objects.create(
        name="test-camp",
        product_description="spray foam",
        target_market="Bay Area GCs",
    )
    source = Source.objects.create(
        key="sam-gov-test",
        kind="contract_bids",
        campaign=camp,
        interval_minutes=360,
        enabled=True,
        config={"provider": "sam_gov"},
    )
    return source


def _fake_agent(candidates: list[BidCandidate]):
    class _A:
        provider = "sam_gov"

        def discover(self, _config):
            return iter(candidates)

    return _A()


def test_discover_contracts_creates_lead_contract_and_enqueues_analysis(
    contract_source, monkeypatch, tmp_path, settings
):
    from outreach.models import Contract, Lead, Task
    from outreach.tasks import discover_contracts

    # Point the docs dir at a temp path so we don't touch real storage.
    settings.CONTRACTS = dict(getattr(settings, "CONTRACTS", {}) or {})
    settings.CONTRACTS["DOCS_DIR"] = str(tmp_path)
    settings.CONTRACTS["DISCOVERY_ENABLED"] = True
    settings.CONTRACTS["ANALYSIS_ENABLED"] = True

    cand = BidCandidate(
        external_id="N-42",
        provider="sam_gov",
        title="Attic insulation retrofit",
        agency="VA",
        source_url="https://sam.gov/opp/N-42",
        deadline=None,
        estimated_value_usd=Decimal("50000"),
        description="R-38 attic insulation.",
        city="Oakland",
        file_urls=[],  # no download → no ANALYZE_CONTRACT enqueued
        raw_payload={"noticeId": "N-42"},
    )
    monkeypatch.setattr(
        discover_contracts,
        "get_discovery_agent",
        lambda _provider: _fake_agent([cand]),
    )

    task = Task.objects.create(
        type=Task.Type.DISCOVER_CONTRACTS,
        campaign=contract_source.campaign,
        source=contract_source,
    )
    discover_contracts.handle(task)

    lead = Lead.objects.get(source=contract_source, external_id="N-42")
    assert lead.title == "Attic insulation retrofit"
    assert lead.stage == Lead.Stage.DISCOVERED

    contract = Contract.objects.get(source=contract_source, external_id="N-42")
    assert contract.lead_id == lead.id
    assert contract.agency == "VA"
    assert contract.provider == "sam_gov"
    # No attachments provided → stays in DISCOVERED and no analysis queued.
    assert contract.status == Contract.Status.DISCOVERED
    assert not Task.objects.filter(type=Task.Type.ANALYZE_CONTRACT).exists()

    contract_source.refresh_from_db()
    assert contract_source.last_run_at is not None


def test_discover_contracts_downloads_file_and_enqueues_analysis(
    contract_source, monkeypatch, tmp_path, settings
):
    from outreach.models import Contract, Task
    from outreach.tasks import discover_contracts

    settings.CONTRACTS = dict(getattr(settings, "CONTRACTS", {}) or {})
    settings.CONTRACTS["DOCS_DIR"] = str(tmp_path)
    settings.CONTRACTS["DISCOVERY_ENABLED"] = True
    settings.CONTRACTS["ANALYSIS_ENABLED"] = True
    settings.CONTRACTS["MAX_FILE_SIZE_MB"] = 1

    cand = BidCandidate(
        external_id="N-99",
        provider="sam_gov",
        title="Weatherization job",
        source_url="https://sam.gov/opp/N-99",
        file_urls=["https://example.com/rfp.pdf"],
        raw_payload={},
    )
    monkeypatch.setattr(
        discover_contracts,
        "get_discovery_agent",
        lambda _provider: _fake_agent([cand]),
    )

    class _FakeResp:
        status_code = 200

        def raise_for_status(self):
            pass

        def iter_content(self, _n):
            yield b"%PDF-1.4 fake\n"

    monkeypatch.setattr(
        discover_contracts.requests, "get", lambda *a, **kw: _FakeResp()
    )

    task = Task.objects.create(
        type=Task.Type.DISCOVER_CONTRACTS,
        campaign=contract_source.campaign,
        source=contract_source,
    )
    discover_contracts.handle(task)

    contract = Contract.objects.get(source=contract_source, external_id="N-99")
    assert contract.status == Contract.Status.DOWNLOADED
    assert contract.file_path
    assert Path(tmp_path / contract.file_path).exists()
    assert contract.file_size_bytes > 0
    assert contract.file_hash

    analysis = Task.objects.get(
        type=Task.Type.ANALYZE_CONTRACT,
        payload__contract_id=contract.pk,
    )
    assert analysis.status == Task.Status.PENDING


# ─── analyze_contract handler ────────────────────────────────────────
def test_analyze_contract_writes_scorecard_and_marks_completed(
    contract_source, monkeypatch, tmp_path, settings
):
    from outreach.models import ActionLog, Contract, Lead, Task
    from outreach.tasks import analyze_contract as ac

    settings.CONTRACTS = dict(getattr(settings, "CONTRACTS", {}) or {})
    settings.CONTRACTS["DOCS_DIR"] = str(tmp_path)
    settings.CONTRACTS["ANALYSIS_ENABLED"] = True

    # Seed lead + contract with a real text file on disk.
    lead = Lead.objects.create(
        external_id="L-1",
        source=contract_source,
        campaign=contract_source.campaign,
        title="Lead 1",
    )
    contract_dir = tmp_path / "c1"
    contract_dir.mkdir()
    doc = contract_dir / "contract.txt"
    doc.write_text("Sample contract text for the analyzer.")
    contract = Contract.objects.create(
        lead=lead,
        source=contract_source,
        external_id="C-1",
        provider="sam_gov",
        title="Sample",
        file_path="c1/contract.txt",
        status=Contract.Status.DOWNLOADED,
    )

    canned = {
        "overall_score": 82,
        "recommendation": "sign",
        "recommendation_reason": "Balanced terms.",
        "scorecard": {"pricing": 8, "liability": 8},
        "redlines": [],
    }

    class _FakeAdapter:
        name = "fake"

        def analyze_contract(self, _text, metadata=None):
            return canned

    monkeypatch.setattr(ac, "get_adapter", lambda: _FakeAdapter())

    task = Task.objects.create(
        type=Task.Type.ANALYZE_CONTRACT,
        campaign=contract_source.campaign,
        source=contract_source,
        lead=lead,
        payload={"contract_id": contract.pk},
    )
    ac.handle(task)

    contract.refresh_from_db()
    assert contract.status == Contract.Status.COMPLETED
    assert contract.recommendation == "sign"
    assert contract.scorecard == {"pricing": 8, "liability": 8}
    assert contract.raw_analysis["overall_score"] == 82
    assert contract.analyzed_at is not None

    assert ActionLog.objects.filter(
        lead=lead, payload__kind="contract_analyzed"
    ).exists()


def test_analyze_contract_marks_failed_when_file_missing(
    contract_source, tmp_path, settings
):
    from outreach.models import Contract, Lead, Task
    from outreach.tasks import analyze_contract as ac

    settings.CONTRACTS = dict(getattr(settings, "CONTRACTS", {}) or {})
    settings.CONTRACTS["DOCS_DIR"] = str(tmp_path)
    settings.CONTRACTS["ANALYSIS_ENABLED"] = True

    lead = Lead.objects.create(
        external_id="L-x",
        source=contract_source,
        campaign=contract_source.campaign,
        title="Lead",
    )
    contract = Contract.objects.create(
        lead=lead,
        source=contract_source,
        external_id="C-x",
        provider="sam_gov",
        file_path="nope/missing.pdf",
        status=Contract.Status.DOWNLOADED,
    )
    task = Task.objects.create(
        type=Task.Type.ANALYZE_CONTRACT,
        campaign=contract_source.campaign,
        source=contract_source,
        lead=lead,
        payload={"contract_id": contract.pk},
    )
    ac.handle(task)

    contract.refresh_from_db()
    assert contract.status == Contract.Status.FAILED
    assert "missing" in contract.error_message
