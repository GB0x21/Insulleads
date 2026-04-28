"""
Tests for the pydantic-ai-backed :class:`AnthropicAdapter` lead methods
(enrich / qualify / write_outreach / write_email).

We construct the adapter with a stub API key so it builds normally, then
replace its cached pydantic-ai Agents with `TestModel`-backed ones that
return canned outputs. This exercises the real adapter wiring (Agent
factory, output unpacking, validation, defensive checks) without hitting
the network.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from outreach.llm.client import AnthropicAdapter
from outreach.llm.schemas import (
    EmailDraft,
    EnrichmentResult,
    OutreachMessage,
    QualificationResult,
)


# ─── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def adapter():
    a = AnthropicAdapter({"ANTHROPIC_API_KEY": "sk-test-stub"})
    try:
        yield a
    finally:
        a.close()


def _stub_lead(**overrides):
    base = dict(
        pk=1,
        title="Insulation re-roof",
        address="100 Main St",
        city="Oakland",
        project_type="reroof",
        project_value=120_000,
        description="Full reroof + R-38 attic.",
        contact_company="Acme Builders",
        contact_name="Jane Doe",
        contact_phone="",
        contact_email="",
        lead_score=72,
    )
    base.update(overrides)
    base["source"] = SimpleNamespace(kind="permits")
    return SimpleNamespace(**base)


def _stub_campaign(**overrides):
    base = dict(
        pk=42,
        name="Bay Area Insulation",
        product_description="Spray-foam and batt insulation.",
        target_market="Bay Area GCs and remodelers.",
        outreach_tone="professional",
        booking_link="https://cal.example/insul",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _override_agent(adapter: AnthropicAdapter, cache_key: str, output, output_type):
    """Replace the cached agent at `cache_key` with a TestModel-backed one."""
    adapter._agents[cache_key] = Agent(
        TestModel(custom_output_args=output.model_dump()),
        output_type=output_type,
    )


# ─── enrich_contact ───────────────────────────────────────────────────


def test_enrich_contact_returns_only_non_empty_fields(adapter):
    canned = EnrichmentResult(
        contact_phone="555-1212",
        contact_email="ops@acme.example",
        notes="Verified via CSLB lookup",
    )
    _override_agent(adapter, "enrich", canned, EnrichmentResult)

    out = adapter.enrich_contact(_stub_lead())

    assert out == {
        "contact_phone": "555-1212",
        "contact_email": "ops@acme.example",
        "notes": "Verified via CSLB lookup",
    }


def test_enrich_contact_swallows_runtime_errors(adapter):
    class _Boom:
        def run_sync(self, *_a, **_k):
            raise RuntimeError("API down")

    adapter._agents["enrich"] = _Boom()

    assert adapter.enrich_contact(_stub_lead()) == {}


# ─── qualify_lead ─────────────────────────────────────────────────────


def test_qualify_lead_returns_clamped_score_and_reason(adapter):
    campaign = _stub_campaign()
    canned = QualificationResult(score=0.84, reason="Active permit, big job, in Oakland.")
    _override_agent(adapter, f"qualify:{campaign.pk}", canned, QualificationResult)

    score, reason = adapter.qualify_lead(_stub_lead(), campaign)

    assert score == pytest.approx(0.84)
    assert reason.startswith("Active permit")


def test_qualify_lead_clamps_score_to_unit_interval(adapter):
    campaign = _stub_campaign()
    # Schema enforces 0-1 already, but the adapter clamps defensively.
    canned = QualificationResult(score=1.0, reason="x")
    _override_agent(adapter, f"qualify:{campaign.pk}", canned, QualificationResult)

    score, _ = adapter.qualify_lead(_stub_lead(), campaign)
    assert 0.0 <= score <= 1.0


def test_qualify_lead_returns_none_on_failure(adapter):
    campaign = _stub_campaign()

    class _Boom:
        def run_sync(self, *_a, **_k):
            raise RuntimeError("boom")

    adapter._agents[f"qualify:{campaign.pk}"] = _Boom()

    assert adapter.qualify_lead(_stub_lead(), campaign) == (None, "")


# ─── write_outreach ───────────────────────────────────────────────────


def test_write_outreach_returns_body(adapter):
    campaign = _stub_campaign()
    canned = OutreachMessage(body="Hi Jane — saw the reroof permit in Oakland…")
    _override_agent(
        adapter, f"writer:{campaign.pk}:professional", canned, OutreachMessage
    )

    body = adapter.write_outreach(_stub_lead(), campaign)

    assert body and "Oakland" in body


def test_write_outreach_returns_none_on_empty_body(adapter):
    campaign = _stub_campaign()
    canned = OutreachMessage(body="   ")
    _override_agent(
        adapter, f"writer:{campaign.pk}:professional", canned, OutreachMessage
    )

    assert adapter.write_outreach(_stub_lead(), campaign) is None


# ─── write_email ──────────────────────────────────────────────────────


def test_write_email_returns_subject_and_body(adapter):
    campaign = _stub_campaign()
    canned = EmailDraft(
        subject="Quick question on your Oakland reroof",
        body="Hi Jane,\n\nI saw the $120k reroof permit you pulled…",
    )
    _override_agent(
        adapter, f"email:{campaign.pk}:professional", canned, EmailDraft
    )

    out = adapter.write_email(_stub_lead(), campaign)
    assert out is not None
    assert out["subject"].startswith("Quick question")
    assert "Hi Jane" in out["body"]


def test_write_email_returns_none_when_subject_or_body_missing(adapter):
    campaign = _stub_campaign()
    canned = EmailDraft(subject="", body="some body")
    _override_agent(
        adapter, f"email:{campaign.pk}:professional", canned, EmailDraft
    )

    assert adapter.write_email(_stub_lead(), campaign) is None


# ─── _client backdoor preserved for contract_discovery ────────────────


def test_adapter_exposes_raw_anthropic_client(adapter):
    """`agents/contract_discovery/web_search.py` reaches in for `_client`
    to use Anthropic's native web_search tool — keep that contract."""
    assert adapter._client is not None
    assert hasattr(adapter._client, "messages")
