"""
Tests for `outreach.tasks.enrich`.

Two layers:

1. Pure-logic tests for the cooldown / max-attempts gates — no DB.
2. End-to-end tests for `enrich_one` and the outreach inline-enrichment
   gate, using `@pytest.mark.django_db` and a stub LLM adapter.
"""
from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from outreach.llm.adapter import LLMAdapter, NoopAdapter
from outreach.tasks.enrich import (
    _is_cooldown_elapsed,
    _should_retry,
    enrich_one,
)


# ─── Stub adapter ─────────────────────────────────────────────────────


class _StubAdapter(LLMAdapter):
    name = "anthropic"  # so the noop short-circuit doesn't fire

    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def enrich_contact(self, lead):
        self.calls += 1
        return dict(self.payload)

    def qualify_lead(self, lead, campaign):  # pragma: no cover
        return (None, "")

    def write_outreach(self, lead, campaign):  # pragma: no cover
        return None

    def write_email(self, lead, campaign):  # pragma: no cover
        return None


# ─── 1. Pure-logic gate tests ─────────────────────────────────────────


def test_is_cooldown_elapsed_when_no_log():
    assert _is_cooldown_elapsed({}) is True


def test_is_cooldown_elapsed_with_old_attempt():
    old = (timezone.now() - timedelta(hours=48)).isoformat()
    assert _is_cooldown_elapsed({"last_attempt_at": old}) is True


def test_is_cooldown_elapsed_with_recent_attempt():
    recent = (timezone.now() - timedelta(minutes=5)).isoformat()
    assert _is_cooldown_elapsed({"last_attempt_at": recent}) is False


def test_should_retry_only_for_empty_status():
    old = (timezone.now() - timedelta(days=2)).isoformat()
    assert _should_retry(
        {"status": "empty", "attempts": 1, "last_attempt_at": old}
    ) is True
    # ok-status leads are never retried via this path
    assert _should_retry(
        {"status": "ok", "attempts": 1, "last_attempt_at": old}
    ) is False


def test_should_retry_respects_max_attempts(settings):
    settings.OUTREACH = {**settings.OUTREACH, "ENRICH_MAX_ATTEMPTS": 3}
    old = (timezone.now() - timedelta(days=2)).isoformat()
    log = {"status": "empty", "attempts": 3, "last_attempt_at": old}
    assert _should_retry(log) is False


def test_should_retry_blocked_by_cooldown():
    recent = (timezone.now() - timedelta(minutes=10)).isoformat()
    log = {"status": "empty", "attempts": 1, "last_attempt_at": recent}
    assert _should_retry(log) is False


# ─── 2. enrich_one end-to-end (uses DB) ───────────────────────────────


@pytest.fixture
def lead(db):
    """A minimal Lead with no contact info, attached to a Source/Campaign."""
    from outreach.models import Campaign, Lead, Source

    campaign = Campaign.objects.create(
        name="t",
        product_description="x",
        target_market="y",
    )
    source = Source.objects.create(
        key="t-permits", kind="permits", campaign=campaign
    )
    return Lead.objects.create(
        external_id="ext-1",
        source=source,
        campaign=campaign,
        title="reroof",
        address="1 Main St",
        city="Oakland",
    )


def test_enrich_one_writes_contact_fields_on_success(lead):
    adapter = _StubAdapter({"contact_phone": "555-1212",
                            "contact_email": "ops@ex.com"})

    changed = enrich_one(lead, adapter)

    lead.refresh_from_db()
    assert changed is True
    assert lead.contact_phone == "555-1212"
    assert lead.contact_email == "ops@ex.com"
    assert lead.enrichment_log["status"] == "ok"
    assert lead.enrichment_log["attempts"] == 1
    assert "last_attempt_at" in lead.enrichment_log
    assert lead.contact_source.startswith("llm:")


def test_enrich_one_records_empty_status_and_increments_attempts(lead):
    adapter = _StubAdapter({})

    changed = enrich_one(lead, adapter)

    lead.refresh_from_db()
    assert changed is False
    assert lead.contact_phone == ""
    assert lead.enrichment_log["status"] == "empty"
    assert lead.enrichment_log["attempts"] == 1


def test_enrich_one_skips_when_lead_already_has_phone_and_email(lead):
    lead.contact_phone = "555-9999"
    lead.contact_email = "x@y.com"
    lead.save()
    adapter = _StubAdapter({"contact_phone": "555-1212"})

    changed = enrich_one(lead, adapter)

    assert changed is False
    assert adapter.calls == 0


def test_enrich_one_respects_cooldown_for_empty_status(lead):
    """A recent empty attempt blocks the retry until cooldown elapses."""
    lead.enrichment_log = {
        "status": "empty",
        "attempts": 1,
        "last_attempt_at": timezone.now().isoformat(),
    }
    lead.save()
    adapter = _StubAdapter({"contact_phone": "555-1212"})

    changed = enrich_one(lead, adapter)

    assert changed is False
    assert adapter.calls == 0  # cooldown blocks the call entirely


def test_enrich_one_force_ignores_cooldown(lead):
    lead.enrichment_log = {
        "status": "empty",
        "attempts": 1,
        "last_attempt_at": timezone.now().isoformat(),
    }
    lead.save()
    adapter = _StubAdapter({"contact_phone": "555-1212"})

    changed = enrich_one(lead, adapter, force=True)

    lead.refresh_from_db()
    assert changed is True
    assert lead.contact_phone == "555-1212"
    assert lead.enrichment_log["attempts"] == 2
    assert lead.enrichment_log["status"] == "ok"
    assert adapter.calls == 1


def test_enrich_one_retries_after_cooldown_elapses(lead, settings):
    settings.OUTREACH = {**settings.OUTREACH, "ENRICH_RETRY_HOURS": 1}
    lead.enrichment_log = {
        "status": "empty",
        "attempts": 1,
        "last_attempt_at": (
            timezone.now() - timedelta(hours=2)
        ).isoformat(),
    }
    lead.save()
    adapter = _StubAdapter({"contact_phone": "555-1212"})

    changed = enrich_one(lead, adapter)

    lead.refresh_from_db()
    assert changed is True
    assert lead.contact_phone == "555-1212"
    assert lead.enrichment_log["attempts"] == 2


def test_enrich_one_recovers_from_existing_ok_log(lead):
    """If a prior attempt produced contact data but write-back failed,
    `enrich_one` recovers from the log without calling the LLM again."""
    lead.enrichment_log = {
        "status": "ok",
        "adapter": "anthropic",
        "attempts": 1,
        "last_attempt_at": timezone.now().isoformat(),
        "contact_phone": "555-3333",
    }
    lead.save()
    adapter = _StubAdapter({})  # would return empty if called

    changed = enrich_one(lead, adapter)

    lead.refresh_from_db()
    assert changed is True
    assert lead.contact_phone == "555-3333"
    assert adapter.calls == 0  # log recovery, no LLM call


def test_enrich_one_noop_adapter_is_a_no_op(lead):
    changed = enrich_one(lead, NoopAdapter())
    assert changed is False
    assert lead.enrichment_log == {}


# ─── 3. Outreach gating + inline enrichment ──────────────────────────


def _make_lead_with_campaign(*, contact_phone="", contact_email=""):
    from outreach.models import Campaign, Lead, Source

    campaign = Campaign.objects.create(
        name="t",
        product_description="x",
        target_market="y",
        max_outreach_per_day=10,
    )
    source = Source.objects.create(
        key="t-permits", kind="permits", campaign=campaign
    )
    lead = Lead.objects.create(
        external_id="ext-1",
        source=source,
        campaign=campaign,
        title="reroof",
        contact_phone=contact_phone,
        contact_email=contact_email,
        stage="QUALIFIED",
        qualification_score=0.8,
        lead_score=70,
    )
    return campaign, lead


def _make_task(campaign):
    from outreach.models import Task

    return Task.objects.create(
        type=Task.Type.OUTREACH, campaign=campaign, scheduled_at=timezone.now()
    )


@pytest.mark.django_db
def test_outreach_skips_lead_without_contact_when_enrich_returns_empty(
    monkeypatch,
):
    """Lead has no contact, inline enrich finds nothing → lead skipped,
    no Telegram call, lead stays QUALIFIED."""
    from outreach.tasks import outreach as outreach_task

    campaign, lead = _make_lead_with_campaign()
    task = _make_task(campaign)

    sent_messages: list[str] = []
    monkeypatch.setattr(
        "utils.telegram.send_message",
        lambda text, **_: (sent_messages.append(text), True)[1],
    )
    # Inline enrich call: stub adapter returns empty.
    monkeypatch.setattr(
        outreach_task,
        "get_adapter",
        lambda: _StubAdapter({}),
    )
    # Make the inline path use our stub adapter via enrich_one.
    monkeypatch.setattr(
        "outreach.tasks.enrich.get_adapter",
        lambda: _StubAdapter({}),
    )

    outreach_task.handle(task)

    lead.refresh_from_db()
    assert sent_messages == []
    assert lead.stage == "QUALIFIED"
    assert lead.enrichment_log.get("status") == "empty"


@pytest.mark.django_db
def test_outreach_inline_enriches_then_sends(monkeypatch):
    """Lead has no contact, inline enrich finds phone → Telegram fires."""
    from outreach.tasks import outreach as outreach_task

    campaign, lead = _make_lead_with_campaign()
    task = _make_task(campaign)

    sent: list[str] = []
    monkeypatch.setattr(
        "utils.telegram.send_message",
        lambda text, **_: (sent.append(text), True)[1],
    )
    stub = _StubAdapter({"contact_phone": "555-7777"})
    monkeypatch.setattr(outreach_task, "get_adapter", lambda: stub)
    monkeypatch.setattr("outreach.tasks.enrich.get_adapter", lambda: stub)

    outreach_task.handle(task)

    lead.refresh_from_db()
    assert lead.contact_phone == "555-7777"
    assert lead.stage == "CONTACTED"
    assert len(sent) == 1
    assert "555-7777" in sent[0]


@pytest.mark.django_db
def test_outreach_sends_lead_that_already_has_contact(monkeypatch):
    """No inline enrichment when contact data is already present."""
    from outreach.tasks import outreach as outreach_task

    campaign, lead = _make_lead_with_campaign(contact_phone="555-0000")
    task = _make_task(campaign)

    sent: list[str] = []
    monkeypatch.setattr(
        "utils.telegram.send_message",
        lambda text, **_: (sent.append(text), True)[1],
    )
    stub = _StubAdapter({"contact_phone": "555-NEW"})
    monkeypatch.setattr(outreach_task, "get_adapter", lambda: stub)
    monkeypatch.setattr("outreach.tasks.enrich.get_adapter", lambda: stub)

    outreach_task.handle(task)

    lead.refresh_from_db()
    # Original phone preserved (we don't overwrite existing contact data).
    assert lead.contact_phone == "555-0000"
    assert lead.stage == "CONTACTED"
    assert stub.calls == 0  # inline enrich not called
