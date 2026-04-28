"""
Tests for the Telegram-bound message formatting in `outreach.tasks.outreach`.

We exercise the formatters with `SimpleNamespace` lead stubs so the tests
don't need a Django DB. The behavior under test is the change that
restores actionable contact data alongside the LLM-written outreach
prose: every Telegram message must include a context block with phone,
email, address, scores, source URL and lead id below the prose.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from outreach.tasks.outreach import (
    _extract_source_url,
    _format_context_block,
    _send,
)


def _stub_lead(**overrides):
    base = dict(
        pk=42,
        title="reroof + frame — 4211 26th",
        address="4211 26th St",
        city="San Francisco",
        project_type="reroof",
        project_value=480_000,
        description="",
        contact_name="Jane Doe",
        contact_company="Acme Builders",
        contact_phone="(415) 555-1212",
        contact_email="jane@acme.example",
        contact_source="CSLB",
        qualification_score=0.78,
        qualification_variance=0.04,
        lead_score=72,
        llm_qualification_reason="Active permit, big job, in SF.",
        llm_outreach_body="Hi — saw the 4211 26th project. Insulleads.",
        raw={"source_url": "https://data.sfgov.org/permits/abc123"},
    )
    base.update(overrides)
    base["source"] = SimpleNamespace(kind="permits")
    return SimpleNamespace(**base)


# ─── _extract_source_url ─────────────────────────────────────────────


def test_extract_source_url_prefers_explicit_source_url():
    lead = _stub_lead(raw={
        "source_url": "https://data.example/permit/1",
        "url": "https://other.example/2",
    })
    assert _extract_source_url(lead) == "https://data.example/permit/1"


def test_extract_source_url_falls_back_through_known_keys():
    lead = _stub_lead(raw={"yelp_url": "https://www.yelp.com/biz/acme"})
    assert _extract_source_url(lead) == "https://www.yelp.com/biz/acme"


def test_extract_source_url_ignores_non_http_values():
    lead = _stub_lead(raw={"source_url": "not-a-url", "url": "ftp://x"})
    assert _extract_source_url(lead) == ""


def test_extract_source_url_handles_empty_raw():
    lead = _stub_lead(raw={})
    assert _extract_source_url(lead) == ""


# ─── _format_context_block ────────────────────────────────────────────


def test_context_block_contains_all_lead_metadata():
    block = _format_context_block(_stub_lead())

    assert "4211 26th St, San Francisco" in block
    assert "reroof" in block
    assert "$480,000" in block
    assert "Acme Builders" in block and "Jane Doe" in block
    assert "(415) 555-1212" in block
    assert "jane@acme.example" in block
    assert "CSLB" in block
    assert "0.78" in block
    assert "72/100" in block
    assert "permits" in block
    assert "Active permit" in block
    assert "https://data.sfgov.org/permits/abc123" in block
    assert "#L42" in block


def test_context_block_drops_missing_fields():
    lead = _stub_lead(
        contact_name="",
        contact_company="",
        contact_phone="",
        contact_email="",
        contact_source="",
        qualification_score=None,
        qualification_variance=None,
        llm_qualification_reason="",
        project_value=None,
        raw={},
    )
    block = _format_context_block(lead)

    # No contact fields → explicit prompt to enrich.
    assert "no contact data" in block
    # No score → only heuristic + source kind.
    assert "0.78" not in block
    assert "72/100" in block
    # No URL line.
    assert "https://" not in block
    # Lead id is always present.
    assert "#L42" in block


def test_context_block_truncates_long_qualification_reason():
    lead = _stub_lead(llm_qualification_reason="x" * 500)
    block = _format_context_block(lead)
    assert "…" in block
    assert "x" * 500 not in block


def test_context_block_truncates_very_long_url():
    lead = _stub_lead(raw={"source_url": "https://example.com/" + "a" * 200})
    block = _format_context_block(lead)
    assert "…" in block
    # The original full URL should not appear in full (truncated for display).
    assert "a" * 200 not in block


# ─── _send composes prose + context ───────────────────────────────────


def test_send_combines_llm_prose_and_context_block():
    lead = _stub_lead()
    campaign = SimpleNamespace(
        pk=1,
        llm_writer_enabled=False,  # use cached llm_outreach_body
        max_outreach_per_day=200,
    )
    sent: list[str] = []

    def _capture(text: str, **_kw) -> bool:
        sent.append(text)
        return True

    with patch("utils.telegram.send_message", side_effect=_capture):
        action = _send(lead, campaign)

    assert action == "telegram"
    assert len(sent) == 1
    msg = sent[0]
    # Prose is at the top.
    assert msg.startswith("Hi — saw the 4211 26th project. Insulleads.")
    # Visual separator between prose and context.
    assert "──────────" in msg
    # Context fields are present.
    assert "(415) 555-1212" in msg
    assert "jane@acme.example" in msg
    assert "Acme Builders" in msg
    assert "#L42" in msg
