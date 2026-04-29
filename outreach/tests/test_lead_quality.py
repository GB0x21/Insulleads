"""
Tests for `outreach.lead_quality.contact_quality_score`.

Covers both call shapes the helper has to support:

  * Plain dicts produced by the legacy `agents/` path. Website /
    linkedin live as top-level dict keys filled by the enrichment
    cascade.
  * Django `Lead`-like objects produced by the pipeline path. Here
    contact_phone / contact_email are model attributes, while website
    and linkedin are sidecar JSON inside `Lead.enrichment_log`.
"""
from __future__ import annotations

from types import SimpleNamespace

from outreach.lead_quality import contact_quality_label, contact_quality_score


# ─── Plain-dict (legacy permits_agent path) ──────────────────────────


def test_dict_zero_when_no_channels():
    assert contact_quality_score({}) == 0


def test_dict_counts_phone_and_email():
    assert contact_quality_score({
        "contact_phone": "555-1212", "contact_email": "x@y.com",
    }) == 2


def test_dict_counts_website_and_linkedin_at_top_level():
    """The legacy enrichment cascade writes website/linkedin as
    top-level keys on the lead dict."""
    assert contact_quality_score({
        "contact_phone": "555-1212",
        "contact_email": "x@y.com",
        "website": "https://acme.example",
        "linkedin": "https://linkedin.com/company/acme",
    }) == 4


def test_dict_blank_strings_dont_count():
    """Empty strings are the default placeholder for missing fields,
    they must not bump the score."""
    assert contact_quality_score({
        "contact_phone": "", "contact_email": "", "website": "",
    }) == 0


# ─── Django-Lead-like (outreach.tasks.outreach path) ─────────────────


def _stub_lead(**overrides):
    base = dict(
        contact_phone="", contact_email="",
        enrichment_log={},
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_django_lead_counts_attribute_phone_and_email():
    lead = _stub_lead(contact_phone="555-1212", contact_email="x@y.com")
    assert contact_quality_score(lead) == 2


def test_django_lead_reads_website_from_enrichment_log():
    """The Django enrich task writes website/linkedin into the
    `enrichment_log` JSONField rather than a Lead column. The score
    must read both."""
    lead = _stub_lead(
        contact_phone="555-1212",
        contact_email="x@y.com",
        enrichment_log={
            "status": "ok",
            "website": "https://acme.example",
            "linkedin": "https://linkedin.com/in/janedoe",
        },
    )
    assert contact_quality_score(lead) == 4


def test_django_lead_partial_enrichment():
    """Only website filled in the log → reachability=3."""
    lead = _stub_lead(
        contact_phone="555-1212",
        contact_email="x@y.com",
        enrichment_log={"status": "ok", "website": "https://acme.example"},
    )
    assert contact_quality_score(lead) == 3


def test_django_lead_handles_missing_enrichment_log():
    """A pre-enrichment lead has `enrichment_log = {}` (model default)."""
    lead = _stub_lead(contact_phone="555-1212")
    assert contact_quality_score(lead) == 1


# ─── Label formatting ────────────────────────────────────────────────


def test_label_clamps_to_0_4():
    assert contact_quality_label(0) == "0/4"
    assert contact_quality_label(3) == "3/4"
    assert contact_quality_label(4) == "4/4"
    # Defensive clamping in case a future change accidentally returns
    # a score > 4.
    assert contact_quality_label(7) == "4/4"
    assert contact_quality_label(-1) == "0/4"
