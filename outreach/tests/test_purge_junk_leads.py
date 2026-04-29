"""
Tests for `manage.py purge_junk_leads`.

End-to-end with `@pytest.mark.django_db`: we seed a mix of good and
bad Lead rows, run the command in --dry-run / --soft / --hard modes,
and verify only the bad ones are touched. CONTACTED leads are never
touched (they have ActionLog entries).
"""
from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command


@pytest.fixture
def seeded(db):
    """Make a campaign + 4 leads spanning the junk/good/already-contacted axes."""
    from outreach.models import Campaign, Lead, Source

    campaign = Campaign.objects.create(
        name="t-purge", product_description="x", target_market="y"
    )
    permits_src = Source.objects.create(
        key="t-permits-purge", kind="permits", campaign=campaign
    )
    constr_src = Source.objects.create(
        key="t-constr-purge", kind="construction", campaign=campaign
    )

    junk_permits = Lead.objects.create(
        external_id="P-JUNK", source=permits_src, campaign=campaign,
        title="PLAN A LOT 16",
        stage="DISCOVERED",
        raw={
            "permit_type": "PLAN A (BEMP 100%) LOT 16",
            "description": "New Construction",
            "issued_date": "2026-04-15",
        },
    )
    junk_construction = Lead.objects.create(
        external_id="C-DRYWALL", source=constr_src, campaign=campaign,
        title="drywall lead",
        stage="QUALIFIED",
        raw={"phase": "drywall", "city": "SF"},
    )
    good_permits = Lead.objects.create(
        external_id="P-OK", source=permits_src, campaign=campaign,
        title="real construction",
        stage="DISCOVERED",
        raw={
            "permit_type": "additions alterations or repairs",
            "description": "Re-roof + R-38 attic",
            "issued_date": "2026-04-15",
        },
    )
    contacted_junk = Lead.objects.create(
        external_id="P-CONTACTED", source=permits_src, campaign=campaign,
        title="already contacted junk",
        stage="CONTACTED",
        raw={
            "permit_type": "PLAN A (BEMP 100%) LOT 16",
            "description": "New Construction",
            "issued_date": "2026-04-15",
        },
    )
    return {
        "campaign": campaign,
        "junk_permits": junk_permits,
        "junk_construction": junk_construction,
        "good_permits": good_permits,
        "contacted_junk": contacted_junk,
    }


def _run(*args) -> str:
    out = StringIO()
    call_command("purge_junk_leads", *args, stdout=out)
    return out.getvalue()


# ─── --dry-run ───────────────────────────────────────────────────


def test_dry_run_does_not_change_db(seeded):
    out = _run("--dry-run")

    # 2 junk leads in DISCOVERED/QUALIFIED stages were flagged.
    assert "2 match the junk filters" in out
    # The contacted junk lead was NOT scanned (CONTACTED stage skipped).
    assert "P-CONTACTED" not in out

    seeded["junk_permits"].refresh_from_db()
    seeded["junk_construction"].refresh_from_db()
    seeded["good_permits"].refresh_from_db()
    seeded["contacted_junk"].refresh_from_db()
    assert seeded["junk_permits"].stage == "DISCOVERED"
    assert seeded["junk_construction"].stage == "QUALIFIED"
    assert seeded["good_permits"].stage == "DISCOVERED"
    assert seeded["contacted_junk"].stage == "CONTACTED"


# ─── --soft (default) ────────────────────────────────────────────


def test_soft_marks_junk_as_lost(seeded):
    _run()  # default = soft purge

    seeded["junk_permits"].refresh_from_db()
    seeded["junk_construction"].refresh_from_db()
    seeded["good_permits"].refresh_from_db()
    seeded["contacted_junk"].refresh_from_db()

    assert seeded["junk_permits"].stage == "LOST"
    assert "non-construction permit" in seeded["junk_permits"].llm_qualification_reason
    assert seeded["junk_construction"].stage == "LOST"
    assert "phase=drywall" in seeded["junk_construction"].llm_qualification_reason

    # Good and already-contacted untouched.
    assert seeded["good_permits"].stage == "DISCOVERED"
    assert seeded["contacted_junk"].stage == "CONTACTED"


# ─── --hard ──────────────────────────────────────────────────────


def test_hard_deletes_junk_rows(seeded):
    from outreach.models import Lead

    _run("--hard")

    assert not Lead.objects.filter(pk=seeded["junk_permits"].pk).exists()
    assert not Lead.objects.filter(pk=seeded["junk_construction"].pk).exists()
    # Good and contacted survive.
    assert Lead.objects.filter(pk=seeded["good_permits"].pk).exists()
    assert Lead.objects.filter(pk=seeded["contacted_junk"].pk).exists()


# ─── --kinds filter ──────────────────────────────────────────────


def test_kinds_filter_isolates_one_source_kind(seeded):
    _run("--kinds", "construction", "--hard")

    from outreach.models import Lead

    # Only the construction junk got deleted.
    assert not Lead.objects.filter(pk=seeded["junk_construction"].pk).exists()
    assert Lead.objects.filter(pk=seeded["junk_permits"].pk).exists()


# ─── --campaign filter ───────────────────────────────────────────


def test_campaign_filter(seeded):
    _run("--campaign", "Bay Area Insulation", "--dry-run")
    # Wrong campaign name → 0 hits.
    out = _run("--campaign", "Bay Area Insulation", "--dry-run")
    assert "no candidate leads" in out
