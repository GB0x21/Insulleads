"""
Tests for `manage.py enable_web_crawler`.

Covers the four operator flows: --show, flip enabled with no schema
change, flip enabled with a new schema file, and --disable. Plus the
guard rails (wrong key, wrong kind, malformed schema JSON).
"""
from __future__ import annotations

import json
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError


@pytest.fixture
def web_crawler_source(db):
    from outreach.models import Campaign, Source

    campaign = Campaign.objects.create(
        name="t", product_description="x", target_market="y"
    )
    return Source.objects.create(
        key="calcerts_raters_test",
        kind="web_crawler",
        campaign=campaign,
        enabled=False,
        config={
            "urls": ["https://example.test/raters"],
            "schema": {"baseSelector": ".old", "fields": []},
            "http_only": True,
        },
    )


def _run(*args) -> str:
    out = StringIO()
    call_command("enable_web_crawler", *args, stdout=out)
    return out.getvalue()


# ─── --show ───────────────────────────────────────────────────────


def test_show_prints_current_config(web_crawler_source):
    out = _run("--key", web_crawler_source.key, "--show")
    assert "calcerts_raters_test" in out
    assert "enabled     False" in out
    assert "https://example.test/raters" in out
    assert ".old" in out


# ─── flip enabled with no schema change ──────────────────────────


def test_flip_enabled_only(web_crawler_source):
    _run("--key", web_crawler_source.key)
    web_crawler_source.refresh_from_db()
    assert web_crawler_source.enabled is True
    # Config preserved.
    assert web_crawler_source.config["schema"]["baseSelector"] == ".old"


# ─── flip enabled and replace schema ─────────────────────────────


def test_flip_enabled_and_replace_schema(web_crawler_source, tmp_path):
    new_schema = {
        "name": "CalCERTSRaters",
        "baseSelector": "tr.rater-row",
        "fields": [
            {"name": "business_name", "selector": "td.company", "type": "text"},
        ],
    }
    schema_path = tmp_path / "calcerts.json"
    schema_path.write_text(json.dumps(new_schema))

    _run("--key", web_crawler_source.key, "--schema-file", str(schema_path))

    web_crawler_source.refresh_from_db()
    assert web_crawler_source.enabled is True
    assert web_crawler_source.config["schema"]["baseSelector"] == "tr.rater-row"
    assert web_crawler_source.config["schema"]["name"] == "CalCERTSRaters"
    # urls / http_only must NOT have been clobbered.
    assert web_crawler_source.config["urls"] == ["https://example.test/raters"]
    assert web_crawler_source.config["http_only"] is True


# ─── --disable ────────────────────────────────────────────────────


def test_disable_flips_back(web_crawler_source):
    web_crawler_source.enabled = True
    web_crawler_source.save()

    _run("--key", web_crawler_source.key, "--disable")

    web_crawler_source.refresh_from_db()
    assert web_crawler_source.enabled is False


# ─── Guards ───────────────────────────────────────────────────────


def test_unknown_key_raises(db):
    with pytest.raises(CommandError, match="No Source"):
        _run("--key", "definitely-not-a-real-key")


def test_non_web_crawler_kind_rejected(db):
    from outreach.models import Campaign, Source

    campaign = Campaign.objects.create(
        name="t2", product_description="x", target_market="y"
    )
    src = Source.objects.create(
        key="permits-real", kind="permits", campaign=campaign,
    )
    with pytest.raises(CommandError, match="kind='permits'"):
        _run("--key", src.key)


def test_malformed_schema_json_rejected(web_crawler_source, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json")
    with pytest.raises(CommandError, match="invalid JSON"):
        _run("--key", web_crawler_source.key, "--schema-file", str(bad))


def test_schema_without_baseselector_rejected(web_crawler_source, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"name": "x", "fields": []}))
    with pytest.raises(CommandError, match="baseSelector"):
        _run("--key", web_crawler_source.key, "--schema-file", str(bad))
