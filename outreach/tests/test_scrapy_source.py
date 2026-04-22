"""
Tests for `outreach.pipeline.scrapy_source.ScrapySource`.

We mock `subprocess.run` so it writes a deterministic JSONL file to the
output path the adapter generated, then assert the dicts come back through
`.fetch()`. No real Scrapy invocation.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from outreach.pipeline.scrapy_source import ScrapySource, ScrapySourceError


def _fake_run_writing(items: list[dict]):
    """Build a subprocess.run replacement that writes `items` to the -O path."""

    def _run(cmd, **kwargs):
        out_path = None
        for idx, part in enumerate(cmd):
            if part == "-O" and idx + 1 < len(cmd):
                spec = cmd[idx + 1]
                out_path = Path(spec.split(":", 1)[0])
                break
        assert out_path is not None, "expected -O in command"
        with out_path.open("w", encoding="utf-8") as fh:
            for item in items:
                fh.write(json.dumps(item) + "\n")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return _run


def test_scrapy_source_returns_items_from_jsonl(monkeypatch):
    items = [
        {"external_id": "a", "title": "A", "contact_company": "Co A"},
        {"external_id": "b", "title": "B"},
    ]
    monkeypatch.setattr(subprocess, "run", _fake_run_writing(items))

    src = ScrapySource(spider_name="cslb_contractors", spider_args={"license_numbers": "1,2"})
    result = src.fetch()

    assert result == items


def test_scrapy_source_requires_spider_name():
    with pytest.raises(ValueError):
        ScrapySource(spider_name="")


def test_scrapy_source_wraps_subprocess_errors(monkeypatch):
    def _boom(cmd, **kwargs):
        raise subprocess.CalledProcessError(
            1, cmd, output="", stderr="spider exploded\n"
        )

    monkeypatch.setattr(subprocess, "run", _boom)

    src = ScrapySource(spider_name="cslb_contractors")
    with pytest.raises(ScrapySourceError) as excinfo:
        src.fetch()
    assert "spider exploded" in str(excinfo.value)


def test_scrapy_source_wraps_missing_executable(monkeypatch):
    def _missing(cmd, **kwargs):
        raise FileNotFoundError("no scrapy")

    monkeypatch.setattr(subprocess, "run", _missing)

    src = ScrapySource(spider_name="cslb_contractors")
    with pytest.raises(ScrapySourceError) as excinfo:
        src.fetch()
    assert "scrapy executable not found" in str(excinfo.value)


def test_scrapy_source_wraps_timeout(monkeypatch):
    def _slow(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, timeout=1)

    monkeypatch.setattr(subprocess, "run", _slow)

    src = ScrapySource(spider_name="cslb_contractors", timeout=1)
    with pytest.raises(ScrapySourceError) as excinfo:
        src.fetch()
    assert "timed out" in str(excinfo.value)
