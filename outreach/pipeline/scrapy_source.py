"""
ScrapySource — adapter that lets the Django daemon treat a Scrapy spider as
just another discovery backend.

We intentionally run Scrapy in a subprocess (`scrapy crawl <name> -O out.jsonl`)
rather than embedding `CrawlerProcess` in-process, because Twisted's reactor
cannot be restarted and the daemon runs many discover tasks over its lifetime.
Subprocess isolation also means a spider crash can never take down the daemon.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from django.conf import settings

logger = logging.getLogger("outreach.pipeline.scrapy_source")

DEFAULT_TIMEOUT = int(os.getenv("SCRAPY_TIMEOUT", "300"))


class ScrapySourceError(RuntimeError):
    pass


class ScrapySource:
    """Run a Scrapy spider and yield normalized lead dicts."""

    key = "scrapy"

    def __init__(
        self,
        spider_name: str,
        spider_args: dict[str, Any] | None = None,
        timeout: int | None = None,
        source_key: str = "",
    ):
        if not spider_name:
            raise ValueError("ScrapySource requires a spider_name")
        self.spider_name = spider_name
        self.spider_args = spider_args or {}
        self.timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
        self.source_key = source_key

    def fetch_leads(self) -> list[dict]:
        """Alias matching the legacy agent protocol (`BaseAgent.fetch_leads`)."""
        return self.fetch()

    def fetch(self) -> list[dict]:
        base_dir = Path(settings.BASE_DIR)
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".jsonl", delete=False
        ) as tmp:
            out_path = Path(tmp.name)

        try:
            cmd = [
                "scrapy",
                "crawl",
                self.spider_name,
                "-O",
                f"{out_path}:jsonlines",
            ]
            if self.source_key:
                cmd += ["-a", f"source_key={self.source_key}"]
            for key, value in self.spider_args.items():
                cmd += ["-a", f"{key}={value}"]

            env = os.environ.copy()
            env.setdefault(
                "DJANGO_SETTINGS_MODULE", "outreach.django_settings"
            )

            logger.info(
                "scrapy_source: running %s (timeout=%ds)",
                " ".join(cmd),
                self.timeout,
            )
            try:
                subprocess.run(
                    cmd,
                    cwd=str(base_dir),
                    env=env,
                    check=True,
                    timeout=self.timeout,
                    capture_output=True,
                    text=True,
                )
            except FileNotFoundError as exc:
                raise ScrapySourceError(
                    "scrapy executable not found; install with "
                    "`pip install scrapy`"
                ) from exc
            except subprocess.TimeoutExpired as exc:
                raise ScrapySourceError(
                    f"spider {self.spider_name!r} timed out after {self.timeout}s"
                ) from exc
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").strip().splitlines()[-10:]
                raise ScrapySourceError(
                    f"spider {self.spider_name!r} failed "
                    f"(exit={exc.returncode}): {' | '.join(stderr)}"
                ) from exc

            return list(_iter_jsonl(out_path))
        finally:
            try:
                out_path.unlink()
            except FileNotFoundError:
                pass


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning("scrapy_source: skipping malformed line: %s", line[:120])
