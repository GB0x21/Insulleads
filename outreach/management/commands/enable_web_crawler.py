"""
`python manage.py enable_web_crawler` — flip a seeded web_crawler
Source from disabled → enabled, optionally swapping in a verified
schema from a JSON file.

The seed catalog (`outreach/seed_data/web_crawler_targets.py`) ships
every target disabled with a best-effort schema marked `# VERIFY`. The
operator workflow is:

  1. `python manage.py test_web_crawler --inspect --url <URL>` to see
     the live HTML's repeating-row class.
  2. Write a verified schema to a JSON file.
  3. Smoke-test it with `test_web_crawler --url ... --schema-file ...`.
  4. **This command** flips the Source's `enabled` flag and replaces
     `config.schema` with the verified version, atomically.

Usage:
    # Flip enabled=True only (no schema change — when the existing one
    # already worked):
    python manage.py enable_web_crawler --key calcerts_raters

    # Flip enabled=True AND replace config.schema:
    python manage.py enable_web_crawler --key calcerts_raters \\
        --schema-file ./schemas/calcerts.json

    # Disable a target:
    python manage.py enable_web_crawler --key calcerts_raters --disable

    # Inspect current config:
    python manage.py enable_web_crawler --key calcerts_raters --show
"""
from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from outreach.models import Source


class Command(BaseCommand):
    help = "Activate / deactivate a seeded web_crawler Source."

    def add_arguments(self, parser):
        parser.add_argument("--key", required=True,
                            help="Source.key to update (e.g. 'calcerts_raters').")
        parser.add_argument("--schema-file", type=Path,
                            help="Replace `config.schema` with the JSON in this file.")
        parser.add_argument("--disable", action="store_true",
                            help="Set enabled=False (default is True).")
        parser.add_argument("--show", action="store_true",
                            help="Print the current config and exit.")

    def handle(self, *args, **opts):
        try:
            src = Source.objects.get(key=opts["key"])
        except Source.DoesNotExist as exc:
            raise CommandError(
                f"No Source with key={opts['key']!r}. Run `setup_crm` first."
            ) from exc

        if src.kind != "web_crawler":
            raise CommandError(
                f"Source {src.key!r} is kind={src.kind!r} — this command only "
                "operates on web_crawler sources."
            )

        if opts["show"]:
            self._show(src)
            return

        cfg = dict(src.config or {})

        if opts["schema_file"]:
            path: Path = opts["schema_file"]
            if not path.exists():
                raise CommandError(f"schema file not found: {path}")
            try:
                schema = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                raise CommandError(f"invalid JSON in {path}: {exc}") from exc
            if not isinstance(schema, dict) or not schema.get("baseSelector"):
                raise CommandError(
                    "schema JSON must be an object with a 'baseSelector' key"
                )
            cfg["schema"] = schema
            self.stdout.write(f"  schema     replaced from {path}")

        previous = src.enabled
        src.enabled = not opts["disable"]
        src.config = cfg
        src.save(update_fields=["enabled", "config", "updated_at"]
                 if hasattr(src, "updated_at") else ["enabled", "config"])

        flip = "→ enabled" if src.enabled else "→ disabled"
        self.stdout.write(self.style.SUCCESS(
            f"\n{src.key}: {flip}  (was enabled={previous})"
        ))
        if src.enabled:
            urls = (src.config or {}).get("urls") or []
            self.stdout.write(
                f"  next discover tick will crawl {len(urls)} URL(s)."
            )

    def _show(self, src: Source) -> None:
        cfg = src.config or {}
        self.stdout.write(self.style.SUCCESS(f"\n{src.key}"))
        self.stdout.write(f"  kind        {src.kind}")
        self.stdout.write(f"  enabled     {src.enabled}")
        self.stdout.write(f"  interval_m  {src.interval_minutes}")
        urls = cfg.get("urls") or []
        for u in urls:
            self.stdout.write(f"  url         {u}")
        schema = cfg.get("schema") or {}
        self.stdout.write(f"  schema.name           {schema.get('name', '—')}")
        self.stdout.write(
            f"  schema.baseSelector   {schema.get('baseSelector', '—')}"
        )
        for f in (schema.get("fields") or []):
            self.stdout.write(
                f"    field {f.get('name', '?'):<16} {f.get('selector', '—')}"
            )
        self.stdout.write(f"  http_only             {cfg.get('http_only', False)}")
