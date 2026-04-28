"""
`python manage.py test_web_crawler` — smoke-test the WebCrawlerAgent
end-to-end without touching the daemon or the Lead table.

Examples:
  # Built-in local fixture (no network, no Chromium needed):
  python manage.py test_web_crawler --fixture

  # Real URL, schema from a JSON file (http_only by default — no Chromium):
  python manage.py test_web_crawler \\
      --url https://example.com/contractors \\
      --schema-file ./contractors_schema.json

  # Use the full Playwright stack (JS-rendered sites):
  python manage.py test_web_crawler --url https://example.com --no-http-only
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from tempfile import NamedTemporaryFile

from django.core.management.base import BaseCommand, CommandError

from agents.web_crawler_agent import WebCrawlerAgent

_FIXTURE_HTML = textwrap.dedent(
    """
    <!doctype html><html><body>
      <div class="card">
        <h3>Acme Insulation</h3>
        <span class="addr">123 Main St, San Francisco</span>
        <span class="tel">555-0001</span>
        <a class="site" href="https://acme.example/">site</a>
      </div>
      <div class="card">
        <h3>Beta Roofing</h3>
        <span class="addr">456 Oak Ave, Oakland</span>
        <span class="tel">555-0002</span>
      </div>
      <div class="card">
        <h3>Gamma HVAC</h3>
        <span class="addr">789 Pine Rd, Berkeley</span>
        <span class="tel">555-0003</span>
      </div>
    </body></html>
    """
)

_FIXTURE_SCHEMA = {
    "name": "Cards",
    "baseSelector": ".card",
    "fields": [
        {"name": "business_name", "selector": "h3", "type": "text"},
        {"name": "address", "selector": ".addr", "type": "text"},
        {"name": "phone", "selector": ".tel", "type": "text"},
        {
            "name": "website",
            "selector": "a.site",
            "type": "attribute",
            "attribute": "href",
        },
    ],
}


class Command(BaseCommand):
    help = "Run WebCrawlerAgent against a URL+schema and print the leads."

    def add_arguments(self, parser):
        parser.add_argument(
            "--url",
            action="append",
            default=[],
            help="URL to crawl (repeat for multiple). Required unless --fixture.",
        )
        parser.add_argument(
            "--schema-file",
            type=Path,
            help="Path to JSON file containing the JsonCssExtractionStrategy schema.",
        )
        parser.add_argument(
            "--fixture",
            action="store_true",
            help="Use a built-in local HTML fixture (no network, no Chromium).",
        )
        parser.add_argument(
            "--city-default",
            default="",
            help="Fallback `city` for leads whose schema doesn't extract one.",
        )
        parser.add_argument(
            "--no-http-only",
            action="store_true",
            help="Use the Playwright stack instead of pure-HTTP. Needs Chromium.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Only print the first N leads (0 = all).",
        )

    def handle(self, *args, **opts):
        if opts["fixture"]:
            tmp = NamedTemporaryFile(
                mode="w", suffix=".html", delete=False, encoding="utf-8"
            )
            tmp.write(_FIXTURE_HTML)
            tmp.flush()
            tmp.close()
            urls = [f"file://{tmp.name}"]
            schema = _FIXTURE_SCHEMA
            self.stdout.write(self.style.NOTICE(f"using fixture {tmp.name}"))
        else:
            urls = opts["url"]
            if not urls:
                raise CommandError("--url is required (or use --fixture)")
            if not opts["schema_file"]:
                raise CommandError("--schema-file is required (or use --fixture)")
            schema = json.loads(opts["schema_file"].read_text())

        agent = WebCrawlerAgent(
            urls=urls,
            schema=schema,
            city_default=opts["city_default"],
            http_only=not opts["no_http_only"],
        )
        leads = agent.fetch_leads()

        if opts["limit"]:
            leads = leads[: opts["limit"]]

        self.stdout.write(
            self.style.SUCCESS(f"\n{len(leads)} lead(s) extracted:\n")
        )
        for lead in leads:
            self.stdout.write(
                f"  [{lead['id']}] {lead['business_name']}\n"
                f"      city={lead['city']!r}  phone={lead['contact_phone']!r}  "
                f"addr={lead['address']!r}\n"
                f"      website={lead.get('website', '')!r}  src={lead['source_url']}"
            )
