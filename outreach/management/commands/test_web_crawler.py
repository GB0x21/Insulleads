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
        parser.add_argument(
            "--inspect",
            action="store_true",
            help=(
                "Fetch the URL, print a markdown excerpt and the most-frequent "
                "CSS classes — handy for deriving a JsonCssExtractionStrategy "
                "schema before enabling a Source. Skips extraction."
            ),
        )

    def handle(self, *args, **opts):
        if opts["inspect"]:
            urls = opts["url"]
            if not urls:
                raise CommandError("--inspect requires --url")
            self._inspect(urls[0], http_only=not opts["no_http_only"])
            return

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

    # ─── --inspect helper ─────────────────────────────────────────────

    def _inspect(self, url: str, *, http_only: bool) -> None:
        """Fetch a URL with crawl4ai and print markdown + class frequencies."""
        import asyncio
        import re
        from collections import Counter

        try:
            from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
        except ImportError as exc:
            raise CommandError(
                "crawl4ai is not installed — `pip install crawl4ai`."
            ) from exc

        async def _run() -> tuple[str, str]:
            crawler_kwargs: dict = {}
            if http_only:
                from crawl4ai.async_crawler_strategy import AsyncHTTPCrawlerStrategy

                crawler_kwargs["crawler_strategy"] = AsyncHTTPCrawlerStrategy()

            async with AsyncWebCrawler(**crawler_kwargs) as crawler:
                result = await crawler.arun(url=url, config=CrawlerRunConfig())
                md = getattr(result, "markdown", "") or ""
                # crawl4ai's markdown can be a MarkdownGenerationResult object
                md = getattr(md, "raw_markdown", md) if not isinstance(md, str) else md
                html = getattr(result, "cleaned_html", "") or getattr(result, "html", "") or ""
                return str(md), str(html)

        markdown, html = asyncio.run(_run())

        self.stdout.write(self.style.NOTICE(f"\n=== markdown excerpt for {url} ===\n"))
        self.stdout.write(markdown[:2000] or "<empty>")

        # Sniff the 25 most frequent class= attribute values — a quick
        # signal for which container class wraps repeating items.
        classes = re.findall(r'class="([^"]+)"', html)
        flat: list[str] = []
        for c in classes:
            flat.extend(c.split())
        top = Counter(flat).most_common(25)

        self.stdout.write(self.style.NOTICE("\n=== top 25 CSS classes ===\n"))
        for cls, count in top:
            self.stdout.write(f"  {count:5d}  .{cls}")
