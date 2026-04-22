# Scrapy integration

Insulleads ships a [Scrapy](https://scrapy.org/) project in `scrapy_crawlers/`
that plugs into the outreach daemon as a new `Source.kind = "scrapy"`. It runs
*alongside* the legacy `agents/` package, not in place of it — use it for
HTML-only sites where the request/BS4 pattern in `agents/*.py` gets noisy
(retries, throttling, User-Agent rotation, caching).

## Architecture

```
outreach/daemon.py
    ↓
outreach/tasks/discover.py::handle
    ↓
outreach/pipeline/sources.py::fetch_source(source)
    ↓
  kind == "scrapy"              kind in {permits, solar, ...}
    ↓                              ↓
outreach/pipeline/scrapy_source.py    agents/<kind>_agent.py
ScrapySource.fetch_leads()            Agent.fetch_leads()
    ↓ subprocess                      ↓ in-process
`scrapy crawl <spider> -O out.jsonl`  requests + BS4
    ↓                                 ↓
[dict, dict, ...]  ─── _normalize ───► Lead.objects.update_or_create
```

The Scrapy subprocess is intentionally isolated: Twisted's reactor cannot be
restarted, and the daemon runs many discover tasks over its lifetime. A crashed
spider can never take down the daemon.

## Adding a new spider

1. Drop a file under `scrapy_crawlers/spiders/`:

    ```python
    from scrapy_crawlers.items import LeadItem
    from scrapy_crawlers.spiders.base import BaseInsulleadsSpider

    class MyPortalSpider(BaseInsulleadsSpider):
        name = "my_portal"
        start_urls = ["https://example.gov/permits/recent"]

        def parse(self, response):
            for row in response.css("table.permits tr"):
                item = LeadItem()
                item["external_id"] = self.build_external_id(
                    "my_portal", row.css("td.id::text").get()
                )
                item["title"] = row.css("td.desc::text").get()
                item["address"] = row.css("td.addr::text").get()
                item["project_value"] = self.parse_money(
                    row.css("td.val::text").get()
                )
                item["contact_source"] = "my_portal"
                item["raw"] = {"url": response.url}
                yield item
    ```

    Emit every field that `outreach/pipeline/sources.py::_normalize` reads.
    Missing fields are filled with safe defaults by
    `scrapy_crawlers.pipelines.NormalizePipeline`.

2. Smoke-test it locally without Django:

    ```bash
    make crawl SPIDER=my_portal
    # or with args:
    make crawl SPIDER=cslb_contractors ARGS='-a license_numbers=123456,789012'
    ```

3. Register it as a `Source` so the daemon schedules it:

    ```python
    from outreach.models import Campaign, Source

    camp = Campaign.objects.get(key="bay-area")
    Source.objects.create(
        campaign=camp,
        key="cslb-enrich",
        kind="scrapy",
        interval_minutes=360,  # every 6 hours
        config={
            "spider": "cslb_contractors",
            "spider_args": {"license_numbers": "123456,789012"},
            # Optional: override the subprocess timeout (seconds).
            "timeout": 300,
        },
    )
    ```

    The daemon enqueues a `DISCOVER` task the next time `ensure_periodic_tasks`
    runs. From there the flow is identical to legacy agents: items become
    `Lead` rows in the `DISCOVERED` stage, then `QUALIFY` / `OUTREACH` tasks
    pick them up.

## Settings & env vars

| Variable | Default | Notes |
|---|---|---|
| `SCRAPY_LOG_LEVEL` | `INFO` | Passed into `scrapy_crawlers.settings`. |
| `SCRAPY_USER_AGENT` | `Insulleads-Crawler/1.0 (+...)` | Overrides the global UA. |
| `SCRAPY_TIMEOUT` | `300` | Subprocess timeout per spider run (seconds). |
| `SCRAPY_HTTPCACHE` | `1` | Set to `0` to disable the on-disk cache. |

`AutoThrottle`, `RETRY_TIMES=3`, `ROBOTSTXT_OBEY=True`, and an HTTP cache at
`data/.scrapy_httpcache/` are enabled by default — see
`scrapy_crawlers/settings.py`.

## Testing

```bash
pytest scrapy_crawlers/tests outreach/tests -v
```

Spider parsing tests use `scrapy.http.HtmlResponse` with fixture HTML — no
network. `ScrapySource` tests mock `subprocess.run` so they exercise the
adapter wiring without invoking real Scrapy.

## Fallback behavior

- `scrapy` not installed → `ScrapySource.fetch()` raises `ScrapySourceError`
  with a clear install hint. The daemon logs the error on the `Source` and
  moves on; no other source is affected.
- Spider exit code != 0 → stderr tail is attached to `Source.last_error`.
- Subprocess timeout → surfaces the same way; retry happens on the next
  scheduled discover interval.
