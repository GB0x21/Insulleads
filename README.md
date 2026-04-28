# Insulleads

**Lead-generation stack for insulation contractors in the Bay Area,
adapted from [OpenOutreach](https://github.com/eracle/OpenOutreach).**

Insulleads takes OpenOutreach's architecture — a self-hosted Django CRM
with a daemon that discovers, qualifies (Bayesian GP) and contacts
prospects — and swaps the LinkedIn-scraping backend for **public-data
agents**: building permits, solar installs, 311 rodent reports, NOAA
flood alerts, real-estate sales, and more. Outreach goes through
Telegram / SendGrid / Twilio instead of LinkedIn messages.

> Describe your product. Define your target market. The daemon finds
> the leads for you, ranks them with a Bayesian qualifier, and messages
> the contacts via the channel you choose.

---

## Quickstart

```bash
make setup     # install deps + migrate + bootstrap default campaign
make run       # start the outreach daemon (interactive onboarding)
make admin     # Django Admin + CRM at http://localhost:8000/
```

Or with Docker:

```bash
make up        # builds compose/outreach/Dockerfile, runs daemon
```

On first run, `rundaemon` prompts for a campaign name, product
description and target market, then seeds one `Source` row per enabled
agent and starts the task-queue loop.

---

## What you can do now

After `make setup`, the daemon exposes the following capabilities. Each
one is wired through the same `Source` / `Lead` / `Task` model so you can
mix and match them in a single campaign.

### Lead discovery — public-data APIs (no scraping required)

These run unmodified out of the box; just toggle `AGENT_*` env vars.

**Quality filters on permit-style leads.** `agents/permits_agent.py`
applies a value floor (`MIN_PERMIT_VALUE`, default $50k), a recency
window (`PERMIT_MONTHS`, default 3) and a *blacklist* that drops
non-construction noise that would otherwise sneak past the keyword
whitelist:

- Enforcement permits — Code Investigation / Code Compliance / complaints.
- Planning-only approvals — San Jose's `PLAN A (BEMP …) LOT N` master
  plans, subdivision / tentative / final maps, lot-line adjustments.
- Out-of-trade scope — fences, signs, pool-only, landscape-only, tree
  removal, demolition-only.
- Stale dates — leads whose `issued_date` is unparseable now drop
  (pre-v9 they passed through, which let years-old records through any
  time the upstream agency served a non-ISO date).

The `notify` Telegram block also hides "next inspection" rows when the
helper returned a date in the past or an enforcement-type visit (which
is what produced the 4/10/2018 "Code Investigation" header in early
operator messages).

The same blacklist is applied at the Django pipeline boundary
(`outreach/pipeline/sources.py`) so junk permits never become `Lead`
rows there either.

- **Building permits**, **active construction**, **deconstruction/demo**
  and **real-estate sales** via Socrata / CKAN endpoints across the nine
  Bay Area counties (`agents/{permits,construction,deconstruction,
  realestate}_agent.py`).
- **Solar installs** (NREL, OpenEI), **NOAA flood alerts**,
  **energy benchmarking**, **311 rodent reports** —
  `agents/{solar,flood,energy,rodents}_agent.py`.
- **Yelp Fusion** + **Google Places** contractor lookups —
  `agents/{yelp,places}_agent.py`.
- **Thermal anomalies** from Landsat (Earth Engine) —
  `agents/thermal_agent.py`.
- **Email-prospect** discovery via Hunter.io / Apollo.io / CSV cache —
  `agents/email_prospect_agent.py`.

### Lead discovery — Scrapy spiders

For HTML-only targets that don't have an API. The daemon runs spiders
in subprocesses so a crash never takes down the worker.

- **CSLB contractor license lookup** — the only spider shipped today
  (`scrapy_crawlers/spiders/cslb_contractors.py`). Add more by writing a
  spider and creating a `Source` row of `kind="scrapy"` with the spider
  name in `config.spider`.

### Lead discovery — crawl4ai web crawler *(new)*

LLM-friendly extraction from contractor directories that have neither
an API nor stable selectors. Driven entirely by `Source.config`, so
adding a new target is "drop a JSON schema, flip `enabled=True`."

- **Five real Bay Area targets** seeded as disabled `Source` rows by
  `make setup` — see `outreach/seed_data/web_crawler_targets.py`:
  BayREN Home+, NARI SF Bay, Build It Green / GreenPoint Rated,
  Energy Upgrade California, Diamond Certified.
- **Two crawl modes**: `http_only=True` (pure aiohttp, no Chromium) for
  static pages, and `http_only=False` (Playwright) for JS-rendered
  sites.
- **Schema discovery helper**: `python manage.py test_web_crawler
  --inspect --url <URL>` fetches the page, dumps a markdown excerpt and
  the 25 most-frequent CSS classes so you can write a working schema in
  minutes.
- **Soft-fails** when `crawl4ai` isn't installed — the daemon keeps
  running and just returns 0 leads for the source.

See the "Web crawler (crawl4ai)" subsection below for the full schema
example and the verification workflow.

### LLM layer — typed, multi-capability *(rebuilt on pydantic-ai)*

Three LLM-powered features sit on top of the pipeline. The whole stack
runs through pydantic-ai with the Anthropic provider and typed
output models in `outreach/llm/schemas.py`, so structured outputs are
validated rather than regex-parsed.

- **Contact enrichment** *(`outreach/llm/client.py:enrich_contact`)* —
  fills missing phone / email / website / CSLB license using
  Anthropic's native `WebSearchTool`. Output type:
  `EnrichmentResult`. Runs three ways: as a periodic daemon task
  (`outreach/tasks/enrich.py`); inline as a *last-chance* attempt
  inside the outreach loop so no Telegram message goes out without
  contact data; and on demand via `python manage.py enrich_leads`
  (see "Backfilling contact data" below).
- **Cold-start qualifier** *(`qualify_lead`)* — scores a lead 0–1 with
  a one-sentence reason while the Gaussian-process qualifier still has
  fewer than 2 labels. Output type: `QualificationResult`.
- **Outreach copy** *(`write_outreach`)* — drafts the Telegram-bound
  first-touch message; pulls in PG&E / Title 24 / ENERGY STAR snippets
  from the RAG index when available. Output type: `OutreachMessage`.
- **Cold email writer** *(`write_email`)* — `EmailDraft(subject, body)`
  for prospects discovered by the email-prospect agent.
- **Multi-agent contract analyzer**
  *(`outreach/llm/contract_analyzer.py`)* — orchestrator + N parallel
  sub-agents + synthesizer. Each role is its own typed `pydantic_ai.
  Agent`; sub-agents run in parallel via a thread-pool. Returns
  `FinalReview(scorecard, recommendation, redlines)`.

Backend selection via `LLM_ADAPTER` (`anthropic` *(default)* / `noop` /
`suna`-stub). Per-campaign toggles
(`Campaign.llm_{enricher,qualifier,writer}_enabled`) live in the
Django admin.

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python manage.py test_llm --feature all   # smoke-test all four methods
```

Provider swap is a 3-line change: replace `AnthropicProvider` /
`AnthropicModel` in `outreach/llm/client.py` with the OpenAI / Gemini /
Groq equivalents — the prompts and typed outputs are reused as-is.

#### Backfilling contact data

The outreach loop will not send a Telegram message for a lead that has
neither a phone nor an email — it tries enrichment one last time and,
if that still comes up empty, leaves the lead in `QUALIFIED` for the
next attempt. Every result lands in `Lead.enrichment_log`:

```jsonc
{
  "status": "ok" | "empty",
  "adapter": "anthropic",
  "attempts": 2,
  "last_attempt_at": "2026-04-28T14:00:00+00:00",
  // ...payload fields when status=ok...
}
```

`status="empty"` leads are retried automatically after
`ENRICH_RETRY_HOURS` (default 24h), up to `ENRICH_MAX_ATTEMPTS`
(default 3). Tunables live in `OUTREACH` (`outreach/django_settings.py`):

```bash
ENRICH_INTERVAL_MIN=30          # daemon enrich tick
ENRICH_RETRY_HOURS=24           # cooldown for status=empty
ENRICH_MAX_ATTEMPTS=3           # give up after this many empty attempts
ENRICH_INLINE_ON_OUTREACH=true  # last-chance attempt inside outreach.handle
```

For ad-hoc backfills (e.g. leads imported before the enricher existed
or stuck at `status=empty`):

```bash
# Best-effort, respects cooldown — safe to run anytime.
python manage.py enrich_leads --campaign "Bay Area Insulation" --limit 100

# Spot-check a single lead, ignoring the cooldown:
python manage.py enrich_leads --lead-id 42 --force

# Force-retry every empty-log lead in the campaign (each call costs an
# Anthropic web_search invocation — use with a sane --limit):
python manage.py enrich_leads --campaign "Bay Area Insulation" \
    --status empty --force --limit 25

# Dry run — list what would be enriched without calling the LLM:
python manage.py enrich_leads --campaign "Bay Area Insulation" --dry-run
```

### CRM, dashboard and contract analysis

- **Django admin + minimal CRM** at `/` and `/admin/` —
  Won/Replied/Lost buttons retrain the per-campaign GP qualifier.
- **Streamlit dashboard** at `make dashboard` (`http://localhost:8501`)
  — pydeck map of geocoded leads, stage funnel, sortable lead table.
- **Contract / RFP discovery + analysis** — `python manage.py
  discover_contracts` pulls from SAM.gov + Anthropic web_search; the
  multi-agent analyzer above produces a scorecard you can review at
  `/admin/outreach/contract/`.

### Outreach channels

Telegram / SendGrid / Twilio (see `utils/notifications.py`). Per-campaign
daily budget enforced via `Campaign.max_outreach_per_day` and tracked in
`ActionLog`.

---

## Mapping to OpenOutreach

| OpenOutreach concept              | Insulleads equivalent                                   |
|-----------------------------------|---------------------------------------------------------|
| `linkedin/` Django app            | `outreach/`                                             |
| `linkedin.daemon.Daemon`          | `outreach.daemon.Daemon`                                |
| `linkedin.models.{Campaign,Task}` | `outreach.models.{Campaign,Lead,Source,Task,ActionLog}` |
| `linkedin.pipeline` (LinkedIn search) | `outreach.pipeline.sources` wrapping `agents/*`     |
| `linkedin.ml` (fastembed + GP)    | `outreach.ml.{embeddings,qualifier}`                    |
| Voyager API + Playwright          | Public-data APIs (Socrata, CKAN, NOAA, NREL, …)         |
| Connect → PENDING → CONNECTED     | `DISCOVERED → QUALIFIED → CONTACTED → REPLIED / WON / LOST` |
| LinkedIn DMs                      | Telegram / SendGrid / Twilio (see `utils/notifications.py`) |
| `compose/linkedin/Dockerfile`     | `compose/outreach/Dockerfile`                           |
| `make setup / run / admin / up`   | same targets, same semantics                            |

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full pipeline diagram.
For the LLM layer (enrichment, cold-start qualifier, outreach copy) see
the "LLM layer" section below and
[`docs/SUNA_INTEGRATION.md`](docs/SUNA_INTEGRATION.md) for the optional
Suna sidecar roadmap.

---

## What's in the box

### Django project

- **`manage.py`** — entrypoint, defaults to `rundaemon` (same behaviour as
  OpenOutreach's `manage.py`).
- **`outreach/django_settings.py`** — SQLite at `data/db.sqlite3`, no
  external services required.
- **`outreach/models.py`** — `SiteConfig`, `Campaign`, `Source`, `Lead`,
  `ActionLog`, `Task` with a `TaskQuerySet` mirroring OpenOutreach's.
- **`outreach/daemon.py`** — single-process task-queue loop; every tick
  pulls the next ready task, dispatches it, and re-seeds periodic jobs.
- **`outreach/management/commands/rundaemon.py`** — interactive onboarding
  + `--onboard config.json` for headless setup.
- **`outreach/management/commands/setup_crm.py`** — creates the default
  "Bay Area Insulation" campaign and one Source row per enabled agent.

### Discovery sources (legacy agents, now pipeline adapters)

`outreach/pipeline/sources.py` exposes a `fetch_source(source)` helper
that lazily imports the matching legacy agent from `agents/` and
normalises the output onto the `Lead` model:

| `Source.kind` | Legacy class (in `agents/`)  |
|---------------|------------------------------|
| permits       | `PermitsAgent`               |
| solar         | `SolarAgent`                 |
| rodents       | `RodentsAgent`               |
| flood         | `FloodAgent`                 |
| construction  | `ConstructionAgent`          |
| deconstruction| `DeconstuctionAgent`         |
| realestate    | `RealEstateAgent`            |
| energy        | `EnergyAgent`                |
| places        | `PlacesAgent`                |
| yelp          | `YelpAgent`                  |
| web_crawler   | `WebCrawlerAgent` (crawl4ai) |

Enable/disable with env vars (unchanged from the original project):

```bash
AGENT_PERMITS=true
AGENT_SOLAR=true
AGENT_RODENTS=true
AGENT_FLOOD=true
AGENT_CONSTRUCTION=true
AGENT_DECONSTRUCTION=true
AGENT_REALESTATE=true
AGENT_ENERGY=true
AGENT_PLACES=false
AGENT_YELP=false
```

#### Web crawler (crawl4ai)

`agents/web_crawler_agent.py` wraps
[crawl4ai](https://github.com/unclecode/crawl4ai) to scrape contractor
directories and association pages that don't expose an API. It's exposed
as `Source.kind="web_crawler"` and is fully driven by `Source.config`:

```json
{
  "urls": ["https://example.com/contractors"],
  "schema": {
    "name": "Contractors",
    "baseSelector": ".contractor-card",
    "fields": [
      {"name": "business_name", "selector": "h3", "type": "text"},
      {"name": "address",       "selector": ".addr", "type": "text"},
      {"name": "phone",         "selector": ".tel",  "type": "text"},
      {"name": "website",       "selector": "a",     "type": "attribute",
       "attribute": "href"}
    ]
  },
  "city_default": "San Francisco",
  "http_only": true
}
```

Two crawl modes:

- **`http_only: true`** *(recommended for static sites)* — uses
  `AsyncHTTPCrawlerStrategy` (pure aiohttp, no browser). Fastest, runs in
  any environment, no Chromium required. Use for server-rendered HTML.
- **`http_only: false`** *(default)* — uses the full Playwright stack.
  Required for JS-rendered sites. Needs Chromium: after
  `pip install crawl4ai`, run `crawl4ai-setup` (or
  `python -m playwright install chromium`) once.

`crawl4ai` itself is optional: if it isn't installed the agent logs a
warning and returns 0 leads, so the daemon stays runnable on a fresh box.

Smoke-test the agent without touching the daemon:

```bash
python manage.py test_web_crawler --fixture
python manage.py test_web_crawler --url https://example.com/contractors \
    --schema-file ./contractors_schema.json
```

`make setup` (which calls `setup_crm`) seeds a curated catalog of real
Bay Area lead-gen targets as **disabled** Source rows — see
`outreach/seed_data/web_crawler_targets.py`:

| Source key                       | What it gives you                                                      |
|----------------------------------|------------------------------------------------------------------------|
| `bayren_home_plus`               | BayREN Home+ pre-vetted energy-upgrade contractors (Bay Area, 9 counties) |
| `nari_sf_bay`                    | NARI SF Bay chapter members — remodelers / GCs that spec insulation    |
| `build_it_green`                 | GreenPoint Rated professionals — green-building contractors and raters |
| `energy_upgrade_ca`              | Energy Upgrade California participating contractors (statewide, filterable) |
| `diamond_certified_insulation`   | Diamond Certified Bay Area insulation / HVAC / weatherization vendors  |

Each schema is best-effort and marked `# VERIFY` — site markup changes
regularly. Before flipping a source to `enabled=True`, run:

```bash
python manage.py test_web_crawler --inspect --url <URL>
```

`--inspect` fetches the page, prints a markdown excerpt and the
25 most-frequent CSS classes, so you can pick the right `baseSelector`
and field selectors. Then re-test with the live config:

```bash
python manage.py test_web_crawler \
    --url <URL> \
    --schema-file ./my_verified_schema.json \
    --limit 5
```

Once happy, flip `enabled=True` from the Django admin or shell.

### Bayesian qualifier

`outreach/ml/qualifier.py` is a 1:1 adaptation of OpenOutreach's
Gaussian-process qualifier:

- Each `Campaign` owns its own pickled `GaussianProcessRegressor`
  (stored in `Campaign.model_blob`).
- `qualify_batch()` returns `(posterior_mean, posterior_variance)` per
  lead. Cold-start campaigns (< 2 labels) fall back to the legacy
  `lead_score` heuristic (0–100).
- `retrain()` refits the GP from every REPLIED/WON (positive) and LOST
  (negative) lead. Called automatically when you label a lead in the CRM.

Embeddings come from `outreach/ml/embeddings.py`. By default we use a
zero-dependency feature-hashing fallback so the repo is runnable on a
fresh machine. Set `OUTREACH_EMBEDDING_BACKEND=fastembed` in `.env` and
add `fastembed` to `requirements/local.txt` for real 384-dim embeddings.

### CRM

A minimal Django CRM lives in `crm/`:

- `/admin/` — full Django admin for Campaign / Source / Lead / ActionLog / Task.
- `/` — dashboard with stage counts and the 50 most recent leads.
- `/leads/<id>/` — lead detail with one-click **Won / Replied / Lost**
  buttons that call `retrain()` on the owning campaign.

### Outreach channels

The `outreach` task reuses the existing Insulleads notification stack
(`utils/telegram.py`, `utils/notifications.py`), so all the Telegram /
SendGrid / Twilio wiring from the original repo keeps working. Daily
budget is enforced per-campaign via `Campaign.max_outreach_per_day`
and tracked in `ActionLog`.

---

### LLM layer

`outreach/llm/` adds three LLM-powered capabilities on top of the
OpenOutreach pipeline. Everything goes through a single `LLMAdapter`
ABC so the backend can be swapped with a single env var:

| Feature            | Where it plugs in                     | What it replaces                               |
|--------------------|---------------------------------------|------------------------------------------------|
| Contact enrichment | `outreach/tasks/enrich.py` (periodic) | Fuzzy match against CSVs only                  |
| Cold-start qualifier | `outreach/tasks/qualify.py` cold-start branch | Heuristic `lead_score` while GP has < 2 labels |
| Outreach copy      | `outreach/tasks/outreach.py:_resolve_body` | Fixed `_format_message` template          |

```bash
export ANTHROPIC_API_KEY=sk-ant-...
python manage.py test_llm --feature all
```

Supported backends (`LLM_ADAPTER` in `.env`):

- `anthropic` *(default)* — backed by [pydantic-ai](https://ai.pydantic.dev)
  with the Anthropic provider. Each adapter method goes through a typed
  `pydantic_ai.Agent` with a Pydantic `output_type` (see
  `outreach/llm/schemas.py`), so structured outputs are validated rather
  than regex-parsed. Prompt caching on the system instructions
  (`anthropic_cache_instructions=True`) and the native Anthropic
  `WebSearchTool` for enrichment are wired in. The raw
  `anthropic.Anthropic` client stays exposed at `adapter._client` so
  callers that need the SDK directly (e.g. contract discovery's web
  search) keep working. Falls back automatically to `noop` when the API
  key is missing.
- `suna` — **stub** for running [Suna](https://github.com/kortix-ai/suna)
  as a sidecar container. See [`docs/SUNA_INTEGRATION.md`](docs/SUNA_INTEGRATION.md)
  for the integration playbook.
- `noop` — disables the LLM layer entirely; the pipeline falls back
  to the legacy template + heuristic qualifier.

Per-campaign toggles (`Campaign.llm_enricher_enabled`,
`llm_qualifier_enabled`, `llm_writer_enabled`, `outreach_tone`) are
exposed in the Django admin.

#### RAG for the outreach writer

The writer can cite real PG&E / Title 24 / ENERGY STAR programs
instead of inventing them. Drop the PDFs into `data/rag_docs/` and
build a local LlamaIndex vector index:

```bash
pip install llama-index-core llama-index-embeddings-huggingface pypdf
python manage.py build_rag_index           # ingest data/rag_docs/
python manage.py build_rag_index --rebuild # force a rebuild
```

The writer queries the index with
`"insulation incentives rebates {city} {project_type}"`, injects the
top-4 matches as a `References:` block into the user message, and is
told to cite one rule if it actually fits (and never to invent a
program). When `llama_index` is missing, `data/rag_docs/` is empty,
or the index isn't built, the writer silently falls back to
unreferenced copy.

### Dashboard (MVP)

A read-only Streamlit dashboard lives in `dashboards/`. It boots
Django in-process and offers three tabs: a pydeck map of geocoded
leads, a stage-count funnel with conversion rates, and a sortable
table with per-lead detail. Run with:

```bash
pip install -r requirements/local.txt   # streamlit + pydeck
make dashboard                           # http://localhost:8501
```

See [`dashboards/README.md`](dashboards/README.md) for notes. The
dashboard is read-only — for labelling and edits use the Django
admin at `/admin/`.

---

## Legacy entrypoint

The original `main.py` scheduler is still in the repo for backwards
compatibility — you can run `python main.py --run permits` to smoke-test
an individual agent outside of the daemon. The Django daemon (`make run`)
is the new primary entrypoint.

---

## Directory layout

```
.
├── manage.py                  # Django entrypoint → rundaemon
├── Makefile
├── local.yml                  # docker compose
├── compose/outreach/Dockerfile
├── requirements/{base,local}.txt
├── pytest.ini
├── ARCHITECTURE.md
│
├── outreach/                  # NEW — Django app (daemon + pipeline + ML)
├── crm/                       # NEW — minimal CRM views + templates
│
├── agents/                    # Legacy discovery agents (still used)
├── utils/                     # Legacy shared utilities
├── contacts/                  # CSV contact lists
├── data/                      # SQLite + generated artefacts
└── main.py                    # Legacy scheduler entrypoint
```

---

## Configuration

All configuration lives in `.env` (see `.env.example`). The new settings
added by this adaptation are:

```
DJANGO_SECRET_KEY=...
DJANGO_DEBUG=true
DJANGO_ALLOWED_HOSTS=*

DISCOVERY_INTERVAL_MIN=60
QUALIFY_INTERVAL_MIN=15
OUTREACH_INTERVAL_MIN=5
MAX_OUTREACH_PER_DAY=200

OUTREACH_EMBEDDING_BACKEND=hash   # or: fastembed
```

Everything else (Telegram / SendGrid / Twilio / Socrata / NREL / etc.)
keeps the same env var names as the original Insulleads project.

---

## Tests

```bash
make test           # pytest + pytest-django
make docker-test    # same, inside the compose container
```

---

## Credits

Architecture derived from
[eracle/OpenOutreach](https://github.com/eracle/OpenOutreach) (MIT).
All discovery agents, contacts loader, hot-zone detector, lead scoring
and notification code are from the original Insulleads project and have
been kept as-is; only the orchestration layer was rewritten to match
OpenOutreach's Django-daemon model.
