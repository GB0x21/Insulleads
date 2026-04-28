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
