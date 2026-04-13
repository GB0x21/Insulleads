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

- `anthropic` *(default)* — uses the official `anthropic` SDK with
  prompt caching on the campaign prefix and `web_search` for
  enrichment. Falls back automatically to `noop` when the API key is
  missing, so the daemon is still runnable on a fresh box.
- `suna` — **stub** for running [Suna](https://github.com/kortix-ai/suna)
  as a sidecar container. See [`docs/SUNA_INTEGRATION.md`](docs/SUNA_INTEGRATION.md)
  for the integration playbook.
- `noop` — disables the LLM layer entirely; the pipeline falls back
  to the legacy template + heuristic qualifier.

Per-campaign toggles (`Campaign.llm_enricher_enabled`,
`llm_qualifier_enabled`, `llm_writer_enabled`, `outreach_tone`) are
exposed in the Django admin.

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
