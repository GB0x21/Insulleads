# Architecture

Insulleads is a self-hosted lead-generation stack for insulation contractors
in the Bay Area. It was **adapted from [OpenOutreach](https://github.com/eracle/OpenOutreach)**
(an AI-powered LinkedIn lead-gen tool) and retargeted from LinkedIn scraping
to **public-data discovery**: building permits, solar installs, 311 rodent
reports, NOAA flood alerts, real-estate sales, etc.

The OpenOutreach architecture stays the same:

- Django project with a SQLite CRM (`data/db.sqlite3`)
- A single `manage.py rundaemon` process owns a task-queue worker loop
- Discovery → Qualification (Bayesian GP) → Outreach pipeline
- Every classification decision retrains the campaign's model

What changed vs. the original project:

| OpenOutreach                       | Insulleads (this repo)                          |
|------------------------------------|-------------------------------------------------|
| `linkedin/` Django app             | `outreach/` Django app                          |
| LinkedIn Voyager API + Playwright  | Public-data agents in `agents/` (no scraping)   |
| Connect / follow-up via LinkedIn   | Telegram / SendGrid / Twilio outreach           |
| fastembed 384-dim embeddings       | Optional; hash-trick fallback by default        |
| Profiles as candidates             | Construction projects + contacts as candidates  |
| `compose/linkedin/Dockerfile`      | `compose/outreach/Dockerfile`                   |

## Directory layout

```
.
├── manage.py                  # Django entrypoint (defaults to rundaemon)
├── Makefile                   # make install / setup / run / admin / up
├── local.yml                  # docker compose
├── compose/outreach/Dockerfile
├── requirements/
│   ├── base.txt
│   └── local.txt
├── pytest.ini
│
├── outreach/                  # Django app — the pipeline
│   ├── django_settings.py
│   ├── urls.py
│   ├── models.py              # SiteConfig, Campaign, Source, Lead, ActionLog, Task
│   ├── admin.py
│   ├── daemon.py              # task-queue worker loop
│   ├── management/commands/
│   │   ├── rundaemon.py       # interactive onboarding + loop
│   │   └── setup_crm.py       # bootstrap default campaign + sources
│   ├── pipeline/
│   │   └── sources.py         # adapters around legacy agents/
│   ├── tasks/
│   │   ├── discover.py        # run a source, persist new Leads
│   │   ├── qualify.py         # embed + GP-score DISCOVERED leads
│   │   └── outreach.py        # Telegram/Email push + follow-up
│   └── ml/
│       ├── embeddings.py      # fastembed backend + hash-trick fallback
│       └── qualifier.py       # Gaussian-process qualifier + retrain()
│
├── crm/                       # Minimal Django CRM (dashboard + label UI)
│   ├── views.py
│   ├── urls.py
│   └── templates/crm/
│
├── agents/                    # Legacy Insulleads discovery agents
│   ├── base.py
│   ├── permits_agent.py
│   ├── solar_agent.py
│   ├── rodents_agent.py
│   └── ... (10 agents)
│
├── utils/                     # Legacy shared utilities — notifications,
│                              # contacts loader, dedup, hot zones, etc.
│
├── contacts/                  # CSV contact lists (fuzzy matched by utils)
└── data/
    └── db.sqlite3             # Auto-created on first `make setup`
```

## Pipeline

```
 ┌──────────────┐  DISCOVER  ┌──────────────┐  ENRICH   ┌──────────────────┐
 │  Source      │ ─────────▶ │  Lead        │ ────────▶ │ LLMAdapter       │
 │  (permits,   │            │  stage=      │           │ .enrich_contact  │
 │  solar, …)   │            │  DISCOVERED  │           │ (web search)     │
 └──────────────┘            └──────┬───────┘           └────────┬─────────┘
                                    │  QUALIFY                   │
                                    ▼                            ▼
                             ┌──────────────┐           ┌──────────────────┐
                             │  Lead (with  │  QUALIFY  │ Qualifier (GP    │
                             │  contact)    │ ────────▶ │  or LLM judge    │
                             └──────────────┘           │  at cold-start)  │
                                                        └────────┬─────────┘
                                                                  │
                                                  µ ≥ 0.55  or    │
                                                 heuristic ≥ 50   ▼
                                                          ┌───────────────┐
                                                          │ QUALIFIED     │
                                                          └───────┬───────┘
                                                                  │  OUTREACH
                                                                  ▼
                                                          ┌───────────────┐
                                                          │ CONTACTED     │
                                                          └───────┬───────┘
                                                                  │  label_lead
                                                  ┌───────────────┼───────────────┐
                                                  ▼               ▼               ▼
                                               REPLIED           WON             LOST
                                                  │               │               │
                                                  └──────┬────────┘               │
                                                         ▼                        │
                                              retrain(campaign)  ◀────────────────┘
```

## Task queue

A single `manage.py rundaemon` process walks `Task.objects.ready()` and
dispatches each task to a handler:

| Task type    | Handler                    | Reschedule         |
|--------------|----------------------------|--------------------|
| `discover`   | `outreach.tasks.discover`  | per-source interval|
| `enrich`     | `outreach.tasks.enrich`    | `ENRICH_INTERVAL_MIN` |
| `qualify`    | `outreach.tasks.qualify`   | `QUALIFY_INTERVAL_MIN` |
| `outreach`   | `outreach.tasks.outreach`  | `OUTREACH_INTERVAL_MIN` |
| `follow_up`  | `outreach.tasks.outreach.handle_follow_up` | 12 h |

## LLM adapter interface

The `outreach/llm/` module sits between the pipeline tasks and whichever
LLM backend is configured. Everything goes through a single ABC:

```python
class LLMAdapter(ABC):
    def enrich_contact(self, lead) -> dict: ...
    def qualify_lead(self, lead, campaign) -> tuple[float | None, str]: ...
    def write_outreach(self, lead, campaign) -> str | None: ...
```

Concrete implementations:

| Adapter               | Module                         | Notes                                                    |
|-----------------------|--------------------------------|----------------------------------------------------------|
| `AnthropicAdapter`    | `outreach.llm.client`          | Default. Anthropic SDK + prompt caching + web_search.    |
| `SunaSidecarAdapter`  | `outreach.llm.adapter` (stub)  | Stub — see `docs/SUNA_INTEGRATION.md`.                   |
| `NoopAdapter`         | `outreach.llm.adapter`         | Auto-used when `ANTHROPIC_API_KEY` is missing.           |

`get_adapter()` picks the backend from `settings.LLM["ADAPTER"]` and
falls back to Noop on any initialisation error, so the daemon never
crashes because of an LLM configuration problem.

After every task the daemon re-seeds periodic jobs so the pipeline keeps
flowing forever without external schedulers.

## Cold-start qualifier

With fewer than 2 labels the Bayesian qualifier is useless, so `qualify.py`
falls back to the legacy `lead_score` heuristic (0–100, see
`utils/lead_scoring.py`). Once a user labels leads in the CRM as
WON / REPLIED / LOST, `outreach.ml.qualifier.retrain()` refits a
`GaussianProcessRegressor` on the accumulated embeddings and the system
starts using posterior means to prioritise outreach.

## Running

```bash
make setup         # install deps, migrate, bootstrap default campaign
make run           # start the daemon (interactive onboarding on first run)
make admin         # Django admin / CRM at http://localhost:8000
```

Or with Docker:

```bash
make up            # builds compose/outreach/Dockerfile and starts daemon
```
