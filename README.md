# Insulleads

**Lead-generation stack for insulation contractors in the Bay Area,
adapted from [OpenOutreach](https://github.com/eracle/OpenOutreach).**

Insulleads takes OpenOutreach's architecture — a self-hosted Django CRM
with a daemon that discovers, qualifies (Bayesian GP) and contacts
prospects — and swaps the LinkedIn-scraping backend for **15 public-data
agents**: building permits, solar installs, 311 rodent reports, NOAA
flood alerts, active construction inspections, real-estate sales, energy
benchmarking, demolition/abatement permits, Google Places, Yelp, Cal Fire
wildfire rebuilds (DINS), HUD/LIHTC multifamily, Shovels.ai national
permits, Accela ACA portals, and Landsat-8 thermal anomalies. Outreach
goes through Telegram / SendGrid / Twilio instead of LinkedIn messages.

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

**California-specific high-value sources** *(opt-in via env)*:

| Agent | Env var | What it gives | Why it matters |
|---|---|---|---|
| `agents/dins_agent.py` | `AGENT_DINS=true` | Cal Fire DINS — every structure damaged in CA wildfires (Park, Borel, Camp, Glass, Dixie, etc.) | Every Destroyed/Major-Damage structure is being rebuilt. Title 24 obliges new insulation on every rebuild. The DINS dataset *is* the entire post-fire CA rebuild market. |
| `agents/hud_agent.py` | `AGENT_HUD_MULTIFAMILY=true` | HUD LIHTC + assisted multifamily | One conversation = a 50-500-unit job; LIHTC properties placed in service ≥20 yrs ago are flagged as rehab candidates. ~10× ROI per outreach hour vs SFR. |
| `agents/shovels_agent.py` | `AGENT_SHOVELS=true` + `SHOVELS_API_KEY` | National permits aggregator (Shovels.ai) | Extends coverage from Bay Area only to every CA jurisdiction Shovels indexes (LA, San Diego, Sacramento, OC, …) plus the rest of the country. Free tier from [shovels.ai](https://shovels.ai/). |

**Statewide auto-discovery**:

```bash
# Find every CA permit dataset on a Socrata portal that permits_agent
# doesn't already know about — outputs sample rows so you can map
# column names before adding it to permits_agent._build_sources.
python manage.py discover_socrata_permits --state CA --new-only --sample
```

**CSLB bulk index** *(replaces the slow ASP.NET scraper)*: request the
free CSLB extract at
[cslb.ca.gov/About_us/Library/Data_Requests](https://www.cslb.ca.gov/About_us/Library/Data_Requests/),
drop the CSV at `data/cslb_bulk.csv` (or set `CSLB_BULK_PATH`), and the
permit enrichment cascade picks it up automatically — lookups go from
~2s/contractor to <1ms. Inspect with `python manage.py cslb_index --info`.

**Five-tier contact enrichment on permit leads.** When the upstream
agency's payload is light on contact info, `agents/permits_agent.py`
fills the gaps progressively without overwriting earlier hits:

1. **Local CSV** (`utils/contacts_loader.py`) — fast, free; phone/email
   from the bundled `contacts/*.csv` indices.
2. **CSLB** — by license number → company name → owner; recovers
   license number, contractor city, and license status.
3. **Hunter.io** — fills email when still missing
   (set `HUNTER_API_KEY` in `.env`).
4. **Apollo.io** — decision-maker email + name + LinkedIn
   (set `APOLLO_API_KEY` in `.env`).
5. **LLM `WebSearchTool`** — the pydantic-ai `enrich_contact` from
   `outreach/llm/` runs as a last-resort tier for whatever's still
   missing (website, license, decision-maker name). Fires only when
   `LLM_ADAPTER=anthropic` and at least one of phone/email/website is
   still blank. Disable with `PERMITS_LLM_ENRICH=false`.

**Reachability sort.** A 0–4 `contact_quality_score`
(`outreach/lead_quality.py`) — +1 for each of phone / email / website /
linkedin — drives the send order in both paths. The legacy permits
agent sorts its batch `(-quality, -value)` so you see the most-
reachable big jobs first; the Django outreach loop sorts
`(qualification, quality, lead_score)` so within each qualification
tier the richer-contact lead wins. The score also appears in every
Telegram message (`📊 Calidad de contacto: 3/4` legacy, `📊 reach 3/4`
on the Django context block) so the operator sees at a glance how
reachable a lead is.

The `notify` Telegram card surfaces the new fields conditionally —
`🌐 Website`, `💼 Cargo`, `🔗 LinkedIn`, plus the existing CSLB block —
so a fully-enriched lead reads like:

```
👷 Contratista (GC): Acme Builders Inc
🪪 Licencia CSLB: 1098765
📞 Teléfono GC: (415) 555-1212  (via CSV (B_CONTACTS_GC.csv) + CSLB + Hunter.io)
✉️  Email GC: ops@acme.example
🌐 Website: https://acmebuilders.example
💼 Cargo: Owner
🔗 LinkedIn: https://linkedin.com/company/acme
🏢 Ciudad GC (CSLB): Oakland
✅ Estado Licencia: Active
```

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

---

## Discovery Agents — Reference

Each agent runs on a configurable interval, fetches leads from one or more public
data sources, scores them with the memory-enhanced scorer, and routes notifications
to Telegram (primary) + WhatsApp / Email for high-score leads. Cross-agent
deduplication (`utils/dedup.py`) automatically merges data from multiple agents
that report the same address into a single enriched lead.

### Standard agents (on by default)

These ten agents start automatically unless their env var is set to `false`.

---

#### 🏗️ `permits` — Building Permits

| | |
|---|---|
| **File** | `agents/permits_agent.py` |
| **Env var** | `AGENT_PERMITS=true` |
| **Interval** | 60 min (tunable: `INTERVAL_PERMITS`) |
| **Engine** | Socrata + CKAN + CKAN SQL |
| **Geography** | San Francisco, San Jose, Oakland, Berkeley, Fremont, Sunnyvale, Richmond, Hayward, Vallejo, Napa, Petaluma, Santa Cruz, Stockton (25+ endpoints) |

**What it finds:** Construction, remodel, addition, and renovation building permits
issued by Bay Area municipal permit offices. Filters on `MIN_PERMIT_VALUE` ($50k
default) and `PERMIT_MONTHS` (3-month window) to surface high-value, recently-active
jobs.

**Why it matters for insulation:** Every building permit for new construction, addition,
or full renovation requires insulation under Title 24. The permit identifies the
general contractor by name and CSLB license number — often before the job starts.

**Contact enrichment (5-tier cascade):**
1. Local CSV cache (`contacts/*.csv`)
2. CSLB license lookup (phone, company city, license status)
3. Hunter.io (email)
4. Apollo.io (decision-maker name + LinkedIn)
5. LLM `WebSearchTool` — last resort (disable with `PERMITS_LLM_ENRICH=false`)

**Quality filters:** Drops enforcement permits, planning-only approvals, and
out-of-trade scope (fences, signs, pools, demolition-only). Stale or unparseable
`issued_date` values are rejected.

---

#### ☀️ `solar` — Solar Installations

| | |
|---|---|
| **File** | `agents/solar_agent.py` |
| **Env var** | `AGENT_SOLAR=true` |
| **Interval** | 60 min (`INTERVAL_SOLAR`) |
| **Engine** | Socrata + CKAN + NREL Solar Resource API |
| **Geography** | SF, San Jose, Oakland, Berkeley, Fremont, Sunnyvale, Richmond (7 cities) |

**What it finds:** Solar installation permits and applications from municipal permit
offices, enriched with NREL solar resource data. Optional paid sources: Google
Solar API (rooftop potential), Aurora Solar (active proposals), EnergySage
(marketplace buyers actively seeking quotes).

**Why it matters:** Solar installers always look for cross-sell opportunities — a home
adding solar panels almost always needs a full energy-efficiency audit, and under-
insulated envelopes are the top reason solar ROI falls short of projections.

**Key env vars:** `NREL_API_KEY` (free), `GOOGLE_SOLAR_API_KEY` ($0.40/req),
`AURORA_SOLAR_API_KEY` ($100+/mo), `MIN_PERMIT_VALUE`, `PERMIT_MONTHS`.

---

#### 🐀 `rodents` — 311 Pest & Rodent Reports

| | |
|---|---|
| **File** | `agents/rodents_agent.py` |
| **Env var** | `AGENT_RODENTS=true` |
| **Interval** | 120 min (`INTERVAL_RODENTS`) |
| **Engine** | SeeClickFix (5 city portals) |
| **Geography** | San Francisco, Oakland, San Jose, Berkeley, Fremont |

**What it finds:** 311 service requests for rodent infestations, termites, wildlife
intrusion (raccoons, squirrels, opossums), cockroaches, and general pest activity
within the last `RODENT_MONTHS` (default: 2 months).

**Pest types and why they matter for insulation:**

| Pest | Severity | Insulation angle |
|------|----------|-----------------|
| Rodents / rats | High | Rodents nest in and shred attic/crawlspace insulation |
| Termites | High | Structural damage always requires insulation replacement |
| Wildlife (raccoons, squirrels) | Medium | Attic insulation contaminated or destroyed |
| Cockroaches / bed bugs | Low | Contamination requires inspection and partial replacement |

Non-pest 311 categories (graffiti, potholes, abandoned vehicles) are filtered out.
Optional enrichment: ATTOM Property API for property age and assessed value.

---

#### 🌊 `flood` — NOAA Flood Alerts

| | |
|---|---|
| **File** | `agents/flood_agent.py` |
| **Env var** | `AGENT_FLOOD=true` |
| **Interval** | 30 min (`INTERVAL_FLOOD`) |
| **Engine** | NOAA Weather API (no auth required) |
| **Geography** | 13 Bay Area forecast zones (SF, Alameda/Oakland, Santa Clara Valley, Contra Costa, San Mateo, Marin, Sonoma, Napa, Solano, East Bay, San Joaquin) |

**What it finds:** Active NOAA flood warnings, watches, and advisories for Bay Area
weather zones. Catches Flood Warning, Flash Flood Warning, Coastal Flood Warning,
Flood Advisory, and related event types.

**Why it matters:** Flood and moisture intrusion is the primary cause of crawlspace
and basement insulation failure. A flood advisory in a neighborhood is an immediate
outreach window — homeowners will be assessing damage within days.

---

#### 🚧 `construction` — Active Construction Inspections

| | |
|---|---|
| **File** | `agents/construction_agent.py` |
| **Env var** | `AGENT_CONSTRUCTION=true` |
| **Interval** | 60 min (`INTERVAL_CONSTRUCTION`) |
| **Engine** | Socrata + CKAN |
| **Geography** | SF, San Jose, Oakland, Sunnyvale, Berkeley, Richmond |

**What it finds:** Scheduled and completed building inspection records for projects
currently under construction — not just permitted, but actively being built.
`CONSTRUCTION_MONTHS` window (default: 1 month, tighter than permits).

**Why it matters:** Inspections reveal the construction _phase_, which tells you
exactly when to call:

| Phase | Signal | Action |
|-------|--------|--------|
| Foundation | Too early | Log for follow-up |
| Framing | **Contact now** | Insulation is the immediate next step |
| Insulation | They're buying | Find out the current supplier |
| Drywall | Last chance | Blown-in retrofit still possible |
| Final | Missed this job | Add to future-upgrades list |

Optional paid enrichment: BuildZoom API for advanced project tracking (`BUILDZOOM_API_KEY`).

---

#### 🏠 `realestate` — Recent Property Sales

| | |
|---|---|
| **File** | `agents/realestate_agent.py` |
| **Env var** | `AGENT_REALESTATE=true` |
| **Interval** | 120 min (`INTERVAL_REALESTATE`) |
| **Engine** | Socrata |
| **Geography** | San Francisco, Alameda County (Oakland, Berkeley, Fremont, Hayward) |

**What it finds:** Property deeds recorded in the last `SALE_MONTHS` (default: 2
months) with a sale price above `MIN_SALE_PRICE` ($400k default).

**Why it matters:** A new homeowner buying a home older than 20 years is one of the
strongest purchase-intent signals in the market. The first 6-12 months after purchase
is when renovation decisions are made. The buyer's name is on the deed and often
findable via county records.

---

#### ⚡ `energy` — Energy Benchmarking / Audits

| | |
|---|---|
| **File** | `agents/energy_agent.py` |
| **Env var** | `AGENT_ENERGY=true` |
| **Interval** | 360 min (`INTERVAL_ENERGY`) |
| **Engine** | Socrata |
| **Geography** | San Francisco (commercial buildings >10k sqft) |

**What it finds:** Commercial buildings with low ENERGY STAR scores (< 50) from SF's
mandatory energy benchmarking dataset. Also picks up permit applications with
"energy audit" or "energy retrofit" in the description.

**Why it matters:** Under SF's Building Performance Ordinance, commercial buildings
that fail to improve their score face escalating fines. A low ENERGY STAR score is
a compliance problem the building owner is legally required to fix. Insulation is
typically the highest-ROI single improvement.

**Key fields surfaced:** `energy_star_score`, `total_ghg_emissions`, `floor_area`,
`year_built`, `primary_property_type`.

---

#### 🔨 `deconstruction` — Demolition & Abatement Permits

| | |
|---|---|
| **File** | `agents/deconstruction_agent.py` |
| **Env var** | `AGENT_DECONSTRUCTION=true` |
| **Interval** | 120 min (`INTERVAL_DECONSTRUCTION`) |
| **Engine** | Socrata + CKAN |
| **Geography** | SF, San Jose, Oakland, Berkeley |

**What it finds:** Demolition permits, asbestos abatement notifications (BAAQMD),
hazardous material removal, and selective deconstruction projects above
`MIN_DECON_VALUE` ($50k default) within `DECON_MONTHS` (3 months).

**Why it matters for insulation:**
1. Demolition → new construction requires all-new insulation (Title 24)
2. Asbestos abatement → old insulation is removed; replacement is mandatory
3. Deep renovation / selective deconstruction → full insulation upgrade
4. Hazmat removal → re-insulation is part of the remediation package

Optional paid enrichment: ATTOM Property pre-foreclosure data (`ATTOM_API_KEY`).

---

#### 📍 `places` — Google Places Contractor Search

| | |
|---|---|
| **File** | `agents/places_agent.py` |
| **Env var** | `AGENT_PLACES=true` (opt-out default: `false`) |
| **Interval** | 1440 min / 24h (`INTERVAL_PLACES`) |
| **Engine** | Google Places API (Nearby Search) |
| **Geography** | SF, Oakland, San Jose, Fremont, Berkeley (5 city centers, 5–10 km radius) |
| **API cost** | $200/mo free credit ≈ 5,000 searches |

**What it finds:** Active general contractors, remodelers, HVAC contractors,
roofing contractors, and construction companies discoverable on Google Maps.

**Why it matters:** These are businesses already doing the work where insulation is
a natural upsell or referral opportunity. A Google-verified business has a phone
number and often a website — contact quality is high.

**Requires:** `GOOGLE_PLACES_API_KEY`.

---

#### ⭐ `yelp` — Yelp Fusion Contractor Directory

| | |
|---|---|
| **File** | `agents/yelp_agent.py` |
| **Env var** | `AGENT_YELP=true` (opt-out default: `false`) |
| **Interval** | 1440 min / 24h (`INTERVAL_YELP`) |
| **Engine** | Yelp Fusion API (5,000 calls/day free) |
| **Geography** | SF, Oakland, San Jose, Fremont, Berkeley, Hayward, Richmond, Sunnyvale |

**What it finds:** Yelp-listed contractors in categories: `contractors`, `hvac`,
`roofing`, `insulation_installation`, `handyman`, `home_energy_auditors` with
recent activity and high ratings.

**Why it matters:** Yelp contractors with active reviews are currently running
jobs. High ratings signal quality GCs who care about their referral reputation —
exactly the partner profile for insulation subcontracting.

**Requires:** `YELP_API_KEY` (free at [yelp.com/developers](https://www.yelp.com/developers/v3/manage_app)).

---

### Opt-in agents (disabled by default)

These five agents target specialized or high-volume data sources. Enable them
individually by setting the corresponding env var to `true`.

---

#### 🔥 `dins` — Cal Fire DINS Post-Wildfire Reconstruction

| | |
|---|---|
| **File** | `agents/dins_agent.py` |
| **Env var** | `AGENT_DINS=true` |
| **Interval** | 1440 min / 24h (`INTERVAL_DINS`) |
| **Engine** | ArcGIS FeatureServer (Cal Fire) |
| **Geography** | California statewide (all major fire perimeters: Camp, Glass, Dixie, Mosquito, Park, Borel, 2023–2025) |

**What it finds:** Every structure inspected by Cal Fire DINS (Damage Inspection)
after a wildfire, categorized by damage level: Destroyed, Major Damage, Minor
Damage, Affected. Default filter: `DINS_DAMAGE_MIN=major` (≥ 26% damage).

**Why it matters:** This is the entire post-wildfire rebuild market in California.
Every "Destroyed" or "Major Damage" structure will be rebuilt — insurance covers
it — and Title 24 mandates new insulation on every rebuild. Signal-to-intent ratio
is effectively 100%.

**Cross-agent synergy:** When `permits` is also enabled, the deduplication engine
automatically merges a DINS address with its active rebuild permit, giving you
the contractor name on top of the property owner address.

**Key env vars:** `DINS_LAYERS` (comma-sep ArcGIS URLs), `DINS_DAMAGE_MIN`,
`DINS_MAX_RECORDS` (default 1000), `DINS_TIMEOUT_S` (default 45).

---

#### 🏘️ `hud` — HUD / LIHTC Multifamily Rehab

| | |
|---|---|
| **File** | `agents/hud_agent.py` |
| **Env var** | `AGENT_HUD_MULTIFAMILY=true` |
| **Interval** | 1440 min / 24h (`INTERVAL_HUD_MULTIFAMILY`) |
| **Engine** | ArcGIS FeatureServer (HUD Open Data) |
| **Geography** | California (configurable via `HUD_STATE`) |

**What it finds:** LIHTC (Low-Income Housing Tax Credit) multifamily properties
placed in service ≥ `HUD_REHAB_WINDOW_YRS` (default 20) years ago — the primary
rehab signal — plus HUD-assisted multifamily properties with ≥ `HUD_MIN_UNITS`
(default 8) units.

**Why it matters:** One successful conversation with a multifamily property
manager or sponsor = a 50–500 unit insulation job. LIHTC properties have
contractual rehabilitation funding cycles every 15–30 years. The dataset ships
with the sponsor/owner contact organization, giving the enrichment cascade a
real entity to look up.

**Scale:** ~10× ROI per outreach hour vs. SFR permits.

**Key env vars:** `HUD_STATE`, `HUD_MIN_UNITS`, `HUD_REHAB_WINDOW_YRS`,
`HUD_MAX_RECORDS` (2000), `HUD_TIMEOUT_S` (60).

---

#### 🪏 `shovels` — Shovels.ai National Permit Aggregator

| | |
|---|---|
| **File** | `agents/shovels_agent.py` |
| **Env var** | `AGENT_SHOVELS=true` |
| **Interval** | 1440 min / 24h (`INTERVAL_SHOVELS`) |
| **Engine** | Shovels.ai REST API |
| **Geography** | Any US state (default CA — extends to LA, San Diego, Sacramento, OC, Inland Empire and beyond) |

**What it finds:** Building permits across thousands of US jurisdictions with a single
normalized schema. Filters: `SHOVELS_PERMIT_TYPES` (residential / additions / new
construction / re-roof), `SHOVELS_MIN_VALUE` ($50k), `SHOVELS_DAYS_BACK` (90 days),
`SHOVELS_STATES`.

**Why it matters:** The `permits` agent covers ~25 Bay Area endpoints. Shovels adds
statewide CA coverage and the rest of the country with one source and a single API
key. Also returns contractor license numbers that feed directly into the 5-tier
enrichment cascade.

**Soft-fails gracefully:** returns `[]` with a warning log when `SHOVELS_API_KEY`
is not set — the daemon keeps running.

**Requires:** `SHOVELS_API_KEY` (free tier at [shovels.ai](https://shovels.ai/)).
Key env vars: `SHOVELS_STATES`, `SHOVELS_MIN_VALUE`, `SHOVELS_DAYS_BACK`,
`SHOVELS_MAX_PER_STATE` (200).

---

#### 🏛️ `accela` — Accela ACA Portal Scraper

| | |
|---|---|
| **File** | `agents/accela_agent.py` |
| **Env var** | `AGENT_ACCELA=true` |
| **Interval** | 720 min / 12h (`INTERVAL_ACCELA`) |
| **Engine** | Playwright / ASP.NET form scraper (stealth TLS) |
| **Geography** | Oakland (Accela ACA portal — no public REST API) |

**What it finds:** Building permits from city portals that use Accela Citizen Access
but don't expose a public REST API. The agent maintains an ASP.NET session,
submits the search form with a `PERMIT_MONTHS` date range, and paginates through
results (up to `ACCELA_MAX_PAGES`, default 5).

**Relevant permit types:** ADDITION, ALTERATION, REMODEL, NEW CONSTRUCTION, ADU,
SOFT STORY, SEISMIC, TENANT IMPROVEMENT.

**Why it matters:** Accela-based cities are a gap in the Socrata/CKAN coverage.
Oakland alone represents a significant permit volume that `permits_agent` doesn't
reach.

**Requires:** `utils/stealth_fetcher.py` (bundled). `ACCELA_MAX_PAGES`,
`PERMIT_MONTHS` env vars.

---

#### 🌡️ `thermal` — Landsat-8 Thermal Anomaly Detection

| | |
|---|---|
| **File** | `agents/thermal_agent.py` |
| **Env var** | `AGENT_THERMAL=true` |
| **Interval** | 1440 min / 24h (`INTERVAL_THERMAL`) |
| **Engine** | Local cache (offline raster pull via Earth Engine) |
| **Geography** | Bay Area (wherever `thermal_pull` was last run) |

**What it finds:** Buildings whose surface temperature exceeds the scene median by
≥ 3 K (Kelvin), derived from Landsat-8 Band 10 thermal infrared imagery cross-
referenced with OpenStreetMap building footprints.

**Why it matters:** Elevated surface temperature = conditioned air escaping through
the building envelope = under-insulated structure. This is the most direct physical
signal that a building needs insulation work — no permit, no 311 report required.

**Two-phase workflow:**
1. Pull and cache raster data offline: `python manage.py thermal_pull`
   (hits Google Earth Engine, writes `data/thermal/anomalies.parquet`)
2. Agent reads the cache on every cycle and emits leads for anomalous buildings.

**Soft-fails:** returns `[]` with an info log when no parquet cache exists.

---

### Agent scoring and routing summary

| Agent | Default interval | Lead type | Contact signal |
|-------|-----------------|-----------|----------------|
| `permits` | 60 min | Building permit | Contractor (CSLB name + license) |
| `solar` | 60 min | Solar permit | Contractor or property address |
| `rodents` | 120 min | 311 pest report | Property address |
| `flood` | 30 min | NOAA alert zone | Geographic zone |
| `construction` | 60 min | Inspection record | Contractor + address |
| `realestate` | 120 min | Deed recording | Buyer name + address |
| `energy` | 360 min | Benchmarking score | Building name + address |
| `deconstruction` | 120 min | Demo/abatement permit | Contractor + address |
| `places` | 24h | Google Maps listing | Business phone/website |
| `yelp` | 24h | Yelp business listing | Business phone/website |
| `dins` *(opt-in)* | 24h | Wildfire damage record | Property address (+ permit cross-ref) |
| `hud` *(opt-in)* | 24h | LIHTC/HUD property | Sponsor org name |
| `shovels` *(opt-in)* | 24h | National permit | Contractor license |
| `accela` *(opt-in)* | 12h | Accela ACA permit | Permit record |
| `thermal` *(opt-in)* | 24h | Thermal anomaly | Building footprint (lat/lon) |

**Scoring → routing:**
- Score ≥ 90 → Telegram + WhatsApp + Email
- Score ≥ 70 → Telegram + Email
- Score < 70 → Telegram only

Run any agent standalone: `python main.py --run <agent_key>`
Drain stuck backlog: `python main.py --flush-backlog [agent_key]`

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
| `calcerts_raters`                | CalCERTS HERS rater registry — referral channel + Title 24 remediation |
| `cheers_raters`                  | CHEERS HERS rater registry — second CA HERS provider                    |
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

#### Activating a seeded web_crawler target end-to-end

The 7 targets seeded by `setup_crm` ship disabled with best-effort
schemas. The four-step activation flow:

```bash
# 1. Inspect — fetches the page and prints the markdown excerpt + the
#    25 most-frequent CSS classes so you can pick the real baseSelector.
python manage.py test_web_crawler --inspect \
    --url https://www.calcerts.com/raters_directory.html

# 2. Write a verified schema to a JSON file (use the classes you saw above)
cat > schemas/calcerts.json <<'JSON'
{
  "name": "CalCERTSRaters",
  "baseSelector": "tr.rater-row",
  "fields": [
    {"name": "business_name", "selector": "td.company", "type": "text"},
    {"name": "phone",         "selector": "td.phone",   "type": "text"},
    {"name": "email", "selector": "a[href^='mailto:']",
     "type": "attribute", "attribute": "href"},
    {"name": "city",          "selector": "td.city",    "type": "text"}
  ]
}
JSON

# 3. Smoke-test the schema against the live URL — make sure the first
#    5 leads look right.
python manage.py test_web_crawler \
    --url https://www.calcerts.com/raters_directory.html \
    --schema-file schemas/calcerts.json --limit 5

# 4. Activate — atomically replaces the seed schema and flips enabled=True.
python manage.py enable_web_crawler \
    --key calcerts_raters \
    --schema-file schemas/calcerts.json

# Inspect / disable later:
python manage.py enable_web_crawler --key calcerts_raters --show
python manage.py enable_web_crawler --key calcerts_raters --disable
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
