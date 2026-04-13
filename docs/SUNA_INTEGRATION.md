# Running Suna as a sidecar for Insulleads

> Status: **stub / roadmap**. The `SunaSidecarAdapter` in
> `outreach/llm/adapter.py` raises `NotImplementedError`. This document
> explains how to plug it in when you want to swap Anthropic for a Suna
> container without touching the pipeline code.

## Why a sidecar?

[Suna](https://github.com/kortix-ai/suna) (by kortix-ai) is a full
agent platform — React UI, Supabase, OpenCode sandbox runtime, 60+
skills, 3000+ Composio integrations. It is **not** a Python library, so
we cannot `import suna` the way we vendored OpenOutreach. The
supported integration path is to run Suna as its own service (Docker
compose) and talk to it over HTTP.

Insulleads was built so that the three LLM features — enrichment,
cold-start qualification and outreach copy — all go through a single
`LLMAdapter` ABC. Swapping backends is a one-env-var change once the
Suna adapter is implemented:

```bash
LLM_ADAPTER=suna
SUNA_BASE_URL=http://suna:3000
```

Nothing in `outreach/tasks/*` needs to change.

## What Suna gives you over the Anthropic adapter

| Capability                              | Anthropic adapter | Suna sidecar |
|-----------------------------------------|-------------------|--------------|
| Personalised outreach copy              | Yes               | Yes          |
| Web-search contact enrichment           | Yes (native tool) | Yes (browser) |
| Cold-start lead qualifier               | Yes               | Yes          |
| Multi-step agentic follow-ups           | No                | Yes          |
| Browser automation (CSLB, BBB, Yelp)    | No                | Yes          |
| CRM integrations out of the box         | No                | 3000+ via Composio |
| Persisted agent state / knowledge base  | No                | Yes (Supabase) |
| Self-hosted, no API key leakage         | No                | Yes          |

If you only need the three headline features today, stay on the
Anthropic adapter — it's ~150 lines of glue and two env vars. Reach
for Suna once you need the agentic follow-up loop, browser automation
against forms that block Claude's `web_search`, or a long list of CRM
sinks.

## Implementation checklist

1. **Run Suna.** Follow the kortix-ai README:

   ```bash
   git clone https://github.com/kortix-ai/suna suna
   cd suna && kortix start
   # or: docker compose -f docker-compose.yml up -d
   ```

   Suna exposes an HTTP API on `http://localhost:3000` (or whatever
   you mapped). Add it to `local.yml` as a sidecar next to the
   `outreach` service so the daemon can reach it at `http://suna:3000`.

2. **Create three Suna agents** (one per feature), with the system
   prompts from `outreach/llm/prompts.py`:

   - `insulleads_enricher` — tools: `web_search`, `browser.navigate`,
     `browser.extract`. System: `SYSTEM_ENRICHER`.
   - `insulleads_qualifier` — no tools. System: `SYSTEM_QUALIFIER` +
     the campaign prefix (can be baked in per-campaign agent).
   - `insulleads_writer` — no tools. System: `SYSTEM_WRITER` + the
     campaign prefix.

   Save the agent IDs — they're what you POST to when creating a
   session.

3. **Implement `SunaSidecarAdapter`** in `outreach/llm/adapter.py`.
   Replace the `_not_implemented()` stub with real HTTP calls:

   ```python
   import httpx
   from .prompts import campaign_prefix, lead_payload

   class SunaSidecarAdapter(LLMAdapter):
       name = "suna"

       def __init__(self, base_url: str) -> None:
           self.base_url = base_url.rstrip("/")
           self._http = httpx.Client(base_url=self.base_url, timeout=90.0)
           # These IDs come from step 2 — store them in env or DB.
           self.agent_ids = {
               "enrich": os.environ["SUNA_ENRICHER_AGENT_ID"],
               "qualify": os.environ["SUNA_QUALIFIER_AGENT_ID"],
               "write":   os.environ["SUNA_WRITER_AGENT_ID"],
           }

       def _run(self, agent: str, user_text: str) -> str:
           r = self._http.post(
               f"/v1/agents/{self.agent_ids[agent]}/sessions",
               json={"input": user_text},
           )
           r.raise_for_status()
           return r.json()["output"]

       def enrich_contact(self, lead):
           out = self._run("enrich", lead_payload(lead))
           return json.loads(out)

       def qualify_lead(self, lead, campaign):
           prompt = f"{campaign_prefix(campaign)}\n\n{lead_payload(lead)}"
           out = self._run("qualify", prompt)
           parsed = json.loads(out)
           return float(parsed["score"]), parsed.get("reason", "")

       def write_outreach(self, lead, campaign):
           prompt = f"{campaign_prefix(campaign)}\n\n{lead_payload(lead)}"
           return self._run("write", prompt)
   ```

   The actual Suna endpoints may differ — check `docs.suna.so/api` for
   the current shapes.

4. **Add `httpx` to `requirements/base.txt`** (it's the only new
   dependency; `anthropic` becomes optional).

5. **Flip the env vars:**

   ```bash
   LLM_ADAPTER=suna
   SUNA_BASE_URL=http://suna:3000
   SUNA_ENRICHER_AGENT_ID=agt_...
   SUNA_QUALIFIER_AGENT_ID=agt_...
   SUNA_WRITER_AGENT_ID=agt_...
   ```

6. **Smoke test:**

   ```bash
   python manage.py test_llm --feature all
   ```

   Should print the adapter as `suna` and return real output from each
   feature.

## Open questions

- **Billing:** Suna charges through its own quota / your self-hosted
  Claude key. Decide whether Insulleads or Suna holds the Anthropic
  credentials.
- **Latency:** Suna sessions are longer-lived than a single Messages
  API call. Tune `ENRICH_INTERVAL_MIN` upward if the sweep starts to
  back up behind long sessions.
- **Tool registration:** the browser tool requires Playwright inside
  the Suna container. Make sure `compose/suna/` pulls the full image,
  not the slim one.

Once all of this is done, `get_adapter()` will return the
`SunaSidecarAdapter`, the rest of the pipeline keeps running
unchanged, and you can delete `from anthropic import Anthropic` from
`outreach/llm/client.py` if you want to drop the SDK entirely.
