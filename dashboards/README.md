# Insulleads — Streamlit MVP dashboard

A tiny read-only dashboard over the daemon's `Lead` / `Campaign` tables.
Meant for the operator to see what the daemon is doing at a glance
without opening the Django admin.

## Run

```bash
pip install -r requirements/local.txt   # pulls streamlit + pydeck
make dashboard                           # streamlit run dashboards/streamlit_app.py
```

The app boots Django in-process using `outreach.django_settings`, so
it hits whatever database the daemon is using (SQLite by default,
Postgres when deployed with `local.yml`). **It never writes** — for
edits / labels use `/admin/` on the regular Django server.

## Tabs

1. **Map** — pydeck `ScatterplotLayer` of geocoded leads, coloured by
   qualification score (grey → red). Falls back to `st.map()` when
   pydeck is missing.
2. **Funnel** — bar chart of stage counts + discovery → qualified →
   replied → won conversion rates.
3. **Leads** — sortable table of the top 200 leads by
   `qualification_score`, with a per-row expander that shows the
   LLM qualification reason and drafted outreach body.

## Notes

- Bind to localhost only — there is no auth layer.
- The dashboard does not depend on the daemon being alive; it just
  reads whatever the daemon has written.
- Phase 1 (thermal engine) will populate `latitude` / `longitude` on
  many more leads; until then the Map tab looks sparse.
