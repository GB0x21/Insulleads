"""
Streamlit MVP dashboard — Phase 4 of the roadmap.

Run with:

    streamlit run dashboards/streamlit_app.py

The app boots Django in-process so every query hits the same
SQLite / Postgres instance the daemon uses. Three tabs:

  * **Map** — pydeck ScatterplotLayer of qualified leads.
  * **Funnel** — stage counts + conversion rates per campaign.
  * **Leads** — paginated table with a per-row detail expander.

This is a deliberately-small MVP: no auth (bind to localhost), no
writes, no caching subtleties. It exists so the operator can see
what the daemon is doing at a glance without opening the Django
admin.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ─── Django bootstrap ───────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "outreach.django_settings")

import django  # noqa: E402

django.setup()

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402
from django.db.models import Count  # noqa: E402

from outreach.models import Campaign, Lead  # noqa: E402

try:
    import pydeck as pdk  # noqa: E402

    _HAS_PYDECK = True
except Exception:  # noqa: BLE001
    _HAS_PYDECK = False


# ─── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Insulleads — MVP dashboard",
    page_icon="🏗️",
    layout="wide",
)

st.title("Insulleads — MVP dashboard")
st.caption(
    "Read-only view on the daemon's Lead / Campaign tables. "
    "For edits use the Django admin at /admin/."
)


# ─── Campaign picker ────────────────────────────────────────────────
campaigns = list(Campaign.objects.filter(is_active=True).order_by("name"))
if not campaigns:
    st.warning("No active campaigns. Run `python manage.py setup_crm` first.")
    st.stop()

campaign_names = ["All campaigns"] + [c.name for c in campaigns]
selected = st.sidebar.selectbox("Campaign", campaign_names, index=0)
selected_campaign = None
if selected != "All campaigns":
    selected_campaign = next(c for c in campaigns if c.name == selected)

lead_qs = Lead.objects.all()
if selected_campaign is not None:
    lead_qs = lead_qs.filter(campaign=selected_campaign)

st.sidebar.metric("Total leads", lead_qs.count())
st.sidebar.metric(
    "Qualified",
    lead_qs.filter(stage=Lead.Stage.QUALIFIED).count(),
)
st.sidebar.metric(
    "Contacted",
    lead_qs.filter(stage=Lead.Stage.CONTACTED).count(),
)


# ─── Tabs ───────────────────────────────────────────────────────────
tab_map, tab_funnel, tab_leads = st.tabs(["🗺️ Map", "📊 Funnel", "📋 Leads"])


# ── Map ─────────────────────────────────────────────────────────────
with tab_map:
    geo_qs = lead_qs.exclude(latitude__isnull=True).exclude(longitude__isnull=True)
    rows = list(
        geo_qs.values(
            "id",
            "title",
            "city",
            "latitude",
            "longitude",
            "qualification_score",
            "lead_score",
            "stage",
        )[:2000]
    )
    if not rows:
        st.info(
            "No geocoded leads yet. The thermal engine (Phase 1) and the "
            "permits agent both populate latitude / longitude."
        )
    elif not _HAS_PYDECK:
        st.warning(
            "pydeck not installed — falling back to st.map(). "
            "`pip install pydeck` for the full view."
        )
        st.map(
            pd.DataFrame(rows).rename(
                columns={"latitude": "lat", "longitude": "lon"}
            )
        )
    else:
        df = pd.DataFrame(rows)
        df["score"] = df["qualification_score"].fillna(df["lead_score"] / 100.0)
        # Colour ramp: grey → red for score 0 → 1
        df["color"] = df["score"].apply(
            lambda s: [int(255 * (s or 0)), 80, int(255 * (1 - (s or 0))), 180]
        )
        view = pdk.ViewState(
            latitude=float(df["latitude"].mean()),
            longitude=float(df["longitude"].mean()),
            zoom=10,
            pitch=0,
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position="[longitude, latitude]",
            get_radius=80,
            get_fill_color="color",
            pickable=True,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view,
                tooltip={"text": "{title}\n{city}\nstage: {stage}"},
            )
        )


# ── Funnel ──────────────────────────────────────────────────────────
with tab_funnel:
    stage_order = [
        Lead.Stage.DISCOVERED,
        Lead.Stage.QUALIFIED,
        Lead.Stage.READY_TO_CONTACT,
        Lead.Stage.CONTACTED,
        Lead.Stage.REPLIED,
        Lead.Stage.WON,
        Lead.Stage.LOST,
    ]
    counts = dict(
        lead_qs.values_list("stage")
        .annotate(n=Count("id"))
        .values_list("stage", "n")
    )
    funnel_df = pd.DataFrame(
        [
            {"stage": str(s.label), "count": counts.get(s.value, 0)}
            for s in stage_order
        ]
    )
    st.subheader("Stage counts")
    st.bar_chart(funnel_df.set_index("stage"))

    total = lead_qs.count() or 1
    replied = counts.get(Lead.Stage.REPLIED.value, 0) + counts.get(
        Lead.Stage.WON.value, 0
    )
    won = counts.get(Lead.Stage.WON.value, 0)
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Discovery → Qualified",
                 f"{(counts.get(Lead.Stage.QUALIFIED.value, 0) / total):.1%}")
    col_b.metric("Discovery → Replied", f"{(replied / total):.1%}")
    col_c.metric("Discovery → Won", f"{(won / total):.1%}")


# ── Leads ───────────────────────────────────────────────────────────
with tab_leads:
    stage_filter = st.multiselect(
        "Stages",
        options=[s.value for s in Lead.Stage],
        default=[],
    )
    filtered = lead_qs
    if stage_filter:
        filtered = filtered.filter(stage__in=stage_filter)
    filtered = filtered.order_by("-qualification_score", "-lead_score")[:200]
    lead_rows = list(
        filtered.values(
            "id",
            "title",
            "city",
            "project_type",
            "project_value",
            "stage",
            "qualification_score",
            "lead_score",
            "contact_company",
            "contact_phone",
            "contact_email",
        )
    )
    if not lead_rows:
        st.info("No leads match these filters.")
    else:
        st.dataframe(pd.DataFrame(lead_rows), width="stretch")
        picked = st.selectbox(
            "Inspect lead",
            options=[r["id"] for r in lead_rows],
            format_func=lambda pk: next(
                f"#{r['id']} — {r['title']}" for r in lead_rows if r["id"] == pk
            ),
        )
        if picked is not None:
            lead = Lead.objects.get(pk=picked)
            with st.expander(lead.title, expanded=True):
                st.write(f"**Stage:** {lead.get_stage_display()}")
                st.write(f"**City:** {lead.city or '—'}")
                st.write(f"**Project type:** {lead.project_type or '—'}")
                st.write(
                    f"**Value:** ${float(lead.project_value or 0):,.0f}"
                )
                st.write(
                    f"**Qualification score:** "
                    f"{lead.qualification_score or 0:.3f}"
                )
                st.write(f"**Lead score (heuristic):** {lead.lead_score}/100")
                if lead.llm_qualification_reason:
                    st.write(
                        f"**LLM reason:** {lead.llm_qualification_reason}"
                    )
                if lead.llm_outreach_body:
                    st.text_area(
                        "Drafted outreach",
                        value=lead.llm_outreach_body,
                        height=160,
                    )
                if lead.description:
                    st.markdown("**Description**")
                    st.write(lead.description)
