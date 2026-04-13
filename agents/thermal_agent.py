"""
agents/thermal_agent.py
🌡️ Thermal anomaly leads — Landsat-8 ST_B10 × OSM footprints.

Phase 1 of the roadmap. The actual raster pull happens offline via
``python manage.py thermal_pull`` (which hits Earth Engine and
writes ``data/thermal/anomalies.parquet``). This agent only reads
the cached parquet and emits leads for buildings whose surface
temperature exceeds the scene median by ``MIN_ANOMALY_K`` Kelvin —
a proxy for poorly-insulated envelopes leaking conditioned air.
"""
from __future__ import annotations

import logging

from agents.base import BaseAgent
from outreach.thermal.cache import load_anomalies
from utils.telegram import send_lead

logger = logging.getLogger(__name__)

MIN_ANOMALY_K = 3.0  # threshold above scene median


class ThermalAgent(BaseAgent):
    name = "🌡️ Thermal anomaly — Landsat-8"
    emoji = "🌡️"
    agent_key = "thermal"

    def fetch_leads(self) -> list:
        df = load_anomalies()
        if df is None:
            logger.info(
                "[thermal] no cached anomalies — run `python manage.py "
                "thermal_pull` first."
            )
            return []

        if "anomaly_k" not in df.columns:
            logger.warning("[thermal] cache missing anomaly_k column")
            return []

        hot = df[df["anomaly_k"] >= MIN_ANOMALY_K]
        logger.info(
            "[thermal] %d buildings above %.1f K anomaly",
            len(hot),
            MIN_ANOMALY_K,
        )

        leads: list[dict] = []
        for _, row in hot.iterrows():
            anomaly = float(row["anomaly_k"])
            building_id = str(row.get("building_id") or row.name)
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            leads.append(
                {
                    "id": f"thermal-{building_id}",
                    "title": f"Thermal anomaly +{anomaly:.1f} K (building {building_id})",
                    "description": (
                        f"Landsat-8 ST_B10 shows this building is "
                        f"{anomaly:.1f} K above the scene median — likely "
                        f"insulation leakage on the envelope."
                    ),
                    "latitude": lat,
                    "longitude": lon,
                    "project_type": "thermal_retrofit",
                    "thermal_anomaly_k": anomaly,
                    "building_footprint_id": building_id,
                    "lead_score": min(100, int(40 + anomaly * 10)),
                }
            )
        return leads

    def notify(self, lead: dict):
        send_lead(lead, emoji=self.emoji)
