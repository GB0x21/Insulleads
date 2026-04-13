"""
Thermal engine — Phase 1 of the roadmap.

Pulls Landsat-8 surface temperature (ST_B10) via Google Earth Engine,
intersects it with OSMnx building footprints, and writes per-building
anomaly-K values to a cached parquet that :mod:`agents.thermal_agent`
reads and emits as leads.

Every submodule is dependency-guarded: missing `geemap`, `rasterio`,
`osmnx`, `earthengine-api`, or the GCP service account JSON all
degrade to a no-op so the rest of the daemon keeps running. See
``docs/THERMAL_SETUP.md`` for the GCP + Earth Engine setup.
"""
from .cache import cache_path, load_anomalies, save_anomalies

__all__ = ["cache_path", "load_anomalies", "save_anomalies"]
