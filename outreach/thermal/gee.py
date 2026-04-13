"""
Google Earth Engine pull — Landsat-8 surface temperature (ST_B10).

Uses the `geemap` + `earthengine-api` stack to fetch a median
composite of Landsat-8 Collection 2 Level-2 surface temperature over
a bbox and date range, then exports it to a local GeoTIFF that the
zonal-stats step can read.

Everything is dependency-guarded: missing `geemap`/`ee` or missing
`GOOGLE_APPLICATION_CREDENTIALS` both raise :class:`ThermalUnavailable`
so the caller can degrade cleanly.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("outreach.thermal.gee")


class ThermalUnavailable(RuntimeError):
    """Raised when the thermal pipeline cannot run in this environment."""


# ─── Optional deps ──────────────────────────────────────────────────
try:
    import ee  # type: ignore
    import geemap  # type: ignore

    _HAS_GEE = True
except Exception:  # noqa: BLE001
    _HAS_GEE = False


def _ensure_initialized() -> None:
    """Authenticate Earth Engine with a service account JSON."""
    if not _HAS_GEE:
        raise ThermalUnavailable(
            "geemap / earthengine-api not installed. `pip install geemap "
            "earthengine-api` and see docs/THERMAL_SETUP.md."
        )
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not creds_path or not Path(creds_path).exists():
        raise ThermalUnavailable(
            "GOOGLE_APPLICATION_CREDENTIALS is not set or the file does "
            "not exist. See docs/THERMAL_SETUP.md for GCP setup."
        )
    try:
        if not ee.data._credentials:  # type: ignore[attr-defined]
            creds = ee.ServiceAccountCredentials(None, creds_path)
            ee.Initialize(credentials=creds)
    except Exception as exc:  # noqa: BLE001
        raise ThermalUnavailable(f"Earth Engine init failed: {exc}") from exc


def pull_landsat_st(
    bbox: tuple[float, float, float, float],
    date_range: tuple[str, str],
    out_path: Path,
    *,
    scale_m: int = 30,
) -> Path:
    """Export a median Landsat-8 ST_B10 composite to ``out_path``.

    Parameters
    ----------
    bbox
        ``(min_lon, min_lat, max_lon, max_lat)``.
    date_range
        ISO-8601 dates, e.g. ``("2024-06-01", "2024-09-30")``.
    out_path
        GeoTIFF destination. Parents are created.
    scale_m
        Target resolution in metres (Landsat native is 30).
    """
    _ensure_initialized()

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    region = ee.Geometry.BBox(*bbox)
    collection = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(*date_range)
        .select("ST_B10")
    )
    # ST_B10 is scaled; formula from USGS Collection 2 spec.
    # surface_temp_K = ST_B10 * 0.00341802 + 149.0
    image = (
        collection.median()
        .multiply(0.00341802)
        .add(149.0)
        .rename("surface_temp_K")
        .clip(region)
    )

    logger.info(
        "[thermal] exporting Landsat ST composite bbox=%s range=%s -> %s",
        bbox,
        date_range,
        out_path,
    )
    geemap.ee_export_image(
        image,
        filename=str(out_path),
        scale=scale_m,
        region=region,
        file_per_band=False,
    )
    return out_path
