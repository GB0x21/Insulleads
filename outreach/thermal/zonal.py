"""
Zonal statistics — per-building thermal anomaly.

Runs ``rasterstats.zonal_stats`` over a building-footprint GeoDataFrame
and a Landsat-8 surface-temperature raster, computes the anomaly in
Kelvin relative to the scene median, and returns a
DataFrame of ``(building_id, lat, lon, anomaly_k)`` rows.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("outreach.thermal.zonal")


try:
    import numpy as np  # type: ignore
    import rasterio  # type: ignore
    from rasterstats import zonal_stats  # type: ignore

    _HAS_RASTER = True
except Exception:  # noqa: BLE001
    _HAS_RASTER = False


class ZonalUnavailable(RuntimeError):
    pass


def zonal_anomaly(raster_path: Path, footprints):
    """Return a DataFrame of per-building anomaly in Kelvin.

    ``footprints`` is a GeoPandas GeoDataFrame in the same CRS as the
    raster (typically EPSG:4326 for Landsat exports from GEE).
    """
    if not _HAS_RASTER:
        raise ZonalUnavailable(
            "rasterio / rasterstats not installed. "
            "`pip install rasterio rasterstats`."
        )
    import pandas as pd  # local: pandas is already pulled in via requirements/local

    with rasterio.open(str(raster_path)) as src:
        affine = src.transform
        arr = src.read(1)

    scene_median = float(np.nanmedian(arr))
    logger.info("[thermal] scene median K = %.2f", scene_median)

    stats = zonal_stats(
        vectors=footprints,
        raster=str(raster_path),
        stats=["mean"],
        affine=affine,
        nodata=src.nodata if hasattr(src, "nodata") else None,
        geojson_out=False,
    )

    centroids = footprints.geometry.centroid
    rows: list[dict] = []
    for i, stat in enumerate(stats):
        mean_k = stat.get("mean") if stat else None
        if mean_k is None:
            continue
        anomaly = float(mean_k) - scene_median
        c = centroids.iloc[i]
        rows.append(
            {
                "building_id": str(i),
                "latitude": float(c.y),
                "longitude": float(c.x),
                "mean_k": float(mean_k),
                "anomaly_k": anomaly,
            }
        )
    df = pd.DataFrame(rows)
    logger.info(
        "[thermal] zonal stats: %d buildings, top anomaly=%.2f K",
        len(df),
        float(df["anomaly_k"].max()) if not df.empty else 0.0,
    )
    return df
