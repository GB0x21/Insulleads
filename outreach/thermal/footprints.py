"""
OSMnx building footprint download.

Pulls every ``building=*`` polygon within a city or bbox into a
GeoDataFrame and caches it to a local GeoPackage. We only need the
geometry + building centroid to run zonal stats against the
Landsat raster, so we drop most OSM tags on the way in.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("outreach.thermal.footprints")


try:
    import geopandas as gpd  # type: ignore
    import osmnx as ox  # type: ignore

    _HAS_OSMNX = True
except Exception:  # noqa: BLE001
    _HAS_OSMNX = False


class FootprintsUnavailable(RuntimeError):
    pass


def download_footprints(
    query: str | tuple[float, float, float, float],
    out_path: Path,
    *,
    refresh: bool = False,
):
    """Download building polygons for ``query`` and cache to ``out_path``.

    ``query`` can be a place name (``"San Francisco, CA, USA"``) or
    a ``(min_lon, min_lat, max_lon, max_lat)`` bbox.
    """
    if not _HAS_OSMNX:
        raise FootprintsUnavailable(
            "osmnx / geopandas not installed. `pip install osmnx geopandas`."
        )

    out_path = Path(out_path)
    if out_path.exists() and not refresh:
        logger.info("[thermal] reusing cached footprints at %s", out_path)
        return gpd.read_file(out_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    tags = {"building": True}
    if isinstance(query, tuple):
        min_lon, min_lat, max_lon, max_lat = query
        gdf = ox.features_from_bbox(
            bbox=(min_lat, max_lat, max_lon, min_lon),
            tags=tags,
        )
    else:
        gdf = ox.features_from_place(query, tags=tags)

    # Drop rows without polygonal geometries and uninteresting tag columns.
    gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]
    keep_cols = [c for c in ("name", "building", "addr:city") if c in gdf.columns]
    gdf = gdf[keep_cols + ["geometry"]]
    gdf = gdf.reset_index(drop=True)
    gdf.to_file(out_path, driver="GPKG")
    logger.info("[thermal] cached %d footprints to %s", len(gdf), out_path)
    return gdf
