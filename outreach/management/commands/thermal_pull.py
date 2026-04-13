"""
thermal_pull — run the Phase 1 thermal pipeline end-to-end.

1. Pulls a Landsat-8 ST_B10 median composite over a bbox + date range
   from Earth Engine (via :mod:`outreach.thermal.gee`).
2. Downloads OSM building footprints for the same area (via
   :mod:`outreach.thermal.footprints`).
3. Runs zonal stats over each footprint and computes the anomaly in
   Kelvin relative to the scene median (via
   :mod:`outreach.thermal.zonal`).
4. Saves the resulting DataFrame to ``data/thermal/anomalies.parquet``
   where :class:`agents.thermal_agent.ThermalAgent` reads it at
   discover time.

Usage::

    python manage.py thermal_pull --city "San Francisco, CA, USA"
    python manage.py thermal_pull --bbox -122.52,37.70,-122.35,37.83 \\
                                  --from 2024-06-01 --to 2024-09-30

Requires the optional deps (`geemap`, `earthengine-api`, `rasterio`,
`rasterstats`, `osmnx`) and a GCP service account JSON pointed to
by ``GOOGLE_APPLICATION_CREDENTIALS``. See ``docs/THERMAL_SETUP.md``.
"""
from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Pull Landsat thermal anomalies and cache per-building Kelvin deltas."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--city", help="OSM place query, e.g. 'San Francisco, CA, USA'.")
        parser.add_argument(
            "--bbox",
            help="min_lon,min_lat,max_lon,max_lat (decimal degrees).",
        )
        parser.add_argument("--from", dest="date_from", default="2024-06-01")
        parser.add_argument("--to", dest="date_to", default="2024-09-30")
        parser.add_argument(
            "--refresh-footprints",
            action="store_true",
            help="Force redownload of OSM footprints even if cached.",
        )

    def handle(self, *args, **opts) -> None:
        if not opts["city"] and not opts["bbox"]:
            raise CommandError("Pass --city or --bbox.")

        # Imports are deferred so `manage.py thermal_pull --help` works
        # even when the optional raster stack is not installed.
        try:
            from outreach.thermal.cache import cache_path, save_anomalies
            from outreach.thermal.footprints import download_footprints
            from outreach.thermal.gee import ThermalUnavailable, pull_landsat_st
            from outreach.thermal.zonal import zonal_anomaly
        except ImportError as exc:
            raise CommandError(
                f"thermal deps missing: {exc}. pip install geemap earthengine-api "
                "rasterio rasterstats osmnx geopandas pyarrow"
            ) from exc

        bbox: tuple[float, float, float, float] | None = None
        if opts["bbox"]:
            try:
                bbox = tuple(float(p) for p in opts["bbox"].split(","))  # type: ignore[assignment]
                assert bbox is not None and len(bbox) == 4
            except Exception as exc:  # noqa: BLE001
                raise CommandError(f"--bbox must be 'min_lon,min_lat,max_lon,max_lat': {exc}") from exc

        base = Path(getattr(settings, "DATA_DIR", "data")) / "thermal"
        raster_path = base / "landsat_st.tif"
        footprints_path = base / "footprints.gpkg"

        # Step 1 — raster
        query_bbox = bbox
        if query_bbox is None and opts["city"]:
            # We need a bbox for the GEE export; geocode the city via OSMnx.
            import osmnx as ox  # type: ignore

            gdf = ox.geocode_to_gdf(opts["city"])
            query_bbox = tuple(gdf.total_bounds)  # (minx, miny, maxx, maxy)
            self.stdout.write(f"[thermal] geocoded {opts['city']} -> {query_bbox}")

        try:
            pull_landsat_st(
                bbox=query_bbox,  # type: ignore[arg-type]
                date_range=(opts["date_from"], opts["date_to"]),
                out_path=raster_path,
            )
        except ThermalUnavailable as exc:
            raise CommandError(f"thermal pull failed: {exc}") from exc

        # Step 2 — footprints
        query = opts["city"] or query_bbox
        footprints = download_footprints(
            query=query,
            out_path=footprints_path,
            refresh=opts["refresh_footprints"],
        )

        # Step 3 — zonal stats
        df = zonal_anomaly(raster_path, footprints)
        if df.empty:
            self.stdout.write(self.style.WARNING(
                "[thermal] zonal stats returned zero rows — aborting cache write."
            ))
            return

        # Step 4 — persist
        out = save_anomalies(df)
        self.stdout.write(self.style.SUCCESS(
            f"[thermal] wrote {len(df)} rows -> {out}"
        ))
        self.stdout.write(f"[thermal] ThermalAgent will read from {cache_path()}")
