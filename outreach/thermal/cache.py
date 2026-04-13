"""
Parquet cache for thermal anomaly results.

The thermal pipeline is expensive (Earth Engine export + OSMnx +
zonal stats), so :func:`outreach.thermal.cache.save_anomalies` writes
the resulting DataFrame to a single parquet that
:mod:`agents.thermal_agent` reads at discover time. This way the
daemon never needs GEE credentials in the hot path — only the
``thermal_pull`` management command does.
"""
from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings

logger = logging.getLogger("outreach.thermal.cache")


def cache_path() -> Path:
    """Return the canonical parquet path under settings.DATA_DIR."""
    base = Path(getattr(settings, "DATA_DIR", "data"))
    return base / "thermal" / "anomalies.parquet"


def save_anomalies(df) -> Path:
    """Persist a DataFrame of anomalies. Returns the written path."""
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("[thermal] cached %d anomalies -> %s", len(df), path)
    return path


def load_anomalies():
    """Load the cached anomaly DataFrame, or ``None`` if missing.

    Returns ``None`` rather than raising so ``agents/thermal_agent.py``
    can degrade to an empty lead list without a try/except at every
    call site.
    """
    path = cache_path()
    if not path.exists():
        return None
    try:
        import pandas as pd  # type: ignore

        return pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[thermal] failed to read %s: %s", path, exc)
        return None
