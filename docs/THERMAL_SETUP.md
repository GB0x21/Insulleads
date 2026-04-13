# Thermal engine setup (Phase 1)

The thermal engine pulls Landsat-8 surface temperature (ST_B10),
intersects it with OSM building footprints, and flags buildings
whose envelope runs N Kelvin hotter than the scene median —
a proxy for poorly-insulated buildings leaking conditioned air.

The pipeline is **offline-first**: `python manage.py thermal_pull`
does all the expensive work (Earth Engine export + OSMnx download +
zonal stats) and writes a single parquet at
`data/thermal/anomalies.parquet`. `agents/thermal_agent.py` then
reads that parquet at discover time — the daemon itself never needs
Earth Engine credentials.

## 1. Install the optional deps

```bash
pip install geemap earthengine-api rasterio rasterstats osmnx geopandas pyarrow
```

These live in `requirements/base.txt` but are all optional — the
daemon degrades to an empty lead list when any of them is missing.

## 2. Create a GCP service account with Earth Engine access

1. Open the [Google Cloud Console](https://console.cloud.google.com/)
   and create a new project (or reuse an existing one).
2. Enable the **Earth Engine API**
   (`IAM & Admin → APIs → Earth Engine API → Enable`).
3. Create a service account:
   `IAM & Admin → Service Accounts → Create Service Account`.
   Give it the `Earth Engine Resource Viewer` role.
4. On the service account's *Keys* tab, create a new JSON key and
   download it.
5. Register the service account with Earth Engine at
   <https://signup.earthengine.google.com/#!/service_accounts> — this
   step is free but mandatory. Use the service account's email.

## 3. Point the daemon at the credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Or set it in `.env`:

```ini
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
THERMAL_BBOX=-122.52,37.70,-122.35,37.83
THERMAL_MIN_ANOMALY_K=3.0
```

`settings.THERMAL["ENABLED"]` auto-flips to `True` when the env var
is set, which is how the rest of the daemon learns the thermal
pipeline is available.

## 4. Pull a scene

```bash
python manage.py thermal_pull --city "San Francisco, CA, USA"
# or
python manage.py thermal_pull \
    --bbox -122.52,37.70,-122.35,37.83 \
    --from 2024-06-01 --to 2024-09-30
```

The command writes `data/thermal/landsat_st.tif`,
`data/thermal/footprints.gpkg`, and
`data/thermal/anomalies.parquet`. Only the parquet is read by the
daemon at discover time — the others are debugging artefacts.

## 5. Register the thermal source on a campaign

Via the Django admin:

```
/admin/outreach/source/add/
    key:       thermal-bayarea
    kind:      thermal
    campaign:  (your active campaign)
    interval_minutes: 1440   # once a day — the data is static
    enabled:   ✓
```

The discover task will now call `ThermalAgent` each interval and
upsert a Lead row per hot building. Each lead carries
`thermal_anomaly_k` (Kelvin over median) which feeds directly into
the LightGBM qualifier's 21-feature vector.

## Troubleshooting

- **"GOOGLE_APPLICATION_CREDENTIALS is not set"** — you skipped
  step 3. The daemon itself does not need this; only `thermal_pull`
  does.
- **"Earth Engine init failed: ... not registered"** — you skipped
  step 2.5. Go to the signup link above.
- **"osmnx / geopandas not installed"** — install step 1's deps.
- **Empty parquet** — the scene may have been fully cloudy. Try a
  longer date range (e.g. a whole summer).
