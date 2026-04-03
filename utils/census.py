"""
utils/census.py
━━━━━━━━━━━━━━━
US Census Bureau API — datos demográficos para scoring geográfico.
API 100% gratuita: https://api.census.gov

Datos relevantes para insulación:
  - Mediana de antigüedad de viviendas (casas viejas = más necesidad)
  - Mediana de ingresos (capacidad de pago)
  - Total de unidades de vivienda (densidad de mercado)
  - Proporción owner-occupied (propietarios toman decisiones)
"""

import os
import logging
import requests
from functools import lru_cache

logger = logging.getLogger(__name__)

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")  # Gratis: https://api.census.gov/data/key_signup.html

# Bay Area counties (FIPS codes)
_BAY_AREA_COUNTIES = {
    "San Francisco":   {"state": "06", "county": "075"},
    "Alameda":         {"state": "06", "county": "001"},
    "Santa Clara":     {"state": "06", "county": "085"},
    "Contra Costa":    {"state": "06", "county": "013"},
    "San Mateo":       {"state": "06", "county": "081"},
    "Marin":           {"state": "06", "county": "041"},
    "Solano":          {"state": "06", "county": "095"},
    "Napa":            {"state": "06", "county": "055"},
    "Sonoma":          {"state": "06", "county": "097"},
    "San Joaquin":     {"state": "06", "county": "077"},
}

# Mapear ciudad → county
_CITY_TO_COUNTY = {
    # San Francisco County
    "san francisco":       "San Francisco",
    # Alameda County
    "oakland":             "Alameda",
    "berkeley":            "Alameda",
    "fremont":             "Alameda",
    "hayward":             "Alameda",
    "dublin":              "Alameda",
    "alameda":             "Alameda",
    "san leandro":         "Alameda",
    "pleasanton":          "Alameda",
    "livermore":           "Alameda",
    "newark":              "Alameda",
    "castro valley":       "Alameda",
    "san lorenzo":         "Alameda",
    "emeryville":          "Alameda",
    "albany":              "Alameda",
    "union city":          "Alameda",
    # Santa Clara County
    "san jose":            "Santa Clara",
    "sunnyvale":           "Santa Clara",
    "santa clara":         "Santa Clara",
    "palo alto":           "Santa Clara",
    "mountain view":       "Santa Clara",
    # Contra Costa County
    "richmond":            "Contra Costa",
    "concord":             "Contra Costa",
    "walnut creek":        "Contra Costa",
    "pleasant hill":       "Contra Costa",
    "martinez":            "Contra Costa",
    "clayton":             "Contra Costa",
    "pittsburg":           "Contra Costa",
    "lafayette":           "Contra Costa",
    "orinda":              "Contra Costa",
    "antioch":             "Contra Costa",
    "moraga":              "Contra Costa",
    "alamo":               "Contra Costa",
    "danville":            "Contra Costa",
    "hercules":            "Contra Costa",
    "pinole":              "Contra Costa",
    "oakley":              "Contra Costa",
    "san ramon":           "Contra Costa",
    "brentwood":           "Contra Costa",
    "el cerrito":          "Contra Costa",
    # San Mateo County
    "redwood city":        "San Mateo",
    "daly city":           "San Mateo",
    "san mateo":           "San Mateo",
    "south san francisco": "San Mateo",
    "san bruno":           "San Mateo",
    "millbrae":            "San Mateo",
    "burlingame":          "San Mateo",
    # Solano County
    "benicia":             "Solano",
    "fairfield":           "Solano",
    "vallejo":             "Solano",
    "suisun city":         "Solano",
    "rio vista":           "Solano",
    "vacaville":           "Solano",
    # Napa County
    "napa":                "Napa",
    # Sonoma County
    "sonoma":              "Sonoma",
    "petaluma":            "Sonoma",
    # Marin County
    "novato":              "Marin",
    "san rafael":          "Marin",
    # San Joaquin County
    "tracy":               "San Joaquin",
    "stockton":            "San Joaquin",
}

# Cache de datos demográficos por county
_demo_cache: dict = {}


def get_demographics(city: str) -> dict | None:
    """
    Obtiene datos demográficos del Census Bureau para un condado de Bay Area.

    Retorna:
      - median_year_built: mediana del año de construcción de viviendas
      - median_income: mediana de ingresos del hogar
      - total_housing_units: total de unidades de vivienda
      - owner_occupied_pct: % de viviendas ocupadas por propietarios
      - insulation_score: score 0-100 de necesidad de insulación
    """
    city_lower = city.lower().strip()
    county_name = _CITY_TO_COUNTY.get(city_lower)

    if not county_name:
        return None

    if county_name in _demo_cache:
        return _demo_cache[county_name]

    county_info = _BAY_AREA_COUNTIES.get(county_name)
    if not county_info:
        return None

    result = _fetch_census_data(county_info["state"], county_info["county"])
    if result:
        _demo_cache[county_name] = result
    return result


def _fetch_census_data(state_fips: str, county_fips: str) -> dict | None:
    """
    Consulta ACS 5-Year Estimates del Census Bureau.
    Variables:
      B25035_001E — Median year structure built
      B19013_001E — Median household income
      B25001_001E — Total housing units
      B25003_002E — Owner-occupied housing units
      B25003_001E — Total occupied housing units
    """
    variables = "B25035_001E,B19013_001E,B25001_001E,B25003_002E,B25003_001E"

    params = {
        "get": variables,
        "for": f"county:{county_fips}",
        "in": f"state:{state_fips}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    try:
        resp = requests.get(
            "https://api.census.gov/data/2022/acs/acs5",
            params=params,
            timeout=15,
        )
        if resp.status_code != 200:
            logger.debug(f"[Census] Error {resp.status_code}")
            return None

        data = resp.json()
        if len(data) < 2:
            return None

        # data[0] = headers, data[1] = values
        values = data[1]

        median_year_built = _safe_int(values[0])
        median_income     = _safe_int(values[1])
        total_housing     = _safe_int(values[2])
        owner_occupied    = _safe_int(values[3])
        total_occupied    = _safe_int(values[4])

        owner_pct = (owner_occupied / total_occupied * 100) if total_occupied else 0

        # Calcular insulation_score basado en antigüedad
        # Casas pre-1980 = alta necesidad, post-2000 = baja necesidad
        insulation_score = 0
        if median_year_built:
            if median_year_built < 1960:
                insulation_score = 95
            elif median_year_built < 1970:
                insulation_score = 85
            elif median_year_built < 1980:
                insulation_score = 75
            elif median_year_built < 1990:
                insulation_score = 60
            elif median_year_built < 2000:
                insulation_score = 45
            else:
                insulation_score = 25

        # Ajustar por ingresos (mayor ingreso = mayor capacidad de pago)
        if median_income and median_income > 120000:
            insulation_score = min(insulation_score + 10, 100)
        elif median_income and median_income > 80000:
            insulation_score = min(insulation_score + 5, 100)

        return {
            "median_year_built":  median_year_built,
            "median_income":      median_income,
            "total_housing_units": total_housing,
            "owner_occupied_pct": round(owner_pct, 1),
            "insulation_score":   insulation_score,
        }

    except Exception as e:
        logger.debug(f"[Census] Error: {e}")
        return None


def _safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def format_demographics(demo: dict) -> str:
    """Formatea datos demográficos para display en Telegram."""
    if not demo:
        return ""
    parts = []
    if demo.get("median_year_built"):
        parts.append(f"Casas ~{demo['median_year_built']}")
    if demo.get("median_income"):
        parts.append(f"Ingreso ${demo['median_income']:,}")
    if demo.get("owner_occupied_pct"):
        parts.append(f"{demo['owner_occupied_pct']}% propietarios")
    return " | ".join(parts)
