"""
utils/hot_zones.py
━━━━━━━━━━━━━━━━━━
Hot Zone Detection — identifica clusters geográficos de leads.

Cuando hay 3+ leads en un radio de 500m = "hot zone":
  - Campaña de puerta a puerta recomendada
  - Alerta especial por Telegram con mapa
  - Prioridad MÁXIMA en lead scoring

Algoritmo: grid-based spatial clustering
  1. Divide Bay Area en celdas de ~200m (geohash simplificado)
  2. Cuenta leads por celda + vecinos adyacentes
  3. Celdas con >= threshold leads = hot zone
  4. Genera alerta con centro, radio, y todos los leads del cluster

Sin dependencias externas — usa solo math estándar.
"""

import os
import math
import logging
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

# Configuración
HOT_ZONE_THRESHOLD  = int(os.getenv("HOT_ZONE_THRESHOLD", "3"))   # Min leads para hot zone
HOT_ZONE_RADIUS_M   = int(os.getenv("HOT_ZONE_RADIUS_M", "500"))  # Radio en metros
HOT_ZONE_WINDOW_HRS = int(os.getenv("HOT_ZONE_WINDOW_HRS", "168")) # Ventana (default: 7 días)

# Tamaño de celda en grados (~200m a latitud Bay Area)
_CELL_SIZE_LAT = 0.0018  # ~200m
_CELL_SIZE_LON = 0.0023  # ~200m (ajustado por latitud 37.7°)


def _to_cell(lat: float, lon: float) -> tuple[int, int]:
    """Convierte coordenadas a celda del grid."""
    return (int(lat / _CELL_SIZE_LAT), int(lon / _CELL_SIZE_LON))


def _cell_center(cell: tuple[int, int]) -> tuple[float, float]:
    """Retorna el centro de una celda."""
    return (
        (cell[0] + 0.5) * _CELL_SIZE_LAT,
        (cell[1] + 0.5) * _CELL_SIZE_LON,
    )


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en metros entre dos puntos (Haversine)."""
    R = 6371000  # Radio de la Tierra en metros
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _neighbor_cells(cell: tuple[int, int]) -> list[tuple[int, int]]:
    """Retorna las 8 celdas vecinas + la celda misma."""
    r, c = cell
    return [
        (r+dr, c+dc)
        for dr in (-1, 0, 1)
        for dc in (-1, 0, 1)
    ]


class HotZoneDetector:
    """
    Detecta clusters geográficos de leads en tiempo real.

    Uso:
        detector = HotZoneDetector()
        detector.add_lead(lead)  # llamar para cada lead
        zones = detector.detect_hot_zones()  # obtener clusters
    """

    def __init__(self):
        self._leads: list[dict] = []
        self._grid: dict[tuple, list] = defaultdict(list)
        self._known_zones: set = set()  # Evitar alertas duplicadas

    def add_lead(self, lead: dict):
        """Agrega un lead al detector. Requiere lat/lon."""
        lat = lead.get("lat") or lead.get("latitude")
        lon = lead.get("lon") or lead.get("longitude")

        if not lat or not lon:
            return

        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (ValueError, TypeError):
            return

        # Solo Bay Area (bounding box aproximado)
        if not (37.0 <= lat_f <= 38.2 and -122.8 <= lon_f <= -121.5):
            return

        lead_entry = {
            "lead": lead,
            "lat": lat_f,
            "lon": lon_f,
            "cell": _to_cell(lat_f, lon_f),
            "timestamp": datetime.utcnow(),
        }

        self._leads.append(lead_entry)
        self._grid[lead_entry["cell"]].append(lead_entry)

    def detect_hot_zones(self) -> list[dict]:
        """
        Detecta clusters de leads que superan el threshold.

        Retorna lista de hot zones:
        [{
            "center_lat": float,
            "center_lon": float,
            "radius_m": int,
            "lead_count": int,
            "leads": [lead_dicts],
            "agent_types": ["permits", "rodents", ...],
            "cities": ["San Francisco"],
            "severity": "CRITICAL" | "HIGH" | "MEDIUM",
            "recommendation": str,
            "maps_url": str,
        }]
        """
        # Limpiar leads fuera de ventana
        self._prune_old_leads()

        hot_zones = []
        visited_cells = set()

        for cell, cell_leads in self._grid.items():
            if cell in visited_cells:
                continue

            # Contar leads en celda + vecinos
            cluster_leads = []
            cluster_cells = set()
            for neighbor in _neighbor_cells(cell):
                if neighbor in self._grid:
                    cluster_leads.extend(self._grid[neighbor])
                    cluster_cells.add(neighbor)

            if len(cluster_leads) < HOT_ZONE_THRESHOLD:
                continue

            # Marcar celdas como visitadas
            visited_cells.update(cluster_cells)

            # Calcular centro del cluster
            avg_lat = sum(l["lat"] for l in cluster_leads) / len(cluster_leads)
            avg_lon = sum(l["lon"] for l in cluster_leads) / len(cluster_leads)

            # Calcular radio real
            max_dist = max(
                _haversine_m(avg_lat, avg_lon, l["lat"], l["lon"])
                for l in cluster_leads
            )

            # Extraer metadata
            agent_types = sorted(set(
                l["lead"].get("_agent_key") or l["lead"].get("agent_key", "unknown")
                for l in cluster_leads
            ))
            cities = sorted(set(
                l["lead"].get("city", "")
                for l in cluster_leads if l["lead"].get("city")
            ))

            # Severidad del cluster
            lead_count = len(cluster_leads)
            if lead_count >= 10 or len(agent_types) >= 4:
                severity = "CRITICAL"
                severity_emoji = "🔴"
            elif lead_count >= 6 or len(agent_types) >= 3:
                severity = "HIGH"
                severity_emoji = "🟠"
            else:
                severity = "MEDIUM"
                severity_emoji = "🟡"

            # Recomendación de acción
            recommendation = _generate_recommendation(
                lead_count, agent_types, cities, severity,
            )

            # Zone ID para evitar alertas duplicadas
            zone_id = f"{int(avg_lat*1000)}_{int(avg_lon*1000)}"

            hot_zone = {
                "zone_id":        zone_id,
                "center_lat":     round(avg_lat, 6),
                "center_lon":     round(avg_lon, 6),
                "radius_m":       int(max(max_dist, HOT_ZONE_RADIUS_M)),
                "lead_count":     lead_count,
                "leads":          [l["lead"] for l in cluster_leads],
                "agent_types":    agent_types,
                "agent_count":    len(agent_types),
                "cities":         cities,
                "severity":       severity,
                "severity_emoji": severity_emoji,
                "recommendation": recommendation,
                "maps_url":       f"https://maps.google.com/?q={avg_lat},{avg_lon}&z=16",
                "detected_at":    datetime.utcnow().isoformat(),
            }

            hot_zones.append(hot_zone)

        # Ordenar por severidad y cantidad de leads
        hot_zones.sort(key=lambda z: (-len(z["agent_types"]), -z["lead_count"]))

        return hot_zones

    def get_new_hot_zones(self) -> list[dict]:
        """Retorna solo hot zones que no han sido alertadas antes."""
        all_zones = self.detect_hot_zones()
        new_zones = [z for z in all_zones if z["zone_id"] not in self._known_zones]
        for z in new_zones:
            self._known_zones.add(z["zone_id"])
        return new_zones

    def _prune_old_leads(self):
        """Elimina leads fuera de la ventana de tiempo."""
        cutoff = datetime.utcnow() - timedelta(hours=HOT_ZONE_WINDOW_HRS)
        self._leads = [l for l in self._leads if l["timestamp"] >= cutoff]

        # Reconstruir grid
        self._grid = defaultdict(list)
        for lead_entry in self._leads:
            self._grid[lead_entry["cell"]].append(lead_entry)

    def get_stats(self) -> dict:
        """Estadísticas del detector."""
        return {
            "total_leads_tracked": len(self._leads),
            "grid_cells_active":   len(self._grid),
            "known_zones":         len(self._known_zones),
        }


def _generate_recommendation(lead_count: int, agent_types: list,
                             cities: list, severity: str) -> str:
    """Genera recomendación de acción basada en el cluster."""
    city_str = "/".join(cities[:2])
    type_str = ", ".join(agent_types[:3])

    if severity == "CRITICAL":
        return (
            f"🔴 ZONA CRÍTICA en {city_str}: {lead_count} señales de {type_str}. "
            f"Recomendar campaña de puerta a puerta INMEDIATA. "
            f"Múltiples propiedades en esta zona necesitan insulación."
        )
    elif severity == "HIGH":
        return (
            f"🟠 ZONA CALIENTE en {city_str}: {lead_count} señales de {type_str}. "
            f"Priorizar contacto con propietarios de la zona. "
            f"Alta densidad de oportunidades."
        )
    else:
        return (
            f"🟡 ZONA ACTIVA en {city_str}: {lead_count} señales. "
            f"Monitorear — puede escalar a zona caliente."
        )


def format_hot_zone_alert(zone: dict) -> str:
    """Formatea una hot zone para envío por Telegram."""
    lines = [
        f"{zone['severity_emoji']} *HOT ZONE DETECTADA*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📍 *{', '.join(zone['cities'])}*",
        f"🎯 *{zone['lead_count']} leads* en radio de {zone['radius_m']}m",
        f"📡 Señales: {' + '.join(zone['agent_types'])}",
        f"⚠️ Severidad: *{zone['severity']}*",
        "",
        f"💡 _{zone['recommendation']}_",
        "",
        f"🗺️ [Ver en mapa]({zone['maps_url']})",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        f"*Leads en la zona:*",
    ]

    for i, lead in enumerate(zone["leads"][:10], 1):
        addr = lead.get("address", "?")[:40]
        agent = lead.get("_agent_key", "?")
        lines.append(f"  {i}. {addr} _({agent})_")

    if zone["lead_count"] > 10:
        lines.append(f"  ... y {zone['lead_count'] - 10} más")

    lines.append(f"\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    return "\n".join(lines)


# ── Singleton global ─────────────────────────────────────────────────
_detector: HotZoneDetector | None = None

def get_hot_zone_detector() -> HotZoneDetector:
    global _detector
    if _detector is None:
        _detector = HotZoneDetector()
    return _detector
