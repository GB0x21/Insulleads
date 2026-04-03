"""
utils/lead_scoring.py
━━━━━━━━━━━━━━━━━━━━━
Motor de Lead Scoring — prioriza leads por probabilidad de conversión.

Score = f(valor_proyecto, antigüedad_zona, tipo_proyecto, potencial_solar,
          severidad_plaga, datos_contacto, demografía)

Escala: 0-100
  90-100: 🔥 HOT    — contactar de inmediato
  70-89:  🟠 WARM   — alta prioridad
  50-69:  🟡 MEDIUM — seguimiento estándar
  25-49:  🔵 COOL   — baja prioridad
  0-24:   ⚪ COLD   — archivo
"""

import logging

logger = logging.getLogger(__name__)


# ── Pesos por categoría (suman ~100 puntos máximo) ───────────────────

_WEIGHTS = {
    "project_value":    20,   # Valor del proyecto ($)
    "project_type":     15,   # Tipo de proyecto (ADU, new construction, etc.)
    "contact_quality":  20,   # Tiene teléfono, email, contratista
    "recency":          15,   # Qué tan reciente es el lead
    "geography":        10,   # Zona demográfica favorable
    "source_type":      10,   # Fuente del lead (permit > solar > rodent > etc.)
    "insulation_signal": 10,  # Señales directas de necesidad de insulación
}


# ── Keywords de alta intención ───────────────────────────────────────

_HIGH_INTENT_KEYWORDS = [
    "insulation", "insulate", "weatherization", "energy retrofit",
    "energy upgrade", "attic", "crawlspace", "crawl space",
    "air sealing", "thermal barrier", "r-value", "fiberglass",
    "spray foam", "blown-in", "cellulose insulation",
]

_MEDIUM_INTENT_KEYWORDS = [
    "adu", "accessory dwelling", "addition", "new construction",
    "remodel", "renovation", "garage conversion", "hvac",
    "solar", "photovoltaic", "roofing", "re-roof",
]

_LOW_INTENT_KEYWORDS = [
    "demolition", "swimming pool", "fence", "sign",
    "electrical panel", "plumbing", "fire sprinkler",
]


def score_lead(lead: dict) -> dict:
    """
    Calcula el score de un lead y retorna dict con:
      - score: int (0-100)
      - grade: str ('HOT', 'WARM', 'MEDIUM', 'COOL', 'COLD')
      - grade_emoji: str
      - reasons: list[str] — factores principales
    """
    total = 0.0
    reasons = []

    # ── 1. Valor del proyecto (0-20 pts) ─────────────────────────
    value = lead.get("value_float", 0)
    if value >= 500000:
        total += 20
        reasons.append(f"Proyecto alto valor (${value:,.0f})")
    elif value >= 200000:
        total += 15
    elif value >= 100000:
        total += 12
    elif value >= 50000:
        total += 8
    elif value > 0:
        total += 4

    # ── 2. Tipo de proyecto (0-15 pts) ───────────────────────────
    desc = ((lead.get("description") or "") + " " +
            (lead.get("permit_type") or "") + " " +
            (lead.get("desc") or "")).lower()

    if any(kw in desc for kw in _HIGH_INTENT_KEYWORDS):
        total += 15
        reasons.append("Mención directa de insulación/energía")
    elif any(kw in desc for kw in _MEDIUM_INTENT_KEYWORDS):
        total += 10
        reasons.append("Proyecto relacionado (ADU/solar/remodelación)")
    elif any(kw in desc for kw in _LOW_INTENT_KEYWORDS):
        total += 3
    else:
        total += 5  # genérico

    # ── 3. Calidad de contacto (0-20 pts) ────────────────────────
    has_phone = bool(lead.get("contact_phone"))
    has_email = bool(lead.get("contact_email"))
    has_contractor = bool(lead.get("contractor"))
    has_owner = bool(lead.get("owner"))

    contact_score = 0
    if has_phone:
        contact_score += 8
    if has_email:
        contact_score += 6
    if has_contractor:
        contact_score += 4
    if has_owner:
        contact_score += 2
    total += min(contact_score, 20)

    if has_phone and has_email:
        reasons.append("Contacto completo (tel + email)")
    elif has_phone:
        reasons.append("Teléfono disponible")

    # ── 4. Recencia (0-15 pts) ───────────────────────────────────
    date_str = (lead.get("date") or lead.get("issued_date") or
                lead.get("filed_date") or "")
    if date_str:
        try:
            from datetime import datetime, timedelta
            lead_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
            days_ago = (datetime.utcnow() - lead_date).days
            if days_ago <= 7:
                total += 15
                reasons.append("Lead de esta semana")
            elif days_ago <= 14:
                total += 12
            elif days_ago <= 30:
                total += 9
            elif days_ago <= 60:
                total += 5
            else:
                total += 2
        except (ValueError, TypeError):
            total += 5  # fecha no parseable, asumir reciente

    # ── 5. Geografía (0-10 pts) ──────────────────────────────────
    # Zonas con casas más antiguas = más demanda de insulación
    city = (lead.get("city") or "").lower()
    _high_demand_cities = {
        # Tier 1 — casas antiguas, alta densidad, alta demanda
        "san francisco": 10, "oakland": 9, "berkeley": 9,
        "richmond": 8, "san jose": 7, "hayward": 7,
        "alameda": 8, "san leandro": 7, "emeryville": 8,
        "albany": 7, "el cerrito": 7,
        # Tier 2 — suburban, demanda media-alta
        "fremont": 6, "sunnyvale": 5, "santa clara": 5,
        "concord": 7, "walnut creek": 6, "martinez": 7,
        "pleasant hill": 6, "pittsburg": 7, "antioch": 7,
        "vallejo": 8, "fairfield": 6, "napa": 6,
        "san rafael": 6, "novato": 5, "petaluma": 5,
        "daly city": 7, "south san francisco": 6,
        "san mateo": 6, "burlingame": 5, "san bruno": 6,
        # Tier 3 — newer suburbs, demanda media
        "dublin": 4, "pleasanton": 4, "livermore": 5,
        "san ramon": 4, "danville": 4, "lafayette": 4,
        "orinda": 4, "moraga": 4, "union city": 5,
        "newark": 5, "castro valley": 6, "san lorenzo": 6,
        "millbrae": 5, "vacaville": 5, "benicia": 5,
        "hercules": 6, "pinole": 6, "oakley": 5,
        "brentwood": 4, "clayton": 4, "tracy": 4,
        "stockton": 5, "sonoma": 4, "suisun city": 5,
        "rio vista": 4, "alamo": 4, "redwood city": 5,
        # County-level sources
        "contra costa county": 7, "alameda county": 7,
        "san mateo county": 6, "solano county": 6,
        "marin county": 6, "napa county": 6,
        "sonoma county": 5, "san joaquin county": 5,
    }
    geo_score = _high_demand_cities.get(city, 4)
    total += geo_score

    # ── 6. Tipo de fuente (0-10 pts) ─────────────────────────────
    agent_key = lead.get("_agent_key", "")
    _source_scores = {
        "permits": 10, "construction": 10, "deconstruction": 9,
        "realestate": 9, "solar": 8, "energy": 7,
        "rodents": 6, "places": 5, "yelp": 4, "flood": 5,
    }
    total += _source_scores.get(agent_key, 5)

    # ── 7. Señales de insulación (0-10 pts) ──────────────────────
    insulation_signals = 0
    pest_type = lead.get("pest_type", "")
    if pest_type in ("rodent", "termite"):
        insulation_signals += 8
        reasons.append(f"Plaga ({pest_type}) = daño a insulación")
    elif pest_type in ("wildlife",):
        insulation_signals += 6

    if lead.get("solar_potential"):
        insulation_signals += 4
        reasons.append("Zona con alto potencial solar")

    if lead.get("energy_score") and lead["energy_score"] < 50:
        insulation_signals += 6
        reasons.append("Baja eficiencia energética en la zona")

    total += min(insulation_signals, 10)

    # ── Calcular grado ───────────────────────────────────────────
    score = min(int(total), 100)

    if score >= 90:
        grade, emoji = "HOT", "🔥"
    elif score >= 70:
        grade, emoji = "WARM", "🟠"
    elif score >= 50:
        grade, emoji = "MEDIUM", "🟡"
    elif score >= 25:
        grade, emoji = "COOL", "🔵"
    else:
        grade, emoji = "COLD", "⚪"

    return {
        "score":       score,
        "grade":       grade,
        "grade_emoji": emoji,
        "reasons":     reasons[:3],  # Top 3 razones
    }


def format_score_line(scoring: dict) -> str:
    """Formatea el score para Telegram."""
    s = scoring
    reasons_str = " | ".join(s["reasons"]) if s["reasons"] else ""
    return f"{s['grade_emoji']} {s['score']}/100 ({s['grade']})" + (
        f" — {reasons_str}" if reasons_str else ""
    )
