"""
agents/solar_agent.py  v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 2 — INSTALACIONES SOLARES

Notas importantes sobre los permisos solares de SF:
  • Costo reportado = $1 (siempre, es una convención del DBI)
  • Número de permiso usa prefijo "S" (ej: S20260109428)
  • El DBI Contacts dataset SÍ cubre estos permisos (contact_type=OWNER)
  • Scoring propio: por kW instalado + batería + tipo de propiedad

Fuentes: SF DataSF · San Jose · Oakland
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import re
import logging
import requests
from datetime import datetime, timedelta

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import (
    enrich_lead,
    should_send_lead,
    contact_score_label,
)

logger = logging.getLogger(__name__)

SOLAR_KEYWORDS = [
    "solar", "photovoltaic", "pv system", "pv panel",
    "solar panel", "solar array", "battery storage",
    "powerwall", "energy storage",
]

LOOKBACK_DAYS = 90


class SolarAgent(BaseAgent):
    name      = "Instalaciones Solares"
    emoji     = "☀️"
    agent_key = "solar"

    def fetch_leads(self) -> list:
        raw_leads = []
        raw_leads += self._fetch_sf()
        raw_leads += self._fetch_sj()
        raw_leads += self._fetch_oakland()

        # Enriquecer con lead_type="solar"
        enriched = []
        for lead in raw_leads:
            try:
                lead = enrich_lead(lead, lead_type="solar")
            except Exception as e:
                logger.warning(f"[solar] Enrich error {lead.get('id')}: {e}")
            enriched.append(lead)

        # Filtrar por calidad (solar usa umbral especial: score >= 5)
        good = []
        for lead in enriched:
            quality = lead.get("lead_quality_score", 0)
            label   = lead.get("quality_label", "")
            if should_send_lead(lead, lead_type="solar"):
                good.append(lead)
                logger.info(
                    f"[solar] ✅ ENVIAR ({label} {quality}/10): "
                    f"{lead.get('city')} — {lead.get('address')} "
                    f"| {lead.get('kw_installed', '?')}"
                )
            else:
                logger.debug(
                    f"[solar] SKIP ({quality}/10 = {label}): {lead.get('address')}"
                )

        logger.info(
            f"[solar] {len(raw_leads)} permisos → {len(good)} leads para enviar"
        )
        return good

    # ── San Francisco ──────────────────────────────────────────────
    def _fetch_sf(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url    = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 1000,
            "$where": (
                f"filed_date >= '{since}' AND "
                "(UPPER(description) LIKE '%SOLAR%' OR "
                " UPPER(description) LIKE '%PHOTOVOLTAIC%' OR "
                " UPPER(description) LIKE '%PV MODULE%')"
            ),
            "$order": "filed_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[solar] SF fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue

            permit_no = item.get("permit_number", "")
            address   = self._sf_address(item)
            kw        = self._extract_kw(desc)

            leads.append({
                "id":                 f"sf_solar_{permit_no}",
                "city":               "San Francisco",
                "address":            address,
                "description":        (item.get("description", "") or "")[:200],
                "filed_date":         (item.get("filed_date", "") or "")[:10],
                "permit_no":          permit_no,
                "kw_installed":       kw,
                # Costo real de solar en SF siempre es $1 (convención DBI)
                # No usamos estimated_cost para solar
                "estimated_cost":     "N/A (solar)",
                # Datos que puede traer el dataset de permisos
                "owner":              (item.get("owner_name", "") or "").strip(),
                "owner_phone":        item.get("owner_phone", "") or "",
                "contractor":         (item.get("contractor_company_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", "") or "",
                "contractor_license": (item.get("contractor_license_number", "") or "").strip(),
            })
        return leads

    # ── San José ───────────────────────────────────────────────────
    def _fetch_sj(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url    = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {
            "$limit": 1000,
            "$where": (
                f"application_date >= '{since}' AND "
                "UPPER(work_description) LIKE '%SOLAR%'"
            ),
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[solar] SJ fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc = (item.get("work_description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue
            permit_no = item.get("permit_number", "")
            leads.append({
                "id":                 f"sj_solar_{permit_no}",
                "city":               "San Jose",
                "address":            (item.get("address", "") or "N/A").title(),
                "description":        (item.get("work_description", "") or "")[:200],
                "filed_date":         (item.get("application_date", "") or "")[:10],
                "permit_no":          permit_no,
                "kw_installed":       self._extract_kw(desc),
                "estimated_cost":     "N/A (solar)",
                "owner":              (item.get("owner_name", "") or "").strip(),
                "owner_phone":        item.get("owner_phone", "") or "",
                "contractor":         (item.get("contractor_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", "") or "",
                "contractor_license": (item.get("contractor_license", "") or "").strip(),
            })
        return leads

    # ── Oakland ────────────────────────────────────────────────────
    def _fetch_oakland(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url    = "https://data.oaklandca.gov/resource/p8h3-ngmm.json"
        params = {
            "$limit": 1000,
            "$where": (
                f"application_date >= '{since}' AND "
                "UPPER(description) LIKE '%SOLAR%'"
            ),
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[solar] Oakland fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in SOLAR_KEYWORDS):
                continue
            permit_no = item.get("permit_number", "")
            leads.append({
                "id":                 f"oak_solar_{permit_no}",
                "city":               "Oakland",
                "address":            (item.get("address", "") or "N/A").title(),
                "description":        (item.get("description", "") or "")[:200],
                "filed_date":         (item.get("application_date", "") or "")[:10],
                "permit_no":          permit_no,
                "kw_installed":       self._extract_kw(desc),
                "estimated_cost":     "N/A (solar)",
                "owner":              (item.get("owner_name", "") or "").strip(),
                "owner_phone":        item.get("owner_phone", "") or "",
                "contractor":         (item.get("contractor_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", "") or "",
                "contractor_license": (item.get("contractor_license", "") or "").strip(),
            })
        return leads

    # ── Telegram notify ────────────────────────────────────────────
    def notify(self, lead: dict):
        quality_score = lead.get("lead_quality_score", 0)
        quality_emoji = lead.get("quality_emoji", "✅")
        quality_label = lead.get("quality_label", "BUENO")
        contact_lbl   = contact_score_label(lead.get("contact_score", 0))

        def _v(key):
            val = (lead.get(key) or "").strip()
            return val if val and val.lower() not in ("no indicado", "no encontrado", "n/a", "—", "") else None

        fields = {
            # ── CALIDAD ─────────────────────────────────────
            f"{quality_emoji} Calidad del Lead":  f"{quality_label}  ({quality_score}/10)",
            "📋 Datos de Contacto":               contact_lbl,

            # ── SISTEMA SOLAR ────────────────────────────────
            "⚡ Sistema Instalado":   lead.get("kw_installed") or "No especificado",
            "🔢 Permiso #":           lead.get("permit_no", "—"),
            "📝 Descripción":         _v("description"),
            "📅 Fecha Instalación":   _v("filed_date"),

            # ── PROPIETARIO ──────────────────────────────────
            "👤 Propietario":         _v("owner"),
            "📬 Dir. Postal":         _v("owner_mail_addr"),
            "📞 Tel. Propietario":    _v("owner_phone"),

            # ── INSTALADOR SOLAR ─────────────────────────────
            "🔧 Instalador Solar":    _v("contractor"),
            "📞 Tel. Instalador":     _v("contractor_phone"),
            "📍 Dir. Instalador":     _v("contractor_addr"),
            "🪪 Lic. CSLB":           _v("contractor_license"),
            "✅ Estado Lic.":         _v("contractor_status"),

            # ── OTROS CONTACTOS ──────────────────────────────
            "📐 Aplicante":           _v("applicant"),
            "📞 Tel. Aplicante":      _v("applicant_phone"),

            # ── LINK ─────────────────────────────────────────
            "🗺️ Ver en Maps":         _v("maps_url"),
        }

        # Filtrar campos None
        fields = {k: v for k, v in fields.items() if v is not None}

        send_lead(
            agent_name="Instalaciones Solares",
            emoji="☀️",
            title=f"{lead.get('city', '')} — {lead.get('address', '')}",
            fields=fields,
            cta=(
                "PITCH: 'Acabas de instalar paneles solares — un buen aislamiento "
                "puede aumentar tu ahorro energetico hasta un 30%. "
                "Evaluacion GRATUITA esta semana.'"
            ),
        )

    # ── Helpers ────────────────────────────────────────────────────
    def _sf_address(self, item: dict) -> str:
        parts = [
            item.get("street_number", ""),
            item.get("street_name", ""),
            item.get("street_suffix", ""),
        ]
        return " ".join(p for p in parts if p).title()

    def _extract_kw(self, text: str) -> str:
        # kW directo: "6.6 kw", "6.6kw"
        m = re.search(r"(\d+\.?\d*)\s*kw", text, re.IGNORECASE)
        if m:
            return f"{m.group(1)} kW"
        # watts: "440 watts", "440w"
        m2 = re.search(r"(\d+)\s*w(?:att)?s?", text, re.IGNORECASE)
        if m2:
            kw = float(m2.group(1)) / 1000
            return f"{kw:.1f} kW"
        # "X modules × Y watts"
        m3 = re.search(r"(\d+)\s*(?:solar\s+)?(?:pv\s+)?modules?\s.*?(\d+)\s*w", text, re.IGNORECASE)
        if m3:
            total = int(m3.group(1)) * int(m3.group(2))
            return f"{total/1000:.1f} kW"
        return "No especificado"
