"""
agents/permits_agent.py  v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENTE 1 — PERMISOS DE CONSTRUCCIÓN

Filtros activos:
  ✅ Solo proyectos >= $50,000
  ✅ Solo leads BUENOS (score 6-7), MUY BUENOS (8-9) o EXCELENTES (10)
  ✅ Enriquecimiento: propietario (assessor) + contratista (CSLB) + Maps

Fuentes: SF DataSF · San Jose Open Data · Oakland Open Data
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import logging
import requests
from datetime import datetime, timedelta

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contact_enricher import (
    enrich_lead,
    should_send_lead,
    contact_score_label,
    _parse_cost,
    calc_lead_quality_score,
)

logger = logging.getLogger(__name__)

PERMIT_KEYWORDS = [
    "addition", "adu", "accessory dwelling", "remodel",
    "new construction", "tenant improvement", "renovation",
    "garage conversion", "basement", "crawl space",
    "new building", "alteration", "dwelling", "second unit",
]

LOOKBACK_DAYS = 90


class PermitsAgent(BaseAgent):
    name      = "Permisos de Construccion"
    emoji     = "🏗️"
    agent_key = "permits"

    def fetch_leads(self) -> list:
        raw_leads = []
        raw_leads += self._fetch_sf()
        raw_leads += self._fetch_san_jose()
        raw_leads += self._fetch_oakland()

        # ── Filtro 1: Valor mínimo $50K ────────────────────────────
        value_filtered = []
        for lead in raw_leads:
            cost = _parse_cost(lead.get("estimated_cost", ""))
            if cost < 50_000:
                logger.debug(
                    f"[permits] SKIP (bajo valor ${cost:,.0f}): {lead.get('address')}"
                )
                continue
            value_filtered.append(lead)

        logger.info(
            f"[permits] {len(raw_leads)} permisos → "
            f"{len(value_filtered)} pasan filtro $50K"
        )

        # ── Enriquecimiento de contacto ────────────────────────────
        enriched = []
        for lead in value_filtered:
            try:
                lead = enrich_lead(lead, lead_type="permit")
            except Exception as e:
                logger.warning(f"[permits] Enrich error {lead.get('id')}: {e}")
            enriched.append(lead)

        # ── Filtro 2: Calidad del lead ──────────────────────────────
        good_leads = []
        for lead in enriched:
            quality = lead.get("lead_quality_score", 0)
            label   = lead.get("quality_label", "")
            if should_send_lead(lead, lead_type="permit"):
                good_leads.append(lead)
                logger.info(
                    f"[permits] ✅ ENVIAR ({label} {quality}/10): "
                    f"{lead.get('city')} — {lead.get('address')} "
                    f"| ${_parse_cost(lead.get('estimated_cost','')):,.0f}"
                )
            else:
                logger.debug(
                    f"[permits] SKIP (calidad {quality}/10 = {label}): "
                    f"{lead.get('address')}"
                )

        logger.info(
            f"[permits] {len(value_filtered)} filtrados por valor → "
            f"{len(good_leads)} leads buenos/excelentes para enviar"
        )
        return good_leads

    # ── San Francisco ──────────────────────────────────────────────
    def _fetch_sf(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url = "https://data.sfgov.org/resource/i98e-djp9.json"
        params = {
            "$limit": 1000,
            "$where": f"filed_date >= '{since}'",
            "$order": "filed_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[permits] SF fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc  = (item.get("description", "") or "").lower()
            ptype = (item.get("permit_type_definition", "") or "").lower()
            if not any(kw in desc or kw in ptype for kw in PERMIT_KEYWORDS):
                continue

            # Pre-filtro rápido de valor antes de enriquecer
            cost_raw = item.get("estimated_cost")
            if cost_raw and _parse_cost(str(cost_raw)) < 50_000:
                continue

            leads.append({
                "id":                 f"sf_{item.get('permit_number', '')}",
                "city":               "San Francisco",
                "address":            self._sf_address(item),
                "permit_type":        item.get("permit_type_definition", "N/A"),
                "description":        (item.get("description", "") or "")[:160],
                "status":             item.get("status", "N/A"),
                "filed_date":         (item.get("filed_date", "") or "")[:10],
                "owner":              (item.get("owner_name", "") or "").title().strip(),
                "owner_phone":        item.get("owner_phone", ""),
                "contractor":         (item.get("contractor_company_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", ""),
                "contractor_license": (item.get("contractor_license_number", "") or "").strip(),
                "estimated_cost":     self._fmt_cost(item.get("estimated_cost")),
                "permit_no":          item.get("permit_number", ""),
                "source_url":         f"https://sfdbi.org/permit/{item.get('permit_number', '')}",
            })
        return leads

    # ── San José ───────────────────────────────────────────────────
    def _fetch_san_jose(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url = "https://data.sanjoseca.gov/resource/5e7j-kygj.json"
        params = {
            "$limit": 1000,
            "$where": f"application_date >= '{since}'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[permits] SJ fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc = (item.get("work_description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue

            cost_raw = item.get("job_value")
            if cost_raw and _parse_cost(str(cost_raw)) < 50_000:
                continue

            leads.append({
                "id":                 f"sj_{item.get('permit_number', '')}",
                "city":               "San Jose",
                "address":            (item.get("address", "") or "N/A").title(),
                "permit_type":        item.get("permit_type", "N/A"),
                "description":        (item.get("work_description", "") or "")[:160],
                "status":             item.get("status", "N/A"),
                "filed_date":         (item.get("application_date", "") or "")[:10],
                "owner":              (item.get("owner_name", "") or "").strip(),
                "owner_phone":        item.get("owner_phone", ""),
                "contractor":         (item.get("contractor_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", ""),
                "contractor_license": (item.get("contractor_license", "") or "").strip(),
                "estimated_cost":     self._fmt_cost(item.get("job_value")),
                "permit_no":          item.get("permit_number", ""),
                "source_url":         (
                    "https://www.sanjoseca.gov/your-government/"
                    "departments-offices/planning-building-code-enforcement"
                ),
            })
        return leads

    # ── Oakland ────────────────────────────────────────────────────
    def _fetch_oakland(self) -> list:
        since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        url = "https://data.oaklandca.gov/resource/p8h3-ngmm.json"
        params = {
            "$limit": 1000,
            "$where": f"application_date >= '{since}'",
            "$order": "application_date DESC",
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()
        except Exception as e:
            logger.warning(f"[permits] Oakland fetch error: {e}")
            return []

        leads = []
        for item in raw:
            desc = (item.get("description", "") or "").lower()
            if not any(kw in desc for kw in PERMIT_KEYWORDS):
                continue

            cost_raw = item.get("valuation")
            if cost_raw and _parse_cost(str(cost_raw)) < 50_000:
                continue

            leads.append({
                "id":                 f"oak_{item.get('permit_number', '')}",
                "city":               "Oakland",
                "address":            (item.get("address", "") or "N/A").title(),
                "permit_type":        item.get("permit_type", "N/A"),
                "description":        (item.get("description", "") or "")[:160],
                "status":             item.get("status", "N/A"),
                "filed_date":         (item.get("application_date", "") or "")[:10],
                "owner":              (item.get("owner_name", "") or "").strip(),
                "owner_phone":        item.get("owner_phone", ""),
                "contractor":         (item.get("contractor_name", "") or "").strip(),
                "contractor_phone":   item.get("contractor_phone", ""),
                "contractor_license": (item.get("contractor_license", "") or "").strip(),
                "estimated_cost":     self._fmt_cost(item.get("valuation")),
                "permit_no":          item.get("permit_number", ""),
                "source_url":         "https://aca.accela.com/OAKLAND",
            })
        return leads

    # ── Telegram notify ────────────────────────────────────────────
    def notify(self, lead: dict):
        quality_score = lead.get("lead_quality_score", 0)
        quality_emoji = lead.get("quality_emoji", "✅")
        quality_label = lead.get("quality_label", "BUENO")
        contact_lbl   = contact_score_label(lead.get("contact_score", 0))

        def _v(key, fallback=None):
            """Retorna el valor solo si no está vacío, None si no hay."""
            val = (lead.get(key) or "").strip()
            if not val or val.lower() in ("no indicado", "no encontrado", "n/a", "—"):
                return fallback
            return val

        fields = {
            # ── CALIDAD ─────────────────────────────────────
            f"{quality_emoji} Calidad del Lead":  f"{quality_label}  ({quality_score}/10)",
            "📋 Datos de Contacto":               contact_lbl,

            # ── PROYECTO ────────────────────────────────────
            "💰 Valor Estimado":    _v("estimated_cost", "—"),
            "🏷️ Tipo de Permiso":   _v("permit_type", "—"),
            "📝 Descripción":       _v("description", "—"),
            "📅 Fecha Solicitud":   _v("filed_date", "—"),
            "🔖 Estado Permiso":    _v("status", "—"),
            "🔢 Permiso #":         _v("permit_no", "—"),

            # ── PROPIETARIO ─────────────────────────────────
            "👤 Propietario":       _v("owner"),
            "📬 Dir. Postal":       _v("owner_mail_addr"),
            "📞 Tel. Propietario":  _v("owner_phone"),

            # ── CONTRATISTA ─────────────────────────────────
            "🔨 Contratista":       _v("contractor"),
            "📞 Tel. Contratista":  _v("contractor_phone"),
            "📍 Dir. Contratista":  _v("contractor_addr"),
            "🪪 Lic. CSLB":         _v("contractor_license"),
            "✅ Estado Lic.":       _v("contractor_status"),
            "🔧 Clasificación":     _v("contractor_types"),

            # ── OTROS CONTACTOS DEL PERMISO ─────────────────
            "📐 Aplicante":         _v("applicant"),
            "📞 Tel. Aplicante":    _v("applicant_phone"),
            "🖊️ Arquitecto":        _v("architect"),

            # ── LINKS ───────────────────────────────────────
            "🗺️ Ver en Maps":       _v("maps_url"),
            "🔗 Permiso Oficial":   _v("source_url", "—"),
        }

        # Eliminar campos None (vacíos)
        fields = {k: v for k, v in fields.items() if v is not None}

        send_lead(
            agent_name="Permisos de Construccion",
            emoji="🏗️",
            title=f"{lead.get('city', '')} — {lead.get('address', '')}",
            fields=fields,
            cta="Contacta al contratista o propietario — proyecto activo, necesita insulacion",
        )

    # ── Helpers ────────────────────────────────────────────────────
    def _sf_address(self, item: dict) -> str:
        parts = [
            item.get("street_number", ""),
            item.get("street_name", ""),
            item.get("street_suffix", ""),
        ]
        return " ".join(p for p in parts if p).title()

    @staticmethod
    def _fmt_cost(raw) -> str:
        if raw is None:
            return "N/A"
        try:
            val = float(str(raw).replace(",", "").replace("$", ""))
            return f"${val:,.0f}"
        except ValueError:
            return str(raw)
