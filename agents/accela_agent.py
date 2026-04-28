"""
agents/accela_agent.py
━━━━━━━━━━━━━━━━━━━━━━
Permisos de construcción — Oakland via Accela Citizen Access

Oakland no tiene API pública de permisos (usa Accela sin SOAP/REST expuesto).
Este agente scrape el portal público con Scrapling (TLS fingerprint Chrome)
manteniendo sesión ASP.NET para enviar el formulario de búsqueda con fechas.

Flujo:
  1. GET portal → extraer ViewState + cookies de sesión
  2. POST async (UpdatePanel) → resultados en formato delta ASP.NET
  3. Parsear filas de la tabla → normalizar a formato de lead estándar
  4. Paginar hasta MAX_PAGES o hasta no haber más resultados
"""
from __future__ import annotations

import re
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from agents.base import BaseAgent
from utils.telegram import send_lead
from utils.contacts_loader import load_all_contacts, lookup_contact
from utils.lead_scoring import score_lead, format_score_line
from utils.notifications import notify_multichannel
from utils.stealth_fetcher import get_raw, post_form

logger = logging.getLogger(__name__)

ACCELA_BASE   = "https://aca-prod.accela.com/OAKLAND"
SEARCH_URL    = f"{ACCELA_BASE}/Cap/CapHome.aspx?module=Building&TabName=Building&SearchByPermit=True"
MAX_PAGES     = int(__import__("os").getenv("ACCELA_MAX_PAGES", "5"))
PERMIT_MONTHS = int(__import__("os").getenv("PERMIT_MONTHS", "3"))

# Tipos de permiso que implican trabajo de construcción/renovación relevante
_RELEVANT_TYPES = {
    "ADDITION", "ALTERATION", "REPAIR", "REMODEL", "RENOVATION",
    "NEW CONSTRUCTION", "NEW BUILDING", "ADU", "ACCESSORY DWELLING",
    "RESIDENTIAL", "COMMERCIAL IMPROVEMENT", "TENANT IMPROVEMENT",
    "SOFT STORY", "SEISMIC", "FOUNDATION",
}

# Statuses considerados "activos"
_ACTIVE_STATUSES = {"permit issued", "issued", "finaled", "in progress", "under review"}


def _cutoff() -> str:
    return (datetime.now() - timedelta(days=30 * PERMIT_MONTHS)).strftime("%m/%d/%Y")


def _is_relevant(permit_type: str, description: str) -> bool:
    combined = f"{permit_type} {description}".upper()
    return any(kw in combined for kw in _RELEVANT_TYPES)


def _extract_viewstate(html: str) -> dict:
    """Extrae campos ocultos ASP.NET del HTML del portal."""
    fields = {}
    for field in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__VIEWSTATEENCRYPTED"):
        m = re.search(rf'id="{field}"\s+value="([^"]*)"', html)
        fields[field] = m.group(1) if m else ""
    m = re.search(r'name="ACA_CS_FIELD"\s+value="([^"]+)"', html)
    fields["ACA_CS_FIELD"] = m.group(1) if m else ""
    return fields


def _build_search_post(hidden: dict, cutoff: str, today: str) -> dict:
    """Payload para la búsqueda inicial (primer POST)."""
    return {
        "ACA_CS_FIELD":       hidden.get("ACA_CS_FIELD", ""),
        "__EVENTTARGET":      "",
        "__EVENTARGUMENT":    "",
        "__LASTFOCUS":        "",
        "__VIEWSTATE":        hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__VIEWSTATEENCRYPTED": hidden.get("__VIEWSTATEENCRYPTED", ""),
        "__ASYNCPOST":        "true",
        "ctl00$ScriptManager1": (
            "ctl00$PlaceHolderMain$btnNewSearch|ctl00$PlaceHolderMain$btnNewSearch"
        ),
        "ctl00$HeaderNavigation$hdnShowReportLink": "Y",
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA": "0",
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate": cutoff,
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate": today,
        "ctl00$PlaceHolderMain$generalSearchForm$ddlGSCapStatus": "",
        "ctl00$PlaceHolderMain$btnNewSearch": "",
    }


def _build_next_page_post(hidden: dict, cutoff: str, today: str, next_ctrl: str) -> dict:
    """Payload para navegar a la siguiente página (UpdatePanel paginación)."""
    return {
        "ACA_CS_FIELD":       hidden.get("ACA_CS_FIELD", ""),
        "__EVENTTARGET":      next_ctrl,
        "__EVENTARGUMENT":    "",
        "__LASTFOCUS":        "",
        "__VIEWSTATE":        hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__VIEWSTATEENCRYPTED": hidden.get("__VIEWSTATEENCRYPTED", ""),
        "__ASYNCPOST":        "true",
        "ctl00$ScriptManager1": (
            f"ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$updatePanelTop|{next_ctrl}"
        ),
        "ctl00$HeaderNavigation$hdnShowReportLink": "Y",
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA": "0",
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate": cutoff,
        "ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate": today,
        "ctl00$PlaceHolderMain$generalSearchForm$ddlGSCapStatus": "",
    }


def _parse_results(delta_body: bytes) -> list[dict]:
    """
    Parsea el cuerpo de respuesta UpdatePanel (ASP.NET delta) y extrae
    los registros de la tabla de resultados.
    """
    html = delta_body.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    permits = []
    for link in soup.find_all("a", href=re.compile(r"CapDetail")):
        href = link.get("href", "")
        permit_num = link.get_text(strip=True)
        if not permit_num:
            continue

        row = link.find_parent("tr")
        if not row:
            continue

        cells = row.find_all("td")
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) < 6:
            continue

        # Estructura de columnas: [checkbox, date, status, permit#, type, address, desc, ...]
        date_str    = texts[1] if len(texts) > 1 else ""
        status      = texts[2] if len(texts) > 2 else ""
        permit_type = texts[4] if len(texts) > 4 else ""
        address     = texts[5] if len(texts) > 5 else ""
        description = texts[6] if len(texts) > 6 else ""

        # Limpiar address: "2101 TELEGRAPH AVE, Oakland CA 94612"
        address = address.replace(", Oakland CA", "").strip()

        # capID en el href para construir URL (el href ya incluye /OAKLAND/)
        detail_url = f"https://aca-prod.accela.com{href}" if href.startswith("/") else href

        permits.append({
            "permit_number": permit_num,
            "date":          date_str,
            "status":        status,
            "permit_type":   permit_type,
            "address":       address,
            "description":   description,
            "url":           detail_url,
        })

    return permits


def _find_next_ctrl(delta_body: bytes) -> str | None:
    """
    Busca el control ID del botón 'Next >' para paginación.
    Retorna el target del __doPostBack o None si no hay página siguiente.
    """
    html = delta_body.decode("utf-8", errors="ignore")
    m = re.search(
        r"javascript:__doPostBack\(&#39;([^&#]+)&#39;,&#39;&#39;\)\">Next",
        html,
    )
    return m.group(1) if m else None


def fetch_oakland_permits() -> list[dict]:
    """
    Scrape del portal Accela de Oakland.
    Retorna lista de dicts con datos de permisos.
    """
    cutoff = _cutoff()
    today  = datetime.now().strftime("%m/%d/%Y")

    # Paso 1: GET inicial para obtener ViewState + cookies
    status, body, cookies = get_raw(SEARCH_URL, timeout=20)
    if status != 200 or not body:
        logger.warning("[Accela/Oakland] No se pudo cargar el portal (HTTP %s)", status)
        return []

    html      = body.decode("utf-8", errors="ignore")
    hidden    = _extract_viewstate(html)
    if not hidden.get("__VIEWSTATE"):
        logger.warning("[Accela/Oakland] ViewState no encontrado")
        return []

    all_permits: list[dict] = []
    seen_ids: set[str] = set()
    _ASYNC_HEADERS = {
        "Referer":           SEARCH_URL,
        "X-Requested-With":  "XMLHttpRequest",
        "X-MicrosoftAjax":   "Delta=true",
    }

    # Página 1: búsqueda inicial
    post_data = _build_search_post(hidden, cutoff, today)
    result = post_form(SEARCH_URL, data=post_data, cookies=cookies,
                       extra_headers=_ASYNC_HEADERS, timeout=30)
    if not result:
        logger.warning("[Accela/Oakland] Sin respuesta en página 1")
        return []

    for page in range(1, MAX_PAGES + 1):
        page_permits = _parse_results(result)
        new_permits = [p for p in page_permits if p["permit_number"] not in seen_ids]
        if not new_permits:
            break

        for p in new_permits:
            seen_ids.add(p["permit_number"])
        all_permits.extend(new_permits)
        logger.debug("[Accela/Oakland] Página %s: %s permisos nuevos", page, len(new_permits))

        next_ctrl = _find_next_ctrl(result)
        if not next_ctrl or page >= MAX_PAGES:
            break

        # Actualizar ViewState desde la respuesta delta
        delta_html = result.decode("utf-8", errors="ignore")
        new_vs = re.search(r"__VIEWSTATE\|([^|]{50,})\|", delta_html)
        if new_vs:
            hidden["__VIEWSTATE"] = new_vs.group(1)

        post_data = _build_next_page_post(hidden, cutoff, today, next_ctrl)
        result = post_form(SEARCH_URL, data=post_data, cookies=cookies,
                           extra_headers=_ASYNC_HEADERS, timeout=30)
        if not result:
            break

    return all_permits


class AccelaAgent(BaseAgent):
    name      = "🏗️ Permisos Oakland — Accela"
    emoji     = "🏗️"
    agent_key = "accela"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._contacts = load_all_contacts()

    def fetch_leads(self) -> list:
        raw_permits = fetch_oakland_permits()
        if not raw_permits:
            logger.info("[Accela/Oakland] 0 permisos obtenidos")
            return []

        leads = []
        for p in raw_permits:
            if not _is_relevant(p["permit_type"], p["description"]):
                continue
            if p["status"].lower() not in _ACTIVE_STATUSES:
                continue

            lead = {
                "id":          f"accela_oakland_{p['permit_number']}",
                "city":        "Oakland",
                "address":     p["address"],
                "description": p["description"] or p["permit_type"],
                "status":      p["status"],
                "date":        p["date"],
                "contractor":  "",
                "owner":       "",
                "value":       "",
                "value_float": 0.0,
                "permit_type": p["permit_type"],
                "source_url":  p["url"],
                "_agent_key":  "accela",
            }

            match = lookup_contact(lead["address"], self._contacts)
            if match:
                lead["contact_phone"]  = match.get("phone", "")
                lead["contact_email"]  = match.get("email", "")
                lead["contact_source"] = f"CSV ({match['source']})"

            lead["_scoring"] = score_lead(lead)
            leads.append(lead)

        logger.info("[Accela/Oakland] %s leads relevantes de %s permisos", len(leads), len(raw_permits))
        return leads

    def notify(self, lead: dict):
        scoring    = lead.get("_scoring", {})
        score_line = format_score_line(scoring) if scoring else ""

        fields = {
            "📍 Ciudad":        lead.get("city"),
            "📝 Tipo":          (lead.get("permit_type") or "")[:120],
            "📋 Descripción":   (lead.get("description") or "")[:200],
            "📊 Estado":        lead.get("status"),
            "📅 Fecha":         lead.get("date"),
            "🏠 Dirección":     lead.get("address"),
            "👷 Contratista":   lead.get("contractor") or "—",
            "📞 Teléfono":      lead.get("contact_phone") or "—",
            "✉️  Email":        lead.get("contact_email") or "—",
            "🔗 Portal":        lead.get("source_url") or "aca-prod.accela.com/OAKLAND",
        }
        if score_line:
            fields["🎯 Lead Score"] = score_line

        send_lead(
            agent_name=self.name, emoji=self.emoji,
            title=f"Oakland — {lead['address']}",
            fields=fields,
            cta="🏗️ Permiso de construcción Oakland — oportunidad de insulación.",
        )

        if scoring.get("score", 0) >= 70:
            notify_multichannel(lead, scoring)
