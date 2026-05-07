"""
agents/base.py  v8
━━━━━━━━━━━━━━━━━
Clase base para todos los agentes.

v8 (Fase 2 — claude-mem inspired):
  1. Memory Engine — cada ciclo registra observaciones en utils/memory.py.
     El agente acumula conocimiento que enriquece futuros ciclos.
  2. Memory-enhanced scoring — usa score_lead_with_memory() que incorpora
     el historial de conversiones WON/LOST para ajustar el score ±15pts.
  3. Observaciones estructuradas: lead_found, lead_sent, pattern, insight.
  4. Context injection: el agente puede consultar su propia memoria antes
     de procesar leads (paralelo al context injection de claude-mem).

v7 (Fase 1 — Hermes-style):
  1. Learning Loop — métricas de ciclo en agent_metrics.py.
  2. Multi-canal routing — Telegram + WhatsApp + Email según score.
  3. Filtro de contacto — sin teléfono ni email no se envía.
"""

import logging
import time
from abc import ABC, abstractmethod

from utils.db import is_sent, mark_sent
from utils.telegram import send_message
from utils.dedup import get_dedup_engine
from utils.hot_zones import get_hot_zone_detector, format_hot_zone_alert
from utils.lead_scoring import score_lead_with_memory
from utils.notifications import send_lead_whatsapp, send_lead_email
from utils.agent_metrics import record_run
from utils.memory import record_observation, get_context, get_agent_patterns

logger = logging.getLogger(__name__)


def _has_contact(lead: dict) -> bool:
    """
    Un lead tiene contacto si:
    - Tiene tel/email DIRECTO (contacto de persona), O
    - Tiene dirección + ciudad (propiedad = contacto implícito para prospección)

    Muchos leads son sobre propiedades (permits, solar, energy, rodents).
    La dirección es el "contacto" para enviar prospectores/contratistas.
    """
    has_direct_contact = bool(
        lead.get("contact_phone")
        or lead.get("phone")
        or lead.get("contact_email")
    )
    has_property_address = bool(
        lead.get("address") and lead.get("city")
    )
    return has_direct_contact or has_property_address


class BaseAgent(ABC):
    name:      str = "Base Agent"
    emoji:     str = "🤖"
    agent_key: str = "base"

    @abstractmethod
    def fetch_leads(self) -> list:
        ...

    @abstractmethod
    def notify(self, lead: dict):
        ...

    def get_memory_context(self, query: str = "") -> str:
        """
        Retorna contexto de memoria relevante para este agente.
        Análogo al context injection de claude-mem — enriquece el ciclo
        con aprendizajes de ciclos anteriores.
        """
        try:
            return get_context(self.agent_key, query=query, max_tokens=300)
        except Exception:
            return ""

    def send_if_new(self, lead: dict) -> bool:
        """Envía el lead solo si no fue enviado antes y tiene datos de contacto."""
        lead_id = lead.get("id")
        if not lead_id or is_sent(self.agent_key, lead_id):
            return False

        if not _has_contact(lead):
            logger.debug(
                f"[{self.agent_key}] Omitido (sin contacto): "
                f"{lead.get('address', '')} — {lead.get('city', '')}"
            )
            return False

        try:
            self.notify(lead)
            mark_sent(self.agent_key, lead_id)
            dedup = get_dedup_engine()
            dedup.mark_notified(lead.get("address", ""), lead.get("city", ""))
            return True
        except Exception as e:
            logger.error(f"[{self.agent_key}] Error al notificar {lead_id}: {e}")
            return False

    def send_batch(self, leads: list) -> int:
        """
        Envía una lista de leads nuevos con:
          1. Deduplicación cross-agent
          2. Filtro de contacto
          3. Memory-enhanced scoring (Fase 2: +/- pts de historial de conversiones)
          4. Multi-canal routing por score
          5. Hot zone detection
          6. Registro de observaciones en memoria (Fase 2: claude-mem style)
          7. Learning loop — métricas del ciclo (Fase 1)

        Retorna el número de leads nuevos enviados.
        """
        t_start   = time.monotonic()
        error_msg = None
        sent_count = 0

        try:
            dedup = get_dedup_engine()
            hz_detector = get_hot_zone_detector()

            # Paso 1: Registrar en dedup + enriquecer cross-agent
            enriched_leads = []
            for lead in leads:
                consolidated = dedup.register_lead(lead, self.agent_key)
                enriched_leads.append(consolidated)

            # Paso 2: Filtrar no enviados
            new_leads = [
                l for l in enriched_leads
                if l.get("id") and not is_sent(self.agent_key, l["id"])
            ]

            # Registrar observación de ciclo en memoria (leads encontrados)
            if new_leads:
                self._record_cycle_observations(new_leads)

            if not new_leads:
                return 0

            # Paso 3: Filtro de contacto
            leads_with_contact    = [l for l in new_leads if _has_contact(l)]
            leads_without_contact = [l for l in new_leads if not _has_contact(l)]

            if leads_without_contact:
                logger.info(
                    f"[{self.agent_key}] {len(leads_without_contact)} leads sin contacto "
                    f"(tel/email) — no se envían"
                )

            if not leads_with_contact:
                return 0

            # Paso 4: Registrar en hot zone detector
            for lead in leads_with_contact:
                hz_detector.add_lead(lead)

            # Paso 5: Enviar con memory-enhanced scoring y multi-canal
            for lead in leads_with_contact:
                try:
                    lead["_agent_key"] = self.agent_key

                    # Fase 2: scoring enriquecido con memoria de conversiones
                    scoring = score_lead_with_memory(lead)
                    lead["_scoring"] = scoring

                    # Canal principal: siempre Telegram
                    self.notify(lead)
                    mark_sent(self.agent_key, lead["id"])
                    dedup.mark_notified(lead.get("address", ""), lead.get("city", ""))
                    sent_count += 1

                    # Registrar en memoria como lead_sent
                    self._record_lead_sent(lead, scoring)

                    # Canales adicionales según score
                    self._route_multichannel(lead, scoring)

                except Exception as e:
                    logger.error(f"[{self.agent_key}] Error notificando {lead.get('id')}: {e}")

            # Paso 6: Detectar y alertar hot zones
            new_zones = hz_detector.get_new_hot_zones()
            for zone in new_zones:
                try:
                    alert_msg = format_hot_zone_alert(zone)
                    send_message(alert_msg)
                    # Registrar hot zone como pattern en memoria
                    self._record_hot_zone(zone)
                    logger.info(
                        f"[HotZone] Zona detectada: {', '.join(zone['cities'])} — "
                        f"{zone['lead_count']} leads, {zone['agent_count']} agentes"
                    )
                except Exception as e:
                    logger.error(f"[HotZone] Error enviando alerta: {e}")

            # Log consolidación cross-agent
            consolidated_count = sum(
                1 for l in leads_with_contact if l.get("_is_consolidated")
            )
            if consolidated_count:
                logger.info(
                    f"[{self.agent_key}] {consolidated_count}/{len(leads_with_contact)} "
                    f"leads consolidados con datos de otros agentes"
                )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[{self.agent_key}] Error en send_batch: {e}", exc_info=True)

        finally:
            # Learning Loop Fase 1: registrar métricas de este ciclo
            duration = time.monotonic() - t_start
            try:
                record_run(
                    agent_key=self.agent_key,
                    leads_found=len(leads),
                    leads_sent=sent_count,
                    duration_s=round(duration, 2),
                    error=error_msg,
                )
            except Exception as e:
                logger.debug(f"[{self.agent_key}] Error registrando métricas: {e}")

        return sent_count

    # ── Métodos de memoria (Fase 2 — claude-mem style) ─────────────────────────

    def _record_cycle_observations(self, new_leads: list):
        """Registra observaciones de leads encontrados en este ciclo."""
        try:
            for lead in new_leads[:10]:  # Max 10 para no saturar
                city    = lead.get("city", "")
                addr    = lead.get("address", "")
                ptype   = lead.get("permit_type") or lead.get("description", "")[:60]
                content = f"{city} — {addr}: {ptype}".strip(" —:")

                record_observation(
                    agent_key=self.agent_key,
                    obs_type="lead_found",
                    content=content,
                    metadata={
                        "city":        city,
                        "address":     addr,
                        "permit_type": ptype,
                        "value_float": lead.get("value_float", 0),
                        "has_phone":   bool(lead.get("contact_phone") or lead.get("phone")),
                        "has_email":   bool(lead.get("contact_email")),
                    },
                )
        except Exception as e:
            logger.debug(f"[{self.agent_key}] Error registrando observaciones: {e}")

    def _record_lead_sent(self, lead: dict, scoring: dict):
        """Registra un lead enviado en la memoria del agente."""
        try:
            city    = lead.get("city", "")
            addr    = lead.get("address", "")
            score   = scoring.get("score", 0)
            grade   = scoring.get("grade", "")
            content = f"Enviado [{grade} {score}/100] {city} — {addr}"

            record_observation(
                agent_key=self.agent_key,
                obs_type="lead_sent",
                content=content,
                metadata={
                    "city":        city,
                    "address":     addr,
                    "score":       score,
                    "grade":       grade,
                    "contractor":  lead.get("contractor", ""),
                    "value_float": lead.get("value_float", 0),
                    "memory_bonus": scoring.get("memory_bonus", 0),
                },
            )
        except Exception as e:
            logger.debug(f"[{self.agent_key}] Error registrando lead_sent: {e}")

    def _record_hot_zone(self, zone: dict):
        """Registra una hot zone detectada como pattern en memoria."""
        try:
            cities  = ", ".join(zone.get("cities", []))
            content = (
                f"Hot Zone detectada: {cities} — "
                f"{zone.get('lead_count', 0)} leads en {zone.get('agent_count', 0)} agentes"
            )
            record_observation(
                agent_key=self.agent_key,
                obs_type="pattern",
                content=content,
                metadata={"zone": zone},
            )
        except Exception as e:
            logger.debug(f"[{self.agent_key}] Error registrando hot zone: {e}")

    def _route_multichannel(self, lead: dict, scoring: dict):
        """
        Routing multi-canal según score del lead:
          🔥 HOT  (≥90): WhatsApp + Email
          🟠 WARM (≥70): Email
          🟡 Resto:       Solo Telegram (ya enviado)
        """
        score = scoring.get("score", 0)

        if score >= 90:
            try:
                send_lead_whatsapp(lead, scoring)
            except Exception as e:
                logger.debug(f"[{self.agent_key}] WhatsApp skip: {e}")
            try:
                send_lead_email(lead, scoring)
            except Exception as e:
                logger.debug(f"[{self.agent_key}] Email skip: {e}")

        elif score >= 70:
            try:
                send_lead_email(lead, scoring)
            except Exception as e:
                logger.debug(f"[{self.agent_key}] Email skip: {e}")
