"""
agents/base.py  v7
━━━━━━━━━━━━━━━━━
Clase base para todos los agentes.

v7 (Fase 1 — Hermes-style):
  1. Learning Loop — cada ciclo registra métricas en agent_metrics.py.
     El scheduler lee esas métricas para adaptive intervals y circuit breaker.
  2. Multi-canal routing — los leads se enrutan según su score:
       🔥 HOT  (≥90): Telegram + WhatsApp + Email
       🟠 WARM (≥70): Telegram + Email
       🟡 MEDIUM-COLD: Telegram solamente
  3. Filtro de contacto — igual que v6: sin teléfono ni email no se envía.
"""

import logging
import time
from abc import ABC, abstractmethod

from utils.db import is_sent, mark_sent
from utils.telegram import send_message
from utils.dedup import get_dedup_engine
from utils.hot_zones import get_hot_zone_detector, format_hot_zone_alert
from utils.lead_scoring import score_lead
from utils.notifications import send_lead_whatsapp, send_lead_email
from utils.agent_metrics import record_run

logger = logging.getLogger(__name__)


def _has_contact(lead: dict) -> bool:
    return bool(
        lead.get("contact_phone")
        or lead.get("phone")
        or lead.get("contact_email")
    )


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
          3. Lead scoring (Hermes-style: score → canal)
          4. Multi-canal routing por score
          5. Hot zone detection
          6. Learning loop — registra métricas del ciclo

        Retorna el número de leads nuevos enviados.
        """
        t_start = time.monotonic()
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

            # Paso 5: Enviar con multi-canal routing por score
            for lead in leads_with_contact:
                try:
                    scoring = score_lead(lead)
                    lead["_scoring"] = scoring

                    # Canal principal: siempre Telegram
                    self.notify(lead)
                    mark_sent(self.agent_key, lead["id"])
                    dedup.mark_notified(lead.get("address", ""), lead.get("city", ""))
                    sent_count += 1

                    # Canales adicionales según score (Hermes multi-canal)
                    self._route_multichannel(lead, scoring)

                except Exception as e:
                    logger.error(f"[{self.agent_key}] Error notificando {lead.get('id')}: {e}")

            # Paso 6: Detectar y alertar hot zones
            new_zones = hz_detector.get_new_hot_zones()
            for zone in new_zones:
                try:
                    alert_msg = format_hot_zone_alert(zone)
                    send_message(alert_msg)
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
            # Learning Loop: registrar métricas de este ciclo
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

    def _route_multichannel(self, lead: dict, scoring: dict):
        """
        Routing multi-canal Hermes-style según score del lead:
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
