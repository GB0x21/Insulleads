"""
agents/base.py  v6
━━━━━━━━━━━━━━━━━
Clase base para todos los agentes.

v6: Filtro de contacto obligatorio — un lead sin teléfono ni email
    no se envía a Telegram. Se registra igualmente en sent_leads para
    evitar re-intentos en el mismo ciclo, pero la próxima vez que el
    agente lo detecte y ya tenga contacto enriquecido sí se enviará.
    La lógica: si no hay forma de contactar al GC, el lead no sirve.
"""

import logging
from abc import ABC, abstractmethod
from utils.db import is_sent, mark_sent
from utils.telegram import send_message
from utils.dedup import get_dedup_engine
from utils.hot_zones import get_hot_zone_detector, format_hot_zone_alert

logger = logging.getLogger(__name__)


def _has_contact(lead: dict) -> bool:
    """Retorna True si el lead tiene al menos teléfono o email de contacto."""
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
          1. Deduplicación cross-agent (consolida leads de múltiples agentes)
          2. Filtro de contacto — solo leads con teléfono o email
          3. Hot zone detection (detecta clusters geográficos)

        Retorna el número de leads nuevos enviados.
        """
        dedup = get_dedup_engine()
        hz_detector = get_hot_zone_detector()

        # Paso 1: Registrar en dedup engine + enriquecer con cross-agent data
        enriched_leads = []
        for lead in leads:
            consolidated = dedup.register_lead(lead, self.agent_key)
            enriched_leads.append(consolidated)

        # Paso 2: Filtrar solo los que no han sido enviados
        new_leads = [
            l for l in enriched_leads
            if l.get("id") and not is_sent(self.agent_key, l["id"])
        ]

        if not new_leads:
            return 0

        # Paso 3: Filtro de contacto — separar leads con y sin datos de contacto
        leads_with_contact    = [l for l in new_leads if _has_contact(l)]
        leads_without_contact = [l for l in new_leads if not _has_contact(l)]

        if leads_without_contact:
            logger.info(
                f"[{self.agent_key}] {len(leads_without_contact)} leads sin contacto "
                f"(tel/email) — no se envían a Telegram"
            )

        if not leads_with_contact:
            return 0

        # Paso 4: Registrar en hot zone detector solo los que se enviarán
        for lead in leads_with_contact:
            hz_detector.add_lead(lead)

        # Paso 5: Enviar leads con contacto
        sent_count = 0
        for lead in leads_with_contact:
            try:
                self.notify(lead)
                mark_sent(self.agent_key, lead["id"])
                dedup.mark_notified(lead.get("address", ""), lead.get("city", ""))
                sent_count += 1
            except Exception as e:
                logger.error(f"[{self.agent_key}] Error notificando {lead.get('id')}: {e}")

        # Paso 6: Detectar y alertar hot zones nuevas
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

        return sent_count
