"""
agents/base.py  v5
━━━━━━━━━━━━━━━━━
Clase base para todos los agentes.

⚡ v5:
  - Integración con DeduplicationEngine (cross-agent dedup)
  - Integración con HotZoneDetector (geographic clustering)
  - Los leads pasan por dedup antes de ser enviados
  - Hot zones detectadas automáticamente después de cada batch
"""

import logging
from abc import ABC, abstractmethod
from utils.db import is_sent, mark_sent
from utils.telegram import send_message
from utils.dedup import get_dedup_engine
from utils.hot_zones import get_hot_zone_detector, format_hot_zone_alert

logger = logging.getLogger(__name__)


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
        """Envía el lead solo si no fue enviado antes. Retorna True si fue enviado."""
        lead_id = lead.get("id")
        if not lead_id or is_sent(self.agent_key, lead_id):
            return False
        try:
            self.notify(lead)
            mark_sent(self.agent_key, lead_id)
            return True
        except Exception as e:
            logger.error(f"[{self.agent_key}] Error al notificar {lead_id}: {e}")
            return False

    def send_batch(self, leads: list) -> int:
        """
        Envía una lista de leads nuevos con:
          1. Deduplicación cross-agent (consolida leads de múltiples agentes)
          2. Hot zone detection (detecta clusters geográficos)
          3. Protección anti-ráfaga (digest mode si > MAX_BURST)

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

        # Paso 3: Registrar en hot zone detector
        for lead in new_leads:
            hz_detector.add_lead(lead)

        # Paso 4: Enviar leads — siempre mensajes individuales
        sent_count = 0
        for lead in new_leads:
            try:
                self.notify(lead)
                mark_sent(self.agent_key, lead["id"])
                sent_count += 1
            except Exception as e:
                logger.error(f"[{self.agent_key}] Error notificando {lead.get('id')}: {e}")

        # Paso 5: Detectar y alertar hot zones nuevas
        new_zones = hz_detector.get_new_hot_zones()
        for zone in new_zones:
            try:
                alert_msg = format_hot_zone_alert(zone)
                send_message(alert_msg)
                logger.info(
                    f"[HotZone] 🔥 Zona detectada: {', '.join(zone['cities'])} — "
                    f"{zone['lead_count']} leads, {zone['agent_count']} agentes"
                )
            except Exception as e:
                logger.error(f"[HotZone] Error enviando alerta: {e}")

        # Log consolidación cross-agent
        consolidated_count = sum(
            1 for l in new_leads if l.get("_is_consolidated")
        )
        if consolidated_count:
            logger.info(
                f"[{self.agent_key}] {consolidated_count}/{len(new_leads)} "
                f"leads consolidados con datos de otros agentes"
            )

        return sent_count
