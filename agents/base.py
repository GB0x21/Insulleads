"""
agents/base.py  v4
━━━━━━━━━━━━━━━━━
Clase base para todos los agentes.

⚡ FIX v4:
  - send_if_new ahora recibe la lista completa y aplica DIGEST MODE
    cuando hay más de MAX_BURST leads nuevos en un ciclo
  - Evita el 429 agrupando ráfagas en un único mensaje resumen
"""

import logging
from abc import ABC, abstractmethod
from utils.db import is_sent, mark_sent
from utils.telegram import send_digest, MAX_BURST

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
        Envía una lista de leads nuevos con protección anti-ráfaga:
          - Si hay ≤ MAX_BURST leads nuevos → envía uno por uno (notify normal)
          - Si hay > MAX_BURST leads nuevos → digest + marca todos como enviados
        Retorna el número de leads nuevos enviados.
        """
        # Filtrar solo los que no han sido enviados
        new_leads = [l for l in leads if l.get("id") and not is_sent(self.agent_key, l["id"])]

        if not new_leads:
            return 0

        if len(new_leads) <= MAX_BURST:
            # Modo normal: mensaje individual por lead
            count = 0
            for lead in new_leads:
                try:
                    self.notify(lead)
                    mark_sent(self.agent_key, lead["id"])
                    count += 1
                except Exception as e:
                    logger.error(f"[{self.agent_key}] Error notificando {lead.get('id')}: {e}")
            return count
        else:
            # Modo digest: un solo mensaje resumen
            logger.info(
                f"[{self.agent_key}] {len(new_leads)} leads nuevos — "
                f"modo DIGEST (>{MAX_BURST})"
            )
            ok = send_digest(self.name, self.emoji, new_leads)
            if ok:
                for lead in new_leads:
                    mark_sent(self.agent_key, lead["id"])
                return len(new_leads)
            return 0
