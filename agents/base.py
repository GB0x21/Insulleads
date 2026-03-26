"""
agents/base.py — Clase base para todos los agentes
Provee: deduplicación via SQLite, wrapper de notificación
"""

import logging
from abc import ABC, abstractmethod
from utils.db import is_sent, mark_sent

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    # Subclases deben definir estos atributos
    name:      str = "Base Agent"
    emoji:     str = "🤖"
    agent_key: str = "base"

    @abstractmethod
    def fetch_leads(self) -> list:
        """Obtiene leads de la fuente de datos. Retorna lista de dicts."""
        ...

    @abstractmethod
    def notify(self, lead: dict):
        """Formatea y envía el lead a Telegram."""
        ...

    def send_if_new(self, lead: dict) -> bool:
        """
        Envía el lead solo si no fue enviado antes.
        Retorna True si fue enviado, False si era duplicado.
        """
        lead_id = lead.get("id")
        if not lead_id:
            logger.warning(f"[{self.agent_key}] Lead sin ID, omitido: {lead}")
            return False

        if is_sent(self.agent_key, lead_id):
            return False

        try:
            self.notify(lead)
            mark_sent(self.agent_key, lead_id)
            return True
        except Exception as e:
            logger.error(f"[{self.agent_key}] Error al notificar lead {lead_id}: {e}")
            return False
