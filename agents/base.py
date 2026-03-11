"""
agents/base.py — Clase base para todos los agentes.
Cada agente hereda de BaseAgent e implementa fetch_leads().
"""
import logging
from abc import ABC, abstractmethod
from utils.db import init_db, is_already_sent, mark_as_sent, log_run
from utils.telegram import send_error

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    name       : str = "Base Agent"
    emoji      : str = "🤖"
    agent_key  : str = "base"

    def run(self) -> int:
        """Ejecuta el agente y retorna el número de nuevos leads enviados."""
        init_db()
        new_leads = 0
        error_msg = None

        try:
            leads = self.fetch_leads()
            logger.info(f"[{self.name}] {len(leads)} registros encontrados")

            for lead in leads:
                uid = lead.get("id")
                if not uid:
                    continue

                if is_already_sent(self.agent_key, str(uid)):
                    continue

                try:
                    self.notify(lead)
                    mark_as_sent(
                        agent=self.agent_key,
                        external_id=str(uid),
                        address=lead.get("address", ""),
                        details=str(lead)
                    )
                    new_leads += 1
                    logger.info(f"[{self.name}] ✅ Nuevo lead enviado: {uid}")
                except Exception as e:
                    logger.warning(f"[{self.name}] Error enviando lead {uid}: {e}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[{self.name}] ERROR CRÍTICO: {e}")
            try:
                send_error(self.name, error_msg)
            except Exception:
                pass

        log_run(self.agent_key, new_leads, error_msg)
        logger.info(f"[{self.name}] Completado. Nuevos leads: {new_leads}")
        return new_leads

    @abstractmethod
    def fetch_leads(self) -> list[dict]:
        """
        Obtiene leads de la fuente de datos.
        Debe retornar lista de dicts, cada uno con al menos 'id'.
        """
        ...

    @abstractmethod
    def notify(self, lead: dict):
        """Formatea y envía el lead por Telegram."""
        ...
