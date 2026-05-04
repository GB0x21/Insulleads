"""
main.py v4 — Scheduler Hermes-style con Learning Loop

Mejoras sobre v3 (Fase 1):
  1. Circuit Breaker — agentes que fallan N veces se pausan con backoff exponencial
  2. Adaptive Intervals — agentes productivos corren más frecuente; silenciosos, menos
  3. Health Reports — resumen periódico a Telegram del estado de todos los agentes
  4. Graceful shutdown — espera que los threads activos terminen antes de salir
  5. init_metrics_db() — inicializa tablas de métricas junto con la DB de leads

Uso:
  python main.py               # inicia todos los agentes
  python main.py --test        # prueba conexión Telegram
  python main.py --run permits # ejecuta un agente manualmente
  python main.py --stats       # estadísticas de leads enviados
  python main.py --health      # health report detallado
"""

from __future__ import annotations

import os
import sys
import time
import logging
import argparse
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

from utils.telegram import send_message
from utils.db import init_db, get_stats
from utils.contacts_loader import load_all_contacts
from utils.agent_metrics import (
    init_metrics_db,
    is_circuit_open,
    get_adaptive_interval,
    set_base_interval,
    get_health_report,
    format_health_telegram,
)
from agents.permits_agent       import PermitsAgent
from agents.solar_agent         import SolarAgent
from agents.rodents_agent       import RodentsAgent
from agents.flood_agent         import FloodAgent
from agents.realestate_agent    import RealEstateAgent
from agents.energy_agent        import EnergyAgent
from agents.places_agent        import PlacesAgent
from agents.yelp_agent          import YelpAgent
from agents.construction_agent  import ConstructionAgent
from agents.deconstruction_agent import DeconstuctionAgent

# ── Registro de agentes ────────────────────────────────────────────────────────
AGENT_REGISTRY: dict[str, dict] = {
    "permits":        {"class": PermitsAgent,        "env_key": "AGENT_PERMITS",        "interval_key": "INTERVAL_PERMITS",        "default_interval": 60},
    "solar":          {"class": SolarAgent,          "env_key": "AGENT_SOLAR",          "interval_key": "INTERVAL_SOLAR",          "default_interval": 60},
    "rodents":        {"class": RodentsAgent,        "env_key": "AGENT_RODENTS",        "interval_key": "INTERVAL_RODENTS",        "default_interval": 120},
    "flood":          {"class": FloodAgent,          "env_key": "AGENT_FLOOD",          "interval_key": "INTERVAL_FLOOD",          "default_interval": 30},
    "construction":   {"class": ConstructionAgent,   "env_key": "AGENT_CONSTRUCTION",   "interval_key": "INTERVAL_CONSTRUCTION",   "default_interval": 60},
    "realestate":     {"class": RealEstateAgent,     "env_key": "AGENT_REALESTATE",     "interval_key": "INTERVAL_REALESTATE",     "default_interval": 120},
    "energy":         {"class": EnergyAgent,         "env_key": "AGENT_ENERGY",         "interval_key": "INTERVAL_ENERGY",         "default_interval": 360},
    "places":         {"class": PlacesAgent,         "env_key": "AGENT_PLACES",         "interval_key": "INTERVAL_PLACES",         "default_interval": 1440},
    "yelp":           {"class": YelpAgent,           "env_key": "AGENT_YELP",           "interval_key": "INTERVAL_YELP",           "default_interval": 1440},
    "deconstruction": {"class": DeconstuctionAgent,  "env_key": "AGENT_DECONSTRUCTION", "interval_key": "INTERVAL_DECONSTRUCTION", "default_interval": 120},
}

# ── Opt-in agents (importación condicional) ────────────────────────────────────
def _try_register_optional():
    """Registra agentes opcionales si están habilitados."""
    if os.getenv("AGENT_DINS", "false").lower() in ("true", "1", "yes"):
        try:
            from agents.dins_agent import DINSAgent
            AGENT_REGISTRY["dins"] = {
                "class": DINSAgent, "env_key": "AGENT_DINS",
                "interval_key": "INTERVAL_DINS", "default_interval": 1440,
            }
        except ImportError:
            logger.warning("[dins] No se pudo importar DINSAgent")

    if os.getenv("AGENT_HUD_MULTIFAMILY", "false").lower() in ("true", "1", "yes"):
        try:
            from agents.hud_agent import HUDMultifamilyAgent
            AGENT_REGISTRY["hud"] = {
                "class": HUDMultifamilyAgent, "env_key": "AGENT_HUD_MULTIFAMILY",
                "interval_key": "INTERVAL_HUD_MULTIFAMILY", "default_interval": 1440,
            }
        except ImportError:
            logger.warning("[hud] No se pudo importar HUDMultifamilyAgent")

    if os.getenv("AGENT_SHOVELS", "false").lower() in ("true", "1", "yes"):
        try:
            from agents.shovels_agent import ShovelsAgent
            AGENT_REGISTRY["shovels"] = {
                "class": ShovelsAgent, "env_key": "AGENT_SHOVELS",
                "interval_key": "INTERVAL_SHOVELS", "default_interval": 1440,
            }
        except ImportError:
            logger.warning("[shovels] No se pudo importar ShovelsAgent")


# ── Singletons de agentes ──────────────────────────────────────────────────────
_AGENT_INSTANCES: dict = {}
_STOP_EVENT = threading.Event()


def _is_enabled(env_key: str) -> bool:
    return os.getenv(env_key, "true").lower() not in ("false", "0", "no")


def _get_or_create_agent(key: str):
    if key not in _AGENT_INSTANCES:
        _AGENT_INSTANCES[key] = AGENT_REGISTRY[key]["class"]()
        logger.info(f"[{key}] Agente instanciado")
    return _AGENT_INSTANCES[key]


# ── Hermes-style Scheduler ─────────────────────────────────────────────────────

class AgentSchedule:
    """Estado de scheduling para un agente individual."""

    def __init__(self, key: str, base_interval_min: int):
        self.key              = key
        self.base_interval    = base_interval_min
        self.next_run_at: float = time.monotonic()  # Corre de inmediato al arrancar

    def effective_interval(self) -> int:
        """Intervalo efectivo considerando adaptación de métricas."""
        return get_adaptive_interval(self.key, self.base_interval)

    def is_due(self) -> bool:
        return time.monotonic() >= self.next_run_at

    def reschedule(self):
        interval_s = self.effective_interval() * 60
        self.next_run_at = time.monotonic() + interval_s

    def seconds_until_next(self) -> float:
        return max(0.0, self.next_run_at - time.monotonic())


def run_agent(agent_key: str) -> tuple[int, int]:
    """
    Ejecuta un ciclo del agente.
    Retorna (leads_found, leads_sent).
    El learning loop (record_run) se llama desde BaseAgent.send_batch().
    """
    # Circuit breaker check
    if is_circuit_open(agent_key):
        logger.info(f"[{agent_key}] Circuit breaker abierto — saltando ciclo")
        return (0, 0)

    agent = _get_or_create_agent(agent_key)
    t0 = time.monotonic()
    try:
        leads = agent.fetch_leads()
        new   = agent.send_batch(leads)
        elapsed = time.monotonic() - t0
        logger.info(
            f"[{agent_key}] {len(leads)} leads encontrados, "
            f"{new} nuevos enviados  ({elapsed:.1f}s)"
        )
        return (len(leads), new)
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error(f"[{agent_key}] Error en ciclo: {e}", exc_info=True)
        return (0, 0)


# ── Health Report ──────────────────────────────────────────────────────────────

def _send_health_report():
    """Envía health report a Telegram."""
    try:
        report = get_health_report()
        msg    = format_health_telegram(report)
        send_message(msg)
        logger.info("[health] Reporte enviado a Telegram")
    except Exception as e:
        logger.error(f"[health] Error generando reporte: {e}")


# ── CLI Commands ───────────────────────────────────────────────────────────────

def cmd_test():
    logger.info("Enviando mensaje de prueba a Telegram...")
    ok = send_message(
        "✅ *Lead Generation Agents v4* conectado correctamente.\n"
        "El bot está listo para enviar leads."
    )
    if ok:
        logger.info("✅ Mensaje enviado. Revisa tu grupo de Telegram.")
    else:
        logger.error("❌ Falló. Verifica TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en .env")


def cmd_stats():
    stats = get_stats()
    print("\n📊 Estadísticas de leads enviados\n" + "─" * 40)
    total = 0
    for key, count in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"  {key:<20} {count:>6} leads")
        total += count
    print("─" * 40)
    print(f"  {'TOTAL':<20} {total:>6} leads\n")


def cmd_health():
    report = get_health_report()
    print("\n" + format_health_telegram(report).replace("*", "").replace("━", "─"))


def cmd_run_one(agent_key: str):
    if agent_key not in AGENT_REGISTRY:
        print(f"❌ Agente desconocido: '{agent_key}'. Opciones: {list(AGENT_REGISTRY)}")
        sys.exit(1)
    logger.info(f"Ejecutando agente '{agent_key}' manualmente...")
    found, sent = run_agent(agent_key)
    logger.info(f"[{agent_key}] Manual: {found} encontrados, {sent} enviados")


def cmd_start():
    """
    Inicia el scheduler Hermes-style.

    Loop principal:
      1. Cada 30s revisa qué agentes tienen ciclo pendiente.
      2. Los agentes pendientes se ejecutan en paralelo (ThreadPoolExecutor).
      3. Circuit breaker y adaptive intervals ajustan dinámicamente.
      4. Health report cada HEALTH_INTERVAL_MIN minutos a Telegram.
    """
    init_db()
    init_metrics_db()

    _try_register_optional()

    contacts = load_all_contacts()
    logger.info(f"📋 {len(contacts):,} contactos disponibles para matching")

    # Registrar handlers de shutdown graceful
    def _shutdown(signum, frame):
        logger.info("Shutdown solicitado — esperando threads activos...")
        _STOP_EVENT.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Construir schedules para agentes habilitados
    schedules: list[AgentSchedule] = []
    for key, cfg in AGENT_REGISTRY.items():
        if not _is_enabled(cfg["env_key"]):
            logger.info(f"[{key}] Desactivado — omitido")
            continue
        raw_iv = os.getenv(cfg["interval_key"], str(cfg["default_interval"])).split("#")[0].strip()
        interval = int(raw_iv) if raw_iv.isdigit() else cfg["default_interval"]
        set_base_interval(key, interval)   # Base para que adaptativo funcione
        schedules.append(AgentSchedule(key, interval))
        _get_or_create_agent(key)          # Pre-instanciar singleton

    if not schedules:
        logger.warning("No hay agentes habilitados. Revisa tu .env")
        sys.exit(1)

    logger.info(
        f"🚀 Scheduler Hermes-style iniciado con {len(schedules)} agente(s): "
        + ", ".join(s.key for s in schedules)
    )

    # Health report configuración
    raw_hi = os.getenv("HEALTH_INTERVAL_MIN", "120").split("#")[0].strip()
    health_interval_min = int(raw_hi) if raw_hi.isdigit() else 120
    health_interval_s   = health_interval_min * 60
    last_health_report  = time.monotonic() - health_interval_s  # Enviar de inmediato al arrancar

    # ── Main loop ──────────────────────────────────────────────────────────────
    executor = ThreadPoolExecutor(max_workers=min(len(schedules), 10), thread_name_prefix="agent")

    try:
        while not _STOP_EVENT.is_set():
            due = [s for s in schedules if s.is_due()]

            if due:
                futures = {executor.submit(run_agent, s.key): s for s in due}
                for fut in as_completed(futures):
                    s = futures[fut]
                    try:
                        fut.result()
                    except Exception as e:
                        logger.error(f"[{s.key}] Error inesperado: {e}")
                    finally:
                        s.reschedule()
                        eff = s.effective_interval()
                        logger.debug(f"[{s.key}] Próximo ciclo en {eff}min")

            # Health report periódico
            if time.monotonic() - last_health_report >= health_interval_s:
                executor.submit(_send_health_report)
                last_health_report = time.monotonic()

            # Esperar hasta el próximo agente pendiente (máx 30s)
            if not _STOP_EVENT.is_set():
                next_wake = min((s.seconds_until_next() for s in schedules), default=30)
                _STOP_EVENT.wait(timeout=min(next_wake, 30))

    finally:
        logger.info("Apagando executor...")
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("✅ Insulleads detenido correctamente")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lead Generation Agents v4 — Hermes-style scheduler")
    parser.add_argument("--test",   action="store_true", help="Prueba conexión Telegram")
    parser.add_argument("--stats",  action="store_true", help="Estadísticas de leads enviados")
    parser.add_argument("--health", action="store_true", help="Health report del sistema")
    parser.add_argument("--run",    metavar="AGENT",     help="Ejecuta un agente manualmente")
    args = parser.parse_args()

    if args.test:
        cmd_test()
    elif args.stats:
        cmd_stats()
    elif args.health:
        init_db()
        init_metrics_db()
        cmd_health()
    elif args.run:
        init_db()
        init_metrics_db()
        load_all_contacts()
        cmd_run_one(args.run)
    else:
        cmd_start()
