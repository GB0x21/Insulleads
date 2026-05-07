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
from utils.memory import compress_memories, needs_compression
from utils.crm_sync import CRMSync
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
# (env_key, agent_key, module, class_name, interval_key, default_interval, descripcion)
_OPTIONAL_AGENTS = [
    ("AGENT_DINS",            "dins",     "agents.dins_agent",     "DINSAgent",
     "INTERVAL_DINS",            1440, "Cal Fire post-wildfire rebuilds"),
    ("AGENT_HUD_MULTIFAMILY", "hud",      "agents.hud_agent",      "HUDMultifamilyAgent",
     "INTERVAL_HUD_MULTIFAMILY", 1440, "HUD/LIHTC multifamily rehab"),
    ("AGENT_SHOVELS",         "shovels",  "agents.shovels_agent",  "ShovelsAgent",
     "INTERVAL_SHOVELS",         1440, "Shovels.ai national permits"),
    ("AGENT_ACCELA",          "accela",   "agents.accela_agent",   "AccelaAgent",
     "INTERVAL_ACCELA",          720,  "Accela ACA portals (multi-city)"),
    ("AGENT_THERMAL",         "thermal",  "agents.thermal_agent",  "ThermalAgent",
     "INTERVAL_THERMAL",         1440, "Thermal anomaly detection (satellite)"),
]


def _try_register_optional():
    """Registra agentes opcionales si están habilitados.

    Loguea SIEMPRE el estado de cada opt-in (habilitado / deshabilitado /
    error de import) para que el operador pueda diagnosticar por qué
    algún agente no aparece en el scheduler.
    """
    enabled_count = 0
    for env_key, agent_key, module, cls_name, interval_key, default_min, descr in _OPTIONAL_AGENTS:
        raw = os.getenv(env_key, "false").lower()
        if raw not in ("true", "1", "yes"):
            logger.info(f"[{agent_key}] Opt-in deshabilitado ({env_key}={raw or 'unset'}) — {descr}")
            continue
        try:
            mod = __import__(module, fromlist=[cls_name])
            cls = getattr(mod, cls_name)
            AGENT_REGISTRY[agent_key] = {
                "class": cls, "env_key": env_key,
                "interval_key": interval_key, "default_interval": default_min,
            }
            logger.info(f"[{agent_key}] ✓ Agente registrado — {descr}")
            enabled_count += 1
        except Exception as e:
            logger.warning(f"[{agent_key}] ✗ No se pudo importar {cls_name}: {e}")

    logger.info(f"[opt-in] {enabled_count}/{len(_OPTIONAL_AGENTS)} agentes opcionales activos")


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


class MemoryCompressionJob:
    """Job periódico para comprimir memorias (cada hora, fuera del hot path)."""

    def __init__(self, agent_keys: list[str], interval_min: int = 60):
        self.agent_keys = agent_keys
        self.interval_min = interval_min
        self.next_run_at = time.monotonic()  # Ejecutar apenas arranque

    def is_due(self) -> bool:
        return time.monotonic() >= self.next_run_at

    def reschedule(self):
        self.next_run_at = time.monotonic() + (self.interval_min * 60)

    def seconds_until_next(self) -> float:
        return max(0.0, self.next_run_at - time.monotonic())


class CRMSyncJob:
    """Job periódico para sincronizar leads con Krayin CRM."""

    def __init__(self, interval_min: int = 10):
        self.interval_min = interval_min
        self.next_run_at = time.monotonic()  # Ejecutar apenas arranque

    def is_due(self) -> bool:
        return time.monotonic() >= self.next_run_at

    def reschedule(self):
        self.next_run_at = time.monotonic() + (self.interval_min * 60)

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

def _compress_memories_job(compression_job: MemoryCompressionJob):
    """Job que comprime memorias para agentes que lo necesitan."""
    try:
        for agent_key in compression_job.agent_keys:
            if needs_compression(agent_key):
                logger.debug(f"[memory] Comprimiendo {agent_key}...")
                compress_memories(agent_key)
    except Exception as e:
        logger.error(f"[memory] Compression job failed: {e}")


def _sync_crm_job(crm_sync_job: CRMSyncJob):
    """Job que sincroniza leads con Krayin CRM."""
    try:
        crm = CRMSync()
        if crm.is_configured():
            count = crm.sync()
            logger.info(f"[crm_sync] {count} leads sincronizados con Krayin")
        else:
            logger.debug("[crm_sync] CRM no configurado, saltando sync")
    except Exception as e:
        logger.error(f"[crm_sync] Sync failed: {e}")


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


def cmd_flush_backlog(agent_key: str | None = None):
    """
    Reprocesa leads atascados en consolidated_leads (notified=0).

    Útil después de cambiar el filtro _has_contact() — leads que antes
    fueron rechazados pueden ahora pasar.

    agent_key=None  → drena todos los agentes registrados
    agent_key="x"   → drena solo el agente 'x'
    """
    _try_register_optional()
    targets = [agent_key] if agent_key else list(AGENT_REGISTRY.keys())
    total_sent = 0
    for key in targets:
        if key not in AGENT_REGISTRY:
            logger.warning(f"[flush] Agente desconocido: '{key}', saltado")
            continue
        agent = _get_or_create_agent(key)
        try:
            sent = agent.flush_backlog()
            total_sent += sent
            logger.info(f"[flush:{key}] {sent} leads enviados desde backlog")
        except Exception as e:
            logger.error(f"[flush:{key}] error: {e}", exc_info=True)
    logger.info(f"[flush] TOTAL: {total_sent} leads drenados del backlog")


def cmd_diagnose_crm():
    """Diagnóstico completo del estado de la integración con Krayin CRM."""
    print("\n══════════════════════════════════════════════════")
    print("  CRM Diagnostic — Insulleads ↔ Krayin")
    print("══════════════════════════════════════════════════\n")

    crm = CRMSync()
    report = crm.diagnose()

    def status(ok: bool) -> str:
        return "✅" if ok else "❌"

    print(f"  {status(report['krayin_env_ok'])} Krayin .env legible")
    print(f"     Path: {report['krayin_env_path']}")
    print(f"  {status(report['mysql_ok'])} MySQL accesible")
    print(f"  {status(report['pipeline_ok'])} Pipeline default configurado"
          + (f" (id={report['pipeline_id']})" if report['pipeline_id'] else ""))
    print(f"  {'✅' if report['sources_count'] > 0 else '⚠️ '} Lead sources: {report['sources_count']}")
    print()
    print(f"  📦 Leads sincronizados:  {report['synced_count']:,}")
    print(f"  ⏳ Leads pendientes:     {report['pending_count']:,}")
    print()

    if report["errors"]:
        print("  ⚠️  Issues detectados:")
        for err in report["errors"]:
            print(f"     • {err}")
        print()

    overall_ok = (
        report["krayin_env_ok"] and report["mysql_ok"]
        and (report["pipeline_ok"] or report["pending_count"] == 0)
    )
    if overall_ok:
        if report["pending_count"] > 0:
            print(f"  ➡️  Listo para sincronizar. Ejecuta: python main.py --sync-crm")
        else:
            print(f"  ✅ Todo OK — nada pendiente que sincronizar.")
    else:
        print(f"  🔧 Corrige los issues anteriores antes de sincronizar.")
    print()
    sys.exit(0 if overall_ok else 1)


def cmd_sync_crm():
    """Ejecuta sync manual del CRM con feedback detallado."""
    crm = CRMSync()
    if not crm.is_configured():
        logger.error("[sync-crm] CRM no configurado. Ejecuta: python main.py --diagnose-crm")
        sys.exit(1)
    count = crm.sync()
    logger.info(f"[sync-crm] {count} leads sincronizados con Krayin")


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
    enabled_agents = []
    for key, cfg in AGENT_REGISTRY.items():
        if not _is_enabled(cfg["env_key"]):
            logger.info(f"[{key}] Desactivado — omitido")
            continue
        raw_iv = os.getenv(cfg["interval_key"], str(cfg["default_interval"])).split("#")[0].strip()
        interval = int(raw_iv) if raw_iv.isdigit() else cfg["default_interval"]
        set_base_interval(key, interval)   # Base para que adaptativo funcione
        schedules.append(AgentSchedule(key, interval))
        enabled_agents.append(key)
        _get_or_create_agent(key)          # Pre-instanciar singleton

    if not schedules:
        logger.warning("No hay agentes habilitados. Revisa tu .env")
        sys.exit(1)

    # Job periódico para compresión de memoria (cada hora, fuera del hot path)
    compression_job = MemoryCompressionJob(enabled_agents, interval_min=60)

    # Job periódico para sincronizar leads con Krayin CRM (cada 10 min)
    crm_sync_job = CRMSyncJob(interval_min=10)

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

            # Memory compression job (cada hora, background)
            if compression_job.is_due():
                executor.submit(_compress_memories_job, compression_job)
                compression_job.reschedule()

            # CRM sync job (cada 10 min, background)
            if crm_sync_job.is_due():
                executor.submit(_sync_crm_job, crm_sync_job)
                crm_sync_job.reschedule()

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
    parser.add_argument("--flush-backlog", nargs="?", const="__ALL__", metavar="AGENT",
                        help="Reprocesa leads atascados en consolidated_leads (todos los agentes si no se especifica)")
    parser.add_argument("--diagnose-crm", action="store_true",
                        help="Diagnóstico de la integración con Krayin CRM")
    parser.add_argument("--sync-crm", action="store_true",
                        help="Ejecuta sync manual de leads pendientes a Krayin CRM")
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
    elif args.flush_backlog:
        init_db()
        init_metrics_db()
        load_all_contacts()
        target = None if args.flush_backlog == "__ALL__" else args.flush_backlog
        cmd_flush_backlog(target)
    elif args.diagnose_crm:
        init_db()
        cmd_diagnose_crm()
    elif args.sync_crm:
        init_db()
        cmd_sync_crm()
    else:
        cmd_start()
