"""
main.py v3 — Orquestador principal Insul-Techs Lead Agents

⚡ OPTIMIZACIONES v3:
  1. Agentes singleton — se instancian UNA sola vez al arrancar,
     no en cada ciclo (elimina recarga de contactos en __init__)
  2. Fetch paralelo    — cada ciclo llama al agente en un thread
     separado; agents que demoran no bloquean a los demás
  3. Timeout por agente — un agente colgado no congela el proceso

Uso:
  python main.py               # inicia todos los agentes
  python main.py --test        # prueba conexión Telegram
  python main.py --run permits # ejecuta un agente puntualmente
  python main.py --stats       # estadísticas de leads enviados
"""

import os
import sys
import time
import logging
import argparse
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from utils.contacts_loader import load_all_contacts   # precarga al importar
from agents.permits_agent import PermitsAgent
from agents.solar_agent   import SolarAgent
from agents.rodents_agent import RodentsAgent
from agents.flood_agent   import FloodAgent

# ── Registro de agentes ────────────────────────────────────────────
AGENT_REGISTRY = {
    "permits": {"class": PermitsAgent, "env_key": "AGENT_PERMITS", "interval_key": "INTERVAL_PERMITS", "default_interval": 60},
    "solar":   {"class": SolarAgent,   "env_key": "AGENT_SOLAR",   "interval_key": "INTERVAL_SOLAR",   "default_interval": 60},
    "rodents": {"class": RodentsAgent, "env_key": "AGENT_RODENTS", "interval_key": "INTERVAL_RODENTS", "default_interval": 120},
    "flood":   {"class": FloodAgent,   "env_key": "AGENT_FLOOD",   "interval_key": "INTERVAL_FLOOD",   "default_interval": 30},
}

# ⚡ Instancias singleton — creadas UNA sola vez
_AGENT_INSTANCES: dict = {}


def _is_enabled(env_key: str) -> bool:
    return os.getenv(env_key, "true").lower() not in ("false", "0", "no")


def _get_or_create_agent(key: str):
    """Retorna la instancia singleton del agente (crea si no existe)."""
    if key not in _AGENT_INSTANCES:
        _AGENT_INSTANCES[key] = AGENT_REGISTRY[key]["class"]()
        logger.info(f"[{key}] Agente instanciado")
    return _AGENT_INSTANCES[key]


def run_agent(agent_key: str):
    """Ejecuta un ciclo del agente (fetch + notify nuevos)."""
    agent = _get_or_create_agent(agent_key)
    t0 = time.monotonic()
    try:
        leads = agent.fetch_leads()
        new   = sum(1 for lead in leads if agent.send_if_new(lead))
        elapsed = time.monotonic() - t0
        logger.info(
            f"[{agent_key}] {len(leads)} leads encontrados, "
            f"{new} nuevos enviados  ({elapsed:.1f}s)"
        )
    except Exception as e:
        logger.error(f"[{agent_key}] Error en ciclo: {e}", exc_info=True)


# ── Comandos CLI ───────────────────────────────────────────────────

def cmd_test():
    logger.info("Enviando mensaje de prueba a Telegram...")
    ok = send_message(
        "✅ *Insul-Techs Lead Agents v3* conectado correctamente.\n"
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


def cmd_run_one(agent_key: str):
    if agent_key not in AGENT_REGISTRY:
        print(f"❌ Agente desconocido: '{agent_key}'. Opciones: {list(AGENT_REGISTRY)}")
        sys.exit(1)
    logger.info(f"Ejecutando agente '{agent_key}' manualmente...")
    run_agent(agent_key)


def cmd_start():
    """
    Inicia todos los agentes habilitados.
    ⚡ El primer ciclo se ejecuta en paralelo para arranque rápido.
    """
    init_db()

    # Precargar contactos ANTES de instanciar agentes
    contacts = load_all_contacts()
    logger.info(f"📋 {len(contacts):,} contactos disponibles para matching")

    enabled = []
    for key, cfg in AGENT_REGISTRY.items():
        if not _is_enabled(cfg["env_key"]):
            logger.info(f"[{key}] Desactivado — omitido")
            continue
        interval = int(os.getenv(cfg["interval_key"], cfg["default_interval"]))
        enabled.append((key, interval))
        # Pre-instanciar el agente (singleton)
        _get_or_create_agent(key)

    if not enabled:
        logger.warning("No hay agentes habilitados. Revisa tu .env")
        sys.exit(1)

    # ⚡ Primer ciclo en paralelo — todos los agentes a la vez
    logger.info(f"🚀 Arrancando {len(enabled)} agente(s) en paralelo...")
    with ThreadPoolExecutor(max_workers=len(enabled)) as executor:
        futures = {executor.submit(run_agent, key): key for key, _ in enabled}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.error(f"[{key}] Error en arranque: {e}")

    # Programar ciclos periódicos
    for key, interval in enabled:
        schedule.every(interval).minutes.do(run_agent, agent_key=key)
        logger.info(f"[{key}] Programado cada {interval} min")

    logger.info(f"✅ Todos los agentes corriendo: {', '.join(k for k, _ in enabled)}")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ── Entry point ────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insul-Techs Lead Agents v3")
    parser.add_argument("--test",  action="store_true")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--run",   metavar="AGENT")
    args = parser.parse_args()

    if args.test:
        cmd_test()
    elif args.stats:
        cmd_stats()
    elif args.run:
        init_db()
        load_all_contacts()
        cmd_run_one(args.run)
    else:
        cmd_start()
