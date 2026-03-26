"""
main.py — Orquestador principal de Insul-Techs Lead Agents
Uso:
  python main.py               # inicia todos los agentes
  python main.py --test        # prueba conexión Telegram
  python main.py --run permits # ejecuta un agente puntualmente
  python main.py --stats       # muestra estadísticas de leads enviados
"""

import os
import sys
import time
import logging
import argparse
import schedule
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ── Imports internos ──────────────────────────────────────────────
from utils.telegram import send_message
from utils.db import init_db, get_stats
from agents.permits_agent import PermitsAgent
from agents.solar_agent import SolarAgent
from agents.rodents_agent import RodentsAgent
from agents.flood_agent import FloodAgent

# ── Registro de agentes ───────────────────────────────────────────
AGENTS = {
    "permits": {
        "class":            PermitsAgent,
        "env_key":          "AGENT_PERMITS",
        "interval_key":     "INTERVAL_PERMITS",
        "default_interval": 60,
    },
    "solar": {
        "class":            SolarAgent,
        "env_key":          "AGENT_SOLAR",
        "interval_key":     "INTERVAL_SOLAR",
        "default_interval": 60,
    },
    "rodents": {
        "class":            RodentsAgent,
        "env_key":          "AGENT_RODENTS",
        "interval_key":     "INTERVAL_RODENTS",
        "default_interval": 120,
    },
    "flood": {
        "class":            FloodAgent,
        "env_key":          "AGENT_FLOOD",
        "interval_key":     "INTERVAL_FLOOD",
        "default_interval": 30,
    },
}


def _is_enabled(env_key: str) -> bool:
    return os.getenv(env_key, "true").lower() not in ("false", "0", "no")


def run_agent(agent_key: str):
    """Instancia y ejecuta un agente por su clave."""
    cfg   = AGENTS[agent_key]
    agent = cfg["class"]()
    try:
        leads = agent.fetch_leads()
        new   = 0
        for lead in leads:
            if agent.send_if_new(lead):
                new += 1
        logger.info(f"[{agent_key}] {len(leads)} leads encontrados, {new} nuevos enviados")
    except Exception as e:
        logger.error(f"[{agent_key}] Error: {e}", exc_info=True)


def cmd_test():
    """Envía un mensaje de prueba a Telegram."""
    logger.info("Enviando mensaje de prueba a Telegram...")
    ok = send_message(
        "✅ *Insul-Techs Lead Agents* conectado correctamente.\n"
        "El bot está listo para enviar leads."
    )
    if ok:
        logger.info("✅ Mensaje de prueba enviado. Revisa tu grupo de Telegram.")
    else:
        logger.error("❌ Falló el envío. Verifica TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en .env")


def cmd_stats():
    """Muestra estadísticas desde la base de datos."""
    stats = get_stats()
    print("\n📊 Estadísticas de leads enviados\n" + "─" * 40)
    total = 0
    for agent_key, count in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"  {agent_key:<20} {count:>6} leads")
        total += count
    print("─" * 40)
    print(f"  {'TOTAL':<20} {total:>6} leads\n")


def cmd_run_one(agent_key: str):
    """Ejecuta un único agente inmediatamente."""
    if agent_key not in AGENTS:
        print(f"❌ Agente desconocido: '{agent_key}'. Opciones: {list(AGENTS)}")
        sys.exit(1)
    logger.info(f"Ejecutando agente '{agent_key}' manualmente...")
    run_agent(agent_key)


def cmd_start():
    """Inicia todos los agentes habilitados con sus intervalos configurados."""
    init_db()
    enabled = []

    for key, cfg in AGENTS.items():
        if not _is_enabled(cfg["env_key"]):
            logger.info(f"[{key}] Desactivado en .env — omitido")
            continue

        interval = int(os.getenv(cfg["interval_key"], cfg["default_interval"]))
        enabled.append(key)

        # Ejecutar inmediatamente al arrancar
        run_agent(key)

        # Programar ejecuciones periódicas
        schedule.every(interval).minutes.do(run_agent, agent_key=key)
        logger.info(f"[{key}] Programado cada {interval} min")

    if not enabled:
        logger.warning("No hay agentes habilitados. Revisa tu .env")
        sys.exit(1)

    logger.info(f"🚀 {len(enabled)} agente(s) corriendo: {', '.join(enabled)}")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insul-Techs Lead Agents")
    parser.add_argument("--test",  action="store_true", help="Probar conexión Telegram")
    parser.add_argument("--stats", action="store_true", help="Ver estadísticas")
    parser.add_argument("--run",   metavar="AGENT",     help="Ejecutar un agente específico")
    args = parser.parse_args()

    if args.test:
        cmd_test()
    elif args.stats:
        cmd_stats()
    elif args.run:
        init_db()
        cmd_run_one(args.run)
    else:
        cmd_start()
