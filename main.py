"""
main.py — Orquestador principal de Insul-Techs Lead Agents

Ejecuta todos los agentes en sus intervalos configurados y
envía un resumen diario a Telegram.

Uso:
    python main.py              # Inicia todos los agentes activos
    python main.py --test       # Prueba la conexión a Telegram
    python main.py --run solar  # Ejecuta solo el agente solar una vez
    python main.py --stats      # Muestra estadísticas de leads
"""

import os
import sys
import logging
import argparse
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────
import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s [%(name)s] %(levelname)s%(reset)s — %(message)s",
    datefmt="%H:%M:%S",
    log_colors={
        "DEBUG":    "cyan",
        "INFO":     "green",
        "WARNING":  "yellow",
        "ERROR":    "red",
        "CRITICAL": "bold_red",
    }
))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("main")

# ── Importar agentes ──────────────────────────────────────────────
from agents.permits_agent import PermitsAgent
from agents.solar_agent   import SolarAgent
from agents.rodents_agent import RodentsAgent
from agents.flood_agent   import FloodAgent
from utils.db             import init_db, get_stats
from utils.telegram       import send_lead, send_summary, send_error

# ── Registro de agentes ───────────────────────────────────────────
AGENTS = {
    "permits": {
        "class":    PermitsAgent,
        "env_key":  "AGENT_PERMITS",
        "interval": "INTERVAL_PERMITS",
        "default_interval": 60,
    },
    "solar": {
        "class":    SolarAgent,
        "env_key":  "AGENT_SOLAR",
        "interval": "INTERVAL_SOLAR",
        "default_interval": 60,
    },
    "rodents": {
        "class":    RodentsAgent,
        "env_key":  "AGENT_RODENTS",
        "interval": "INTERVAL_RODENTS",
        "default_interval": 120,
    },
    "flood": {
        "class":    FloodAgent,
        "env_key":  "AGENT_FLOOD",
        "interval": "INTERVAL_FLOOD",
        "default_interval": 30,
    },
}


def run_agent(agent_key: str):
    """Ejecuta un agente específico de forma segura."""
    cfg = AGENTS.get(agent_key)
    if not cfg:
        logger.error(f"Agente '{agent_key}' no encontrado.")
        return
    agent = cfg["class"]()
    logger.info(f"▶ Ejecutando: {agent.name}")
    new = agent.run()
    logger.info(f"✅ {agent.name} — {new} nuevos leads enviados")


def schedule_agents():
    """Programa todos los agentes activos según su intervalo."""
    for key, cfg in AGENTS.items():
        env_active = os.getenv(cfg["env_key"], "true").lower()
        if env_active != "true":
            logger.info(f"⏸  {key} desactivado en .env")
            continue

        minutes = int(os.getenv(cfg["interval"], cfg["default_interval"]))
        agent_cls = cfg["class"]
        agent_name = agent_cls.name if hasattr(agent_cls, "name") else key

        # Primera ejecución inmediata
        schedule.every(minutes).minutes.do(run_agent, agent_key=key)
        logger.info(f"🕐 {key} programado cada {minutes} minutos")

    # Resumen diario a las 8am
    schedule.every().day.at("08:00").do(send_daily_summary)
    logger.info("📊 Resumen diario programado para las 08:00")


def send_daily_summary():
    stats = get_stats()
    send_summary(stats)


def test_telegram():
    """Prueba que Telegram esté correctamente configurado."""
    logger.info("Enviando mensaje de prueba a Telegram...")
    try:
        send_lead(
            agent_name="Sistema — Prueba de Conexión",
            emoji="🤖",
            title="Insul-Techs Lead Agents — ACTIVO",
            fields={
                "Estado":    "✅ Conexión exitosa",
                "Agentes":   ", ".join(AGENTS.keys()),
                "Hora":      datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "Servidor":  os.uname().nodename if hasattr(os, "uname") else "Windows",
            },
            cta="El sistema está listo para enviar leads automáticamente."
        )
        logger.info("✅ Mensaje de prueba enviado correctamente.")
    except Exception as e:
        logger.error(f"❌ Error de Telegram: {e}")
        logger.error("Verifica TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en tu .env")


def show_stats():
    """Muestra estadísticas de leads enviados."""
    init_db()
    stats = get_stats()
    if not stats:
        print("\n📭 Aún no hay leads registrados.\n")
        return
    print("\n📊 ESTADÍSTICAS DE LEADS ENVIADOS")
    print("=" * 50)
    total = 0
    for row in stats:
        print(f"  {row['agent']:<20} → {row['total']:>4} leads  (último: {row['last_lead'][:10]})")
        total += row["total"]
    print("=" * 50)
    print(f"  {'TOTAL':<20} → {total:>4} leads")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Insul-Techs Lead Agents — Sistema de detección automática de leads"
    )
    parser.add_argument("--test",  action="store_true", help="Prueba la conexión a Telegram")
    parser.add_argument("--stats", action="store_true", help="Muestra estadísticas")
    parser.add_argument("--run",   type=str,            help="Ejecuta un agente específico una vez (permits|solar|rodents|flood)")
    args = parser.parse_args()

    init_db()

    if args.test:
        test_telegram()
        return

    if args.stats:
        show_stats()
        return

    if args.run:
        run_agent(args.run)
        return

    # ── Modo normal: programar y correr indefinidamente ──────────
    logger.info("=" * 60)
    logger.info("  INSUL-TECHS LEAD AGENTS — INICIANDO")
    logger.info("=" * 60)

    # Verificar credenciales
    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        logger.error("❌ TELEGRAM_BOT_TOKEN no configurado. Verifica tu .env")
        sys.exit(1)

    schedule_agents()

    # Ejecutar todos los agentes inmediatamente al inicio
    logger.info("🚀 Ejecutando todos los agentes por primera vez...")
    for key in AGENTS:
        env_active = os.getenv(AGENTS[key]["env_key"], "true").lower()
        if env_active == "true":
            run_agent(key)

    logger.info("🔄 Sistema en ejecución. Ctrl+C para detener.\n")

    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("\n⏹  Sistema detenido manualmente.")
            break
        except Exception as e:
            logger.error(f"Error en el loop principal: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()


# ── Agregar al parser en main() si se llama directamente ──
# python main.py --reset solar   → Borra historial y re-procesa todos los leads del agente
