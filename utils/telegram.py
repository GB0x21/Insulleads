"""
utils/telegram.py — Envía mensajes formateados a Telegram.
Usa la API REST directamente (sin asyncio) para simplicidad.
"""
import os
import requests
from datetime import datetime


def send_lead(agent_name: str, emoji: str, title: str, fields: dict, cta: str = ""):
    """
    Construye y envía un mensaje de lead a Telegram.

    Args:
        agent_name : nombre del agente (ej. "Permisos de Construcción")
        emoji      : emoji identificador del agente
        title      : título principal del lead
        fields     : dict con los campos a mostrar
        cta        : call-to-action opcional (ej. email del realtor)
    """
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise EnvironmentError("Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en .env")

    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    lines = [
        f"{emoji} *{agent_name.upper()}*",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"📌 *{_esc(title)}*",
        "",
    ]

    for key, value in fields.items():
        if value:
            lines.append(f"▸ *{_esc(key)}:* {_esc(str(value))}")

    if cta:
        lines += ["", f"🎯 {_esc(cta)}"]

    lines += ["", f"🕐 _{now}_"]

    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def send_summary(stats: list):
    """Envía un resumen diario de actividad."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    lines = [
        "📊 *RESUMEN DIARIO — INSUL-TECHS LEAD AGENTS*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    total = 0
    for row in stats:
        lines.append(f"▸ {row['agent']}: *{row['total']}* leads totales")
        total += row["total"]

    lines += ["", f"🏆 *Total acumulado: {total} leads*",
              f"🕐 _{datetime.now().strftime('%d/%m/%Y %H:%M')}_"]

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, json={
        "chat_id": chat_id,
        "text": "\n".join(lines),
        "parse_mode": "Markdown",
    }, timeout=10)


def send_error(agent_name: str, error: str):
    """Notifica errores del sistema."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    text = f"⚠️ *ERROR — {agent_name}*\n`{error[:300]}`"
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
        timeout=10
    )


def _esc(text: str) -> str:
    """Escapa caracteres especiales de Markdown de Telegram."""
    for ch in ["_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", "."]:
        text = text.replace(ch, f"\\{ch}")
    return text
