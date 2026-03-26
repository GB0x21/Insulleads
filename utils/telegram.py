"""
utils/telegram.py — Formatea y envía mensajes a Telegram
"""

import os
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _token() -> str:
    t = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not t:
        raise EnvironmentError("TELEGRAM_BOT_TOKEN no configurado en .env")
    return t


def _chat_id() -> str:
    c = os.getenv("TELEGRAM_CHAT_ID", "")
    if not c:
        raise EnvironmentError("TELEGRAM_CHAT_ID no configurado en .env")
    return c


def send_message(text: str) -> bool:
    """Envía un mensaje de texto plano/markdown a Telegram."""
    try:
        resp = requests.post(
            TELEGRAM_API.format(token=_token()),
            json={
                "chat_id":    _chat_id(),
                "text":       text,
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        return False


def send_lead(
    agent_name: str,
    emoji:      str,
    title:      str,
    fields:     dict,
    url:        str  = None,
    cta:        str  = None,
) -> bool:
    """
    Formatea y envía un lead estructurado a Telegram.

    Genera un mensaje con este formato:
    ─────────────────────────
    🏗️ PERMISOS DE CONSTRUCCIÓN
    ━━━━━━━━━━━━━━━━━━━━
    📌 San Francisco — 1420 Market St

    ▸ Campo 1: valor
    ▸ Campo 2: valor
    ...

    💡 Call to action

    🕐 26/03/2026 09:45
    ─────────────────────────
    """
    lines = []

    # Header
    agent_label = agent_name.upper().replace(emoji, "").strip()
    lines.append(f"{emoji} *{agent_label}*")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📌 *{_escape(title)}*")
    lines.append("")

    # Campos
    for label, value in fields.items():
        if value and str(value).strip() and str(value).strip() not in ("—", "-", ""):
            lines.append(f"▸ *{label}:* {_escape(str(value))}")

    # URL del permiso
    if url:
        lines.append(f"▸ *🔗 Ver detalle:* {url}")

    # Call to action
    if cta:
        lines.append("")
        lines.append(f"💡 _{_escape(cta)}_")

    # Timestamp
    lines.append("")
    lines.append(f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    text = "\n".join(lines)
    return send_message(text)


def _escape(text: str) -> str:
    """Escapa caracteres especiales de Markdown para Telegram."""
    # Solo escapar los que causan problemas en modo Markdown básico
    for ch in ["_", "*", "`", "["]:
        text = text.replace(ch, f"\\{ch}")
    return text
