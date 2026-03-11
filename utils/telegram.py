"""
utils/telegram.py  v3.0
Envía mensajes limpios a Telegram usando HTML (no Markdown).
HTML evita los caracteres de escape que aparecían como  \. \# etc.
"""
import os
import requests
from datetime import datetime


def send_lead(agent_name: str, emoji: str, title: str,
              fields: dict, cta: str = "") -> dict:
    """
    Construye y envía un mensaje de lead a Telegram.
    Usa parse_mode=HTML para evitar problemas de escape.
    Campos con valor None o vacío se omiten automáticamente.
    """
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        raise EnvironmentError(
            "Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en .env"
        )

    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    lines = [
        f"<b>{emoji} {agent_name.upper()}</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"<b>📌 {_h(title)}</b>",
        "",
    ]

    for key, value in fields.items():
        if value is None:
            continue
        val_str = str(value).strip()
        if not val_str or val_str in ("—", "N/A", "None"):
            continue
        # Links como texto clicable
        if val_str.startswith("http"):
            lines.append(f"▸ <b>{_h(key)}:</b> <a href='{val_str}'>{_h(key)}</a>")
        else:
            lines.append(f"▸ <b>{_h(key)}:</b> {_h(val_str)}")

    if cta:
        lines += ["", f"🎯 <i>{_h(cta)}</i>"]

    lines += ["", f"<i>🕐 {now}</i>"]

    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }

    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def send_summary(stats: list) -> None:
    """Envía resumen diario de actividad."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return

    lines = [
        "📊 <b>RESUMEN DIARIO — INSUL-TECHS LEAD AGENTS</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    total = 0
    for row in stats:
        lines.append(
            f"▸ {_h(str(row['agent']))}: <b>{row['total']}</b> leads totales"
        )
        total += row["total"]

    lines += [
        "",
        f"🏆 <b>Total acumulado: {total} leads</b>",
        f"<i>🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>",
    ]

    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id":    chat_id,
            "text":       "\n".join(lines),
            "parse_mode": "HTML",
        },
        timeout=10,
    )


def send_error(agent_name: str, error: str) -> None:
    """Notifica errores del sistema."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    text = f"⚠️ <b>ERROR — {_h(agent_name)}</b>\n<code>{_h(error[:300])}</code>"
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=10,
    )


def _h(text: str) -> str:
    """Escapa caracteres HTML especiales."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
