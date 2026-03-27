"""
utils/telegram.py  v4
━━━━━━━━━━━━━━━━━━━
Formatea y envía mensajes a Telegram.

⚡ FIXES v4:
  - Rate limiter global: máx 20 msg/min (Telegram permite ~30, usamos 20 con margen)
  - Retry automático con backoff en 429 Too Many Requests
  - Respeta el retry_after que devuelve Telegram en el header
  - Modo DIGEST: si hay >MAX_BURST leads nuevos de golpe, los agrupa
    en un solo mensaje resumen en lugar de mandar cientos de mensajes
"""

import os
import time
import logging
import threading
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# ── Rate limiter ───────────────────────────────────────────────────
# Telegram: máx ~30 msg/min en grupos. Usamos 20 para tener margen.
_MAX_MSG_PER_MINUTE = int(os.getenv("TELEGRAM_MAX_MSG_MIN", "20"))
_MIN_INTERVAL       = 60.0 / _MAX_MSG_PER_MINUTE   # segundos entre mensajes

# Modo digest: si hay más de N leads nuevos en un ciclo, se agrupan
MAX_BURST = int(os.getenv("TELEGRAM_MAX_BURST", "15"))

_rate_lock      = threading.Lock()
_last_send_time = 0.0   # timestamp del último envío exitoso


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


def _wait_for_slot():
    """
    Bloquea el thread actual hasta que pueda enviar sin exceder el rate limit.
    Thread-safe via lock.
    """
    global _last_send_time
    with _rate_lock:
        now     = time.monotonic()
        elapsed = now - _last_send_time
        wait    = _MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        _last_send_time = time.monotonic()


def send_message(text: str, max_retries: int = 3) -> bool:
    """
    Envía un mensaje respetando el rate limit.
    Reintenta automáticamente en 429 con el retry_after de Telegram.
    """
    _wait_for_slot()

    url = TELEGRAM_API.format(token=_token())

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                url,
                json={
                    "chat_id":                  _chat_id(),
                    "text":                     text[:4096],   # límite Telegram
                    "parse_mode":               "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )

            if resp.status_code == 429:
                # Telegram nos dice cuánto esperar
                retry_after = resp.json().get("parameters", {}).get("retry_after", 30)
                logger.warning(f"Telegram 429 — esperando {retry_after}s (intento {attempt+1})")
                time.sleep(retry_after + 1)
                _wait_for_slot()
                continue

            resp.raise_for_status()
            return True

        except requests.exceptions.HTTPError as e:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                logger.warning(f"Telegram HTTP error, reintentando en {wait}s: {e}")
                time.sleep(wait)
            else:
                logger.error(f"Telegram send error: {e}")

        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

    return False


def send_lead(
    agent_name: str,
    emoji:      str,
    title:      str,
    fields:     dict,
    url:        str  = None,
    cta:        str  = None,
) -> bool:
    """Formatea un lead estructurado y lo envía."""
    lines = []
    agent_label = agent_name.upper().replace(emoji, "").strip()
    lines.append(f"{emoji} *{agent_label}*")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📌 *{_escape(title)}*")
    lines.append("")

    for label, value in fields.items():
        if value and str(value).strip() not in ("—", "-", ""):
            lines.append(f"▸ *{label}:* {_escape(str(value))}")

    if url:
        lines.append(f"▸ *🔗 Ver detalle:* {url}")
    if cta:
        lines.append("")
        lines.append(f"💡 _{_escape(cta)}_")

    lines.append("")
    lines.append(f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    return send_message("\n".join(lines))


def send_digest(agent_name: str, emoji: str, leads: list) -> bool:
    """
    Envía un único mensaje resumen cuando hay muchos leads de golpe.
    Se usa cuando len(leads) > MAX_BURST para evitar el 429.
    """
    lines = [
        f"{emoji} *{agent_name.upper().replace(emoji,'').strip()}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📦 *{len(leads)} leads nuevos encontrados*",
        "",
    ]

    for i, lead in enumerate(leads[:50], 1):   # máx 50 en el digest
        addr       = lead.get("address") or lead.get("city") or "—"
        city       = lead.get("city", "")
        contractor = lead.get("contractor") or lead.get("contact_name") or "—"
        phone      = lead.get("contact_phone") or "—"
        value      = f"${float(lead['value']):,.0f}" if lead.get("value") else ""

        line = f"*{i}.* {_escape(city)} — {_escape(addr)}"
        if contractor and contractor != "—":
            line += f"\n   👷 {_escape(contractor)}  📞 {_escape(phone)}"
        if value:
            line += f"  💰 {value}"
        lines.append(line)

    if len(leads) > 50:
        lines.append(f"\n_... y {len(leads)-50} más._")

    lines.append(f"\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    return send_message("\n".join(lines))


def _escape(text: str) -> str:
    for ch in ["_", "*", "`", "["]:
        text = text.replace(ch, f"\\{ch}")
    return text
