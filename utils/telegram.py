"""
utils/telegram.py  v5
━━━━━━━━━━━━━━━━━━━
Rate limiter + retry 429 + digest mejorado.

FIXES v5:
  ✅ Digest rediseñado: cada lead en bloque separado, claro y escaneable
  ✅ Mantiene rate limiter y retry automático de v4
"""

import os
import time
import logging
import threading
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_API        = "https://api.telegram.org/bot{token}/sendMessage"
_MAX_MSG_PER_MINUTE = int(os.getenv("TELEGRAM_MAX_MSG_MIN", "20"))
_MIN_INTERVAL       = 60.0 / _MAX_MSG_PER_MINUTE
MAX_BURST           = int(os.getenv("TELEGRAM_MAX_BURST", "10"))

_rate_lock      = threading.Lock()
_last_send_time = 0.0


def _token() -> str:
    t = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not t:
        raise EnvironmentError("TELEGRAM_BOT_TOKEN no configurado")
    return t


def _chat_id() -> str:
    c = os.getenv("TELEGRAM_CHAT_ID", "")
    if not c:
        raise EnvironmentError("TELEGRAM_CHAT_ID no configurado")
    return c


def _wait_for_slot():
    global _last_send_time
    with _rate_lock:
        elapsed = time.monotonic() - _last_send_time
        wait    = _MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        _last_send_time = time.monotonic()


def send_message(text: str, max_retries: int = 4) -> bool:
    _wait_for_slot()
    url = TELEGRAM_API.format(token=_token())
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                url,
                json={
                    "chat_id":                  _chat_id(),
                    "text":                     text[:4096],
                    "parse_mode":               "Markdown",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 30)
                logger.warning(f"Telegram 429 — esperando {retry_after}s")
                time.sleep(retry_after + 2)
                _wait_for_slot()
                continue
            resp.raise_for_status()
            return True
        except requests.exceptions.HTTPError as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"Telegram send error: {e}")
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False
    return False


def send_lead(agent_name, emoji, title, fields, url=None, cta=None) -> bool:
    lines = []
    label = agent_name.upper().replace(emoji, "").strip()
    lines.append(f"{emoji} *{label}*")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📌 *{_esc(title)}*")
    lines.append("")
    for lbl, val in fields.items():
        if val and str(val).strip() not in ("—", "-", ""):
            lines.append(f"▸ *{lbl}:* {_esc(str(val))}")
    if url:
        lines.append(f"▸ *🔗 Ver permiso:* {url}")
    if cta:
        lines.append(f"\n💡 _{_esc(cta)}_")
    lines.append(f"\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    return send_message("\n".join(lines))


def send_digest(agent_name: str, emoji: str, leads: list) -> bool:
    """
    Digest v5 — bloques separados, fáciles de escanear.
    Si hay muchos leads se parte en páginas de 20.
    """
    total      = len(leads)
    page_size  = 20
    pages      = [leads[i:i+page_size] for i in range(0, min(total, 100), page_size)]
    label      = agent_name.upper().replace(emoji, "").strip()
    timestamp  = datetime.now().strftime("%d/%m/%Y %H:%M")
    ok         = True

    for p_idx, page in enumerate(pages):
        page_label = f"página {p_idx+1}/{len(pages)}" if len(pages) > 1 else ""
        lines = [
            f"{emoji} *{label}*",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"📦 *{total} leads nuevos* {page_label}  •  🕐 {timestamp}",
            "",
        ]

        for i, lead in enumerate(page, start=p_idx * page_size + 1):
            city       = _esc(lead.get("city", ""))
            addr       = _esc(lead.get("address", "—"))
            contractor = lead.get("contractor") or lead.get("contact_name") or ""
            phone      = lead.get("contact_phone") or ""
            email      = lead.get("contact_email") or ""
            permit_type= lead.get("permit_type") or ""
            issued     = lead.get("issued_date") or ""
            value      = lead.get("value_float") or 0
            url        = lead.get("permit_url") or ""

            # Encabezado del lead
            lines.append(f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
            lines.append(f"*#{i} — {city}*")
            lines.append(f"📍 {addr}")

            # Tipo y fecha
            if permit_type:
                lines.append(f"🔖 {_esc(permit_type)}")
            if issued:
                lines.append(f"📅 Emitido: {issued}")

            # Valor
            if value and value > 0:
                lines.append(f"💰 *${value:,.0f}*")

            # Contacto GC
            if contractor:
                lines.append(f"👷 {_esc(contractor)}")
            if phone:
                lines.append(f"📞 {_esc(phone)}")
            if email:
                lines.append(f"✉️  {_esc(email)}")

            # Enlace
            if url and "http" in url:
                lines.append(f"🔗 {url}")

            lines.append("")   # espacio entre leads

        ok = send_message("\n".join(lines)) and ok

    return ok


def _esc(text: str) -> str:
    for ch in ["_", "*", "`", "["]:
        text = str(text).replace(ch, f"\\{ch}")
    return text
