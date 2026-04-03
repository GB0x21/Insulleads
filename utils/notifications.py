"""
utils/notifications.py
━━━━━━━━━━━━━━━━━━━━━━
Sistema multi-canal de notificaciones.

Canales soportados:
  1. Telegram     — (existente, en utils/telegram.py)
  2. SendGrid     — Email outreach ($15/mes, 100 emails/día gratis)
  3. Twilio WhatsApp — Mensajes directos ($50/mes)
  4. Slack Webhook — Para equipos internos (gratis)

Lógica de routing:
  - Score >= 90 (HOT):    Telegram + WhatsApp + Email
  - Score >= 70 (WARM):   Telegram + Email
  - Score >= 50 (MEDIUM): Telegram
  - Score < 50:           Digest diario por email
"""

import os
import json
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Configuración ────────────────────────────────────────────────────
SENDGRID_API_KEY     = os.getenv("SENDGRID_API_KEY", "")
SENDGRID_FROM_EMAIL  = os.getenv("SENDGRID_FROM_EMAIL", "leads@example.com")
SENDGRID_TO_EMAIL    = os.getenv("SENDGRID_TO_EMAIL", "")

TWILIO_ACCOUNT_SID   = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN     = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")  # whatsapp:+14155238886
TWILIO_WHATSAPP_TO   = os.getenv("TWILIO_WHATSAPP_TO", "")    # whatsapp:+1XXXXXXXXXX

SLACK_WEBHOOK_URL    = os.getenv("SLACK_WEBHOOK_URL", "")

# Buffer para digest diario
_email_digest_buffer: list = []


# ── SendGrid Email ───────────────────────────────────────────────────

def send_email(subject: str, html_body: str, to_email: str = None) -> bool:
    """Envía email vía SendGrid API v3."""
    if not SENDGRID_API_KEY:
        logger.debug("[SendGrid] No configurado — omitido")
        return False

    to = to_email or SENDGRID_TO_EMAIL
    if not to:
        return False

    try:
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": to}]}],
                "from": {"email": SENDGRID_FROM_EMAIL, "name": "Lead Generation Alerts"},
                "subject": subject,
                "content": [{"type": "text/html", "value": html_body}],
            },
            timeout=15,
        )
        if resp.status_code in (200, 201, 202):
            logger.info(f"[SendGrid] Email enviado a {to}: {subject}")
            return True
        else:
            logger.warning(f"[SendGrid] Error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"[SendGrid] Error: {e}")
        return False


def send_lead_email(lead: dict, scoring: dict = None) -> bool:
    """Envía un lead individual por email con formato HTML."""
    city = lead.get("city", "")
    address = lead.get("address", "")
    score_info = scoring or {}

    subject = f"[{score_info.get('grade', 'NEW')}] Lead: {city} — {address}"

    # Template HTML del lead
    fields_html = ""
    for key in ["city", "address", "description", "contractor", "owner",
                "contact_phone", "contact_email", "value"]:
        val = lead.get(key, "")
        if val:
            label = key.replace("_", " ").title()
            fields_html += f"<tr><td style='padding:4px 8px;font-weight:bold'>{label}</td><td style='padding:4px 8px'>{val}</td></tr>"

    score_html = ""
    if score_info:
        reasons = "<br>".join(score_info.get("reasons", []))
        score_html = f"""
        <div style='background:#f0f0f0;padding:10px;border-radius:5px;margin:10px 0'>
            <strong>Score:</strong> {score_info.get('grade_emoji','')} {score_info.get('score',0)}/100
            ({score_info.get('grade','')})
            <br><small>{reasons}</small>
        </div>"""

    html_body = f"""
    <div style='font-family:Arial,sans-serif;max-width:600px;margin:0 auto'>
        <div style='background:#1a73e8;color:white;padding:15px;border-radius:5px 5px 0 0'>
            <h2 style='margin:0'>Lead Generation Agents — Nuevo Lead</h2>
        </div>
        <div style='border:1px solid #ddd;padding:15px;border-radius:0 0 5px 5px'>
            {score_html}
            <table style='width:100%;border-collapse:collapse'>
                {fields_html}
            </table>
            <div style='margin-top:15px;padding:10px;background:#e8f5e9;border-radius:5px'>
                <strong>Accion:</strong> Contacta al contratista y ofrece servicios de insulacion.
            </div>
            <p style='color:#888;font-size:12px;margin-top:15px'>
                Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}
            </p>
        </div>
    </div>
    """
    return send_email(subject, html_body)


# ── Twilio WhatsApp ──────────────────────────────────────────────────

def send_whatsapp(message: str) -> bool:
    """Envía mensaje vía Twilio WhatsApp Business API."""
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN,
                TWILIO_WHATSAPP_FROM, TWILIO_WHATSAPP_TO]):
        logger.debug("[WhatsApp] No configurado — omitido")
        return False

    try:
        resp = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            data={
                "From": TWILIO_WHATSAPP_FROM,
                "To": TWILIO_WHATSAPP_TO,
                "Body": message[:1600],  # Límite WhatsApp
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            logger.info("[WhatsApp] Mensaje enviado")
            return True
        else:
            logger.warning(f"[WhatsApp] Error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        logger.error(f"[WhatsApp] Error: {e}")
        return False


def send_lead_whatsapp(lead: dict, scoring: dict = None) -> bool:
    """Envía un lead como mensaje WhatsApp formateado."""
    score_info = scoring or {}
    grade = score_info.get("grade_emoji", "")
    score = score_info.get("score", 0)

    lines = [
        f"{grade} *NUEVO LEAD — Score {score}/100*",
        f"Ciudad: {lead.get('city', '')}",
        f"Direccion: {lead.get('address', '')}",
    ]
    if lead.get("description"):
        lines.append(f"Proyecto: {lead['description'][:100]}")
    if lead.get("contractor"):
        lines.append(f"Contratista: {lead['contractor']}")
    if lead.get("contact_phone"):
        lines.append(f"Tel: {lead['contact_phone']}")
    if lead.get("value_float"):
        lines.append(f"Valor: ${lead['value_float']:,.0f}")

    return send_whatsapp("\n".join(lines))


# ── Slack Webhook ────────────────────────────────────────────────────

def send_slack(message: str) -> bool:
    """Envía mensaje a Slack vía webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("[Slack] No configurado — omitido")
        return False

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"[Slack] Error: {e}")
        return False


# ── Router multi-canal ───────────────────────────────────────────────

def notify_multichannel(lead: dict, scoring: dict = None):
    """
    Envía el lead por múltiples canales según su score.
    Telegram se maneja aparte (en el agente). Esto agrega canales extra.

    Score >= 90 (HOT):    WhatsApp + Email
    Score >= 70 (WARM):   Email
    Score < 70:           Se acumula en digest
    """
    score_info = scoring or {}
    score = score_info.get("score", 0)

    if score >= 90:
        send_lead_whatsapp(lead, scoring)
        send_lead_email(lead, scoring)
    elif score >= 70:
        send_lead_email(lead, scoring)
    else:
        # Acumular para digest
        _email_digest_buffer.append({"lead": lead, "scoring": scoring})


def flush_digest() -> bool:
    """
    Envía el digest acumulado de leads de bajo score por email.
    Llamar al final de cada ciclo.
    """
    if not _email_digest_buffer:
        return True

    count = len(_email_digest_buffer)
    rows_html = ""
    for item in _email_digest_buffer[:50]:  # Máx 50 leads por digest
        lead = item["lead"]
        scoring = item.get("scoring") or {}
        rows_html += f"""
        <tr>
            <td style='padding:4px;border-bottom:1px solid #eee'>
                {scoring.get('grade_emoji','')} {scoring.get('score',0)}
            </td>
            <td style='padding:4px;border-bottom:1px solid #eee'>
                {lead.get('city','')}
            </td>
            <td style='padding:4px;border-bottom:1px solid #eee'>
                {lead.get('address','')[:40]}
            </td>
            <td style='padding:4px;border-bottom:1px solid #eee'>
                {(lead.get('description') or '')[:50]}
            </td>
            <td style='padding:4px;border-bottom:1px solid #eee'>
                ${lead.get('value_float',0):,.0f}
            </td>
        </tr>"""

    html = f"""
    <div style='font-family:Arial,sans-serif'>
        <h2>Lead Generation Agents — Digest de {count} leads</h2>
        <p>{datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        <table style='width:100%;border-collapse:collapse'>
            <tr style='background:#1a73e8;color:white'>
                <th style='padding:8px'>Score</th>
                <th style='padding:8px'>Ciudad</th>
                <th style='padding:8px'>Direccion</th>
                <th style='padding:8px'>Proyecto</th>
                <th style='padding:8px'>Valor</th>
            </tr>
            {rows_html}
        </table>
    </div>
    """

    result = send_email(
        f"[Digest] {count} leads nuevos — {datetime.now().strftime('%d/%m/%Y')}",
        html,
    )
    if result:
        _email_digest_buffer.clear()
    return result
