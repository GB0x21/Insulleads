"""
outreach/tasks/email_prospect_outreach.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Email prospect outreach task.

Picks QUALIFIED leads that have a contact_email set but have not yet been
emailed directly (email_prospect_sent_at is null), generates a personalized
subject + body via the LLM adapter, and sends through SendGrid.

This runs alongside the existing Telegram outreach task and respects its own
daily budget (MAX_EMAIL_PROSPECT_PER_DAY, default 50).

Flow:
  QUALIFIED lead with contact_email
      → LLM write_email()  →  SendGrid send
      → Lead.email_prospect_sent_at = now
      → ActionLog(EMAIL_PROSPECT)
"""
from __future__ import annotations

import logging
import textwrap
from datetime import datetime

from django.conf import settings
from django.utils import timezone

from outreach.llm import get_adapter
from outreach.models import ActionLog, Campaign, Lead, Task

logger = logging.getLogger("outreach.tasks.email_prospect")

BATCH_SIZE = 20


def _sent_today(campaign: Campaign) -> int:
    since = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return ActionLog.objects.filter(
        campaign=campaign,
        action=ActionLog.Action.EMAIL_PROSPECT,
        created_at__gte=since,
    ).count()


def _plain_to_html(plain: str, contact_name: str = "") -> str:
    """Wrap a plain-text email body in a minimal, safe HTML envelope."""
    greeting = f"Hi {contact_name.split()[0]}," if contact_name else ""
    paragraphs = "\n".join(
        f"<p style='margin:0 0 12px 0'>{line}</p>"
        for line in plain.split("\n")
        if line.strip()
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Georgia,serif;font-size:15px;line-height:1.6;color:#222;
             max-width:620px;margin:40px auto;padding:0 20px">
  {"<p style='margin:0 0 12px 0'>" + greeting + "</p>" if greeting else ""}
  {paragraphs}
  <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
  <p style="font-size:11px;color:#aaa">
    You're receiving this because your company appeared in a Bay Area
    construction permit or project record. To stop receiving emails from us,
    reply with "unsubscribe".
  </p>
</body>
</html>"""


def _fallback_email(lead: Lead, campaign: Campaign) -> dict[str, str]:
    """Template-based email when the LLM is unavailable."""
    city = lead.city or "the Bay Area"
    company = lead.contact_company or "your company"
    project = lead.project_type or "your upcoming project"
    subject = f"Insulation for {company}'s {city} project"[:55]
    body = textwrap.dedent(f"""\
        I came across {company} in connection with {project} in {city} and \
        wanted to reach out about insulation services.

        We're a Bay Area spray-foam and batt insulation contractor specializing \
        in helping general contractors meet Title 24 compliance, maintain \
        schedule, and eliminate callbacks.

        Would you be open to a quick 15-minute call to see if we might be a \
        fit for this job?

        — Insulleads
    """)
    return {"subject": subject, "body": body}


def handle(task: Task) -> None:
    campaign = task.campaign
    max_per_day = settings.OUTREACH.get("MAX_EMAIL_PROSPECT_PER_DAY", 50)
    interval = settings.OUTREACH.get("EMAIL_PROSPECT_INTERVAL_MIN", 30)

    budget = max_per_day - _sent_today(campaign)
    if budget <= 0:
        logger.info("[email_prospect] daily budget exhausted for %s", campaign.name)
        task.reschedule(minutes=60)
        return

    # Pick QUALIFIED leads with an email that haven't been contacted yet.
    batch = list(
        Lead.objects.filter(
            campaign=campaign,
            stage=Lead.Stage.QUALIFIED,
            email_prospect_sent_at__isnull=True,
        )
        .exclude(contact_email="")
        .order_by("-qualification_score", "-lead_score")[: min(budget, BATCH_SIZE)]
    )

    if not batch:
        logger.debug("[email_prospect] no eligible leads for %s", campaign.name)
        task.reschedule(minutes=interval)
        return

    adapter = get_adapter()
    sent = 0

    for lead in batch:
        email_data = None

        # Try LLM-generated copy first
        if campaign.llm_writer_enabled and adapter.name != "noop":
            try:
                email_data = adapter.write_email(lead, campaign)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[email_prospect] LLM write_email failed for lead=%s: %s",
                    lead.pk, exc,
                )

        if not email_data:
            email_data = _fallback_email(lead, campaign)

        subject = email_data["subject"]
        body_text = email_data["body"]
        body_html = _plain_to_html(body_text, lead.contact_name)

        ok = _send_via_sendgrid(
            to_email=lead.contact_email,
            to_name=lead.contact_name or lead.contact_company,
            from_name=_sender_name(campaign),
            subject=subject,
            body_html=body_html,
            body_text=body_text,
        )

        if not ok:
            logger.warning(
                "[email_prospect] SendGrid send failed for lead=%s email=%s",
                lead.pk, lead.contact_email,
            )
            continue

        # Persist tracking fields
        now = timezone.now()
        lead.email_prospect_sent_at = now
        lead.email_prospect_subject = subject[:255]
        lead.save(update_fields=["email_prospect_sent_at", "email_prospect_subject", "updated_at"])

        ActionLog.objects.create(
            lead=lead,
            campaign=campaign,
            action=ActionLog.Action.EMAIL_PROSPECT,
            payload={
                "to": lead.contact_email,
                "subject": subject,
                "qualifier_score": lead.qualification_score,
                "adapter": adapter.name,
            },
        )
        sent += 1
        logger.info(
            "[email_prospect] sent to %s (%s) — subject: %s",
            lead.contact_email,
            lead.contact_company or lead.contact_name,
            subject,
        )

    logger.info("[email_prospect] campaign=%s sent=%d", campaign.name, sent)
    task.reschedule(minutes=interval)


def _sender_name(campaign: Campaign) -> str:
    return "Insulleads"


def _send_via_sendgrid(
    to_email: str,
    to_name: str,
    from_name: str,
    subject: str,
    body_html: str,
    body_text: str,
) -> bool:
    """Send through SendGrid API v3. Returns True on success."""
    import os

    import requests

    api_key = os.getenv("SENDGRID_API_KEY", "")
    from_email = os.getenv("SENDGRID_FROM_EMAIL", "")

    if not api_key or not from_email:
        logger.debug("[email_prospect] SendGrid not configured — skipped")
        return False

    payload = {
        "personalizations": [
            {
                "to": [{"email": to_email, "name": to_name or to_email}],
            }
        ],
        "from": {"email": from_email, "name": from_name},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": body_text},
            {"type": "text/html", "value": body_html},
        ],
        "tracking_settings": {
            "click_tracking": {"enable": True},
            "open_tracking": {"enable": True},
        },
    }

    try:
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 201, 202):
            return True
        logger.warning(
            "[email_prospect] SendGrid %s: %s", resp.status_code, resp.text[:200]
        )
        return False
    except Exception as exc:  # noqa: BLE001
        logger.error("[email_prospect] SendGrid error: %s", exc)
        return False
