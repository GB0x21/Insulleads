"""
outreach task — pull QUALIFIED leads and push messages through the
existing Insulleads notification stack (Telegram / SendGrid / Twilio).

Mirrors OpenOutreach's connect+follow_up phase, but the "channel" is
not LinkedIn: we use the legacy `utils.telegram` + `utils.notifications`
modules so all the user's existing wiring keeps working.
"""
from __future__ import annotations

import logging

from django.conf import settings
from django.utils import timezone

from outreach.models import ActionLog, Campaign, Lead, Task

logger = logging.getLogger("outreach.tasks.outreach")


def _sent_today(campaign: Campaign) -> int:
    since = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return ActionLog.objects.filter(
        campaign=campaign,
        action__in=[
            ActionLog.Action.TELEGRAM,
            ActionLog.Action.EMAIL,
            ActionLog.Action.WHATSAPP,
        ],
        created_at__gte=since,
    ).count()


def _format_message(lead: Lead) -> str:
    parts = [
        f"🏗️ {lead.title}",
        f"📍 {lead.address or '—'}, {lead.city or '—'}",
    ]
    if lead.project_value:
        parts.append(f"💵 Value: ${lead.project_value:,.0f}")
    if lead.project_type:
        parts.append(f"🔧 Type: {lead.project_type}")
    if lead.contact_company or lead.contact_name:
        parts.append(
            f"👤 GC: {lead.contact_company or lead.contact_name}"
        )
    if lead.contact_phone:
        parts.append(f"📞 {lead.contact_phone}")
    if lead.contact_email:
        parts.append(f"✉️ {lead.contact_email}")
    if lead.qualification_score is not None:
        parts.append(
            f"🧠 Qualifier: {lead.qualification_score:.2f} "
            f"(σ²={lead.qualification_variance or 0:.2f})"
        )
    parts.append(f"🔥 Lead score: {lead.lead_score}/100")
    return "\n".join(parts)


def _send(lead: Lead) -> str:
    """Send through the existing Insulleads notification stack.
    Returns the action name that was taken."""
    from utils.telegram import send_message

    body = _format_message(lead)
    ok = send_message(body)
    if not ok:
        raise RuntimeError("telegram send failed")
    return ActionLog.Action.TELEGRAM


def handle(task: Task) -> None:
    campaign = task.campaign
    budget = campaign.max_outreach_per_day - _sent_today(campaign)
    if budget <= 0:
        logger.info("[outreach] daily budget exhausted for %s", campaign.name)
        task.reschedule(minutes=60)
        return

    # Highest qualifier score first, fall back to heuristic lead_score.
    batch = list(
        Lead.objects.filter(
            campaign=campaign,
            stage=Lead.Stage.QUALIFIED,
        )
        .order_by("-qualification_score", "-lead_score")[:budget]
    )
    if not batch:
        task.reschedule(minutes=settings.OUTREACH["OUTREACH_INTERVAL_MIN"])
        return

    sent = 0
    for lead in batch:
        try:
            action = _send(lead)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[outreach] failed for lead=%s: %s", lead.pk, exc)
            continue

        ActionLog.objects.create(
            lead=lead,
            campaign=campaign,
            action=action,
            payload={"qualifier": lead.qualification_score},
        )
        lead.advance(Lead.Stage.CONTACTED)
        sent += 1

    logger.info("[outreach] campaign=%s sent=%d", campaign.name, sent)
    task.reschedule(minutes=settings.OUTREACH["OUTREACH_INTERVAL_MIN"])


def handle_follow_up(task: Task) -> None:
    """Placeholder for the multi-turn follow-up loop.

    OpenOutreach runs a ReAct agent here; Insulleads' channel is one-way
    broadcast (Telegram/SendGrid), so we just mark old CONTACTED leads
    as LOST if no reply was registered after 14 days.
    """
    from datetime import timedelta

    cutoff = timezone.now() - timedelta(days=14)
    stale = Lead.objects.filter(
        campaign=task.campaign,
        stage=Lead.Stage.CONTACTED,
        stage_changed_at__lt=cutoff,
    )
    count = stale.update(stage=Lead.Stage.LOST, stage_changed_at=timezone.now())
    logger.info("[follow_up] archived %d stale leads", count)
    task.reschedule(minutes=60 * 12)
