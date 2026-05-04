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

from outreach.lead_quality import contact_quality_label, contact_quality_score
from outreach.llm import get_adapter
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


# Keys we look at, in order, when extracting a "source URL" from the
# lead's raw payload. Different agents store this under different names.
_RAW_URL_KEYS = (
    "source_url",
    "url",
    "permit_url",
    "permit_link",
    "yelp_url",
    "places_url",
    "listing_url",
    "detail_url",
    "website",
)


def _extract_source_url(lead: Lead) -> str:
    raw = lead.raw or {}
    for key in _RAW_URL_KEYS:
        val = raw.get(key)
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            return val
    return ""


def _format_context_block(lead: Lead) -> str:
    """Compact, single-message footer with the actionable info the LLM
    prose intentionally leaves out: contact data, scores, source URL, id.

    Designed to be appended after the LLM-written outreach copy with a
    visual separator so the operator can copy/paste the prose and still
    see who to call without leaving Telegram."""
    where_bits: list[str] = []
    if lead.address:
        where_bits.append(lead.address)
    if lead.city:
        where_bits.append(lead.city)
    where = ", ".join(where_bits) or "—"

    proj_bits: list[str] = []
    if lead.project_type:
        proj_bits.append(lead.project_type)
    if lead.project_value:
        proj_bits.append(f"${lead.project_value:,.0f}")
    proj = " · ".join(proj_bits)

    is_places = (lead.source.kind == "places")
    addr_emoji = "🏢" if is_places else "🏗️"
    lines: list[str] = ["──────────", f"{addr_emoji} {where}" + (f" · {proj}" if proj else "")]

    # Contact: company / name plus phone / email on the next line.
    company = lead.contact_company or lead.contact_name
    if company:
        if lead.contact_company and lead.contact_name:
            lines.append(f"👤 {lead.contact_company} — {lead.contact_name}")
        else:
            lines.append(f"👤 {company}")

    contact_bits: list[str] = []
    if lead.contact_phone:
        contact_bits.append(f"📞 {lead.contact_phone}")
    if lead.contact_email:
        contact_bits.append(f"✉️ {lead.contact_email}")
    if contact_bits:
        lines.append("  ".join(contact_bits))
    elif not company:
        lines.append("👤 (no contact data — run enrichment)")

    if lead.contact_source:
        lines.append(f"🔎 Contact via: {lead.contact_source}")

    # Additional contacts from enrichment (up to 4 more GC team members)
    extra_contacts = (lead.enrichment_log or {}).get("additional_contacts", [])
    if extra_contacts:
        lines.append("👥 Contactos adicionales:")
        for c in extra_contacts[:4]:
            name = c.get("name") or ""
            pos = c.get("position") or ""
            label = f"{name} ({pos})" if pos else name
            bits = []
            if c.get("phone"):
                bits.append(f"📞 {c['phone']}")
            if c.get("email"):
                bits.append(f"✉️ {c['email']}")
            if c.get("linkedin_url"):
                bits.append(f"🔗 {c['linkedin_url']}")
            contact_line = "  ".join(bits)
            if label and contact_line:
                lines.append(f"  • {label} — {contact_line}")
            elif label:
                lines.append(f"  • {label}")
            elif contact_line:
                lines.append(f"  • {contact_line}")

    # Scores
    score_bits: list[str] = []
    if lead.qualification_score is not None:
        score_bits.append(
            f"🧠 {lead.qualification_score:.2f}"
            f" (σ²={lead.qualification_variance or 0:.2f})"
        )
    score_bits.append(f"🔥 {lead.lead_score}/100")
    score_bits.append(
        f"📊 reach {contact_quality_label(contact_quality_score(lead))}"
    )
    score_bits.append(f"📡 {lead.source.kind}")
    lines.append(" · ".join(score_bits))

    if lead.llm_qualification_reason:
        reason = lead.llm_qualification_reason.strip()
        if len(reason) > 200:
            reason = reason[:200] + "…"
        lines.append(f"💬 {reason}")

    src_url = _extract_source_url(lead)
    if src_url:
        # Telegram trims very long URLs visually; keep the full one for
        # click-through but truncate the display side defensively.
        display = src_url if len(src_url) <= 120 else src_url[:117] + "…"
        lines.append(f"↪︎ {display}")

    lines.append(f"#L{lead.pk}")
    return "\n".join(lines)



def _resolve_body(lead: Lead, campaign: Campaign) -> str:
    """Pick an outreach body: cached LLM copy → fresh LLM copy → template."""
    if lead.llm_outreach_body:
        return lead.llm_outreach_body

    if campaign.llm_writer_enabled:
        adapter = get_adapter()
        if adapter.name != "noop":
            try:
                body = adapter.write_outreach(lead, campaign)
            except Exception as exc:  # noqa: BLE001
                logger.warning("[outreach] LLM writer failed for lead=%s: %s",
                               lead.pk, exc)
                body = None
            if body:
                lead.llm_outreach_body = body
                lead.save(update_fields=["llm_outreach_body", "updated_at"])
                return body

    return _format_message(lead)


def _send(lead: Lead, campaign: Campaign) -> str:
    """Send through the existing Insulleads notification stack.
    Returns the action name that was taken."""
    from utils.telegram import send_message

    body = _resolve_body(lead, campaign)
    context = _format_context_block(lead)
    # The LLM-written prose is the pitch the operator might paste into
    # an email/SMS; the context block below the separator is the
    # actionable lead data (contact, scores, source URL, lead id) so
    # they don't have to leave Telegram to act on it.
    message = f"{body}\n\n{context}"
    ok = send_message(message)
    if not ok:
        raise RuntimeError("telegram send failed")
    return ActionLog.Action.TELEGRAM


def _has_contact(lead: Lead) -> bool:
    return bool(lead.contact_phone or lead.contact_email)


def handle(task: Task) -> None:
    campaign = task.campaign
    budget = campaign.max_outreach_per_day - _sent_today(campaign)
    if budget <= 0:
        logger.info("[outreach] daily budget exhausted for %s", campaign.name)
        task.reschedule(minutes=60)
        return

    # Pull a wider batch than the budget — we may skip some leads that
    # still have no contact data after a last-chance enrichment attempt.
    batch = list(
        Lead.objects.filter(
            campaign=campaign,
            stage=Lead.Stage.QUALIFIED,
        )
        .order_by("-qualification_score", "-lead_score")[: budget * 3]
    )
    if not batch:
        task.reschedule(minutes=settings.OUTREACH["OUTREACH_INTERVAL_MIN"])
        return

    # Re-sort so high-reachability leads get the budget first. Tie-break
    # falls back to the SQL order (qualification → lead_score), which is
    # already stable.
    batch.sort(
        key=lambda l: (
            -(l.qualification_score or 0.0),
            -contact_quality_score(l),
            -(l.lead_score or 0),
        )
    )

    inline_enrich = (
        settings.OUTREACH.get("ENRICH_INLINE_ON_OUTREACH", True)
        and campaign.llm_enricher_enabled
    )

    sent = 0
    skipped_no_contact = 0
    for lead in batch:
        if sent >= budget:
            break

        # Last-chance enrichment: don't waste outreach budget on a
        # message the operator can't act on. The enricher respects its
        # own cooldown so this is cheap when nothing's changed.
        if not _has_contact(lead) and inline_enrich:
            from outreach.tasks.enrich import enrich_one

            try:
                enrich_one(lead)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[outreach] inline enrich failed for lead=%s: %s",
                    lead.pk,
                    exc,
                )
            lead.refresh_from_db(fields=["contact_phone", "contact_email",
                                         "contact_name", "contact_company",
                                         "contact_source"])

        if not _has_contact(lead):
            skipped_no_contact += 1
            # Lead stays QUALIFIED — the next enrich tick (or a manual
            # `manage.py enrich_leads --lead-id …`) will retry until the
            # cooldown / max-attempts policy gives up.
            continue

        try:
            action = _send(lead, campaign)
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

    logger.info(
        "[outreach] campaign=%s sent=%d skipped_no_contact=%d",
        campaign.name,
        sent,
        skipped_no_contact,
    )
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
