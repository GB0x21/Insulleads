"""
enrich task — LLM contact enrichment.

Pulls DISCOVERED leads that are missing contact info and asks the
configured :class:`outreach.llm.LLMAdapter` to find phone / email / website
via web search. Merges the results back into the Lead row (non-destructive:
we never overwrite existing fields) and logs everything to
``Lead.enrichment_log``.

Cheap when idle: if there are no incomplete leads (or the adapter is Noop),
the task just reschedules.
"""
from __future__ import annotations

import logging

from django.conf import settings
from django.db.models import Q

from outreach.llm import get_adapter
from outreach.models import Lead, Task

logger = logging.getLogger("outreach.tasks.enrich")

ENRICH_BATCH_SIZE = 20

# Allowed keys in the enricher payload, matched to Lead fields.
_FIELD_MAP = {
    "contact_phone": "contact_phone",
    "contact_email": "contact_email",
    "contact_name": "contact_name",
    "contact_company": "contact_company",
}


def _needs_enrichment() -> Q:
    return (
        Q(contact_phone="") | Q(contact_email="")
    ) & Q(enrichment_log={})


def handle(task: Task) -> None:
    campaign = task.campaign
    interval = settings.OUTREACH["ENRICH_INTERVAL_MIN"]

    if not campaign.llm_enricher_enabled:
        task.reschedule(minutes=interval)
        return

    adapter = get_adapter()
    if adapter.name == "noop":
        logger.debug("[enrich] noop adapter, skipping")
        task.reschedule(minutes=interval)
        return

    batch = list(
        Lead.objects.filter(
            campaign=campaign,
            stage__in=[Lead.Stage.DISCOVERED, Lead.Stage.QUALIFIED],
        )
        .filter(_needs_enrichment())
        .order_by("-lead_score", "-created_at")[:ENRICH_BATCH_SIZE]
    )

    enriched = 0
    for lead in batch:
        try:
            payload = adapter.enrich_contact(lead)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[enrich] adapter failed for lead=%s: %s", lead.pk, exc)
            continue

        if not payload:
            # Mark as attempted so we don't hammer the same lead forever.
            lead.enrichment_log = {"status": "empty"}
            lead.save(update_fields=["enrichment_log", "updated_at"])
            continue

        dirty = ["enrichment_log", "updated_at"]
        for key, field in _FIELD_MAP.items():
            value = (payload.get(key) or "").strip() if payload.get(key) else ""
            if value and not getattr(lead, field):
                setattr(lead, field, value[:256])
                dirty.append(field)

        lead.enrichment_log = {
            "status": "ok",
            "adapter": adapter.name,
            **{k: v for k, v in payload.items() if v},
        }
        if not lead.contact_source:
            lead.contact_source = f"llm:{adapter.name}"
            dirty.append("contact_source")
        lead.save(update_fields=list(dict.fromkeys(dirty)))
        enriched += 1

    logger.info(
        "[enrich] campaign=%s attempted=%d enriched=%d",
        campaign.name,
        len(batch),
        enriched,
    )
    task.reschedule(minutes=interval)
