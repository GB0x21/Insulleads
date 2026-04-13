"""
qualify task — embed + Bayesian-score all DISCOVERED leads for a campaign.

Mirrors OpenOutreach's qualification phase:
  - Compute an embedding for each new lead.
  - Ask the campaign's Gaussian-process qualifier for a (score, variance).
  - If score > threshold -> advance to QUALIFIED / READY_TO_CONTACT.
  - If model is cold (few labels) -> fall back to heuristic `lead_score`.
"""
from __future__ import annotations

import logging

from django.conf import settings

from outreach.ml.embeddings import embed_lead
from outreach.ml.qualifier import qualify_batch
from outreach.models import ActionLog, Lead, Task

logger = logging.getLogger("outreach.tasks.qualify")

QUALIFY_SCORE_THRESHOLD = 0.55  # posterior mean (0-1)
HEURISTIC_THRESHOLD = 50        # legacy 0-100 score


def handle(task: Task) -> None:
    campaign = task.campaign

    batch = list(
        Lead.objects.filter(campaign=campaign, stage=Lead.Stage.DISCOVERED)
        .order_by("-created_at")[:200]
    )
    if not batch:
        task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
        return

    # 1) Ensure every lead has an embedding (cheap + cached in DB).
    for lead in batch:
        if lead.embedding is None:
            lead.embedding = embed_lead(lead)
            lead.save(update_fields=["embedding"])

    # 2) Bayesian scoring (cold-start aware).
    results = qualify_batch(campaign, batch)

    promoted = 0
    for lead, (mean, var) in zip(batch, results, strict=True):
        lead.qualification_score = mean
        lead.qualification_variance = var

        model_ready = (
            campaign.positive_labels + campaign.negative_labels >= 2
        )
        if model_ready:
            passes = mean >= QUALIFY_SCORE_THRESHOLD
        else:
            passes = (lead.lead_score or 0) >= HEURISTIC_THRESHOLD

        if passes:
            lead.stage = Lead.Stage.QUALIFIED
            promoted += 1
            ActionLog.objects.create(
                lead=lead,
                campaign=campaign,
                action=ActionLog.Action.QUALIFIED,
                payload={"score": mean, "variance": var},
            )
        lead.save(
            update_fields=[
                "qualification_score",
                "qualification_variance",
                "stage",
                "stage_changed_at",
                "updated_at",
            ]
        )

    logger.info(
        "[qualify] campaign=%s evaluated=%d promoted=%d",
        campaign.name,
        len(batch),
        promoted,
    )
    task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
