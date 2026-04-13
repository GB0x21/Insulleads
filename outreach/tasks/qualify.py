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

from outreach.llm import get_adapter
from outreach.ml.embeddings import embed_lead
from outreach.ml.qualifier import qualify_batch
from outreach.models import ActionLog, Lead, Task

logger = logging.getLogger("outreach.tasks.qualify")

QUALIFY_SCORE_THRESHOLD = 0.55  # posterior mean (0-1)
LLM_SCORE_THRESHOLD = 0.55      # LLM cold-start judge (0-1)
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

    model_ready = (
        campaign.positive_labels + campaign.negative_labels >= 2
    )
    use_llm = (not model_ready) and campaign.llm_qualifier_enabled
    adapter = get_adapter() if use_llm else None
    if adapter is not None and adapter.name == "noop":
        adapter = None  # Nothing to gain; stick with the heuristic.

    promoted = 0
    for lead, (mean, var) in zip(batch, results, strict=True):
        lead.qualification_score = mean
        lead.qualification_variance = var
        update_fields = [
            "qualification_score",
            "qualification_variance",
            "stage",
            "stage_changed_at",
            "updated_at",
        ]

        if model_ready:
            passes = mean >= QUALIFY_SCORE_THRESHOLD
            decision_meta = {"score": mean, "variance": var, "judge": "gp"}
        elif adapter is not None:
            try:
                llm_score, reason = adapter.qualify_lead(lead, campaign)
            except Exception:  # noqa: BLE001
                logger.warning("[qualify] LLM judge failed for lead=%s",
                               lead.pk, exc_info=True)
                llm_score, reason = (None, "")

            if llm_score is not None:
                lead.qualification_score = llm_score
                lead.llm_qualification_reason = reason
                update_fields.append("llm_qualification_reason")
                passes = llm_score >= LLM_SCORE_THRESHOLD
                decision_meta = {
                    "score": llm_score,
                    "reason": reason,
                    "judge": f"llm:{adapter.name}",
                }
            else:
                passes = (lead.lead_score or 0) >= HEURISTIC_THRESHOLD
                decision_meta = {
                    "lead_score": lead.lead_score,
                    "judge": "heuristic",
                }
        else:
            passes = (lead.lead_score or 0) >= HEURISTIC_THRESHOLD
            decision_meta = {
                "lead_score": lead.lead_score,
                "judge": "heuristic",
            }

        if passes:
            lead.stage = Lead.Stage.QUALIFIED
            promoted += 1
            ActionLog.objects.create(
                lead=lead,
                campaign=campaign,
                action=ActionLog.Action.QUALIFIED,
                payload=decision_meta,
            )
        lead.save(update_fields=list(dict.fromkeys(update_fields)))

    logger.info(
        "[qualify] campaign=%s evaluated=%d promoted=%d",
        campaign.name,
        len(batch),
        promoted,
    )
    task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
