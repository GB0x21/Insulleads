"""
qualify task — score all DISCOVERED leads for a campaign.

Decision ladder, in priority order:

  1. LightGBM (``outreach.ml.lgbm``) — primary once the campaign has
     ≥ ``MIN_LABELS_FOR_TRAINING`` labels (Phase 2 of the roadmap).
  2. Gaussian Process (``outreach.ml.qualifier``) — legacy backend,
     used when 2-9 labels are available.
  3. LLM judge (``outreach.llm``) — cold-start fallback when < 2
     labels are available and a campaign-level toggle allows it.
  4. Heuristic (``Lead.lead_score``) — final safety net when nothing
     else is available.
"""
from __future__ import annotations

import logging
import os

from django.conf import settings
from django.utils import timezone

from outreach.llm import get_adapter
from outreach.ml import lgbm as lgbm_qualifier
from outreach.ml.embeddings import embed_lead
from outreach.ml.qualifier import qualify_batch as gp_qualify_batch
from outreach.models import ActionLog, Lead, Task

logger = logging.getLogger("outreach.tasks.qualify")

QUALIFY_SCORE_THRESHOLD = 0.55  # posterior mean (0-1)
LGBM_SCORE_THRESHOLD = 0.55     # LightGBM probability (0-1)
LLM_SCORE_THRESHOLD = 0.55      # LLM cold-start judge (0-1)
HEURISTIC_THRESHOLD = 50        # legacy 0-100 score
QUALIFY_BATCH_SIZE = int(os.getenv("QUALIFY_BATCH_SIZE", "30"))


def _pick_backend(campaign) -> str:
    """Choose the qualifier backend for this campaign — see module docstring."""
    if lgbm_qualifier.is_available(campaign):
        return "lgbm"
    if campaign.positive_labels + campaign.negative_labels >= 2:
        return "gp"
    if campaign.llm_qualifier_enabled:
        adapter = get_adapter()
        if adapter.name != "noop":
            return "llm"
    return "heuristic"


def handle(task: Task) -> None:
    campaign = task.campaign

    batch = list(
        Lead.objects.filter(campaign=campaign, stage=Lead.Stage.DISCOVERED)
        .select_related("source")
        .order_by("-created_at")[:QUALIFY_BATCH_SIZE]
    )
    if not batch:
        task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
        return

    backend = _pick_backend(campaign)
    logger.info("[qualify] campaign=%s backend=%s", campaign.name, backend)

    # GP still needs embeddings; LightGBM does not. Populate them only
    # when the chosen backend is the GP.
    if backend == "gp":
        for lead in batch:
            if lead.embedding is None:
                lead.embedding = embed_lead(lead)
                lead.save(update_fields=["embedding"])

    if backend == "lgbm":
        results = lgbm_qualifier.qualify_batch(campaign, batch)
    elif backend == "gp":
        results = gp_qualify_batch(campaign, batch)
    else:
        # No model yet — populate with neutral scores; the per-lead
        # loop below picks between LLM judge and heuristic.
        results = [(0.5, 1.0)] * len(batch)

    adapter = get_adapter() if backend == "llm" else None

    promoted = 0
    now = timezone.now()
    for lead, (mean, var) in zip(batch, results, strict=True):
        lead.qualification_score = mean
        lead.qualification_variance = var
        update_fields = [
            "qualification_score",
            "qualification_variance",
            "updated_at",
        ]

        if backend == "lgbm":
            passes = mean >= LGBM_SCORE_THRESHOLD
            decision_meta = {"score": mean, "judge": "lgbm"}
        elif backend == "gp":
            passes = mean >= QUALIFY_SCORE_THRESHOLD
            decision_meta = {"score": mean, "variance": var, "judge": "gp"}
        elif backend == "llm":
            try:
                llm_score, reason = adapter.qualify_lead(lead, campaign)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "[qualify] LLM judge failed for lead=%s", lead.pk,
                    exc_info=True,
                )
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
        else:  # heuristic
            passes = (lead.lead_score or 0) >= HEURISTIC_THRESHOLD
            decision_meta = {"lead_score": lead.lead_score, "judge": "heuristic"}

        if passes:
            lead.stage = Lead.Stage.QUALIFIED
            lead.stage_changed_at = now
            update_fields += ["stage", "stage_changed_at"]
            promoted += 1
            ActionLog.objects.create(
                lead=lead,
                campaign=campaign,
                action=ActionLog.Action.QUALIFIED,
                payload=decision_meta,
            )
        lead.save(update_fields=update_fields)

    logger.info(
        "[qualify] campaign=%s backend=%s evaluated=%d promoted=%d",
        campaign.name,
        backend,
        len(batch),
        promoted,
    )
    task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
