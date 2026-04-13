"""
qualify task — score all DISCOVERED leads for a campaign.

Decision ladder, in priority order:

  1. LightGBM (``outreach.ml.lgbm``) — primary once the campaign has
     ≥ ``MIN_LABELS_FOR_TRAINING`` labels (Phase 2 of the roadmap).
  2. Gaussian Process (``outreach.ml.qualifier``) — legacy backend,
     used when 2-9 labels are available.
  3. Heuristic (``Lead.lead_score``) — cold-start default and final
     safety net. The LLM judge is only reserved for leads sitting in an
     ambiguous band around the heuristic threshold and is hard-capped
     per cycle so we don't meltdown the Anthropic rate limit on a
     fresh DB with hundreds of new leads.
"""
from __future__ import annotations

import logging

from django.conf import settings

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

# Heuristic band (in legacy 0-100 units) around the threshold where the
# LLM judge is allowed to break the tie. Outside this band the heuristic
# is trusted on its own.
LLM_AMBIGUOUS_BAND = 15
# Hard cap on LLM judge calls per qualify cycle. Prevents rate-limit
# meltdowns on cold starts where the discover sweep just dropped
# hundreds of new leads into the DB.
LLM_MAX_CALLS_PER_CYCLE = 20


def _pick_backend(campaign) -> str:
    """Choose the qualifier backend for this campaign — see module docstring."""
    if lgbm_qualifier.is_available(campaign):
        return "lgbm"
    if campaign.positive_labels + campaign.negative_labels >= 2:
        return "gp"
    return "heuristic"


def handle(task: Task) -> None:
    campaign = task.campaign

    batch = list(
        Lead.objects.filter(campaign=campaign, stage=Lead.Stage.DISCOVERED)
        .select_related("source")
        .order_by("-created_at")[:200]
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
        # No model yet — the heuristic is the primary signal. Start
        # every lead at 0.5/1.0 so qualification_score stays populated
        # even when the LLM tie-breaker is not consulted.
        results = [(0.5, 1.0)] * len(batch)

    # Set up the LLM judge as a bounded tie-breaker only when the
    # heuristic is the primary backend and the campaign opts in. We
    # hard-cap calls per cycle so a fresh DB with 400+ new leads can't
    # melt the Anthropic rate limit.
    llm_adapter = None
    llm_calls_remaining = 0
    if backend == "heuristic" and campaign.llm_qualifier_enabled:
        adapter = get_adapter()
        if adapter.name != "noop":
            llm_adapter = adapter
            llm_calls_remaining = LLM_MAX_CALLS_PER_CYCLE

    promoted = 0
    llm_calls_used = 0
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

        if backend == "lgbm":
            passes = mean >= LGBM_SCORE_THRESHOLD
            decision_meta = {"score": mean, "judge": "lgbm"}
        elif backend == "gp":
            passes = mean >= QUALIFY_SCORE_THRESHOLD
            decision_meta = {"score": mean, "variance": var, "judge": "gp"}
        else:  # heuristic (optionally tie-broken by LLM)
            heur = lead.lead_score or 0
            passes = heur >= HEURISTIC_THRESHOLD
            decision_meta = {"lead_score": heur, "judge": "heuristic"}

            # Ambiguous band → spend an LLM call, if we still have any.
            ambiguous = abs(heur - HEURISTIC_THRESHOLD) <= LLM_AMBIGUOUS_BAND
            if (
                llm_adapter is not None
                and llm_calls_remaining > 0
                and ambiguous
            ):
                llm_calls_remaining -= 1
                llm_calls_used += 1
                try:
                    llm_score, reason = llm_adapter.qualify_lead(lead, campaign)
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "[qualify] LLM judge failed for lead=%s — "
                        "disabling LLM tie-break for the rest of the cycle",
                        lead.pk,
                        exc_info=True,
                    )
                    llm_score, reason = (None, "")
                    # Stop hammering the API if it's erroring (429 etc.)
                    llm_calls_remaining = 0

                if llm_score is not None:
                    lead.qualification_score = llm_score
                    lead.llm_qualification_reason = reason
                    update_fields.append("llm_qualification_reason")
                    passes = llm_score >= LLM_SCORE_THRESHOLD
                    decision_meta = {
                        "score": llm_score,
                        "reason": reason,
                        "heuristic": heur,
                        "judge": f"llm:{llm_adapter.name}",
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
        "[qualify] campaign=%s backend=%s evaluated=%d promoted=%d llm_calls=%d",
        campaign.name,
        backend,
        len(batch),
        promoted,
        llm_calls_used,
    )
    task.reschedule(minutes=settings.OUTREACH["QUALIFY_INTERVAL_MIN"])
