"""
Bayesian lead qualifier — Gaussian-process regression over lead embeddings.

Adapted from OpenOutreach's `linkedin.ml` qualifier. Each Campaign owns its
own pickled model (stored in `Campaign.model_blob`). The model learns from
`ActionLog` outcomes:

  - Positive label: a lead reached stage REPLIED / WON.
  - Negative label: a lead reached stage LOST.

Cold-start (fewer than 2 labels): this module returns `(heuristic, 1.0)`
so the caller knows the posterior is uninformative and can fall back to
the legacy `lead_score`.

We keep the scikit-learn dep optional: if it's missing we still expose
the API but return cold-start values for every prediction.
"""
from __future__ import annotations

import logging
import pickle
from typing import TYPE_CHECKING

import numpy as np

from .embeddings import load_embedding

if TYPE_CHECKING:
    from outreach.models import Campaign, Lead

logger = logging.getLogger("outreach.ml.qualifier")


# ─── Optional sklearn import ────────────────────────────────────────
try:
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C

    _HAS_SKLEARN = True
except Exception:  # noqa: BLE001
    _HAS_SKLEARN = False


def _new_model():
    if not _HAS_SKLEARN:
        return None
    kernel = C(1.0, (1e-3, 1e3)) * RBF(1.0, (1e-2, 1e2))
    return GaussianProcessRegressor(
        kernel=kernel,
        normalize_y=True,
        n_restarts_optimizer=1,
        alpha=1e-3,
    )


def _heuristic(lead: "Lead") -> float:
    """Map the legacy 0-100 lead_score into a (0, 1) prior."""
    return max(0.01, min(0.99, (lead.lead_score or 0) / 100.0))


# ─── Public API ─────────────────────────────────────────────────────
def qualify_batch(
    campaign: "Campaign",
    leads: list["Lead"],
) -> list[tuple[float, float]]:
    """Return `[(posterior_mean, posterior_variance), ...]` per lead.

    A cold-start campaign (< 2 labels, or sklearn missing) gets
    `(heuristic, 1.0)` which the caller treats as "use the heuristic".
    """
    has_labels = campaign.positive_labels + campaign.negative_labels >= 2
    if not _HAS_SKLEARN or not has_labels or campaign.model_blob is None:
        return [(_heuristic(l), 1.0) for l in leads]

    try:
        model = pickle.loads(campaign.model_blob)
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to unpickle campaign model: %s", exc)
        return [(_heuristic(l), 1.0) for l in leads]

    X = np.stack([load_embedding(l.embedding) for l in leads])
    mean, std = model.predict(X, return_std=True)
    # Clip posterior mean to (0, 1) so downstream threshold logic is simple.
    mean = np.clip(mean, 0.0, 1.0)
    return [(float(m), float(s * s)) for m, s in zip(mean, std, strict=True)]


def retrain(campaign: "Campaign") -> None:
    """Refit the GP on every labeled lead of this campaign.

    Call this whenever a user moves a Lead into REPLIED/WON (positive) or
    LOST (negative). Cheap enough to run synchronously for < 2k labels.
    """
    if not _HAS_SKLEARN:
        return

    from outreach.models import Lead

    qs = Lead.objects.filter(
        campaign=campaign,
        stage__in=[Lead.Stage.REPLIED, Lead.Stage.WON, Lead.Stage.LOST],
    ).exclude(embedding=None)

    X, y = [], []
    pos = neg = 0
    for lead in qs:
        emb = load_embedding(lead.embedding)
        if emb is None:
            continue
        label = 1.0 if lead.stage in (Lead.Stage.REPLIED, Lead.Stage.WON) else 0.0
        X.append(emb)
        y.append(label)
        if label > 0.5:
            pos += 1
        else:
            neg += 1

    if len(X) < 2:
        campaign.positive_labels = pos
        campaign.negative_labels = neg
        campaign.save(update_fields=["positive_labels", "negative_labels"])
        return

    from django.utils import timezone

    model = _new_model()
    model.fit(np.stack(X), np.asarray(y))

    campaign.model_blob = pickle.dumps(model)
    campaign.model_trained_at = timezone.now()
    campaign.positive_labels = pos
    campaign.negative_labels = neg
    campaign.save(
        update_fields=[
            "model_blob",
            "model_trained_at",
            "positive_labels",
            "negative_labels",
        ]
    )
    logger.info(
        "[qualifier] retrained campaign=%s pos=%d neg=%d",
        campaign.name,
        pos,
        neg,
    )
