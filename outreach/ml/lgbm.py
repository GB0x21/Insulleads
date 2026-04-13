"""
LightGBM lead qualifier — Phase 2 of the roadmap.

Mirrors the API of :mod:`outreach.ml.qualifier` (the legacy Gaussian
Process model) so :func:`outreach.tasks.qualify.handle` can swap
between the two without ceremony:

  * ``qualify_batch(campaign, leads) -> list[(prob, 1 - prob)]``
  * ``retrain(campaign)`` — refit from labeled leads.

The trained booster is pickled into
``Campaign.lgbm_model_blob`` alongside the feature-importance blob in
``Campaign.lgbm_feature_importance``.

LightGBM is imported lazily and the whole module degrades to a no-op
when the package is missing, so the daemon keeps running on fresh
boxes without the extra dep.
"""
from __future__ import annotations

import logging
import pickle
from typing import TYPE_CHECKING, Any

from .features import CATEGORICAL_FEATURES, FEATURE_ORDER, extract_features, to_row

if TYPE_CHECKING:
    from outreach.models import Campaign, Lead

logger = logging.getLogger("outreach.ml.lgbm")


# ─── Optional dependency guard ───────────────────────────────────────
try:
    import lightgbm as lgb
    import numpy as np

    _HAS_LGBM = True
except Exception:  # noqa: BLE001
    _HAS_LGBM = False


MIN_LABELS_FOR_TRAINING = 10
LGBM_PARAMS: dict[str, Any] = {
    "objective": "binary",
    "metric": "binary_logloss",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 5,
    "verbose": -1,
}


def _new_model():
    if not _HAS_LGBM:
        return None
    return lgb.LGBMClassifier(**LGBM_PARAMS, n_estimators=200)


# ─── Public API ─────────────────────────────────────────────────────
def is_available(campaign: "Campaign") -> bool:
    """True iff a trained LightGBM model exists on this campaign."""
    return bool(_HAS_LGBM and getattr(campaign, "lgbm_model_blob", None))


def qualify_batch(
    campaign: "Campaign",
    leads: list["Lead"],
) -> list[tuple[float, float]]:
    """Return ``[(score, 1 - score), ...]`` per lead.

    Mirrors the shape of :func:`outreach.ml.qualifier.qualify_batch`
    (``(mean, variance)``): LightGBM doesn't expose uncertainty, so we
    stash ``1 - score`` in the second slot as a coarse proxy — higher
    when the model is unsure (score near 0.5).
    """
    if not is_available(campaign):
        return [(0.5, 1.0) for _ in leads]

    try:
        model = pickle.loads(campaign.lgbm_model_blob)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[lgbm] unpickle failed: %s", exc)
        return [(0.5, 1.0) for _ in leads]

    rows = [to_row(extract_features(l)) for l in leads]
    X = np.asarray(rows, dtype=float)
    probs = model.predict_proba(X)[:, 1]
    return [
        (float(p), float(abs(0.5 - p) * 2))  # proxy variance (0..1)
        for p in probs
    ]


def retrain(campaign: "Campaign") -> dict[str, Any]:
    """Refit the LightGBM classifier on every labeled lead.

    Returns a summary dict ``{"status", "pos", "neg", "features"}`` so
    the caller can log the outcome. A status of ``skipped`` means the
    campaign has fewer than :data:`MIN_LABELS_FOR_TRAINING` labels.
    """
    if not _HAS_LGBM:
        return {"status": "no_lgbm"}

    from outreach.models import Lead

    qs = Lead.objects.filter(
        campaign=campaign,
        stage__in=[Lead.Stage.REPLIED, Lead.Stage.WON, Lead.Stage.LOST],
    ).select_related("source")

    X_rows: list[list[Any]] = []
    y: list[int] = []
    pos = neg = 0
    for lead in qs:
        feats = extract_features(lead)
        X_rows.append(to_row(feats))
        label = 1 if lead.stage in (Lead.Stage.REPLIED, Lead.Stage.WON) else 0
        y.append(label)
        if label:
            pos += 1
        else:
            neg += 1

    if pos + neg < MIN_LABELS_FOR_TRAINING or pos == 0 or neg == 0:
        return {"status": "skipped", "pos": pos, "neg": neg}

    from django.utils import timezone

    X = np.asarray(X_rows, dtype=float)
    y_arr = np.asarray(y, dtype=int)

    model = _new_model()
    cat_indices = [FEATURE_ORDER.index(c) for c in CATEGORICAL_FEATURES]
    model.fit(X, y_arr, categorical_feature=cat_indices)

    importance = dict(
        zip(FEATURE_ORDER, [int(v) for v in model.booster_.feature_importance()])
    )

    campaign.lgbm_model_blob = pickle.dumps(model)
    campaign.lgbm_trained_at = timezone.now()
    campaign.lgbm_feature_importance = importance
    campaign.save(
        update_fields=[
            "lgbm_model_blob",
            "lgbm_trained_at",
            "lgbm_feature_importance",
        ]
    )
    logger.info(
        "[lgbm] retrained campaign=%s pos=%d neg=%d top=%s",
        campaign.name,
        pos,
        neg,
        sorted(importance.items(), key=lambda kv: -kv[1])[:3],
    )
    return {"status": "ok", "pos": pos, "neg": neg, "features": importance}
