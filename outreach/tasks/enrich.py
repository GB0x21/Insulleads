"""
enrich task — LLM contact enrichment.

Pulls DISCOVERED / QUALIFIED leads that are missing contact info and asks
the configured :class:`outreach.llm.LLMAdapter` to find phone / email /
website via web search. Merges the results back into the Lead row
(non-destructive: never overwrite existing fields) and logs everything
to ``Lead.enrichment_log``.

Retry policy
------------
``Lead.enrichment_log`` records every attempt:

  {"status": "ok"|"empty",
   "adapter": "anthropic",
   "attempts": <int>,
   "last_attempt_at": "<ISO 8601 UTC>",
   ...payload fields when status=ok...}

* ``status="ok"`` leads aren't retried — we wrote contact data once.
* ``status="empty"`` leads are retried after ``ENRICH_RETRY_HOURS`` and
  up to ``ENRICH_MAX_ATTEMPTS`` total attempts. Web data changes; a
  contractor's site may go up between attempts.

The :func:`enrich_one` helper is also called inline by
``outreach.tasks.outreach.handle`` as a last-chance attempt right before
sending — so a lead never hits Telegram without at least one enrichment
try having been made.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from outreach.llm import LLMAdapter, get_adapter
from outreach.models import Lead, Task

logger = logging.getLogger("outreach.tasks.enrich")

ENRICH_BATCH_SIZE = 50

# Allowed keys in the enricher payload, matched to Lead fields.
_FIELD_MAP = {
    "contact_phone": "contact_phone",
    "contact_email": "contact_email",
    "contact_name": "contact_name",
    "contact_company": "contact_company",
}


# ─── Cooldown helpers ────────────────────────────────────────────────


def _now_iso() -> str:
    return timezone.now().isoformat()


def _cooldown_seconds() -> int:
    return int(settings.OUTREACH.get("ENRICH_RETRY_HOURS", 24)) * 3600


def _max_attempts() -> int:
    return int(settings.OUTREACH.get("ENRICH_MAX_ATTEMPTS", 3))


def _is_cooldown_elapsed(log: dict[str, Any]) -> bool:
    """True if `last_attempt_at` is older than the cooldown (or missing)."""
    last = log.get("last_attempt_at")
    if not last:
        return True
    try:
        from datetime import datetime

        last_dt = datetime.fromisoformat(last)
    except ValueError:
        return True
    return (timezone.now() - last_dt).total_seconds() >= _cooldown_seconds()


def _should_retry(log: dict[str, Any]) -> bool:
    """A `status="empty"` log is retry-eligible while attempts remain
    AND the cooldown has elapsed since the last attempt."""
    if log.get("status") != "empty":
        return False
    if int(log.get("attempts", 0)) >= _max_attempts():
        return False
    return _is_cooldown_elapsed(log)


# ─── Candidate selection ─────────────────────────────────────────────


def _needs_enrichment() -> Q:
    """Broad SQL-side filter — Python-side filtering then handles the
    cooldown / max-attempts gate so JSONField date math stays simple."""
    missing_contact = Q(contact_phone="") | Q(contact_email="")
    not_yet_attempted = Q(enrichment_log={})
    has_recoverable = (
        Q(enrichment_log__status="ok")
        & (
            ~Q(enrichment_log__contact_phone="")
            | ~Q(enrichment_log__contact_email="")
        )
    )
    is_empty_status = Q(enrichment_log__status="empty")
    return missing_contact & (not_yet_attempted | has_recoverable | is_empty_status)


# ─── Public helpers ──────────────────────────────────────────────────


def _recover_from_log(lead: Lead) -> bool:
    """Write contact fields from an existing successful enrichment_log.
    Returns True if anything changed. Used when an earlier `enrich_contact`
    succeeded but the write-back failed."""
    log = lead.enrichment_log or {}
    if log.get("status") != "ok":
        return False
    dirty = ["updated_at"]
    for key, field in _FIELD_MAP.items():
        value = (log.get(key) or "").strip()
        if value and not getattr(lead, field):
            setattr(lead, field, value[:256])
            dirty.append(field)
    if len(dirty) == 1:
        return False
    if not lead.contact_source:
        lead.contact_source = f"llm:{log.get('adapter', 'anthropic')}"
        dirty.append("contact_source")
    lead.save(update_fields=list(dict.fromkeys(dirty)))
    return True


def enrich_one(
    lead: Lead, adapter: LLMAdapter | None = None, *, force: bool = False
) -> bool:
    """Enrich a single lead in-process. Returns True if any contact field
    was written. Safe to call from any task (the outreach task uses it
    inline as a last-chance attempt before sending).

    `force=True` ignores the retry cooldown / max-attempts gates — used by
    the manual `manage.py enrich_leads --force` flag.
    """
    if adapter is None:
        adapter = get_adapter()
    if adapter.name == "noop":
        return False

    log = lead.enrichment_log or {}

    # Lead already has both phone and email — nothing to do.
    if lead.contact_phone and lead.contact_email:
        return False

    # Earlier successful run: try recovering contact fields from the log.
    if log.get("status") == "ok":
        return _recover_from_log(lead)

    # Earlier empty run: respect cooldown unless forced.
    if log.get("status") == "empty" and not force and not _should_retry(log):
        return False

    try:
        payload = adapter.enrich_contact(lead)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[enrich] adapter failed for lead=%s: %s", lead.pk, exc)
        return False

    attempts = int(log.get("attempts", 0)) + 1
    base_log = {
        "adapter": adapter.name,
        "attempts": attempts,
        "last_attempt_at": _now_iso(),
    }

    if not payload:
        lead.enrichment_log = {**base_log, "status": "empty"}
        lead.save(update_fields=["enrichment_log", "updated_at"])
        return False

    dirty = ["enrichment_log", "updated_at"]
    for key, field in _FIELD_MAP.items():
        value = (payload.get(key) or "").strip()
        if value and not getattr(lead, field):
            setattr(lead, field, value[:256])
            dirty.append(field)

    lead.enrichment_log = {
        **base_log,
        "status": "ok",
        **{k: v for k, v in payload.items() if v},
    }
    if not lead.contact_source:
        lead.contact_source = f"llm:{adapter.name}"
        dirty.append("contact_source")
    lead.save(update_fields=dirty)
    return True


# ─── Daemon task ─────────────────────────────────────────────────────


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

    candidates = list(
        Lead.objects.filter(
            campaign=campaign,
            stage__in=[Lead.Stage.DISCOVERED, Lead.Stage.QUALIFIED],
        )
        .filter(_needs_enrichment())
        .order_by("-lead_score", "-created_at")[: ENRICH_BATCH_SIZE * 2]
    )

    # Filter cooldown-blocked retries Python-side; cap to batch size.
    batch: list[Lead] = []
    for lead in candidates:
        log = lead.enrichment_log or {}
        if log.get("status") == "empty" and not _should_retry(log):
            continue
        batch.append(lead)
        if len(batch) >= ENRICH_BATCH_SIZE:
            break

    # Dedup: same GC company appearing in multiple leads gets enriched once
    # per batch — avoids burning LLM credits on duplicate contractor lookups.
    seen_companies: set[str] = set()
    deduped: list[Lead] = []
    for lead in batch:
        key = (lead.contact_company or "").strip().lower()
        if key and key in seen_companies:
            continue
        if key:
            seen_companies.add(key)
        deduped.append(lead)

    max_workers = int(settings.OUTREACH.get("ENRICH_WORKERS", 8))
    with ThreadPoolExecutor(max_workers=min(max_workers, len(deduped) or 1)) as pool:
        futs = {pool.submit(enrich_one, lead, adapter): lead for lead in deduped}
        enriched = sum(1 for f in as_completed(futs) if f.result())

    logger.info(
        "[enrich] campaign=%s candidates=%d attempted=%d (deduped=%d) enriched=%d",
        campaign.name,
        len(candidates),
        len(batch),
        len(deduped),
        enriched,
    )
    task.reschedule(minutes=interval)
