"""
Task-queue worker loop — the heart of the Insulleads outreach daemon.

Adapted from OpenOutreach's `linkedin.daemon`. Instead of scraping LinkedIn,
this daemon orchestrates the public-data agents in `agents/`, qualifies new
leads via the Bayesian qualifier, and pushes outreach through Telegram /
Email / WhatsApp.

A single process owns the loop:

  1. Pick up the next READY Task (ordered by `scheduled_at`).
  2. Dispatch it to a handler in `outreach.tasks.*`.
  3. Reschedule periodic tasks (discover, qualify, outreach) forever.
"""
from __future__ import annotations

import logging
import signal
import time
from typing import Callable

from django.db import close_old_connections
from django.utils import timezone

from .models import Campaign, Source, Task

logger = logging.getLogger("outreach.daemon")


# ─── Task handler dispatch ─────────────────────────────────────────
def _handlers() -> dict[str, Callable[[Task], None]]:
    # Imported lazily to avoid Django app-loading cycles.
    from .tasks import discover, enrich, outreach, qualify

    return {
        Task.Type.DISCOVER: discover.handle,
        Task.Type.ENRICH: enrich.handle,
        Task.Type.QUALIFY: qualify.handle,
        Task.Type.OUTREACH: outreach.handle,
        Task.Type.FOLLOW_UP: outreach.handle_follow_up,
    }


# ─── Seeding ────────────────────────────────────────────────────────
def ensure_periodic_tasks() -> None:
    """Make sure every active Source has a pending DISCOVER task,
    and every active Campaign has pending QUALIFY + OUTREACH tasks.
    """
    now = timezone.now()

    for source in Source.objects.filter(enabled=True):
        if not Task.objects.filter(
            type=Task.Type.DISCOVER, source=source, status=Task.Status.PENDING
        ).exists():
            Task.objects.create(
                type=Task.Type.DISCOVER,
                campaign=source.campaign,
                source=source,
                scheduled_at=now,
            )
            logger.info("[seed] discover queued for source=%s", source.key)

    for campaign in Campaign.objects.filter(is_active=True):
        periodic = [Task.Type.QUALIFY, Task.Type.OUTREACH]
        if campaign.llm_enricher_enabled:
            periodic.append(Task.Type.ENRICH)
        for ttype in periodic:
            if not Task.objects.filter(
                type=ttype, campaign=campaign, status=Task.Status.PENDING
            ).exists():
                Task.objects.create(
                    type=ttype, campaign=campaign, scheduled_at=now
                )
                logger.info("[seed] %s queued for campaign=%s", ttype, campaign.name)


# ─── Main loop ──────────────────────────────────────────────────────
class Daemon:
    def __init__(self) -> None:
        self._stop = False
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_args) -> None:
        logger.info("Shutdown requested, draining loop...")
        self._stop = True

    def run_forever(self) -> None:
        logger.info("Insulleads daemon starting")
        ensure_periodic_tasks()
        handlers = _handlers()

        while not self._stop:
            close_old_connections()
            task = Task.objects.ready().order_by("scheduled_at").first()

            if task is None:
                wait = Task.objects.seconds_until_next()
                time.sleep(min(wait, 30))
                continue

            task.mark_running()
            handler = handlers.get(task.type)
            if handler is None:
                task.mark_failed(f"No handler for type {task.type}")
                continue

            logger.info("[run] %s #%d", task.type, task.pk)
            try:
                handler(task)
                task.mark_done()
            except Exception as exc:  # noqa: BLE001
                logger.exception("[fail] %s #%d: %s", task.type, task.pk, exc)
                task.mark_failed(str(exc))

            # Always re-seed periodic tasks so the pipeline keeps flowing.
            ensure_periodic_tasks()

        logger.info("Insulleads daemon stopped")
