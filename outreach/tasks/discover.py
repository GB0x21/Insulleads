"""
discover task — run a pipeline source and persist new leads.

Equivalent of OpenOutreach's `linkedin.tasks.connect` for discovery: it
pulls candidate profiles (here: construction leads) from a source and
pushes them into the DB as Lead rows in the DISCOVERED stage.
"""
from __future__ import annotations

import logging

from django.utils import timezone

from outreach.models import ActionLog, Lead, Task
from outreach.pipeline.sources import fetch_source

logger = logging.getLogger("outreach.tasks.discover")


def handle(task: Task) -> None:
    source = task.source
    if source is None:
        raise RuntimeError("discover task requires a source")

    new_count = 0
    updated_count = 0

    for payload in fetch_source(source):
        ext_id = payload.pop("external_id")
        defaults = {
            **payload,
            "source": source,
            "campaign": task.campaign,
            "stage": Lead.Stage.DISCOVERED,
        }
        lead, created = Lead.objects.update_or_create(
            source=source,
            external_id=ext_id,
            defaults=defaults,
        )
        if created:
            new_count += 1
            ActionLog.objects.create(
                lead=lead,
                campaign=task.campaign,
                action=ActionLog.Action.DISCOVERED,
                payload={"source": source.key},
            )
        else:
            updated_count += 1

    source.last_run_at = timezone.now()
    source.last_error = ""
    source.save(update_fields=["last_run_at", "last_error"])

    logger.info(
        "[discover] source=%s new=%d updated=%d",
        source.key,
        new_count,
        updated_count,
    )

    # Reschedule for next interval
    task.reschedule(minutes=source.interval_minutes)
