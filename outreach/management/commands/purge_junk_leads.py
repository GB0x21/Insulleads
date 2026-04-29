"""
`python manage.py purge_junk_leads` — apply the same junk filters used
at the pipeline boundary to the existing Lead table and delete (or
mark LOST) any historical leads that today's filters would have
caught.

Use after upgrading the project — older Lead rows were ingested
before the v9 quality / phase / recency filters existed, so the
operator still sees them in /admin/ and the outreach loop still picks
them up.

Filters applied (in order):

  1. permits-kind leads whose `raw` matches the non-construction
     blacklist (Code Investigation / PLAN A LOT / fence-only / …)
     — see `agents.permits_agent._is_quality_permit`.

  2. construction-kind leads whose `raw.phase` is not in
     `agents.construction_agent.PHASES_INCLUDE` (default: drop
     drywall / insulation / final).

  3. permits-kind leads whose `raw.issued_date` is unparseable OR
     older than `PERMIT_MONTHS` — see `_is_recent`.

Safety:
  - Only acts on leads still in DISCOVERED or QUALIFIED stages
    (CONTACTED / REPLIED / WON / LOST are preserved — they have
    ActionLog entries).
  - --dry-run prints what would be touched without changing anything.
  - --soft (default) flips stage→LOST + records the reason in
    `llm_qualification_reason` so you can audit later.
  - --hard deletes the rows.

Examples:
    python manage.py purge_junk_leads --dry-run
    python manage.py purge_junk_leads --campaign "Bay Area Insulation"
    python manage.py purge_junk_leads --hard --kinds permits,construction
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from outreach.models import Campaign, Lead


class Command(BaseCommand):
    help = "Purge stale / junk leads from the existing Lead table."

    def add_arguments(self, parser):
        parser.add_argument("--campaign", help="Limit to leads on this campaign name (substring).")
        parser.add_argument(
            "--kinds",
            default="permits,construction",
            help="Comma-separated source.kind to scan (default permits,construction).",
        )
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Print what would change without touching the DB.",
        )
        parser.add_argument(
            "--hard", action="store_true",
            help="DELETE the rows. Default is to flip stage→LOST + record the reason.",
        )
        parser.add_argument(
            "--limit", type=int, default=0,
            help="Cap how many leads to scan (0 = no cap).",
        )

    def handle(self, *args, **opts):
        kinds = [k.strip() for k in opts["kinds"].split(",") if k.strip()]
        qs = Lead.objects.filter(
            stage__in=[Lead.Stage.DISCOVERED, Lead.Stage.QUALIFIED],
            source__kind__in=kinds,
        )
        if opts["campaign"]:
            campaigns = list(Campaign.objects.filter(name__icontains=opts["campaign"]))
            qs = qs.filter(campaign__in=campaigns)

        if opts["limit"] > 0:
            qs = qs[: opts["limit"]]

        # Materialise once so we can show counts, then iterate.
        leads = list(qs.select_related("source"))
        if not leads:
            self.stdout.write(self.style.WARNING("no candidate leads"))
            return

        from agents.construction_agent import PHASES_INCLUDE
        from agents.permits_agent import _is_quality_permit, _is_recent

        actions: list[tuple[Lead, str]] = []
        for lead in leads:
            reason = _classify_lead(lead, _is_quality_permit, _is_recent, PHASES_INCLUDE)
            if reason:
                actions.append((lead, reason))

        self.stdout.write(
            f"\nScanned {len(leads)} lead(s); "
            f"{len(actions)} match the junk filters."
        )

        if opts["dry_run"]:
            for lead, reason in actions[:50]:
                self.stdout.write(
                    f"  L{lead.pk:>5}  {lead.source.kind:14}  "
                    f"{(lead.title or '')[:50]:50}  {reason}"
                )
            if len(actions) > 50:
                self.stdout.write(f"  ... +{len(actions) - 50} more")
            return

        if opts["hard"]:
            ids = [lead.pk for lead, _ in actions]
            deleted, _ = Lead.objects.filter(pk__in=ids).delete()
            self.stdout.write(self.style.SUCCESS(
                f"\nHARD-deleted {deleted} lead(s)."
            ))
            return

        # Soft purge — flip stage to LOST and record the reason.
        now = timezone.now()
        for lead, reason in actions:
            lead.stage = Lead.Stage.LOST
            lead.stage_changed_at = now
            existing = (lead.llm_qualification_reason or "").strip()
            tag = f"[purge] {reason}"
            lead.llm_qualification_reason = (
                f"{existing}\n{tag}" if existing else tag
            )[:1000]
            lead.save(update_fields=[
                "stage", "stage_changed_at",
                "llm_qualification_reason", "updated_at",
            ])

        self.stdout.write(self.style.SUCCESS(
            f"\nMarked {len(actions)} lead(s) as LOST. "
            "Use --hard to delete them outright."
        ))


def _classify_lead(lead, is_quality_permit, is_recent, phases_include) -> str:
    """Return a short reason string when the lead is junk, '' otherwise."""
    raw = lead.raw or {}
    kind = lead.source.kind

    if kind == "permits":
        ok, why = is_quality_permit(raw)
        if not ok:
            return f"non-construction permit ({why})"
        # Recency uses raw.issued_date — same parse logic as the agent.
        recency_lead = {
            "issued_date": raw.get("issued_date") or raw.get("filed_date") or "",
            "filed_date": raw.get("filed_date") or "",
        }
        if recency_lead["issued_date"] and not is_recent(recency_lead):
            return "stale issued_date (outside PERMIT_MONTHS window)"
        return ""

    if kind == "construction":
        phase = (raw.get("phase") or "").lower()
        if phase and phase not in phases_include:
            return f"phase={phase} (excluded by whitelist)"
        return ""

    return ""
