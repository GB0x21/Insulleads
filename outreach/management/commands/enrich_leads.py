"""
`python manage.py enrich_leads` — manual / batch contact enrichment.

Useful when:
  * Backfilling historical leads imported before the enricher existed.
  * Forcing a retry on leads stuck at `enrichment_log.status=empty`.
  * Spot-checking a single lead by id during ops debugging.

Examples:

    # Best-effort enrichment of up to 100 leads in a campaign that have
    # no contact info — respects the cooldown / max-attempts policy.
    python manage.py enrich_leads --campaign "Bay Area Insulation" --limit 100

    # Force a retry of one lead, ignoring cooldown:
    python manage.py enrich_leads --lead-id 42 --force

    # Force-retry every empty-log lead in the campaign (use with care —
    # each call costs an Anthropic web_search invocation):
    python manage.py enrich_leads --campaign "Bay Area Insulation" \\
        --status empty --force --limit 25
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from outreach.llm import get_adapter
from outreach.models import Campaign, Lead
from outreach.tasks.enrich import enrich_one


class Command(BaseCommand):
    help = "Manually run LLM contact enrichment on leads."

    def add_arguments(self, parser):
        parser.add_argument("--campaign", help="Campaign name (substring match).")
        parser.add_argument(
            "--lead-id", type=int, help="Enrich a single lead by primary key."
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Max leads to process when --lead-id is not set.",
        )
        parser.add_argument(
            "--status",
            choices=["any", "missing", "empty"],
            default="missing",
            help=(
                "any → every lead in scope; "
                "missing → only leads with no phone+email (default); "
                "empty → only leads whose last enrichment came back empty."
            ),
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Ignore the retry cooldown / max-attempts gate.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List which leads would be enriched without calling the LLM.",
        )

    def handle(self, *args, **opts):
        adapter = get_adapter()
        if adapter.name == "noop" and not opts["dry_run"]:
            raise CommandError(
                "LLM adapter is `noop`. Set ANTHROPIC_API_KEY and "
                "LLM_ADAPTER=anthropic, or use --dry-run."
            )

        qs = Lead.objects.all()
        if opts["lead_id"] is not None:
            qs = qs.filter(pk=opts["lead_id"])
        else:
            if opts["campaign"]:
                campaigns = list(
                    Campaign.objects.filter(name__icontains=opts["campaign"])
                )
                if not campaigns:
                    raise CommandError(
                        f"No campaign matching {opts['campaign']!r}."
                    )
                qs = qs.filter(campaign__in=campaigns)

            if opts["status"] == "missing":
                qs = qs.filter(contact_phone="").filter(contact_email="")
            elif opts["status"] == "empty":
                qs = qs.filter(enrichment_log__status="empty")

            qs = qs.order_by("-lead_score", "-created_at")[: opts["limit"]]

        leads = list(qs)
        if not leads:
            self.stdout.write(self.style.WARNING("No matching leads."))
            return

        self.stdout.write(
            f"Selected {len(leads)} lead(s) "
            f"(adapter={adapter.name}, force={opts['force']})."
        )
        if opts["dry_run"]:
            for lead in leads:
                log = lead.enrichment_log or {}
                self.stdout.write(
                    f"  L{lead.pk:>5}  {lead.title[:60]:60}  "
                    f"phone={'y' if lead.contact_phone else 'n'}  "
                    f"email={'y' if lead.contact_email else 'n'}  "
                    f"log.status={log.get('status', '—')}  "
                    f"attempts={log.get('attempts', 0)}"
                )
            return

        enriched = 0
        for lead in leads:
            try:
                changed = enrich_one(lead, adapter, force=opts["force"])
            except Exception as exc:  # noqa: BLE001
                self.stdout.write(
                    self.style.ERROR(f"  L{lead.pk}: {exc}")
                )
                continue

            mark = "+" if changed else "·"
            log = (lead.enrichment_log or {})
            self.stdout.write(
                f"  {mark} L{lead.pk}  status={log.get('status', '—')}  "
                f"phone={lead.contact_phone or '—'}  "
                f"email={lead.contact_email or '—'}"
            )
            if changed:
                enriched += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. {enriched}/{len(leads)} lead(s) enriched."
            )
        )
