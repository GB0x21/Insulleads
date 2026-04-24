"""
discover_contracts — CLI to force a contract-discovery run.

    python manage.py discover_contracts                       # run every enabled contract_bids source
    python manage.py discover_contracts --source sam-gov-01   # one specific source by key
    python manage.py discover_contracts --provider sam_gov \
        --campaign <id>                                       # ad-hoc run without a persisted Source
    python manage.py discover_contracts --sync                # run inline; default also runs inline (this is a CLI)

This wraps the same task handler the daemon uses, so behaviour is
identical between the two entry points.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from outreach.models import Campaign, Source, Task
from outreach.tasks import discover_contracts as handler


class Command(BaseCommand):
    help = "Discover open bids / RFPs via contract-discovery agents."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--source",
            help="Run only the contract_bids Source with this key.",
        )
        parser.add_argument(
            "--provider",
            help=(
                "Run an ad-hoc discovery with this provider (sam_gov | "
                "web_search | state_bids | construction) without a "
                "persisted Source. Requires --campaign."
            ),
        )
        parser.add_argument(
            "--campaign",
            type=int,
            help="Campaign id to attach ad-hoc runs to (used with --provider).",
        )

    def handle(self, *args, **opts) -> None:
        if opts["provider"]:
            self._run_adhoc(opts["provider"], opts.get("campaign"))
            return

        qs = Source.objects.filter(enabled=True, kind="contract_bids")
        if opts.get("source"):
            qs = qs.filter(key=opts["source"])
        sources = list(qs)
        if not sources:
            raise CommandError(
                "No enabled Source(kind='contract_bids') found. "
                "Create one with python manage.py shell, or use --provider + --campaign."
            )

        for source in sources:
            task = Task(
                type=Task.Type.DISCOVER_CONTRACTS,
                campaign=source.campaign,
                source=source,
                scheduled_at=timezone.now(),
            )
            task.save()
            task.mark_running()
            try:
                handler.handle(task)
                task.mark_done()
            except Exception as exc:  # noqa: BLE001
                task.mark_failed(str(exc))
                self.stderr.write(f"[{source.key}] failed: {exc}")
                continue
            self.stdout.write(self.style.SUCCESS(f"[{source.key}] ok"))

    def _run_adhoc(self, provider: str, campaign_id: int | None) -> None:
        if not campaign_id:
            raise CommandError("--provider requires --campaign <id>")
        try:
            campaign = Campaign.objects.get(pk=campaign_id)
        except Campaign.DoesNotExist as exc:
            raise CommandError(f"Campaign id {campaign_id} not found") from exc

        # Reuse-or-create a hidden Source row so the task handler path is
        # identical to the daemon path.
        source, _ = Source.objects.get_or_create(
            key=f"adhoc-{provider}",
            defaults={
                "kind": "contract_bids",
                "campaign": campaign,
                "interval_minutes": 360,
                "enabled": False,
                "config": {"provider": provider},
            },
        )
        if source.kind != "contract_bids":
            raise CommandError(
                f"Source {source.key!r} exists but kind={source.kind!r} — refusing to overwrite."
            )
        # Make sure the provider is set on the config for this run.
        cfg = dict(source.config or {})
        cfg["provider"] = provider
        source.config = cfg
        source.campaign = campaign
        source.save()

        task = Task(
            type=Task.Type.DISCOVER_CONTRACTS,
            campaign=campaign,
            source=source,
            scheduled_at=timezone.now(),
        )
        task.save()
        task.mark_running()
        try:
            handler.handle(task)
            task.mark_done()
            self.stdout.write(self.style.SUCCESS(f"[adhoc/{provider}] ok"))
        except Exception as exc:  # noqa: BLE001
            task.mark_failed(str(exc))
            raise CommandError(f"[adhoc/{provider}] failed: {exc}") from exc
