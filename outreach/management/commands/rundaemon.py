"""
`python manage.py rundaemon` — entrypoint for the Insulleads daemon.

Adapted from OpenOutreach's `rundaemon` command. Supports interactive
onboarding and a `--onboard config.json` flag for headless bootstraps.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from outreach.daemon import Daemon
from outreach.models import Campaign, SiteConfig, Source

logger = logging.getLogger("outreach.rundaemon")


class Command(BaseCommand):
    help = "Start the Insulleads outreach daemon (task-queue worker loop)."

    def add_arguments(self, parser) -> None:  # noqa: D401
        parser.add_argument(
            "--onboard",
            type=str,
            help="Path to a JSON file with initial campaign + source config.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Process one task and exit (useful for debugging / CI).",
        )

    def handle(self, *args, **options) -> None:
        if options.get("onboard"):
            self._onboard(Path(options["onboard"]))
        else:
            self._interactive_onboard_if_empty()

        daemon = Daemon()
        if options.get("once"):
            daemon._stop = False
            from outreach.daemon import ensure_periodic_tasks

            ensure_periodic_tasks()
            from outreach.models import Task

            task = Task.objects.ready().order_by("scheduled_at").first()
            if task is None:
                self.stdout.write(self.style.WARNING("No ready task."))
                return
            task.mark_running()
            from outreach.daemon import _handlers

            try:
                _handlers()[task.type](task)
                task.mark_done()
                self.stdout.write(self.style.SUCCESS(f"Ran {task.type} #{task.pk}"))
            except Exception as exc:  # noqa: BLE001
                task.mark_failed(str(exc))
                raise
            return

        daemon.run_forever()

    # ─── Onboarding ─────────────────────────────────────────────
    def _onboard(self, config_path: Path) -> None:
        data = json.loads(config_path.read_text())
        SiteConfig.load()  # ensure row exists

        campaign, _ = Campaign.objects.get_or_create(
            name=data["campaign"]["name"],
            defaults={
                "product_description": data["campaign"].get("product_description", ""),
                "target_market": data["campaign"].get("target_market", ""),
                "booking_link": data["campaign"].get("booking_link", ""),
            },
        )
        enabled = data.get("sources") or settings.OUTREACH["SOURCES_ENABLED"]
        for kind in enabled:
            Source.objects.get_or_create(
                key=kind,
                defaults={
                    "kind": kind,
                    "campaign": campaign,
                    "interval_minutes": data.get("interval_minutes", 60),
                },
            )
        self.stdout.write(
            self.style.SUCCESS(f"Onboarded campaign '{campaign.name}' with sources: {enabled}")
        )

    def _interactive_onboard_if_empty(self) -> None:
        SiteConfig.load()
        if Campaign.objects.exists():
            return

        self.stdout.write(
            self.style.NOTICE(
                "\n=== Insulleads first-run onboarding ===\n"
                "Press Enter to accept the defaults.\n"
            )
        )
        name = input("Campaign name [Bay Area Insulation]: ") or "Bay Area Insulation"
        product = (
            input("Product description [Spray-foam + batt insulation services]: ")
            or "Spray-foam and batt insulation services for GCs in the Bay Area."
        )
        market = (
            input("Target market [GCs / remodelers / ADU builders in 9-county Bay Area]: ")
            or "General contractors and remodelers working on SFH/ADU projects "
            "in the 9-county Bay Area."
        )
        campaign = Campaign.objects.create(
            name=name, product_description=product, target_market=market
        )
        for kind in settings.OUTREACH["SOURCES_ENABLED"]:
            Source.objects.get_or_create(
                key=kind,
                defaults={"kind": kind, "campaign": campaign, "interval_minutes": 60},
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"Created campaign '{campaign.name}' with "
                f"{len(settings.OUTREACH['SOURCES_ENABLED'])} sources.\n"
            )
        )
