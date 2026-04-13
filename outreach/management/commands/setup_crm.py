"""
`python manage.py setup_crm` — bootstrap a default SiteConfig + Campaign
so `make run` works on a fresh checkout.
"""
from django.conf import settings
from django.core.management.base import BaseCommand

from outreach.models import Campaign, SiteConfig, Source


class Command(BaseCommand):
    help = "Bootstrap SiteConfig + default campaign + sources."

    def handle(self, *args, **options) -> None:
        SiteConfig.load()
        campaign, created = Campaign.objects.get_or_create(
            name="Bay Area Insulation",
            defaults={
                "product_description": (
                    "Spray-foam and batt insulation services for GCs, "
                    "remodelers and ADU builders in the Bay Area."
                ),
                "target_market": (
                    "General contractors and remodelers working on SFH/ADU "
                    "projects in Alameda, Contra Costa, San Mateo, Santa Clara, "
                    "Marin, Sonoma, Napa, Solano and San Francisco counties."
                ),
            },
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"{'Created' if created else 'Using'} campaign '{campaign.name}'"
            )
        )

        for kind in settings.OUTREACH["SOURCES_ENABLED"]:
            src, s_created = Source.objects.get_or_create(
                key=kind,
                defaults={
                    "kind": kind,
                    "campaign": campaign,
                    "interval_minutes": 60,
                },
            )
            self.stdout.write(
                f"  {'+' if s_created else '='} source {src.key}"
            )
        self.stdout.write(self.style.SUCCESS("CRM bootstrap complete."))
