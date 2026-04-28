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

        # Seed a disabled example for the crawl4ai-backed web_crawler kind so
        # users can clone the row, swap urls/schema and enable it. Disabled
        # by default — we don't ship a default target site.
        example_key = "web_crawler_example"
        web_src, w_created = Source.objects.get_or_create(
            key=example_key,
            defaults={
                "kind": "web_crawler",
                "campaign": campaign,
                "interval_minutes": 360,
                "enabled": False,
                "config": {
                    "urls": ["https://example.com/contractors"],
                    "schema": {
                        "name": "Contractors",
                        "baseSelector": ".contractor-card",
                        "fields": [
                            {"name": "business_name", "selector": "h3",
                             "type": "text"},
                            {"name": "address", "selector": ".address",
                             "type": "text"},
                            {"name": "phone", "selector": ".phone",
                             "type": "text"},
                            {"name": "website", "selector": "a",
                             "type": "attribute", "attribute": "href"},
                        ],
                    },
                    "city_default": "San Francisco",
                    "http_only": True,
                },
            },
        )
        self.stdout.write(
            f"  {'+' if w_created else '='} source {web_src.key} "
            f"(disabled — edit config and flip `enabled` to use)"
        )

        self.stdout.write(self.style.SUCCESS("CRM bootstrap complete."))
