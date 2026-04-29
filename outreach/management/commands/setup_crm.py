"""
`python manage.py setup_crm` — bootstrap a default SiteConfig + Campaign
so `make run` works on a fresh checkout.
"""
import os

from django.conf import settings
from django.core.management.base import BaseCommand

from outreach.models import Campaign, SiteConfig, Source
from outreach.seed_data.web_crawler_targets import iter_targets as _web_crawler_targets

# Default polling intervals (minutes) per source kind.  Slow-moving
# public datasets run once-daily; high-churn permit feeds run hourly.
_DEFAULT_INTERVALS: dict[str, int] = {
    "permits":         60,
    "solar":           60,
    "construction":    60,
    "deconstruction":  120,
    "rodents":         120,
    "realestate":      120,
    "flood":           30,
    "energy":          360,
    "places":          1440,
    "yelp":            1440,
    "thermal":         1440,
    "dins":            1440,  # Cal Fire DINS — wildfire data is stable; daily is plenty
    "hud_multifamily": 1440,  # HUD/LIHTC — rehab windows span years; daily refresh
    "shovels":         1440,  # Shovels.ai — broad permit sweep; daily avoids quota burn
}


def _interval(kind: str) -> int:
    """Return the polling interval for *kind*, honouring INTERVAL_<KIND> env overrides."""
    env_key = f"INTERVAL_{kind.upper()}"
    raw = os.getenv(env_key, "")
    if raw.strip().isdigit():
        return int(raw.strip())
    return _DEFAULT_INTERVALS.get(kind, 60)


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
                    "interval_minutes": _interval(kind),
                },
            )
            self.stdout.write(
                f"  {'+' if s_created else '='} source {src.key}"
            )

        # Seed the curated catalog of crawl4ai targets (Bay Area
        # contractor / energy-upgrade directories). All disabled by
        # default — selectors should be verified with
        # `manage.py test_web_crawler --inspect --url <url>` first.
        for key, kind, defaults, rationale in _web_crawler_targets():
            src, c_created = Source.objects.get_or_create(
                key=key,
                defaults={"kind": kind, "campaign": campaign, **defaults},
            )
            self.stdout.write(
                f"  {'+' if c_created else '='} source {src.key} "
                f"({'disabled' if not src.enabled else 'enabled'}) — {rationale}"
            )

        self.stdout.write(self.style.SUCCESS("CRM bootstrap complete."))
