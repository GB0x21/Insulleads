"""
Curated catalog of real Bay Area lead-gen target sites for the
crawl4ai-backed `WebCrawlerAgent` (`Source.kind="web_crawler"`).

Each entry seeds one disabled `Source` row in the CRM. The user is
expected to (1) verify the URL still works, (2) inspect the page with
`python manage.py test_web_crawler --inspect --url <URL>` and adjust
the CSS schema selectors to match the live HTML, then (3) flip
`enabled=True` from the Django admin or shell.

Selection criteria:
  - Public directory of professionals working in / near insulation
    (energy upgrades, remodeling, weatherization, green building).
  - Bay Area focus (or filterable to Bay Area).
  - Not already covered by an existing agent (CSLB has a Scrapy spider;
    permits/solar/rodents/etc. have their own Socrata-backed agents;
    Yelp/Google Places have API-based agents).
  - No API available — that's why crawl4ai is the right tool.

Schemas use `JsonCssExtractionStrategy` syntax. `# VERIFY` markers flag
selectors that need to be confirmed against the live HTML before the
source is enabled — site markup changes regularly and the sandbox where
this catalog was curated could not introspect the pages directly.
"""
from __future__ import annotations

from typing import Any

# ─── Schema fragments ────────────────────────────────────────────────
# Common field shapes shared across multiple targets, kept as constants
# so a markup change in one place is easy to fix everywhere.

_NAME = {"name": "business_name", "selector": "h2, h3, .name, .title", "type": "text"}
_PHONE = {"name": "phone", "selector": ".phone, .tel, a[href^='tel:']", "type": "text"}
_ADDR = {"name": "address", "selector": ".address, .addr, .location", "type": "text"}
_EMAIL = {"name": "email", "selector": "a[href^='mailto:']", "type": "attribute",
          "attribute": "href"}
_WEB = {"name": "website", "selector": "a.website, a.url, a[rel='external']",
        "type": "attribute", "attribute": "href"}


# ─── Catalog ─────────────────────────────────────────────────────────
WEB_CRAWLER_TARGETS: list[dict[str, Any]] = [
    {
        # BayREN Home+ participating contractors. Pre-vetted Bay Area
        # contractors doing energy-upgrade work — direct cross-sell for
        # insulation. Run by the Bay Area Regional Energy Network (a
        # public-sector collaboration of the nine Bay Area counties).
        "key": "bayren_home_plus",
        "interval_minutes": 1440,  # daily — the list changes slowly
        "rationale": (
            "BayREN Home+ pre-vetted energy-upgrade contractors. Direct "
            "cross-sell for insulation work."
        ),
        "config": {
            "urls": [
                "https://www.bayren.org/find-contractor",
                "https://www.bayren.org/single-family/find-contractor",
            ],
            "schema": {
                "name": "BayRENContractors",
                # VERIFY: BayREN's directory is rendered with a
                # third-party widget — once enabled, run `--inspect` to
                # discover the true card class.
                "baseSelector": ".contractor-card, .contractor-item, .pro-card",
                "fields": [_NAME, _PHONE, _ADDR, _EMAIL, _WEB,
                           {"name": "city", "selector": ".city, .location-city",
                            "type": "text"}],
            },
            "city_default": "",
            "http_only": False,  # likely JS-rendered widget — needs Chromium
        },
    },
    {
        # NARI San Francisco Bay chapter — remodelers, GCs and trade
        # professionals. Insulation is a frequent line item in remodeling
        # work, so chapter members are a natural referral channel.
        "key": "nari_sf_bay",
        "interval_minutes": 1440,
        "rationale": (
            "NARI SF Bay member directory — remodelers / GCs who routinely "
            "spec insulation."
        ),
        "config": {
            "urls": ["https://www.narisfbay.org/find-a-pro/"],
            "schema": {
                "name": "NARIMembers",
                # VERIFY
                "baseSelector": ".member-card, article.member, .directory-item",
                "fields": [_NAME, _PHONE, _ADDR, _EMAIL, _WEB,
                           {"name": "category", "selector": ".specialties, .category",
                            "type": "text"}],
            },
            "city_default": "",
            "http_only": True,
        },
    },
    {
        # Build It Green professional directory — GreenPoint Rated
        # contractors and raters. Heavy overlap with the kind of
        # remodels that include insulation upgrades.
        "key": "build_it_green",
        "interval_minutes": 1440,
        "rationale": (
            "Build It Green / GreenPoint Rated professionals — green "
            "building contractors and energy raters."
        ),
        "config": {
            "urls": ["https://www.builditgreen.org/contractors-directory"],
            "schema": {
                "name": "BIGProfessionals",
                # VERIFY
                "baseSelector": ".contractor, .pro-listing, .directory-row",
                "fields": [_NAME, _PHONE, _ADDR, _EMAIL, _WEB,
                           {"name": "category", "selector": ".certification, .type",
                            "type": "text"}],
            },
            "city_default": "",
            "http_only": True,
        },
    },
    {
        # Energy Upgrade California — statewide contractor locator.
        # Filter post-fetch by Bay Area cities.
        "key": "energy_upgrade_ca",
        "interval_minutes": 1440,
        "rationale": (
            "Energy Upgrade California participating contractors — "
            "weatherization & whole-home energy upgrades."
        ),
        "config": {
            "urls": ["https://www.energyupgradeca.org/find-a-pro/"],
            "schema": {
                "name": "EUCContractors",
                # VERIFY
                "baseSelector": ".contractor-card, .pro-card, li.contractor",
                "fields": [_NAME, _PHONE, _ADDR, _EMAIL, _WEB,
                           {"name": "city", "selector": ".city", "type": "text"}],
            },
            "city_default": "",
            "http_only": False,  # heavy JS — use Playwright
        },
    },
    {
        # Diamond Certified — Bay Area focused, quality-vetted local
        # service businesses. Filter by insulation-adjacent categories
        # (HVAC, weatherization, roofing, remodeling).
        "key": "diamond_certified_insulation",
        "interval_minutes": 1440,
        "rationale": (
            "Diamond Certified Bay Area insulation / HVAC / weatherization "
            "contractors — high-quality vetted local businesses."
        ),
        "config": {
            "urls": [
                "https://www.diamondcertified.org/services/insulation-contractor/",
                "https://www.diamondcertified.org/services/heating-and-air-conditioning-contractor/",
                "https://www.diamondcertified.org/services/weatherproofing-and-weather-stripping/",
            ],
            "schema": {
                "name": "DiamondCertified",
                # VERIFY
                "baseSelector": ".company-card, article.company, .listing",
                "fields": [_NAME, _PHONE, _ADDR, _EMAIL, _WEB,
                           {"name": "city", "selector": ".city, .company-city",
                            "type": "text"},
                           {"name": "category",
                            "selector": ".service-type, .category", "type": "text"}],
            },
            "city_default": "",
            "http_only": True,
        },
    },
]


def iter_targets():
    """Yield (key, kind, defaults_for_get_or_create) tuples for setup_crm."""
    for t in WEB_CRAWLER_TARGETS:
        yield (
            t["key"],
            "web_crawler",
            {
                "interval_minutes": t.get("interval_minutes", 1440),
                "enabled": False,
                "config": t["config"],
            },
            t.get("rationale", ""),
        )
