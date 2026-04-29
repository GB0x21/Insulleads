"""
`python manage.py discover_socrata_permits` — find permit datasets
across every Socrata-hosted open data portal.

Usage:
    # All CA permit datasets, top 25 by row count
    python manage.py discover_socrata_permits --state CA --limit 25

    # Show a sample row from each hit so you can map column names
    # before adding the dataset to agents/permits_agent.py
    python manage.py discover_socrata_permits --state CA --sample

    # Show only datasets on domains permits_agent doesn't know about
    python manage.py discover_socrata_permits --state CA --new-only

The output is meant to be copy-pasteable into
`agents/permits_agent.py:_build_sources` after you eyeball the column
names. The command itself doesn't mutate the project — it's pure
discovery.
"""
from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from utils.socrata_discovery import discover, sample_rows


# Domains already wired into agents/permits_agent.py — the command
# can suppress them with --new-only.
KNOWN_DOMAINS = {
    "data.sfgov.org",
    "data.sanjoseca.gov",
    "data.sunnyvale.ca.gov",
    "data.santa-clara.ca.gov",
    "data.ci.richmond.ca.us",
    "www.fremont.gov",
    "hayward.permitportal.us",
    "data.oaklandca.gov",
    "data.cityofberkeley.info",
    "data.cityofpaloalto.org",
    "data.mountainview.gov",
    "data.redwoodcity.org",
    "data.dalycity.org",
    "data.cityofsanmateo.org",
    "data.concordca.gov",
    "data.walnut-creek.org",
    "data.contracosta.gov",
    "data.acgov.org",
    "data.alamedaca.gov",
    "data.smcgov.org",
    "data.solanocounty.com",
    "data.cityofvallejo.net",
    "data.marincounty.org",
    "data.countyofnapa.org",
    "data.sonomacounty.ca.gov",
    "data.sjgov.org",
}


class Command(BaseCommand):
    help = "Discover Socrata permit datasets across every CA jurisdiction."

    def add_arguments(self, parser):
        parser.add_argument("--state", default="CA",
                            help="Two-letter state filter (default CA)")
        parser.add_argument("--query", default="building permits",
                            help="Catalog search query (default 'building permits')")
        parser.add_argument("--limit", type=int, default=25,
                            help="Max hits to return (default 25)")
        parser.add_argument("--new-only", action="store_true",
                            help="Suppress datasets on domains already used by permits_agent")
        parser.add_argument("--sample", action="store_true",
                            help="Print 1 sample row from each hit so you can map column names")

    def handle(self, *args, **opts):
        hits = discover(
            query=opts["query"],
            state=opts["state"],
            limit=opts["limit"],
        )

        if opts["new_only"]:
            hits = [h for h in hits if h["domain"] not in KNOWN_DOMAINS]

        if not hits:
            self.stdout.write(self.style.WARNING("no datasets matched"))
            return

        for h in hits:
            mark = " (NEW)" if h["domain"] not in KNOWN_DOMAINS else ""
            self.stdout.write(self.style.SUCCESS(
                f"\n{h['name']}{mark}"
            ))
            self.stdout.write(f"  domain   : {h['domain']}")
            self.stdout.write(f"  id       : {h['id']}")
            self.stdout.write(f"  rows     : {h['rows']:,}")
            self.stdout.write(f"  updated  : {h['updated']}")
            self.stdout.write(f"  url      : {h['url']}")
            self.stdout.write(f"  json_url : {h['json_url']}")

            if opts["sample"]:
                rows = sample_rows(h["json_url"], limit=1)
                if rows:
                    sample = rows[0]
                    keys = ", ".join(sorted(sample.keys())[:25])
                    self.stdout.write(f"  fields   : {keys}{' ...' if len(sample) > 25 else ''}")
                    snippet = json.dumps(
                        {k: sample[k] for k in list(sample.keys())[:6]},
                        indent=2, default=str,
                    )
                    self.stdout.write("  sample   :")
                    for line in snippet.splitlines():
                        self.stdout.write(f"             {line}")
                else:
                    self.stdout.write("  sample   : <empty / fetch failed>")

        self.stdout.write(self.style.SUCCESS(f"\n{len(hits)} dataset(s) listed"))
