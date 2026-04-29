"""
`python manage.py cslb_index` — inspect / smoke-test the local CSLB
bulk index.

The index lives at ``data/cslb_bulk.csv`` (or ``$CSLB_BULK_PATH``).
Request the extract from CSLB at
https://www.cslb.ca.gov/About_Us/Library/Data_Requests/ — they email
a download link within ~2 business days. Drop the CSV in the configured
path and the rest of the project picks it up on next boot.

Examples:
    python manage.py cslb_index --info
    python manage.py cslb_index --license 1098765
    python manage.py cslb_index --company "Speed Construction"
"""
from __future__ import annotations

from django.core.management.base import BaseCommand

from utils import cslb_bulk


class Command(BaseCommand):
    help = "Inspect the local CSLB bulk index used by permits_agent."

    def add_arguments(self, parser):
        parser.add_argument("--info", action="store_true",
                            help="Show whether the index is loaded.")
        parser.add_argument("--license", help="Lookup by license number.")
        parser.add_argument("--company", help="Lookup by company name.")
        parser.add_argument("--reload", action="store_true",
                            help="Drop the in-memory cache and re-read the file.")

    def handle(self, *args, **opts):
        if opts["reload"]:
            cslb_bulk.reset_cache()

        if opts["info"] or not (opts["license"] or opts["company"]):
            data = cslb_bulk.info()
            for k, v in data.items():
                self.stdout.write(f"  {k:14}  {v}")
            if not data["available"]:
                self.stdout.write(self.style.WARNING(
                    "\nIndex not loaded. Request the extract from CSLB and "
                    "save it as data/cslb_bulk.csv (or set CSLB_BULK_PATH)."
                ))
            return

        rec = cslb_bulk.lookup(
            license_number=opts["license"],
            company_name=opts["company"],
        )
        if not rec:
            self.stdout.write(self.style.WARNING("not found"))
            return
        for k, v in rec.items():
            self.stdout.write(f"  {k:18}  {v or '—'}")
