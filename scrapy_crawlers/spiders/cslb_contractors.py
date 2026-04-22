"""
CSLB (California State License Board) contractor lookup spider.

Given a set of license numbers, fetch the public "License Detail" page for
each one and emit a `LeadItem` per licensed contractor. Used to enrich the
contact directory with fresh license status / classification / business
address data — all public information served as plain HTML, so Scrapy's
stack (retries, AutoThrottle, HttpCache) is a much better fit than the
hand-rolled `requests` calls elsewhere in the project.

Spider arguments (comma-separated license numbers, all optional):
    -a license_numbers=123456,789012

If no argument is passed the spider is a no-op — the daemon can pre-populate
`spider_args` from the Django `Source.config` JSON when scheduling.
"""
from __future__ import annotations

import urllib.parse
from typing import Iterable

from scrapy.http import Response

from scrapy_crawlers.items import LeadItem
from scrapy_crawlers.spiders.base import BaseInsulleadsSpider


CSLB_DETAIL_URL = (
    "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx"
)


class CslbContractorsSpider(BaseInsulleadsSpider):
    name = "cslb_contractors"
    allowed_domains = ["cslb.ca.gov"]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    }

    def __init__(self, license_numbers: str = "", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.license_numbers = [
            n.strip() for n in (license_numbers or "").split(",") if n.strip()
        ]

    def start_requests(self) -> Iterable:
        if not self.license_numbers:
            self.logger.warning(
                "cslb_contractors: no license_numbers provided, nothing to crawl"
            )
            return
        for lic in self.license_numbers:
            qs = urllib.parse.urlencode({"LicNum": lic})
            yield self._request(f"{CSLB_DETAIL_URL}?{qs}", license_number=lic)

    def _request(self, url: str, *, license_number: str):
        from scrapy import Request

        return Request(
            url,
            callback=self.parse,
            cb_kwargs={"license_number": license_number},
            dont_filter=True,
        )

    def parse(self, response: Response, license_number: str):
        def cell(label: str) -> str:
            xp = (
                f"//td[normalize-space(text())={label!r}]/following-sibling::td[1]"
                "//text()"
            )
            parts = [t.strip() for t in response.xpath(xp).getall() if t.strip()]
            return " ".join(parts)

        business = cell("Business Information")
        address = cell("Mailing Address") or cell("Business Address")
        phone = cell("Business Phone Number") or cell("Phone Number")
        classification = cell("Classifications")
        status = cell("License Status") or cell("Primary Status")
        expiration = cell("Expiration Date")

        if not business:
            self.logger.info(
                "cslb_contractors: no business info for license %s", license_number
            )
            return

        item = LeadItem()
        item["external_id"] = self.build_external_id("cslb", license_number)
        item["title"] = f"CSLB {license_number}: {business}"[:200]
        item["address"] = address
        item["city"] = ""
        item["project_type"] = classification
        item["description"] = (
            f"License {license_number} — {status or 'unknown status'}"
            + (f", expires {expiration}" if expiration else "")
        )
        item["contact_name"] = ""
        item["contact_company"] = business
        item["contact_phone"] = phone
        item["contact_email"] = ""
        item["contact_source"] = "cslb"
        item["lead_score"] = 0
        item["raw"] = {
            "license_number": license_number,
            "business": business,
            "address": address,
            "phone": phone,
            "classification": classification,
            "status": status,
            "expiration": expiration,
            "url": response.url,
        }
        yield item
