"""
Offline parse() test for the CSLB demo spider.

We build a synthetic HTML response that mimics the table layout CSLB returns
for a License Detail page and assert the spider emits a correctly shaped
`LeadItem`. No real HTTP is made.
"""
from __future__ import annotations

from scrapy.http import HtmlResponse, Request

from scrapy_crawlers.spiders.cslb_contractors import (
    CSLB_DETAIL_URL,
    CslbContractorsSpider,
)


FIXTURE_HTML = """
<html><body>
<table>
  <tr><td>Business Information</td><td>Acme Insulation Inc</td></tr>
  <tr><td>Mailing Address</td><td>123 Market St<br>San Francisco, CA 94103</td></tr>
  <tr><td>Business Phone Number</td><td>(415) 555-1212</td></tr>
  <tr><td>Classifications</td><td>C-2 - INSULATION AND ACOUSTICAL</td></tr>
  <tr><td>License Status</td><td>ACTIVE (This license is current and active)</td></tr>
  <tr><td>Expiration Date</td><td>12/31/2026</td></tr>
</table>
</body></html>
"""


def _fake_response(license_number: str) -> HtmlResponse:
    url = f"{CSLB_DETAIL_URL}?LicNum={license_number}"
    return HtmlResponse(
        url=url,
        request=Request(url),
        body=FIXTURE_HTML.encode("utf-8"),
        encoding="utf-8",
    )


def test_cslb_parse_emits_lead_item():
    spider = CslbContractorsSpider(license_numbers="999999")
    response = _fake_response("999999")

    items = list(spider.parse(response, license_number="999999"))

    assert len(items) == 1
    item = items[0]
    assert item["external_id"] == spider.build_external_id("cslb", "999999")
    assert item["contact_company"] == "Acme Insulation Inc"
    assert "Market St" in item["address"]
    assert item["contact_phone"] == "(415) 555-1212"
    assert item["project_type"].startswith("C-2")
    assert item["contact_source"] == "cslb"
    assert "ACTIVE" in item["description"]
    assert item["raw"]["license_number"] == "999999"


def test_cslb_parse_skips_empty_page():
    spider = CslbContractorsSpider(license_numbers="000000")
    url = f"{CSLB_DETAIL_URL}?LicNum=000000"
    response = HtmlResponse(
        url=url, request=Request(url), body=b"<html><body></body></html>"
    )
    assert list(spider.parse(response, license_number="000000")) == []


def test_cslb_spider_with_no_license_numbers_yields_no_requests():
    spider = CslbContractorsSpider()
    assert list(spider.start_requests()) == []
