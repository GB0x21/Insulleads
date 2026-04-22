"""
Scrapy items produced by Insulleads spiders.

The field set mirrors what `outreach.pipeline.sources._normalize` accepts so
items can flow into the Django `Lead` model with zero transformation.
"""
from __future__ import annotations

import scrapy


class LeadItem(scrapy.Item):
    external_id = scrapy.Field()
    title = scrapy.Field()
    address = scrapy.Field()
    city = scrapy.Field()
    project_value = scrapy.Field()
    project_type = scrapy.Field()
    description = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    contact_name = scrapy.Field()
    contact_company = scrapy.Field()
    contact_phone = scrapy.Field()
    contact_email = scrapy.Field()
    contact_source = scrapy.Field()
    lead_score = scrapy.Field()
    raw = scrapy.Field()
