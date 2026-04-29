from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("outreach", "0009_alter_actionlog_action"),
    ]

    operations = [
        migrations.AlterField(
            model_name="source",
            name="kind",
            field=models.CharField(
                choices=[
                    ("permits", "Building permits"),
                    ("solar", "Solar installations"),
                    ("rodents", "Rodent 311 reports"),
                    ("flood", "NOAA flood"),
                    ("construction", "Active construction"),
                    ("deconstruction", "Deconstruction / demo"),
                    ("realestate", "Real-estate sales"),
                    ("energy", "Energy benchmarking"),
                    ("places", "Google Places"),
                    ("yelp", "Yelp contractors"),
                    ("thermal", "Thermal anomaly (Landsat)"),
                    ("csv", "CSV import"),
                    ("email_prospect", "Email prospect discovery"),
                    ("scrapy", "Scrapy spider"),
                    ("contract_bids", "Contract bids (multi-provider)"),
                    ("web_crawler", "Web crawler (crawl4ai)"),
                    ("dins", "Cal Fire DINS (post-wildfire rebuilds)"),
                    ("hud_multifamily", "HUD multifamily / LIHTC"),
                    ("shovels", "Shovels.ai (national permits)"),
                ],
                max_length=32,
            ),
        ),
    ]
