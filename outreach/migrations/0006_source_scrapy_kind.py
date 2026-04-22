from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("outreach", "0005_email_prospect_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="source",
            name="config",
            field=models.JSONField(blank=True, default=dict),
        ),
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
                ],
                max_length=32,
            ),
        ),
    ]
