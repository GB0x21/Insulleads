from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("outreach", "0006_source_scrapy_kind"),
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
                ],
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="task",
            name="type",
            field=models.CharField(
                choices=[
                    ("discover", "Run discovery source"),
                    ("enrich", "LLM contact enrichment"),
                    ("qualify", "Qualify discovered leads"),
                    ("outreach", "Send outreach for a lead"),
                    ("follow_up", "Follow up after N days"),
                    ("email_prospect", "Send personalized email to prospect"),
                    ("discover_contracts", "Discover open bids / RFPs"),
                    ("analyze_contract", "Multi-agent contract analysis"),
                ],
                max_length=32,
            ),
        ),
        migrations.CreateModel(
            name="Contract",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("external_id", models.CharField(db_index=True, max_length=200)),
                ("provider", models.CharField(blank=True, default="", max_length=64)),
                ("source_url", models.URLField(blank=True, default="", max_length=1000)),
                ("title", models.CharField(blank=True, default="", max_length=500)),
                ("agency", models.CharField(blank=True, default="", max_length=300)),
                ("deadline", models.DateTimeField(blank=True, null=True)),
                (
                    "estimated_value_usd",
                    models.DecimalField(
                        blank=True, decimal_places=2, max_digits=14, null=True
                    ),
                ),
                ("file_path", models.CharField(blank=True, default="", max_length=500)),
                (
                    "file_hash",
                    models.CharField(
                        blank=True, db_index=True, default="", max_length=64
                    ),
                ),
                ("file_size_bytes", models.BigIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("discovered", "Discovered"),
                            ("downloaded", "Downloaded"),
                            ("analyzing", "Analyzing"),
                            ("completed", "Completed"),
                            ("failed", "Failed"),
                        ],
                        default="discovered",
                        max_length=20,
                    ),
                ),
                ("scorecard", models.JSONField(blank=True, default=dict)),
                ("redlines", models.JSONField(blank=True, default=list)),
                (
                    "recommendation",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("sign", "Sign as-is"),
                            ("negotiate", "Negotiate redlines"),
                            ("reject", "Reject"),
                        ],
                        default="",
                        max_length=20,
                    ),
                ),
                (
                    "recommendation_reason",
                    models.TextField(blank=True, default=""),
                ),
                ("raw_analysis", models.JSONField(blank=True, default=dict)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("error_message", models.TextField(blank=True, default="")),
                ("discovered_at", models.DateTimeField(auto_now_add=True)),
                ("analyzed_at", models.DateTimeField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "lead",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="contracts",
                        to="outreach.lead",
                    ),
                ),
                (
                    "source",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="contracts",
                        to="outreach.source",
                    ),
                ),
            ],
            options={
                "unique_together": {("source", "external_id")},
                "indexes": [
                    models.Index(
                        fields=["status", "-discovered_at"],
                        name="outreach_co_status_b6f7c7_idx",
                    ),
                    models.Index(
                        fields=["recommendation", "-analyzed_at"],
                        name="outreach_co_recomme_00ab17_idx",
                    ),
                ],
            },
        ),
    ]
