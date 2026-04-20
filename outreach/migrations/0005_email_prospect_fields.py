from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("outreach", "0004_lead_building_footprint_id_lead_thermal_anomaly_k_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="email_prospect_sent_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="email_prospect_subject",
            field=models.CharField(blank=True, default="", max_length=256),
        ),
    ]
