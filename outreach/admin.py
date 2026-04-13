from django.contrib import admin

from .models import ActionLog, Campaign, Lead, SiteConfig, Source, Task


@admin.register(SiteConfig)
class SiteConfigAdmin(admin.ModelAdmin):
    list_display = ("id", "llm_model", "updated_at")


@admin.register(Campaign)
class CampaignAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "is_active",
        "outreach_tone",
        "llm_enricher_enabled",
        "llm_qualifier_enabled",
        "llm_writer_enabled",
        "positive_labels",
        "negative_labels",
        "model_trained_at",
    )
    search_fields = ("name",)
    list_filter = (
        "is_active",
        "outreach_tone",
        "llm_enricher_enabled",
        "llm_qualifier_enabled",
        "llm_writer_enabled",
    )


@admin.register(Source)
class SourceAdmin(admin.ModelAdmin):
    list_display = ("key", "kind", "campaign", "enabled", "last_run_at")
    list_filter = ("kind", "enabled", "campaign")
    search_fields = ("key",)


@admin.register(Lead)
class LeadAdmin(admin.ModelAdmin):
    list_display = (
        "title",
        "city",
        "stage",
        "lead_score",
        "qualification_score",
        "source",
        "created_at",
    )
    list_filter = ("stage", "source", "campaign", "city")
    search_fields = (
        "title",
        "address",
        "city",
        "contact_name",
        "contact_company",
        "contact_email",
        "external_id",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "stage_changed_at",
        "raw",
        "enrichment_log",
        "llm_qualification_reason",
        "llm_outreach_body",
    )


@admin.register(ActionLog)
class ActionLogAdmin(admin.ModelAdmin):
    list_display = ("lead", "action", "campaign", "created_at")
    list_filter = ("action", "campaign")
    search_fields = ("lead__title",)


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "type",
        "status",
        "campaign",
        "scheduled_at",
        "attempts",
    )
    list_filter = ("type", "status", "campaign")
    ordering = ("-scheduled_at",)
