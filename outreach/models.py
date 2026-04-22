"""
Django ORM models — Insulleads outreach pipeline.

Structure adapted from OpenOutreach (`linkedin.models`) but retargeted from
LinkedIn prospects to public-data construction leads (permits, solar,
deconstruction, ...). Lead lifecycle mirrors the OpenOutreach stage machine:

    DISCOVERED -> QUALIFIED -> READY_TO_CONTACT -> CONTACTED -> REPLIED -> WON / LOST
"""
from __future__ import annotations

from datetime import timedelta

from django.db import models
from django.utils import timezone


# ─── Singleton config ────────────────────────────────────────────────
class SiteConfig(models.Model):
    """Global configuration singleton (one row)."""

    llm_api_key = models.CharField(max_length=512, blank=True, default="")
    llm_model = models.CharField(max_length=128, default="claude-opus-4-6")
    llm_base_url = models.CharField(max_length=512, blank=True, default="")
    telegram_bot_token = models.CharField(max_length=256, blank=True, default="")
    telegram_chat_id = models.CharField(max_length=64, blank=True, default="")
    sendgrid_api_key = models.CharField(max_length=256, blank=True, default="")
    twilio_sid = models.CharField(max_length=128, blank=True, default="")
    twilio_token = models.CharField(max_length=256, blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Site configuration"
        verbose_name_plural = "Site configuration"

    def save(self, *args, **kwargs):
        self.pk = 1  # enforce singleton
        super().save(*args, **kwargs)

    @classmethod
    def load(cls) -> "SiteConfig":
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj

    def __str__(self) -> str:
        return "Site configuration"


# ─── Campaign ────────────────────────────────────────────────────────
class Campaign(models.Model):
    """A product + target-market definition. Drives discovery + ranking."""

    name = models.CharField(max_length=200, unique=True)
    product_description = models.TextField(
        help_text="What are you selling? (e.g. spray-foam insulation services)",
    )
    target_market = models.TextField(
        help_text="Who are you targeting? (e.g. GCs remodeling SFH in Bay Area)",
    )
    booking_link = models.URLField(blank=True, default="")

    # Daily budgets / rate limits
    max_outreach_per_day = models.PositiveIntegerField(default=200)
    max_outreach_per_week = models.PositiveIntegerField(default=1000)

    # ML model state (pickled Bayesian GP regressor — legacy fallback)
    model_blob = models.BinaryField(blank=True, null=True)
    model_trained_at = models.DateTimeField(blank=True, null=True)
    positive_labels = models.PositiveIntegerField(default=0)
    negative_labels = models.PositiveIntegerField(default=0)

    # LightGBM qualifier — Phase 2. Becomes primary once there are
    # ≥ MIN_LABELS_FOR_TRAINING labels; GP stays available as fallback.
    lgbm_model_blob = models.BinaryField(blank=True, null=True)
    lgbm_trained_at = models.DateTimeField(blank=True, null=True)
    lgbm_feature_importance = models.JSONField(default=dict, blank=True)

    # LLM layer toggles (see outreach/llm/)
    class Tone(models.TextChoices):
        PROFESSIONAL = "professional", "Professional"
        FRIENDLY = "friendly", "Friendly"
        URGENT = "urgent", "Urgent"

    outreach_tone = models.CharField(
        max_length=16,
        choices=Tone.choices,
        default=Tone.PROFESSIONAL,
    )
    llm_enricher_enabled = models.BooleanField(default=True)
    llm_qualifier_enabled = models.BooleanField(default=True)
    llm_writer_enabled = models.BooleanField(default=True)

    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return self.name


# ─── Source ──────────────────────────────────────────────────────────
class Source(models.Model):
    """
    A discovery source. Wraps an Insulleads agent (permits, solar, ...)
    or a manual CSV import.
    """

    KIND_CHOICES = [
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
    ]

    key = models.CharField(max_length=64, unique=True)
    kind = models.CharField(max_length=32, choices=KIND_CHOICES)
    campaign = models.ForeignKey(
        Campaign, on_delete=models.CASCADE, related_name="sources"
    )
    interval_minutes = models.PositiveIntegerField(default=60)
    last_run_at = models.DateTimeField(blank=True, null=True)
    last_error = models.TextField(blank=True, default="")
    enabled = models.BooleanField(default=True)
    config = models.JSONField(default=dict, blank=True)

    def __str__(self) -> str:
        return f"{self.get_kind_display()} ({self.key})"


# ─── Lead ────────────────────────────────────────────────────────────
class Lead(models.Model):
    """A construction lead produced by a Source."""

    class Stage(models.TextChoices):
        DISCOVERED = "DISCOVERED", "Discovered"
        QUALIFIED = "QUALIFIED", "Qualified"
        READY_TO_CONTACT = "READY_TO_CONTACT", "Ready to contact"
        CONTACTED = "CONTACTED", "Contacted"
        REPLIED = "REPLIED", "Replied"
        WON = "WON", "Won"
        LOST = "LOST", "Lost"

    # Identity
    external_id = models.CharField(max_length=256, db_index=True)
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="leads")
    campaign = models.ForeignKey(
        Campaign, on_delete=models.CASCADE, related_name="leads"
    )

    # Payload
    title = models.CharField(max_length=512)
    address = models.CharField(max_length=512, blank=True, default="")
    city = models.CharField(max_length=128, blank=True, default="")
    project_value = models.DecimalField(
        max_digits=12, decimal_places=2, blank=True, null=True
    )
    project_type = models.CharField(max_length=128, blank=True, default="")
    description = models.TextField(blank=True, default="")
    latitude = models.FloatField(blank=True, null=True)
    longitude = models.FloatField(blank=True, null=True)

    # Contact (from fuzzy match against contacts/ CSVs, CSLB, Hunter, Apollo)
    contact_name = models.CharField(max_length=256, blank=True, default="")
    contact_company = models.CharField(max_length=256, blank=True, default="")
    contact_phone = models.CharField(max_length=64, blank=True, default="")
    contact_email = models.CharField(max_length=256, blank=True, default="")
    contact_source = models.CharField(max_length=128, blank=True, default="")

    # ML
    embedding = models.BinaryField(blank=True, null=True)
    qualification_score = models.FloatField(blank=True, null=True)
    qualification_variance = models.FloatField(blank=True, null=True)
    lead_score = models.PositiveIntegerField(default=0)  # 0-100 heuristic

    # Thermal engine (Phase 1) — populated by the thermal agent.
    thermal_anomaly_k = models.FloatField(blank=True, null=True)
    building_footprint_id = models.CharField(max_length=64, blank=True, default="")

    # State machine
    stage = models.CharField(
        max_length=32, choices=Stage.choices, default=Stage.DISCOVERED
    )
    stage_changed_at = models.DateTimeField(default=timezone.now)
    raw = models.JSONField(default=dict, blank=True)

    # Email prospect outreach tracking
    email_prospect_sent_at = models.DateTimeField(blank=True, null=True)
    email_prospect_subject = models.CharField(max_length=256, blank=True, default="")

    # LLM layer output cache (outreach/llm/)
    enrichment_log = models.JSONField(default=dict, blank=True)
    llm_qualification_reason = models.TextField(blank=True, default="")
    llm_outreach_body = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [("source", "external_id")]
        indexes = [
            models.Index(fields=["stage", "-qualification_score"]),
            models.Index(fields=["campaign", "stage"]),
        ]

    def __str__(self) -> str:
        return f"{self.title} [{self.stage}]"

    def advance(self, new_stage: str) -> None:
        self.stage = new_stage
        self.stage_changed_at = timezone.now()
        self.save(update_fields=["stage", "stage_changed_at", "updated_at"])


# ─── ActionLog ───────────────────────────────────────────────────────
class ActionLog(models.Model):
    """Every outbound action the daemon takes against a Lead."""

    class Action(models.TextChoices):
        DISCOVERED = "discovered", "Discovered"
        QUALIFIED = "qualified", "Qualified"
        TELEGRAM = "telegram", "Telegram sent"
        EMAIL = "email", "Email sent"
        EMAIL_PROSPECT = "email_prospect", "Personalized email sent to prospect"
        WHATSAPP = "whatsapp", "WhatsApp sent"
        REPLY = "reply", "Reply received"
        NOTE = "note", "Manual note"

    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name="actions")
    campaign = models.ForeignKey(
        Campaign, on_delete=models.CASCADE, related_name="actions"
    )
    action = models.CharField(max_length=32, choices=Action.choices)
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        indexes = [models.Index(fields=["campaign", "action", "-created_at"])]


# ─── Task queue ──────────────────────────────────────────────────────
class TaskQuerySet(models.QuerySet):
    def ready(self):
        now = timezone.now()
        return self.filter(status=Task.Status.PENDING, scheduled_at__lte=now)

    def seconds_until_next(self) -> int:
        nxt = (
            self.filter(status=Task.Status.PENDING)
            .order_by("scheduled_at")
            .first()
        )
        if not nxt:
            return 60
        delta = (nxt.scheduled_at - timezone.now()).total_seconds()
        return max(1, int(delta))


class Task(models.Model):
    """Daemon task queue entry — adapted from OpenOutreach's Task model."""

    class Type(models.TextChoices):
        DISCOVER = "discover", "Run discovery source"
        ENRICH = "enrich", "LLM contact enrichment"
        QUALIFY = "qualify", "Qualify discovered leads"
        OUTREACH = "outreach", "Send outreach for a lead"
        FOLLOW_UP = "follow_up", "Follow up after N days"
        EMAIL_PROSPECT = "email_prospect", "Send personalized email to prospect"

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"

    type = models.CharField(max_length=32, choices=Type.choices)
    status = models.CharField(
        max_length=16, choices=Status.choices, default=Status.PENDING
    )
    campaign = models.ForeignKey(
        Campaign, on_delete=models.CASCADE, related_name="tasks"
    )
    source = models.ForeignKey(
        Source, on_delete=models.CASCADE, null=True, blank=True, related_name="tasks"
    )
    lead = models.ForeignKey(
        Lead, on_delete=models.CASCADE, null=True, blank=True, related_name="tasks"
    )
    payload = models.JSONField(default=dict, blank=True)

    scheduled_at = models.DateTimeField(default=timezone.now, db_index=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    objects = TaskQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["status", "scheduled_at"]),
            models.Index(fields=["type", "status"]),
        ]

    def mark_running(self) -> None:
        self.status = self.Status.RUNNING
        self.started_at = timezone.now()
        self.attempts += 1
        self.save(update_fields=["status", "started_at", "attempts"])

    def mark_done(self) -> None:
        self.status = self.Status.COMPLETED
        self.finished_at = timezone.now()
        self.save(update_fields=["status", "finished_at"])

    def mark_failed(self, err: str) -> None:
        self.status = self.Status.FAILED
        self.finished_at = timezone.now()
        self.last_error = err[:5000]
        self.save(update_fields=["status", "finished_at", "last_error"])

    def reschedule(self, minutes: int) -> None:
        self.status = self.Status.PENDING
        self.scheduled_at = timezone.now() + timedelta(minutes=minutes)
        self.started_at = None
        self.finished_at = None
        self.save(
            update_fields=["status", "scheduled_at", "started_at", "finished_at"]
        )

    def __str__(self) -> str:
        return f"{self.type}#{self.pk} [{self.status}]"
