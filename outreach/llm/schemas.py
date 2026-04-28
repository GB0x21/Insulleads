"""
Pydantic output models for the LLM layer.

Each model is the `output_type` of a `pydantic_ai.Agent` so we get typed,
validated structured outputs instead of hand-rolled JSON parsing. Field
defaults are intentionally permissive (empty strings / lists rather than
required) so the LLM can omit information it couldn't verify and the
adapter still gets a well-formed object back.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ─── Lead-stage outputs ──────────────────────────────────────────────


class EnrichmentResult(BaseModel):
    """Contact-enrichment output. All fields optional; omit what you
    couldn't verify rather than guessing."""

    website: str = ""
    contact_phone: str = ""
    contact_email: str = ""
    contact_name: str = ""
    contact_company: str = ""
    cslb_license: str = ""
    linkedin: str = ""
    notes: str = ""


class QualificationResult(BaseModel):
    """Cold-start lead score (used until the GP qualifier has >=2 labels)."""

    score: float = Field(ge=0.0, le=1.0)
    reason: str = ""


class OutreachMessage(BaseModel):
    """Single Telegram-bound outreach body."""

    body: str


class EmailDraft(BaseModel):
    """Cold-prospect email — subject + plain-text body."""

    subject: str
    body: str


# ─── Contract-analyzer outputs ───────────────────────────────────────


Severity = Literal["low", "med", "high"]
Recommendation = Literal["sign", "negotiate", "reject"]


class Redline(BaseModel):
    """One problematic clause flagged by a sub-agent or carried into
    the final synthesis."""

    category: str = ""
    clause_text: str
    issue: str
    suggested_change: str
    severity: Severity


class OrchestratorOutput(BaseModel):
    """Step 1 — segments the contract and routes sections to sub-agents."""

    executive_summary: str = ""
    contract_type: str = ""
    counterparty: str = ""
    sections_by_category: dict[str, list[str]] = Field(default_factory=dict)
    missing_categories: list[str] = Field(default_factory=list)


class CategoryFindings(BaseModel):
    """Step 2 — one sub-agent's review of a single category."""

    category: str
    score: int = Field(ge=0, le=10)
    findings: list[str] = Field(default_factory=list)
    redlines: list[Redline] = Field(default_factory=list)


class FinalReview(BaseModel):
    """Step 3 — synthesizer's consolidated scorecard + redlines."""

    overall_score: int = Field(ge=0, le=100)
    recommendation: Recommendation
    recommendation_reason: str
    top_risks: list[str] = Field(default_factory=list)
    scorecard: dict[str, int] = Field(default_factory=dict)
    redlines: list[Redline] = Field(default_factory=list)
