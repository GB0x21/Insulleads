"""
Anthropic client wrapper — the default :class:`LLMAdapter` implementation.

Uses the official ``anthropic`` Python SDK. All three methods are thin glue
around :meth:`anthropic.Anthropic.messages.create` with prompt caching on
the campaign context, plus a single JSON-parsing helper.

Model defaults (overridable per-campaign via env):

  * ``LLM_MODEL``           — qualifier + writer (default: ``claude-opus-4-6``).
  * ``LLM_ENRICH_MODEL``    — contact enrichment (default: same as above;
                              feel free to downgrade to Haiku for volume).

The web-search tool is enabled on the enricher call. We use the current
``web_search_20250305`` tool name, which is the GA version at the time of
writing; bump it when Anthropic releases a newer one.
"""
from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any

from .adapter import LLMAdapter
from .prompts import (
    SYSTEM_EMAIL_WRITER,
    SYSTEM_ENRICHER,
    SYSTEM_QUALIFIER,
    SYSTEM_WRITER,
    campaign_prefix,
    lead_payload,
)
from .rag import get_default_index

if TYPE_CHECKING:
    from outreach.models import Campaign, Lead

logger = logging.getLogger("outreach.llm.client")


class AnthropicAdapter(LLMAdapter):
    """Thin wrapper over the Anthropic Python SDK."""

    name = "anthropic"

    def __init__(self, cfg: dict[str, Any]) -> None:
        try:
            import anthropic  # noqa: F401
        except ImportError as exc:  # pragma: no cover - surfaced by get_adapter
            raise RuntimeError(
                "anthropic SDK not installed. Add `anthropic>=0.39.0` "
                "to requirements/base.txt or `pip install anthropic`."
            ) from exc

        api_key = cfg.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. Export it in .env or switch "
                "to LLM_ADAPTER=noop."
            )

        from anthropic import Anthropic

        self._client = Anthropic(api_key=api_key)
        self._model_default = cfg.get("MODEL") or "claude-opus-4-6"
        self._model_enrich = cfg.get("ENRICH_MODEL") or self._model_default
        self._max_tokens = int(cfg.get("MAX_TOKENS") or 1024)

    # ── shared helpers ────────────────────────────────────────────
    def _retrieve_references(self, lead: "Lead") -> str:
        """Pull top-k RAG snippets for this lead and format them for
        injection into the writer's user message. Empty string when
        the RAG index is a no-op or retrieval fails."""
        try:
            index = get_default_index()
        except Exception as exc:  # noqa: BLE001
            logger.debug("rag index unavailable: %s", exc)
            return ""
        if not index.available:
            return ""

        query_parts = [
            "insulation incentives rebates",
            lead.city or "",
            lead.project_type or "",
        ]
        query = " ".join(p for p in query_parts if p).strip()
        snippets = index.retrieve(query, k=4)
        if not snippets:
            return ""
        return "\n".join(f"- {s.format()}" for s in snippets)

    def _cached_system(self, body: str) -> list[dict[str, Any]]:
        """Return a system-block list with one cache breakpoint at the end."""
        return [
            {
                "type": "text",
                "text": body,
                "cache_control": {"type": "ephemeral"},
            }
        ]

    def _collect_text(self, response) -> str:
        """Concatenate all text blocks from a Messages response."""
        out = []
        for block in response.content:
            # SDK returns typed blocks; text blocks carry `.text`.
            text = getattr(block, "text", None)
            if text:
                out.append(text)
        return "".join(out).strip()

    def _parse_json(self, text: str) -> dict[str, Any] | None:
        """Accept either raw JSON or JSON embedded in prose."""
        text = text.strip()
        if not text:
            return None
        # Fast path
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        # Strip ```json fences
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
        return None

    # ── 1. enrich_contact ─────────────────────────────────────────
    def enrich_contact(self, lead: "Lead") -> dict[str, Any]:
        user_prompt = (
            "Enrich the following construction lead. Use web search to find "
            "the contractor's business phone, email, website, and CSLB "
            "license number.\n\n"
            f"{lead_payload(lead)}\n\n"
            "Respond with a single JSON object using any subset of these "
            'keys: {"website": str, "contact_phone": str, "contact_email": '
            'str, "contact_name": str, "contact_company": str, "cslb_license":'
            ' str, "linkedin": str, "notes": str}. Omit keys you could not '
            "verify. Do not guess."
        )
        try:
            response = self._client.messages.create(
                model=self._model_enrich,
                max_tokens=self._max_tokens,
                system=self._cached_system(SYSTEM_ENRICHER),
                tools=[
                    {
                        "type": "web_search_20250305",
                        "name": "web_search",
                        "max_uses": 5,
                    }
                ],
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("enrich_contact failed for lead=%s: %s", lead.pk, exc)
            return {}

        parsed = self._parse_json(self._collect_text(response))
        if not isinstance(parsed, dict):
            logger.info("enrich_contact: no JSON for lead=%s", lead.pk)
            return {}
        return parsed

    # ── 2. qualify_lead ───────────────────────────────────────────
    def qualify_lead(
        self, lead: "Lead", campaign: "Campaign"
    ) -> tuple[float | None, str]:
        system_body = f"{SYSTEM_QUALIFIER}\n\n{campaign_prefix(campaign)}"
        user_prompt = (
            "Score this lead. Return JSON only.\n\n"
            f"{lead_payload(lead)}"
        )
        try:
            response = self._client.messages.create(
                model=self._model_default,
                max_tokens=256,
                system=self._cached_system(system_body),
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("qualify_lead failed for lead=%s: %s", lead.pk, exc)
            return (None, "")

        parsed = self._parse_json(self._collect_text(response))
        if not isinstance(parsed, dict):
            return (None, "")
        try:
            score = float(parsed.get("score"))
        except (TypeError, ValueError):
            return (None, "")
        score = max(0.0, min(1.0, score))
        reason = str(parsed.get("reason") or "")[:500]
        return (score, reason)

    # ── 3. write_outreach ─────────────────────────────────────────
    def write_outreach(
        self, lead: "Lead", campaign: "Campaign"
    ) -> str | None:
        tone = getattr(campaign, "outreach_tone", "professional") or "professional"
        booking = campaign.booking_link or ""
        system_body = (
            f"{SYSTEM_WRITER}\n\n"
            f"Tone: {tone}.\n"
            f"Booking link (optional, include only if natural): "
            f"{booking or '(none)'}\n\n"
            f"{campaign_prefix(campaign)}"
        )
        references_block = self._retrieve_references(lead)
        user_prompt = (
            "Draft the outreach message for this lead. Output ONLY the "
            "message body.\n\n"
            f"{lead_payload(lead)}"
        )
        if references_block:
            user_prompt += f"\n\nReferences:\n{references_block}"
        try:
            response = self._client.messages.create(
                model=self._model_default,
                max_tokens=600,
                system=self._cached_system(system_body),
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("write_outreach failed for lead=%s: %s", lead.pk, exc)
            return None

        body = self._collect_text(response)
        return body or None

    # ── 4. write_email ────────────────────────────────────────────
    def write_email(
        self, lead: "Lead", campaign: "Campaign"
    ) -> dict[str, str] | None:
        """Return {"subject": str, "body": str} for a cold prospect email,
        or None on failure. Body is plain text; the caller wraps it in HTML."""
        tone = getattr(campaign, "outreach_tone", "professional") or "professional"
        booking = campaign.booking_link or ""
        system_body = (
            f"{SYSTEM_EMAIL_WRITER}\n\n"
            f"Tone: {tone}.\n"
            f"Booking/Calendly link (include only if natural): "
            f"{booking or '(none)'}\n\n"
            f"{campaign_prefix(campaign)}"
        )
        references_block = self._retrieve_references(lead)
        user_prompt = (
            "Write the cold-prospect email for this lead. "
            "Output ONLY the JSON object.\n\n"
            f"{lead_payload(lead)}"
        )
        if references_block:
            user_prompt += f"\n\nReferences:\n{references_block}"
        try:
            response = self._client.messages.create(
                model=self._model_default,
                max_tokens=700,
                system=self._cached_system(system_body),
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("write_email failed for lead=%s: %s", lead.pk, exc)
            return None

        parsed = self._parse_json(self._collect_text(response))
        if not isinstance(parsed, dict):
            return None
        subject = str(parsed.get("subject") or "").strip()[:255]
        body = str(parsed.get("body") or "").strip()
        if not subject or not body:
            return None
        return {"subject": subject, "body": body}
