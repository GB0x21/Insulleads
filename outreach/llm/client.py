"""
Default :class:`LLMAdapter` implementation, backed by pydantic-ai with
the Anthropic provider.

Each call site (enricher / qualifier / writer / email writer) goes
through a typed :class:`pydantic_ai.Agent` with a Pydantic ``output_type``,
so we never parse JSON by hand and validation errors surface as
exceptions instead of silent garbage.

We still keep an ``anthropic.Anthropic`` instance at ``self._client``
because two code paths reach into the raw SDK:

  * :mod:`agents.contract_discovery.web_search` uses the native Anthropic
    ``web_search_20250305`` tool to fan out queries.
  * The contract analyzer (:mod:`outreach.llm.contract_analyzer`) shares
    the same provider so its 8 calls (orchestrator + 6 sub-agents +
    synthesizer) reuse one HTTP connection pool.

Model defaults (overridable per-campaign via env):

  * ``LLM_MODEL``        — qualifier + writer (default: ``claude-opus-4-6``).
  * ``LLM_ENRICH_MODEL`` — contact enrichment.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from django.conf import settings

from .adapter import LLMAdapter
from .contract_analyzer import ContractAnalyzer
from .prompts import (
    SYSTEM_EMAIL_WRITER,
    SYSTEM_ENRICHER,
    SYSTEM_QUALIFIER,
    SYSTEM_WRITER,
    campaign_prefix,
    lead_payload,
)
from .rag import get_default_index
from .schemas import (
    EmailDraft,
    EnrichmentResult,
    OutreachMessage,
    QualificationResult,
)

if TYPE_CHECKING:
    from outreach.models import Campaign, Lead

logger = logging.getLogger("outreach.llm.client")


class AnthropicAdapter(LLMAdapter):
    """pydantic-ai-backed adapter (provider: Anthropic).

    The class name is kept for backward-compat — it's imported by
    :func:`outreach.llm.adapter.get_adapter` and referenced in tests
    that look up adapter type by name. The implementation underneath
    is pure pydantic-ai.
    """

    name = "anthropic"

    def __init__(self, cfg: dict[str, Any]) -> None:
        try:
            from anthropic import Anthropic, AsyncAnthropic
        except ImportError as exc:  # pragma: no cover - surfaced by get_adapter
            raise RuntimeError(
                "anthropic SDK not installed. Add `anthropic>=0.39.0` "
                "to requirements/base.txt or `pip install anthropic`."
            ) from exc
        try:
            from pydantic_ai.providers.anthropic import AnthropicProvider
        except ImportError as exc:
            raise RuntimeError(
                "pydantic-ai-slim[anthropic] not installed. Add "
                "`pydantic-ai-slim[anthropic]>=1.0,<2.0` to requirements/base.txt."
            ) from exc

        api_key = cfg.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. Export it in .env or switch "
                "to LLM_ADAPTER=noop."
            )

        # Sync client — kept for callers that use the native SDK directly
        # (web_search tool, contract analyzer).
        self._client = Anthropic(api_key=api_key)
        # pydantic-ai requires an AsyncAnthropic client internally because
        # all its model calls go through async coroutines. Passing the sync
        # Anthropic client caused "BetaMessage can't be used in 'await'
        # expression" when anthropic_cache_instructions=True triggered the
        # beta endpoint path.
        self._async_client = AsyncAnthropic(api_key=api_key)
        self._provider = AnthropicProvider(anthropic_client=self._async_client)

        self._model_default_id = cfg.get("MODEL") or "claude-opus-4-6"
        self._model_enrich_id = cfg.get("ENRICH_MODEL") or self._model_default_id
        self._max_tokens = int(cfg.get("MAX_TOKENS") or 1024)

        # Per-(role, campaign) Agent cache — Agents are cheap to build
        # but caching avoids recomputing the system prompt on every call.
        self._agents: dict[str, Any] = {}

    def close(self) -> None:
        """Release the underlying HTTP connection pool. Optional in
        long-running processes (the daemon) but useful in test fixtures
        so sockets don't leak between cases."""
        for attr in ("_client", "_async_client"):
            client = getattr(self, attr, None)
            if client is not None and hasattr(client, "close"):
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    # ─── Agent factory ────────────────────────────────────────────────

    def _agent(
        self,
        cache_key: str,
        model_id: str,
        system: str,
        output_type,
        max_tokens: int,
        builtin_tools: tuple = (),
    ):
        if cache_key in self._agents:
            return self._agents[cache_key]

        from pydantic_ai import Agent
        from pydantic_ai.models.anthropic import (
            AnthropicModel,
            AnthropicModelSettings,
        )

        model = AnthropicModel(model_id, provider=self._provider)
        model_settings = AnthropicModelSettings(
            max_tokens=max_tokens,
            anthropic_cache_instructions=True,
        )
        agent = Agent(
            model,
            output_type=output_type,
            system_prompt=system,
            model_settings=model_settings,
            builtin_tools=builtin_tools,
        )
        self._agents[cache_key] = agent
        return agent

    # ─── Shared helpers ───────────────────────────────────────────────

    def _retrieve_references(self, lead: "Lead") -> str:
        """Pull top-k RAG snippets for this lead and format them for
        injection into the writer's user message."""
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

    # ─── 1. enrich_contact ────────────────────────────────────────────

    def enrich_contact(self, lead: "Lead") -> dict[str, Any]:
        from pydantic_ai.builtin_tools import WebSearchTool

        agent = self._agent(
            cache_key="enrich",
            model_id=self._model_enrich_id,
            system=SYSTEM_ENRICHER,
            output_type=EnrichmentResult,
            max_tokens=self._max_tokens,
            builtin_tools=(WebSearchTool(max_uses=5),),
        )
        user = (
            "Enrich the following construction lead. Use web search to find "
            "the contractor's business phone, email, website, and CSLB "
            "license number.\n\n"
            f"{lead_payload(lead)}\n\n"
            "Fill any subset of the schema fields. Omit fields you could "
            "not verify — never guess."
        )
        try:
            result = agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("enrich_contact failed for lead=%s: %s", lead.pk, exc)
            return {}

        out = result.output
        if not isinstance(out, EnrichmentResult):
            return {}
        # Only return non-empty fields so the merge into the Lead model
        # mirrors the previous "omit unknown keys" semantics.
        payload = out.model_dump()
        return {k: v for k, v in payload.items() if v}

    # ─── 2. qualify_lead ──────────────────────────────────────────────

    def qualify_lead(
        self, lead: "Lead", campaign: "Campaign"
    ) -> tuple[float | None, str]:
        agent = self._agent(
            cache_key=f"qualify:{campaign.pk}",
            model_id=self._model_default_id,
            system=f"{SYSTEM_QUALIFIER}\n\n{campaign_prefix(campaign)}",
            output_type=QualificationResult,
            max_tokens=256,
        )
        user = (
            "Score this lead. Return a structured QualificationResult.\n\n"
            f"{lead_payload(lead)}"
        )
        try:
            result = agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("qualify_lead failed for lead=%s: %s", lead.pk, exc)
            return (None, "")

        out = result.output
        if not isinstance(out, QualificationResult):
            return (None, "")
        score = max(0.0, min(1.0, float(out.score)))
        reason = (out.reason or "")[:500]
        return (score, reason)

    # ─── 3. write_outreach ────────────────────────────────────────────

    def write_outreach(
        self, lead: "Lead", campaign: "Campaign"
    ) -> str | None:
        tone = getattr(campaign, "outreach_tone", "professional") or "professional"
        booking = campaign.booking_link or ""
        system = (
            f"{SYSTEM_WRITER}\n\n"
            f"Tone: {tone}.\n"
            f"Booking link (optional, include only if natural): "
            f"{booking or '(none)'}\n\n"
            f"{campaign_prefix(campaign)}"
        )
        agent = self._agent(
            cache_key=f"writer:{campaign.pk}:{tone}",
            model_id=self._model_default_id,
            system=system,
            output_type=OutreachMessage,
            max_tokens=600,
        )
        references_block = self._retrieve_references(lead)
        user = (
            "Draft the outreach message for this lead. Output only the "
            "message body in the `body` field.\n\n"
            f"{lead_payload(lead)}"
        )
        if references_block:
            user += f"\n\nReferences:\n{references_block}"

        try:
            result = agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("write_outreach failed for lead=%s: %s", lead.pk, exc)
            return None

        out = result.output
        if not isinstance(out, OutreachMessage):
            return None
        body = (out.body or "").strip()
        return body or None

    # ─── 3b. analyze_contract ─────────────────────────────────────────

    def analyze_contract(
        self, text: str, metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Delegate to :class:`ContractAnalyzer` — uses the same provider
        so all 8 calls share one HTTP connection pool."""
        cfg = getattr(settings, "CONTRACTS", {}) or {}
        analyzer = ContractAnalyzer(
            provider=self._provider,
            orchestrator_model=cfg.get("ORCHESTRATOR_MODEL") or self._model_default_id,
            subagent_model=cfg.get("SUBAGENT_MODEL") or self._model_default_id,
            synthesizer_model=cfg.get("SYNTHESIZER_MODEL") or self._model_default_id,
            categories=cfg.get("CATEGORIES")
            or [
                "pricing",
                "liability",
                "termination",
                "sla_performance",
                "ip_confidentiality",
                "governing_law",
            ],
            parallelism=int(cfg.get("SUBAGENT_PARALLELISM") or 4),
            max_input_chars=int(cfg.get("MAX_INPUT_CHARS") or 600_000),
        )
        return analyzer.analyze(text, metadata=metadata)

    # ─── 4. write_email ───────────────────────────────────────────────

    def write_email(
        self, lead: "Lead", campaign: "Campaign"
    ) -> dict[str, str] | None:
        tone = getattr(campaign, "outreach_tone", "professional") or "professional"
        booking = campaign.booking_link or ""
        system = (
            f"{SYSTEM_EMAIL_WRITER}\n\n"
            f"Tone: {tone}.\n"
            f"Booking/Calendly link (include only if natural): "
            f"{booking or '(none)'}\n\n"
            f"{campaign_prefix(campaign)}"
        )
        agent = self._agent(
            cache_key=f"email:{campaign.pk}:{tone}",
            model_id=self._model_default_id,
            system=system,
            output_type=EmailDraft,
            max_tokens=700,
        )
        references_block = self._retrieve_references(lead)
        user = (
            "Write the cold-prospect email for this lead. Return both "
            "`subject` and `body` fields.\n\n"
            f"{lead_payload(lead)}"
        )
        if references_block:
            user += f"\n\nReferences:\n{references_block}"

        try:
            result = agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("write_email failed for lead=%s: %s", lead.pk, exc)
            return None

        out = result.output
        if not isinstance(out, EmailDraft):
            return None
        subject = (out.subject or "").strip()[:255]
        body = (out.body or "").strip()
        if not subject or not body:
            return None
        return {"subject": subject, "body": body}
