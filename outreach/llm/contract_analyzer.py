"""
Multi-agent contract analyzer (pydantic-ai).

Given the plain text of a contract and light metadata, runs:
  1. Orchestrator   (Opus)        — segments the doc, returns sections_by_category
  2. Sub-agents     (Haiku × N)   — one call per category in parallel
  3. Synthesizer    (Opus)        — merges outputs, produces scorecard + redlines

Each role is a separate :class:`pydantic_ai.Agent` with a typed
``output_type`` (see :mod:`outreach.llm.schemas`). The contract text is
embedded in every system prompt and marked cacheable via
``anthropic_cache_instructions=True`` so the 8 calls share one cache
breakpoint where they overlap.

Robust by design: any sub-agent that fails just produces no output,
the synthesizer keeps going, and there's a deterministic
:meth:`_fallback_merge` that runs when the synthesizer call itself fails.

Called from :class:`outreach.llm.client.AnthropicAdapter.analyze_contract`
in production. Tests inject :class:`pydantic_ai.models.test.TestModel`
via :meth:`pydantic_ai.Agent.override` so no network / no API key is
needed.
"""
from __future__ import annotations

import concurrent.futures
import logging
from typing import Any

from .contract_prompts import (
    CATEGORY_RUBRICS,
    SYSTEM_ORCHESTRATOR,
    SYSTEM_SUBAGENT,
    SYSTEM_SYNTHESIZER,
    metadata_footer,
)
from .schemas import (
    CategoryFindings,
    FinalReview,
    OrchestratorOutput,
)

logger = logging.getLogger("outreach.llm.contract_analyzer")


# ─── Public class ────────────────────────────────────────────────────


class ContractAnalyzer:
    """Orchestrator + sub-agents + synthesizer driver.

    Stateful only in the sense that it builds (and caches) one Agent per
    role + category up-front. Cheap to construct per call.
    """

    def __init__(
        self,
        *,
        provider,  # pydantic_ai.providers.anthropic.AnthropicProvider
        orchestrator_model: str,
        subagent_model: str,
        synthesizer_model: str,
        categories: list[str],
        parallelism: int = 4,
        max_input_chars: int = 600_000,
    ) -> None:
        self._provider = provider
        self._orchestrator_model = orchestrator_model
        self._subagent_model = subagent_model
        self._synthesizer_model = synthesizer_model
        self._categories = [c for c in categories if c in CATEGORY_RUBRICS]
        self._parallelism = max(1, parallelism)
        self._max_input_chars = max_input_chars

        # Agents are bound after we see the contract text (the contract
        # goes into the system prompt so it can be cached).
        self._orchestrator_agent = None
        self._sub_agents: dict[str, Any] = {}
        self._synthesizer_agent = None

    # ─── Public entry ─────────────────────────────────────────────────

    def analyze(
        self, text: str, metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if not text or not text.strip():
            return {}

        trimmed = self._trim(text)
        cached_body = trimmed + metadata_footer(metadata)

        self._build_agents(cached_body)

        orchestrator = self._call_orchestrator()
        sections_by_cat = (
            orchestrator.sections_by_category if orchestrator else {}
        )

        sub_outputs = self._call_subagents(sections_by_cat)
        synthesized = self._call_synthesizer(orchestrator, sub_outputs)

        if synthesized is None:
            logger.warning("synthesizer returned empty; using fallback merge")
            synth_dump = self._fallback_merge(sub_outputs)
        else:
            synth_dump = synthesized.model_dump()

        synth_dump["sub_agent_outputs"] = {
            cat: out.model_dump() for cat, out in sub_outputs.items()
        }
        synth_dump["orchestrator_summary"] = (
            orchestrator.executive_summary if orchestrator else ""
        )
        synth_dump["contract_type"] = (
            orchestrator.contract_type if orchestrator else ""
        )
        synth_dump["counterparty"] = (
            orchestrator.counterparty if orchestrator else ""
        )
        return synth_dump

    # ─── Agent construction ───────────────────────────────────────────

    def _build_agents(self, cached_body: str) -> None:
        """Build one Agent per role with the contract text baked into the
        system prompt. ``anthropic_cache_instructions=True`` marks the
        whole instruction cacheable, so subsequent sub-agent calls hit
        the cache for the contract prefix (which is identical across
        all 8 calls)."""
        from pydantic_ai import Agent
        from pydantic_ai.models.anthropic import (
            AnthropicModel,
            AnthropicModelSettings,
        )

        contract_block = "CONTRACT DOCUMENT:\n\n" + cached_body

        def _build(
            model_id: str, role_system: str, output_type, max_tokens: int
        ):
            model = AnthropicModel(model_id, provider=self._provider)
            settings = AnthropicModelSettings(
                max_tokens=max_tokens,
                anthropic_cache_instructions=True,
            )
            return Agent(
                model,
                output_type=output_type,
                # Sequence form — pydantic-ai sends each item as its own
                # system message, so the contract block stays byte-
                # identical across roles and Anthropic can match it.
                system_prompt=(contract_block, role_system),
                model_settings=settings,
            )

        self._orchestrator_agent = _build(
            self._orchestrator_model, SYSTEM_ORCHESTRATOR, OrchestratorOutput, 2000
        )
        self._synthesizer_agent = _build(
            self._synthesizer_model, SYSTEM_SYNTHESIZER, FinalReview, 3000
        )
        self._sub_agents = {}
        for cat in self._categories:
            rubric = CATEGORY_RUBRICS.get(cat, "")
            trailer = SYSTEM_SUBAGENT + "\n\n" + rubric
            self._sub_agents[cat] = _build(
                self._subagent_model, trailer, CategoryFindings, 1500
            )

    # ─── Internals ────────────────────────────────────────────────────

    def _trim(self, text: str) -> str:
        if len(text) <= self._max_input_chars:
            return text
        marker = "\n\n[... truncated for context window ...]\n\n"
        budget = max(self._max_input_chars - len(marker), 0)
        head = budget // 2
        tail = max(budget - head, 0)
        return text[:head] + marker + (text[-tail:] if tail > 0 else "")

    def _call_orchestrator(self) -> OrchestratorOutput | None:
        categories_list = ", ".join(self._categories)
        user = (
            "Segment the contract above and return the structured output. "
            f"Categories to route: {categories_list}."
        )
        try:
            result = self._orchestrator_agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("orchestrator failed: %s", exc)
            return None
        return result.output if isinstance(result.output, OrchestratorOutput) else None

    def _call_subagents(
        self, sections_by_cat: dict[str, list[str]]
    ) -> dict[str, CategoryFindings]:
        outputs: dict[str, CategoryFindings] = {}

        def _run(cat: str) -> tuple[str, CategoryFindings | None]:
            return cat, self._call_subagent(cat, sections_by_cat.get(cat) or [])

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._parallelism
        ) as pool:
            for cat, result in pool.map(_run, self._categories):
                if result is not None:
                    outputs[cat] = result
        return outputs

    def _call_subagent(
        self, category: str, section_hints: list[str]
    ) -> CategoryFindings | None:
        agent = self._sub_agents.get(category)
        if agent is None:
            return None

        hints_block = ""
        if section_hints:
            hints_block = (
                "\n\nSections the orchestrator flagged for this category:\n"
                + "\n".join(f"- {h}" for h in section_hints[:20])
            )
        user = (
            f"Review the {category!r} aspects of the contract above. "
            "Return a structured CategoryFindings."
            + hints_block
        )
        try:
            result = agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("sub-agent %s failed: %s", category, exc)
            return None
        return result.output if isinstance(result.output, CategoryFindings) else None

    def _call_synthesizer(
        self,
        orchestrator: OrchestratorOutput | None,
        sub_outputs: dict[str, CategoryFindings],
    ) -> FinalReview | None:
        if not sub_outputs:
            return None
        # Compact JSON-style payload — the synthesizer doesn't need the
        # full Pydantic objects, just the values.
        import json

        payload = {
            "orchestrator": orchestrator.model_dump() if orchestrator else {},
            "sub_agents": {
                cat: out.model_dump() for cat, out in sub_outputs.items()
            },
        }
        user = (
            "Consolidate the sub-agent outputs into the final review. "
            "Return a structured FinalReview.\n\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )
        try:
            result = self._synthesizer_agent.run_sync(user)
        except Exception as exc:  # noqa: BLE001
            logger.warning("synthesizer failed: %s", exc)
            return None
        return result.output if isinstance(result.output, FinalReview) else None

    # ─── Fallback when synthesizer fails ──────────────────────────────

    def _fallback_merge(
        self, sub_outputs: dict[str, CategoryFindings]
    ) -> dict[str, Any]:
        """Deterministic fallback if the synthesizer call fails. Same
        weighted-mean math as the synthesizer prompt so the numbers
        agree whether the LLM or this code produced them."""
        weights = {
            "liability": 0.25,
            "termination": 0.20,
            "pricing": 0.20,
            "sla_performance": 0.15,
            "ip_confidentiality": 0.10,
            "governing_law": 0.10,
        }
        scorecard: dict[str, int] = {}
        redlines: list[dict[str, Any]] = []
        for cat, out in sub_outputs.items():
            scorecard[cat] = int(out.score)
            for r in out.redlines:
                merged = r.model_dump()
                merged["category"] = cat
                redlines.append(merged)

        if scorecard:
            overall = (
                sum(scorecard.get(cat, 5) * weights.get(cat, 0) for cat in weights)
                * 10
            )
        else:
            overall = 0
        overall = max(0, min(100, int(round(overall))))

        high_count = sum(1 for r in redlines if r.get("severity") == "high")
        if overall >= 75 and high_count == 0:
            rec = "sign"
        elif overall < 50 or high_count >= 3:
            rec = "reject"
        else:
            rec = "negotiate"

        severity_rank = {"high": 0, "med": 1, "low": 2}
        redlines.sort(
            key=lambda r: (
                severity_rank.get(r.get("severity", "low"), 3),
                r.get("category", ""),
            )
        )

        return {
            "overall_score": overall,
            "recommendation": rec,
            "recommendation_reason": "Fallback merge from sub-agent outputs "
            "(synthesizer unavailable).",
            "top_risks": [r["issue"] for r in redlines[:5] if r.get("issue")],
            "scorecard": scorecard,
            "redlines": redlines,
        }
