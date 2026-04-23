"""
Multi-agent contract analyzer.

Given the plain text of a contract and light metadata, runs:
  1. Orchestrator   (Opus)        — segments the doc, returns sections_by_category
  2. Sub-agents     (Haiku × N)   — one call per category in parallel
  3. Synthesizer    (Opus)        — merges outputs, produces scorecard + redlines

The contract text is sent as a cached system block on every call so the
8 calls share one cache breakpoint. Built to be robust: any sub-agent
that fails returns an empty output and the synthesizer keeps going.

Called only from :class:`outreach.llm.client.AnthropicAdapter.analyze_contract`
(and from tests directly with a fake Anthropic client).
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import re
from typing import Any

from .contract_prompts import (
    CATEGORY_RUBRICS,
    SYSTEM_ORCHESTRATOR,
    SYSTEM_SUBAGENT,
    SYSTEM_SYNTHESIZER,
    metadata_footer,
)

logger = logging.getLogger("outreach.llm.contract_analyzer")


class ContractAnalyzer:
    """Orchestrator + sub-agents + synthesizer driver.

    Stateless — cheap to construct per call. Accepts an ``anthropic`` SDK
    client so tests can inject a fake. All model IDs + parallelism come
    from ``settings.CONTRACTS``.
    """

    def __init__(
        self,
        client,
        *,
        orchestrator_model: str,
        subagent_model: str,
        synthesizer_model: str,
        categories: list[str],
        parallelism: int = 4,
        max_input_chars: int = 600_000,
    ) -> None:
        self._client = client
        self._orchestrator_model = orchestrator_model
        self._subagent_model = subagent_model
        self._synthesizer_model = synthesizer_model
        self._categories = [c for c in categories if c in CATEGORY_RUBRICS]
        self._parallelism = max(1, parallelism)
        self._max_input_chars = max_input_chars

    # ── Public entry ──────────────────────────────────────────────
    def analyze(self, text: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        if not text or not text.strip():
            return {}

        trimmed = self._trim(text)
        footer = metadata_footer(metadata)
        cached_body = trimmed + footer

        orchestrator = self._call_orchestrator(cached_body)
        sections_by_cat = (orchestrator or {}).get("sections_by_category") or {}

        sub_outputs = self._call_subagents(cached_body, sections_by_cat)
        synthesized = self._call_synthesizer(cached_body, orchestrator, sub_outputs)

        if not isinstance(synthesized, dict) or not synthesized:
            logger.warning("synthesizer returned empty; falling back to sub-agent merge")
            synthesized = self._fallback_merge(sub_outputs)

        synthesized["sub_agent_outputs"] = sub_outputs
        synthesized["orchestrator_summary"] = (orchestrator or {}).get(
            "executive_summary", ""
        )
        synthesized["contract_type"] = (orchestrator or {}).get("contract_type", "")
        synthesized["counterparty"] = (orchestrator or {}).get("counterparty", "")
        return synthesized

    # ── Internals ─────────────────────────────────────────────────
    def _trim(self, text: str) -> str:
        if len(text) <= self._max_input_chars:
            return text
        marker = "\n\n[... truncated for context window ...]\n\n"
        budget = max(self._max_input_chars - len(marker), 0)
        head = budget // 2
        tail = max(budget - head, 0)
        head_part = text[:head]
        tail_part = text[-tail:] if tail > 0 else ""
        return head_part + marker + tail_part

    def _cached_system(self, body: str, trailer: str) -> list[dict[str, Any]]:
        """System blocks: (1) cached contract text, (2) per-role trailer.

        Splitting keeps the contract block byte-identical across calls so
        the cache lands, while the small role-specific trailer varies.
        """
        return [
            {
                "type": "text",
                "text": "CONTRACT DOCUMENT:\n\n" + body,
                "cache_control": {"type": "ephemeral"},
            },
            {"type": "text", "text": trailer},
        ]

    def _collect_text(self, response) -> str:
        out = []
        for block in response.content:
            text = getattr(block, "text", None)
            if text:
                out.append(text)
        return "".join(out).strip()

    def _parse_json(self, text: str) -> dict[str, Any] | None:
        text = (text or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
        return None

    def _call_orchestrator(self, cached_body: str) -> dict[str, Any] | None:
        categories_list = ", ".join(self._categories)
        user = (
            "Segment the contract above and return the JSON described in the "
            f"system prompt. Categories to route: {categories_list}. "
            "Respond with JSON only."
        )
        try:
            resp = self._client.messages.create(
                model=self._orchestrator_model,
                max_tokens=2000,
                system=self._cached_system(cached_body, SYSTEM_ORCHESTRATOR),
                messages=[{"role": "user", "content": user}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("orchestrator failed: %s", exc)
            return None
        return self._parse_json(self._collect_text(resp))

    def _call_subagents(
        self, cached_body: str, sections_by_cat: dict[str, list[str]]
    ) -> dict[str, dict[str, Any]]:
        outputs: dict[str, dict[str, Any]] = {}

        def _run(cat: str) -> tuple[str, dict[str, Any] | None]:
            return cat, self._call_subagent(cached_body, cat, sections_by_cat.get(cat) or [])

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._parallelism
        ) as pool:
            for cat, result in pool.map(_run, self._categories):
                if result is not None:
                    outputs[cat] = result
        return outputs

    def _call_subagent(
        self, cached_body: str, category: str, section_hints: list[str]
    ) -> dict[str, Any] | None:
        rubric = CATEGORY_RUBRICS.get(category, "")
        trailer = SYSTEM_SUBAGENT + "\n\n" + rubric
        hints_block = ""
        if section_hints:
            hints_block = "\n\nSections the orchestrator flagged for this category:\n" + "\n".join(
                f"- {h}" for h in section_hints[:20]
            )
        user = (
            f"Review the {category!r} aspects of the contract above. "
            "Return JSON matching the schema in the system prompt."
            + hints_block
        )
        try:
            resp = self._client.messages.create(
                model=self._subagent_model,
                max_tokens=1500,
                system=self._cached_system(cached_body, trailer),
                messages=[{"role": "user", "content": user}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("sub-agent %s failed: %s", category, exc)
            return None
        return self._parse_json(self._collect_text(resp))

    def _call_synthesizer(
        self,
        cached_body: str,
        orchestrator: dict[str, Any] | None,
        sub_outputs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        if not sub_outputs:
            return {}
        user_payload = {
            "orchestrator": orchestrator or {},
            "sub_agents": sub_outputs,
        }
        user = (
            "Consolidate the sub-agent outputs into the final review. "
            "Return JSON matching the schema in the system prompt.\n\n"
            f"{json.dumps(user_payload, ensure_ascii=False)}"
        )
        try:
            resp = self._client.messages.create(
                model=self._synthesizer_model,
                max_tokens=3000,
                system=self._cached_system(cached_body, SYSTEM_SYNTHESIZER),
                messages=[{"role": "user", "content": user}],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("synthesizer failed: %s", exc)
            return {}
        return self._parse_json(self._collect_text(resp)) or {}

    def _fallback_merge(
        self, sub_outputs: dict[str, dict[str, Any]]
    ) -> dict[str, Any]:
        """Deterministic fallback if the synthesizer call fails.

        Same weighted-mean math as the synthesizer prompt so the numbers
        agree whether the LLM or this code produced them.
        """
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
            score = out.get("score")
            if isinstance(score, (int, float)):
                scorecard[cat] = int(score)
            for r in out.get("redlines") or []:
                merged = dict(r)
                merged["category"] = cat
                redlines.append(merged)

        if scorecard:
            overall = sum(
                scorecard.get(cat, 5) * weights.get(cat, 0) for cat in weights
            ) * 10
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
            "top_risks": [
                r["issue"] for r in redlines[:5] if r.get("issue")
            ],
            "scorecard": scorecard,
            "redlines": redlines,
        }
