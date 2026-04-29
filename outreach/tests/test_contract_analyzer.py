"""
Unit tests for :class:`outreach.llm.contract_analyzer.ContractAnalyzer`.

The analyzer wraps three pydantic-ai Agents (orchestrator + N sub-agents
+ synthesizer). We swap their underlying models for
:class:`pydantic_ai.models.test.TestModel` instances pre-loaded with
canned outputs, so no network / no API key is needed.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pydantic_ai", reason="pydantic-ai-slim not installed")

from pydantic_ai import Agent  # noqa: E402
from pydantic_ai.models.test import TestModel  # noqa: E402

from outreach.llm.contract_analyzer import ContractAnalyzer
from outreach.llm.schemas import (
    CategoryFindings,
    FinalReview,
    OrchestratorOutput,
    Redline,
)


# ─── Helpers ─────────────────────────────────────────────────────────


def _make_analyzer() -> ContractAnalyzer:
    return ContractAnalyzer(
        provider=None,  # never touched — _build_agents is patched per-test
        orchestrator_model="opus",
        subagent_model="haiku",
        synthesizer_model="opus",
        categories=[
            "pricing",
            "liability",
            "termination",
            "sla_performance",
            "ip_confidentiality",
            "governing_law",
        ],
        parallelism=2,
        max_input_chars=10_000,
    )


def _patch_agents(
    analyzer: ContractAnalyzer,
    *,
    orchestrator: OrchestratorOutput | None,
    sub_outputs: dict[str, CategoryFindings],
    synthesizer: FinalReview | None,
) -> dict[str, int]:
    """Replace the analyzer's `_build_agents` with one that wires up
    TestModel-backed agents returning the canned outputs above. Returns
    a mutable counter dict so tests can assert call counts."""
    counts = {"orchestrator": 0, "subagents": 0, "synthesizer": 0}

    def _build(cached_body: str) -> None:
        # Orchestrator
        if orchestrator is not None:
            orch_agent = Agent(
                TestModel(custom_output_args=orchestrator.model_dump()),
                output_type=OrchestratorOutput,
            )
        else:
            # Force a failure path
            orch_agent = Agent(
                TestModel(custom_output_args={"executive_summary": ""}),
                output_type=OrchestratorOutput,
            )

        # Wrap run_sync so we can count calls.
        _orig_run_sync_orch = orch_agent.run_sync

        def _orch_counted(user, **kw):
            counts["orchestrator"] += 1
            return _orig_run_sync_orch(user, **kw)

        orch_agent.run_sync = _orch_counted  # type: ignore[assignment]
        analyzer._orchestrator_agent = orch_agent

        # Synthesizer
        if synthesizer is not None:
            synth_agent = Agent(
                TestModel(custom_output_args=synthesizer.model_dump()),
                output_type=FinalReview,
            )
            _orig_run_sync_synth = synth_agent.run_sync

            def _synth_counted(user, **kw):
                counts["synthesizer"] += 1
                return _orig_run_sync_synth(user, **kw)

            synth_agent.run_sync = _synth_counted  # type: ignore[assignment]
        else:
            # Build a synthesizer that always raises so the analyzer
            # exercises its fallback path.
            class _Boom:
                def run_sync(self, *_a, **_k):
                    counts["synthesizer"] += 1
                    raise RuntimeError("synthesizer offline")

            synth_agent = _Boom()
        analyzer._synthesizer_agent = synth_agent

        # Sub-agents — one per category. Categories not in sub_outputs
        # get a "raises" agent so they're filtered out.
        analyzer._sub_agents = {}
        for cat in analyzer._categories:
            if cat in sub_outputs:
                a = Agent(
                    TestModel(custom_output_args=sub_outputs[cat].model_dump()),
                    output_type=CategoryFindings,
                )
                _orig_run_sync = a.run_sync

                def _counted(user, _cat=cat, _orig=_orig_run_sync, **kw):
                    counts["subagents"] += 1
                    return _orig(user, **kw)

                a.run_sync = _counted  # type: ignore[assignment]
                analyzer._sub_agents[cat] = a
            else:
                class _BoomSub:
                    def run_sync(self, *_a, **_k):
                        counts["subagents"] += 1
                        raise RuntimeError("subagent offline")

                analyzer._sub_agents[cat] = _BoomSub()

    analyzer._build_agents = _build  # type: ignore[assignment]
    return counts


def _redline(severity: str, label: str) -> Redline:
    return Redline(
        category="",
        clause_text=f"{label} clause",
        issue=f"{label} issue",
        suggested_change=f"fix {label}",
        severity=severity,  # type: ignore[arg-type]
    )


# ─── Tests ───────────────────────────────────────────────────────────


def test_analyze_full_flow_returns_synthesized_result():
    orch = OrchestratorOutput(
        executive_summary="Fixed-price services agreement.",
        contract_type="firm-fixed-price services",
        counterparty="Acme Corp",
        sections_by_category={
            "pricing": ["§3 Payment"],
            "liability": ["§7 Indemnification"],
        },
        missing_categories=[],
    )
    sub_outputs = {
        "pricing": CategoryFindings(category="pricing", score=7),
        "liability": CategoryFindings(
            category="liability",
            score=4,
            redlines=[_redline("high", "liability")],
        ),
        "termination": CategoryFindings(category="termination", score=6),
        "sla_performance": CategoryFindings(category="sla_performance", score=8),
        "ip_confidentiality": CategoryFindings(category="ip_confidentiality", score=9),
        "governing_law": CategoryFindings(category="governing_law", score=7),
    }
    synth = FinalReview(
        overall_score=68,
        recommendation="negotiate",
        recommendation_reason="High-severity liability cap issue.",
        top_risks=["Unlimited liability"],
        scorecard={
            "pricing": 7,
            "liability": 4,
            "termination": 6,
            "sla_performance": 8,
            "ip_confidentiality": 9,
            "governing_law": 7,
        },
        redlines=[
            Redline(
                category="liability",
                clause_text="unlimited liability",
                issue="no cap",
                suggested_change="cap at 1x fees",
                severity="high",
            )
        ],
    )

    analyzer = _make_analyzer()
    counts = _patch_agents(
        analyzer, orchestrator=orch, sub_outputs=sub_outputs, synthesizer=synth
    )

    result = analyzer.analyze("Sample contract text.", metadata={"title": "t"})

    assert result["overall_score"] == 68
    assert result["recommendation"] == "negotiate"
    assert result["scorecard"]["liability"] == 4
    assert result["redlines"][0]["severity"] == "high"
    assert result["orchestrator_summary"].startswith("Fixed-price")
    assert result["contract_type"] == "firm-fixed-price services"
    assert result["counterparty"] == "Acme Corp"
    assert set(result["sub_agent_outputs"].keys()) == set(sub_outputs.keys())
    # 1 orchestrator + 6 sub-agents + 1 synthesizer
    assert counts == {"orchestrator": 1, "subagents": 6, "synthesizer": 1}


def test_analyze_falls_back_when_synthesizer_empty():
    sub_outputs = {
        "pricing": CategoryFindings(category="pricing", score=6),
        "liability": CategoryFindings(
            category="liability",
            score=3,
            redlines=[
                _redline("high", "l1"),
                _redline("high", "l2"),
                _redline("high", "l3"),
            ],
        ),
        "termination": CategoryFindings(
            category="termination",
            score=2,
            redlines=[_redline("high", "t1")],
        ),
        "sla_performance": CategoryFindings(category="sla_performance", score=5),
        "ip_confidentiality": CategoryFindings(category="ip_confidentiality", score=5),
        "governing_law": CategoryFindings(category="governing_law", score=5),
    }
    analyzer = _make_analyzer()
    _patch_agents(
        analyzer,
        orchestrator=OrchestratorOutput(),
        sub_outputs=sub_outputs,
        synthesizer=None,  # synth raises → fallback path
    )

    result = analyzer.analyze("text")

    # 4 high-severity redlines → fallback should say reject.
    assert result["recommendation"] == "reject"
    assert result["overall_score"] < 50
    assert len(result["redlines"]) == 4
    assert all(r["severity"] == "high" for r in result["redlines"])


def test_analyze_returns_empty_on_blank_text():
    analyzer = _make_analyzer()
    counts = _patch_agents(
        analyzer,
        orchestrator=OrchestratorOutput(),
        sub_outputs={},
        synthesizer=FinalReview(
            overall_score=0, recommendation="reject", recommendation_reason="x"
        ),
    )
    assert analyzer.analyze("") == {}
    # Nothing should have been called.
    assert counts == {"orchestrator": 0, "subagents": 0, "synthesizer": 0}


def test_trim_drops_middle_when_text_too_long():
    analyzer = _make_analyzer()
    analyzer._max_input_chars = 100
    trimmed = analyzer._trim("A" * 1000)
    assert "truncated" in trimmed
    assert len(trimmed) < 200
