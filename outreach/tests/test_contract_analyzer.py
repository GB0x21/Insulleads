"""
Unit tests for :class:`outreach.llm.contract_analyzer.ContractAnalyzer`.

We inject a fake Anthropic client so no network / no API key is needed.
The fake returns canned responses keyed on the ``system`` blocks so we
can verify the analyzer routes each role to the right model and merges
outputs correctly.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from outreach.llm.contract_analyzer import ContractAnalyzer
from outreach.llm.contract_prompts import (
    SYSTEM_ORCHESTRATOR,
    SYSTEM_SUBAGENT,
    SYSTEM_SYNTHESIZER,
)


class _FakeBlock:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.content = [_FakeBlock(text)]


class _FakeMessages:
    """Routes each ``create()`` to a canned response by inspecting the trailer
    block of the system prompt."""

    def __init__(self, scripted: dict[str, str]) -> None:
        self.scripted = scripted
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        # system blocks: [cached_contract_text, role_trailer]
        system = kwargs["system"]
        trailer = system[-1]["text"] if isinstance(system, list) else ""
        if trailer.startswith(SYSTEM_ORCHESTRATOR[:40]):
            return _FakeResponse(self.scripted["orchestrator"])
        if trailer.startswith(SYSTEM_SYNTHESIZER[:40]):
            return _FakeResponse(self.scripted["synthesizer"])
        # sub-agent: trailer = SYSTEM_SUBAGENT + rubric
        # Pull category from the user message instead.
        user = kwargs["messages"][0]["content"]
        for cat in (
            "pricing",
            "liability",
            "termination",
            "sla_performance",
            "ip_confidentiality",
            "governing_law",
        ):
            if repr(cat) in user:
                return _FakeResponse(self.scripted["subagent"][cat])
        return _FakeResponse("{}")


class _FakeClient:
    def __init__(self, scripted: dict[str, str]) -> None:
        self.messages = _FakeMessages(scripted)


def _make_analyzer(client) -> ContractAnalyzer:
    return ContractAnalyzer(
        client,
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


def _subagent_json(cat: str, score: int, redlines: list[dict] | None = None) -> str:
    return json.dumps(
        {
            "category": cat,
            "score": score,
            "findings": [f"{cat} observation"],
            "redlines": redlines or [],
        }
    )


def test_analyze_full_flow_returns_synthesized_result():
    scripted = {
        "orchestrator": json.dumps(
            {
                "executive_summary": "Fixed-price services agreement.",
                "contract_type": "firm-fixed-price services",
                "counterparty": "Acme Corp",
                "sections_by_category": {
                    "pricing": ["§3 Payment"],
                    "liability": ["§7 Indemnification"],
                    "termination": ["§9 Termination"],
                    "sla_performance": ["§5 SLAs"],
                    "ip_confidentiality": ["§11 IP"],
                    "governing_law": ["§14 Governing Law"],
                },
                "missing_categories": [],
            }
        ),
        "subagent": {
            "pricing": _subagent_json("pricing", 7),
            "liability": _subagent_json(
                "liability",
                4,
                [
                    {
                        "clause_text": "unlimited liability",
                        "issue": "no cap",
                        "suggested_change": "cap at 1x fees",
                        "severity": "high",
                    }
                ],
            ),
            "termination": _subagent_json("termination", 6),
            "sla_performance": _subagent_json("sla_performance", 8),
            "ip_confidentiality": _subagent_json("ip_confidentiality", 9),
            "governing_law": _subagent_json("governing_law", 7),
        },
        "synthesizer": json.dumps(
            {
                "overall_score": 68,
                "recommendation": "negotiate",
                "recommendation_reason": "High-severity liability cap issue.",
                "top_risks": ["Unlimited liability"],
                "scorecard": {
                    "pricing": 7,
                    "liability": 4,
                    "termination": 6,
                    "sla_performance": 8,
                    "ip_confidentiality": 9,
                    "governing_law": 7,
                },
                "redlines": [
                    {
                        "category": "liability",
                        "clause_text": "unlimited liability",
                        "issue": "no cap",
                        "suggested_change": "cap at 1x fees",
                        "severity": "high",
                    }
                ],
            }
        ),
    }
    client = _FakeClient(scripted)
    analyzer = _make_analyzer(client)

    result = analyzer.analyze("Sample contract text.", metadata={"title": "t"})

    assert result["overall_score"] == 68
    assert result["recommendation"] == "negotiate"
    assert result["scorecard"]["liability"] == 4
    assert result["redlines"][0]["severity"] == "high"
    assert result["orchestrator_summary"].startswith("Fixed-price")
    assert set(result["sub_agent_outputs"].keys()) == {
        "pricing", "liability", "termination",
        "sla_performance", "ip_confidentiality", "governing_law",
    }
    # 1 orchestrator + 6 sub-agents + 1 synthesizer = 8 calls
    assert len(client.messages.calls) == 8


def test_analyze_falls_back_when_synthesizer_empty():
    scripted = {
        "orchestrator": "{}",
        "subagent": {
            "pricing": _subagent_json("pricing", 6),
            "liability": _subagent_json(
                "liability",
                3,
                [
                    {
                        "clause_text": "c1",
                        "issue": "i1",
                        "suggested_change": "s1",
                        "severity": "high",
                    },
                    {
                        "clause_text": "c2",
                        "issue": "i2",
                        "suggested_change": "s2",
                        "severity": "high",
                    },
                    {
                        "clause_text": "c3",
                        "issue": "i3",
                        "suggested_change": "s3",
                        "severity": "high",
                    },
                ],
            ),
            "termination": _subagent_json(
                "termination",
                2,
                [
                    {
                        "clause_text": "c4",
                        "issue": "i4",
                        "suggested_change": "s4",
                        "severity": "high",
                    }
                ],
            ),
            "sla_performance": _subagent_json("sla_performance", 5),
            "ip_confidentiality": _subagent_json("ip_confidentiality", 5),
            "governing_law": _subagent_json("governing_law", 5),
        },
        # Synthesizer returns empty → analyzer should fall back.
        "synthesizer": "",
    }
    client = _FakeClient(scripted)
    analyzer = _make_analyzer(client)

    result = analyzer.analyze("text")

    # 4 high-severity redlines → fallback should say reject.
    assert result["recommendation"] == "reject"
    assert result["overall_score"] < 50
    assert len(result["redlines"]) == 4
    assert all(r["severity"] == "high" for r in result["redlines"])


def test_analyze_returns_empty_on_blank_text():
    client = _FakeClient({"orchestrator": "{}", "subagent": {}, "synthesizer": "{}"})
    analyzer = _make_analyzer(client)
    assert analyzer.analyze("") == {}
    assert client.messages.calls == []


def test_trim_drops_middle_when_text_too_long():
    client = _FakeClient({"orchestrator": "{}", "subagent": {}, "synthesizer": "{}"})
    analyzer = _make_analyzer(client)
    analyzer._max_input_chars = 100  # type: ignore[attr-defined]
    trimmed = analyzer._trim("A" * 1000)
    assert "truncated" in trimmed
    assert len(trimmed) < 200
