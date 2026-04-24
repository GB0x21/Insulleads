"""
analyze_contract task — run the multi-agent analyzer over one Contract.

Payload must carry ``{"contract_id": <pk>}``. Loads the contract file
from ``settings.CONTRACTS["DOCS_DIR"]`` (PDF/DOCX via LlamaIndex's
SimpleDirectoryReader — same loader as the RAG layer), passes the text
to :meth:`LLMAdapter.analyze_contract`, and writes scorecard / redlines /
recommendation back to the row.
"""
from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from outreach.llm import get_adapter
from outreach.models import ActionLog, Contract, Task

logger = logging.getLogger("outreach.tasks.analyze_contract")


def handle(task: Task) -> None:
    contract_id = (task.payload or {}).get("contract_id")
    if not contract_id:
        raise RuntimeError("analyze_contract task missing payload.contract_id")

    try:
        contract = Contract.objects.select_related("lead", "lead__campaign").get(
            pk=contract_id
        )
    except Contract.DoesNotExist:
        logger.warning("[analyze_contract] contract_id=%s not found", contract_id)
        return

    contracts_cfg = getattr(settings, "CONTRACTS", {}) or {}
    if not contracts_cfg.get("ANALYSIS_ENABLED", True):
        logger.info("[analyze_contract] disabled by settings; skipping")
        return

    if not contract.file_path:
        contract.status = Contract.Status.FAILED
        contract.error_message = "no file_path on contract"
        contract.save(update_fields=["status", "error_message", "updated_at"])
        return

    docs_dir = Path(contracts_cfg.get("DOCS_DIR") or "data/contracts")
    abs_path = docs_dir / contract.file_path
    if not abs_path.exists():
        contract.status = Contract.Status.FAILED
        contract.error_message = f"file missing: {abs_path}"
        contract.save(update_fields=["status", "error_message", "updated_at"])
        return

    text = _load_text(abs_path)
    if not text.strip():
        contract.status = Contract.Status.FAILED
        contract.error_message = "empty document after extraction"
        contract.save(update_fields=["status", "error_message", "updated_at"])
        return

    contract.status = Contract.Status.ANALYZING
    contract.error_message = ""
    contract.save(update_fields=["status", "error_message", "updated_at"])

    adapter = get_adapter()
    if adapter.name == "noop":
        logger.info("[analyze_contract] noop adapter; marking analyzed with empty result")
        _finalize(contract, result={}, recommendation="", reason="noop adapter")
        return

    metadata = {
        "title": contract.title,
        "agency": contract.agency,
        "provider": contract.provider,
        "source_url": contract.source_url,
        "deadline": contract.deadline.isoformat() if contract.deadline else "",
    }

    try:
        result = adapter.analyze_contract(text, metadata=metadata)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[analyze_contract] adapter failed contract=%s", contract.pk)
        contract.status = Contract.Status.FAILED
        contract.error_message = f"{type(exc).__name__}: {exc}"[:5000]
        contract.save(update_fields=["status", "error_message", "updated_at"])
        raise

    _finalize(contract, result=result or {})


def _finalize(
    contract: Contract,
    *,
    result: dict,
    recommendation: str | None = None,
    reason: str | None = None,
) -> None:
    contract.raw_analysis = result or {}
    contract.scorecard = (result or {}).get("scorecard") or {}
    contract.redlines = (result or {}).get("redlines") or []

    rec_value = recommendation
    if rec_value is None:
        rec_value = (result or {}).get("recommendation") or ""
    if rec_value and rec_value not in Contract.Recommendation.values:
        rec_value = ""
    contract.recommendation = rec_value or ""

    contract.recommendation_reason = (
        reason if reason is not None else (result or {}).get("recommendation_reason") or ""
    )[:10000]

    contract.analyzed_at = timezone.now()
    contract.status = Contract.Status.COMPLETED
    contract.save(
        update_fields=[
            "raw_analysis",
            "scorecard",
            "redlines",
            "recommendation",
            "recommendation_reason",
            "analyzed_at",
            "status",
            "updated_at",
        ]
    )

    ActionLog.objects.create(
        lead=contract.lead,
        campaign=contract.lead.campaign,
        action=ActionLog.Action.NOTE,
        payload={
            "kind": "contract_analyzed",
            "contract_id": contract.pk,
            "recommendation": contract.recommendation,
            "overall_score": (result or {}).get("overall_score"),
        },
    )


def _load_text(path: Path) -> str:
    """Extract plain text from the contract file. Delegates to LlamaIndex
    for the heavy lifting (same loader the RAG index uses); falls back to
    a naive UTF-8 read if LlamaIndex is unavailable so a .txt contract
    still works in tests."""
    try:
        from llama_index.core import SimpleDirectoryReader

        reader = SimpleDirectoryReader(input_files=[str(path)])
        docs = reader.load_data()
        return "\n\n".join(d.text for d in docs if getattr(d, "text", ""))
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[analyze_contract] llama_index load failed (%s); falling back to raw read",
            exc,
        )
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc2:  # noqa: BLE001
            logger.error("[analyze_contract] raw read failed: %s", exc2)
            return ""
