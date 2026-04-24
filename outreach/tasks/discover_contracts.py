"""
discover_contracts task — find open bids/RFPs and persist Lead + Contract.

Scheduled per ``Source(kind="contract_bids")`` row. The source's
``config["provider"]`` picks the concrete discovery agent (sam_gov,
web_search, state_bids, construction). For each yielded
:class:`BidCandidate` we:

  1. Upsert a Lead (stage=DISCOVERED) keyed on (source, external_id).
  2. Upsert a Contract (status=DISCOVERED/DOWNLOADED).
  3. Download attached PDFs to ``settings.CONTRACTS["DOCS_DIR"]/<contract_id>/``
     within the configured size cap.
  4. Enqueue an ANALYZE_CONTRACT task for the contract when file is ready.

Everything is idempotent: re-running the task on the same set of bids is
a no-op beyond updating timestamps.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from urllib.parse import urlparse

import requests
from django.conf import settings
from django.utils import timezone

from agents.contract_discovery import BidCandidate, get_discovery_agent
from outreach.models import ActionLog, Contract, Lead, Source, Task

logger = logging.getLogger("outreach.tasks.discover_contracts")

DOWNLOAD_TIMEOUT = 60
DOWNLOAD_CHUNK = 1024 * 64


def handle(task: Task) -> None:
    source = task.source
    if source is None or source.kind != "contract_bids":
        raise RuntimeError(
            "discover_contracts task requires a Source(kind='contract_bids')"
        )

    contracts_cfg = getattr(settings, "CONTRACTS", {}) or {}
    if not contracts_cfg.get("DISCOVERY_ENABLED", True):
        task.reschedule(minutes=source.interval_minutes)
        return

    provider = (source.config or {}).get("provider") or source.key
    try:
        agent = get_discovery_agent(provider)
    except ValueError as exc:
        logger.warning("[discover_contracts] %s", exc)
        source.last_error = str(exc)[:5000]
        source.save(update_fields=["last_error"])
        task.reschedule(minutes=source.interval_minutes)
        return

    candidates: list[BidCandidate] = []
    try:
        for cand in agent.discover(source.config or {}):
            if cand and cand.external_id:
                candidates.append(cand)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[discover_contracts] provider=%s failed", provider)
        source.last_error = f"{type(exc).__name__}: {exc}"[:5000]
        source.save(update_fields=["last_error"])
        task.reschedule(minutes=source.interval_minutes)
        raise

    new_contracts = 0
    updated_contracts = 0
    enqueued = 0

    for cand in candidates:
        lead = _upsert_lead(cand, source, task)
        contract, created = _upsert_contract(cand, lead, source)
        if created:
            new_contracts += 1
        else:
            updated_contracts += 1

        downloaded = _download_attachments(contract, cand.file_urls, contracts_cfg)
        if downloaded and contract.status in (
            Contract.Status.DISCOVERED,
            Contract.Status.FAILED,
        ):
            contract.status = Contract.Status.DOWNLOADED
            contract.save(update_fields=["status", "updated_at"])

        if (
            contracts_cfg.get("ANALYSIS_ENABLED", True)
            and contract.status
            in (Contract.Status.DOWNLOADED, Contract.Status.DISCOVERED)
            and contract.file_path
        ):
            _enqueue_analysis(contract, source)
            enqueued += 1

    source.last_run_at = timezone.now()
    source.last_error = ""
    source.save(update_fields=["last_run_at", "last_error"])

    logger.info(
        "[discover_contracts] provider=%s candidates=%d new=%d updated=%d "
        "analysis_enqueued=%d",
        provider,
        len(candidates),
        new_contracts,
        updated_contracts,
        enqueued,
    )

    task.reschedule(minutes=source.interval_minutes)


# ─── Helpers ────────────────────────────────────────────────────────
def _upsert_lead(cand: BidCandidate, source: Source, task: Task) -> Lead:
    """Create (or update) the Lead row that owns this bid.

    Uses ``(source, external_id)`` as dedupe key. Lead title doubles as
    the bid title so the CRM shows something meaningful out of the box.
    """
    defaults = {
        "title": cand.title[:512] or f"Bid {cand.external_id}",
        "description": cand.description[:4000],
        "city": cand.city[:128],
        "project_value": cand.estimated_value_usd,
        "project_type": "bid",
        "contact_company": cand.agency[:256],
        "contact_source": f"contract_discovery:{cand.provider}",
        "stage": Lead.Stage.DISCOVERED,
        "source": source,
        "campaign": task.campaign,
        "raw": cand.raw_payload or {},
    }
    lead, created = Lead.objects.update_or_create(
        source=source,
        external_id=cand.external_id,
        defaults=defaults,
    )
    if created:
        ActionLog.objects.create(
            lead=lead,
            campaign=task.campaign,
            action=ActionLog.Action.DISCOVERED,
            payload={"source": source.key, "provider": cand.provider},
        )
    return lead


def _upsert_contract(
    cand: BidCandidate, lead: Lead, source: Source
) -> tuple[Contract, bool]:
    defaults = {
        "lead": lead,
        "provider": cand.provider,
        "title": cand.title[:500],
        "agency": cand.agency[:300],
        "source_url": cand.source_url[:1000],
        "deadline": cand.deadline,
        "estimated_value_usd": cand.estimated_value_usd,
        "raw_payload": cand.raw_payload or {},
    }
    contract, created = Contract.objects.update_or_create(
        source=source,
        external_id=cand.external_id,
        defaults=defaults,
    )
    return contract, created


def _download_attachments(
    contract: Contract, urls: list[str], contracts_cfg: dict
) -> bool:
    """Download the first downloadable URL into the contract's dir.

    Returns True when ``contract.file_path`` now points at a real file.
    Skips silently when no URL is downloadable so the analyzer can still
    run on the metadata (or not run at all) without failing the task.
    """
    if contract.file_path and _file_exists(contracts_cfg, contract.file_path):
        return True
    if not urls:
        return False

    docs_dir = Path(contracts_cfg.get("DOCS_DIR") or "data/contracts")
    target_dir = docs_dir / str(contract.pk)
    max_bytes = int(contracts_cfg.get("MAX_FILE_SIZE_MB", 50)) * 1024 * 1024

    for url in urls:
        try:
            filename = _safe_filename(url)
            target_dir.mkdir(parents=True, exist_ok=True)
            out_path = target_dir / filename
            resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            resp.raise_for_status()
            total = 0
            hasher = hashlib.sha256()
            with out_path.open("wb") as fh:
                for chunk in resp.iter_content(DOWNLOAD_CHUNK):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        fh.close()
                        out_path.unlink(missing_ok=True)
                        raise IOError(
                            f"file exceeds max size {max_bytes} bytes"
                        )
                    hasher.update(chunk)
                    fh.write(chunk)
            rel_path = out_path.relative_to(docs_dir).as_posix()
            contract.file_path = rel_path
            contract.file_size_bytes = total
            contract.file_hash = hasher.hexdigest()
            contract.save(
                update_fields=["file_path", "file_size_bytes", "file_hash", "updated_at"]
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[discover_contracts] download failed url=%s err=%s", url, exc
            )
            continue
    return False


def _file_exists(contracts_cfg: dict, rel_path: str) -> bool:
    docs_dir = Path(contracts_cfg.get("DOCS_DIR") or "data/contracts")
    return (docs_dir / rel_path).exists()


def _safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "document.pdf"
    # sanitise — keep alphanumerics, dot, dash, underscore
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:120] or "document.pdf"


def _enqueue_analysis(contract: Contract, source: Source) -> None:
    """Queue an ANALYZE_CONTRACT task unless one is already pending."""
    exists = Task.objects.filter(
        type=Task.Type.ANALYZE_CONTRACT,
        status=Task.Status.PENDING,
        payload__contract_id=contract.pk,
    ).exists()
    if exists:
        return
    Task.objects.create(
        type=Task.Type.ANALYZE_CONTRACT,
        campaign=contract.lead.campaign,
        source=source,
        lead=contract.lead,
        payload={"contract_id": contract.pk},
    )
