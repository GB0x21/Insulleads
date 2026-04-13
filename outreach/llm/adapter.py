"""
LLM adapter interface.

Keeps the pipeline (``outreach/tasks/*``) free of any vendor imports.
``get_adapter()`` returns a singleton chosen by ``settings.LLM["ADAPTER"]``:

* ``anthropic`` → :class:`AnthropicAdapter` (see ``outreach.llm.client``).
* ``suna``      → :class:`SunaSidecarAdapter` (stub, see
                  ``docs/SUNA_INTEGRATION.md``).
* ``noop``      → :class:`NoopAdapter` — safe default when no API key is set.

Any call site must gracefully tolerate the Noop path: ``enrich_contact``
returns ``{}``, ``qualify_lead`` returns ``(None, "")``, ``write_outreach``
returns ``None`` (meaning "use the legacy template").
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from threading import Lock
from typing import TYPE_CHECKING, Any

from django.conf import settings

if TYPE_CHECKING:
    from outreach.models import Campaign, Lead

logger = logging.getLogger("outreach.llm")


class LLMAdapter(ABC):
    """Stable interface for every LLM backend.

    The three methods are the *only* contract the pipeline relies on, so a
    future Suna sidecar can drop in without touching ``outreach.tasks``.
    """

    name: str = "base"

    @abstractmethod
    def enrich_contact(self, lead: "Lead") -> dict[str, Any]:
        """Return a dict of contact fields to merge into the lead.

        Allowed keys (all optional): ``website``, ``contact_phone``,
        ``contact_email``, ``contact_name``, ``contact_company``,
        ``cslb_license``, ``linkedin``, ``notes``.
        """

    @abstractmethod
    def qualify_lead(
        self, lead: "Lead", campaign: "Campaign"
    ) -> tuple[float | None, str]:
        """Return ``(score_0_to_1, one_line_reason)``.

        Only called during cold-start (GP qualifier has < 2 labels).
        ``score`` is ``None`` when the adapter declines / fails.
        """

    @abstractmethod
    def write_outreach(
        self, lead: "Lead", campaign: "Campaign"
    ) -> str | None:
        """Return a ready-to-send message body, or ``None`` to fall back
        to the legacy template in ``outreach.tasks.outreach``."""


class NoopAdapter(LLMAdapter):
    """Zero-dependency fallback. Always used when ``ANTHROPIC_API_KEY`` is
    missing or ``LLM_ADAPTER=noop``. Keeps the daemon runnable on a fresh
    box with no secrets."""

    name = "noop"

    def enrich_contact(self, lead):  # noqa: D401
        return {}

    def qualify_lead(self, lead, campaign):
        return (None, "")

    def write_outreach(self, lead, campaign):
        return None


class SunaSidecarAdapter(LLMAdapter):
    """Stub for a Suna sidecar backend.

    See ``docs/SUNA_INTEGRATION.md`` for the full integration playbook.
    The interface is identical on purpose so swapping to Suna is a
    one-env-var change.
    """

    name = "suna"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def _not_implemented(self) -> None:
        raise NotImplementedError(
            "SunaSidecarAdapter is a stub. Run Suna as a sidecar container "
            "and implement the HTTP calls — see docs/SUNA_INTEGRATION.md."
        )

    def enrich_contact(self, lead):
        self._not_implemented()

    def qualify_lead(self, lead, campaign):
        self._not_implemented()

    def write_outreach(self, lead, campaign):
        self._not_implemented()


# ─── Singleton factory ───────────────────────────────────────────────
_adapter: LLMAdapter | None = None
_adapter_lock = Lock()


def get_adapter() -> LLMAdapter:
    """Return the process-wide LLM adapter, lazily constructed.

    Falls back to :class:`NoopAdapter` whenever the chosen backend cannot
    be initialised (no API key, SDK missing, …) so callers never have to
    guard against ``ImportError``.
    """
    global _adapter
    if _adapter is not None:
        return _adapter

    with _adapter_lock:
        if _adapter is not None:
            return _adapter

        cfg = getattr(settings, "LLM", {}) or {}
        backend = (cfg.get("ADAPTER") or "anthropic").lower()

        if backend == "noop" or not cfg.get("ENABLED"):
            _adapter = NoopAdapter()
            return _adapter

        if backend == "suna":
            base_url = cfg.get("SUNA_BASE_URL") or ""
            if not base_url:
                logger.warning("LLM_ADAPTER=suna but SUNA_BASE_URL is empty; "
                               "falling back to NoopAdapter")
                _adapter = NoopAdapter()
                return _adapter
            _adapter = SunaSidecarAdapter(base_url)
            return _adapter

        # Default: Anthropic
        try:
            from .client import AnthropicAdapter

            _adapter = AnthropicAdapter(cfg)
            logger.info("LLM adapter initialised: anthropic")
            return _adapter
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "AnthropicAdapter unavailable (%s); using NoopAdapter", exc
            )
            _adapter = NoopAdapter()
            return _adapter


def reset_adapter() -> None:
    """Test hook — drop the cached adapter so the next ``get_adapter()``
    re-reads ``settings.LLM``."""
    global _adapter
    with _adapter_lock:
        _adapter = None
