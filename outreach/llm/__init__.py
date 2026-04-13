"""
LLM layer for Insulleads.

Adds three LLM-powered capabilities on top of the OpenOutreach pipeline:

  1. ``enrich_contact(lead)``  — fill missing phone / email / website / CSLB
     license via web search.
  2. ``qualify_lead(lead, campaign)`` — cold-start Bayesian qualifier fallback:
     while the GP has < 2 labels, ask an LLM to judge the lead.
  3. ``write_outreach(lead, campaign)`` — personalised Telegram/email copy,
     replacing the fixed template in ``outreach.tasks.outreach._format_message``.

Everything is hidden behind an ``LLMAdapter`` ABC so the concrete backend can
be swapped by flipping ``LLM_ADAPTER`` in ``.env``:

  * ``anthropic`` — default. Uses the ``anthropic`` Python SDK directly.
  * ``suna``      — stub for a future Suna sidecar (``docs/SUNA_INTEGRATION.md``).
  * ``noop``      — fallback used automatically when no ``ANTHROPIC_API_KEY``
                    is set. Returns empty / heuristic values so the daemon
                    keeps running on a fresh machine.
"""
from .adapter import LLMAdapter, NoopAdapter, get_adapter

__all__ = ["LLMAdapter", "NoopAdapter", "get_adapter"]
