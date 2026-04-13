"""
Demand forecasting stub — Phase 5 of the roadmap.

The eventual implementation will fit a Prophet model on the weekly
volume of WON leads per campaign to produce a short-horizon demand
forecast that feeds capacity planning. For now this module only
declares the API shape so callers can be wired up; invoking it
raises :class:`NotImplementedError` with a pointer to ARCHITECTURE.md.

Keeping this as a stub avoids pulling Prophet (a heavy dep) into
the base environment until there is actual label volume to justify
training.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from outreach.models import Campaign


def forecast_weekly(
    campaign: "Campaign",
    horizon_weeks: int = 8,
) -> list[dict[str, Any]]:
    """Return a list of ``{"week", "yhat", "yhat_lower", "yhat_upper"}`` dicts.

    Not yet implemented — see the "Roadmap: demand forecasting"
    section in ARCHITECTURE.md for the design.
    """
    raise NotImplementedError(
        "forecast_weekly() is a Phase 5 stub. See ARCHITECTURE.md "
        "(Roadmap: demand forecasting) for the Prophet-based design."
    )
