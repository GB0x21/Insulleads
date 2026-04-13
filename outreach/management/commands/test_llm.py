"""
test_llm — smoke-test the LLM adapter from the CLI.

    python manage.py test_llm --feature enrich
    python manage.py test_llm --feature qualify
    python manage.py test_llm --feature write
    python manage.py test_llm --feature all

If the DB has no campaigns / leads the command builds an in-memory synthetic
pair so the smoke test always has something to operate on.
"""
from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from outreach.llm import get_adapter
from outreach.models import Campaign, Lead, Source


def _synthetic_campaign() -> Campaign:
    return Campaign(
        name="__smoke__",
        product_description=(
            "Spray-foam and blown-in attic insulation services for Bay Area "
            "residential remodels. Title-24 compliance, 1-day install."
        ),
        target_market=(
            "General contractors running SFH remodels, ADU builds and "
            "reroof jobs in San Francisco, Oakland, Berkeley and the Peninsula."
        ),
        outreach_tone=Campaign.Tone.PROFESSIONAL,
    )


def _synthetic_lead(campaign: Campaign) -> Lead:
    src = Source(
        key="__smoke__",
        kind="permits",
        campaign=campaign,
    )
    lead = Lead(
        external_id="smoke-1",
        source=src,
        campaign=campaign,
        title="Kitchen + primary bath remodel, rough-in stage",
        address="1234 Page St",
        city="San Francisco",
        project_value=145000,
        project_type="Remodel",
        description=(
            "Full gut remodel, opening walls between kitchen and living, "
            "replacing exterior sheathing, framing stage, permit issued "
            "last week."
        ),
        contact_name="Maria Rossi",
        contact_company="Rossi Builders Inc.",
        lead_score=72,
    )
    return lead


def _emit(label: str, value) -> None:
    print(f"\n━━━ {label} ━━━")
    if isinstance(value, (dict, list)):
        print(json.dumps(value, indent=2))
    else:
        print(value)


class Command(BaseCommand):
    help = "Smoke-test the outreach.llm adapter (enrich / qualify / write)."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--feature",
            choices=["enrich", "qualify", "write", "all"],
            default="all",
        )
        parser.add_argument(
            "--lead-id",
            type=int,
            default=None,
            help="Use an existing lead from the DB instead of the synthetic one.",
        )

    def handle(self, *args, **opts) -> None:
        adapter = get_adapter()
        self.stdout.write(f"Adapter: {adapter.name}")

        if opts["lead_id"]:
            lead = Lead.objects.select_related("campaign", "source").get(
                pk=opts["lead_id"]
            )
            campaign = lead.campaign
        else:
            campaign = _synthetic_campaign()
            lead = _synthetic_lead(campaign)
            self.stdout.write("Using synthetic campaign + lead (nothing saved).")

        feature = opts["feature"]

        if feature in ("enrich", "all"):
            _emit("enrich_contact", adapter.enrich_contact(lead))

        if feature in ("qualify", "all"):
            score, reason = adapter.qualify_lead(lead, campaign)
            _emit("qualify_lead", {"score": score, "reason": reason})

        if feature in ("write", "all"):
            body = adapter.write_outreach(lead, campaign)
            _emit("write_outreach", body or "(none — falling back to template)")
