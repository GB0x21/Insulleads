"""
Minimal CRM views.

For the full list/edit experience use `/admin/`. These views give a quick
at-a-glance dashboard and a one-click endpoint to label a lead as
positive/negative, which retrains the campaign's GP qualifier.

Fase 2: label_lead también registra el outcome en utils/lead_outcomes.py
para alimentar el feedback loop de memoria hacia los agentes.
"""
import logging

from django.contrib.auth.decorators import login_required
from django.db.models import Count
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from outreach.ml.lgbm import retrain as lgbm_retrain
from outreach.ml.qualifier import retrain as gp_retrain
from outreach.models import Campaign, Lead

logger = logging.getLogger(__name__)


@login_required
def dashboard(request):
    campaigns = Campaign.objects.filter(is_active=True)
    by_stage = (
        Lead.objects.values("stage")
        .annotate(count=Count("id"))
        .order_by("-count")
    )
    recent = Lead.objects.order_by("-created_at")[:50]
    return render(
        request,
        "crm/dashboard.html",
        {
            "campaigns": campaigns,
            "by_stage": by_stage,
            "recent": recent,
        },
    )


@login_required
def lead_detail(request, pk: int):
    lead = get_object_or_404(Lead, pk=pk)
    return render(request, "crm/lead_detail.html", {"lead": lead})


@login_required
@require_POST
def label_lead(request, pk: int):
    lead = get_object_or_404(Lead, pk=pk)
    label = request.POST.get("label")
    if label == "won":
        lead.advance(Lead.Stage.WON)
    elif label == "replied":
        lead.advance(Lead.Stage.REPLIED)
    elif label == "lost":
        lead.advance(Lead.Stage.LOST)

    # Retrain both qualifiers: the GP for legacy compatibility and
    # LightGBM as the new primary (Phase 2 of the roadmap). Both are
    # no-ops when their sklearn/lightgbm dep is missing, so this is safe.
    gp_retrain(lead.campaign)
    lgbm_retrain(lead.campaign)

    # Fase 2: registrar outcome en memoria para feedback loop de agentes.
    # Convierte el ORM Lead a dict para que lead_outcomes no dependa de Django.
    if label in ("won", "replied", "lost"):
        try:
            from utils.lead_outcomes import register_outcome
            lead_dict = {
                "id":            str(lead.pk),
                "city":          lead.city or "",
                "address":       lead.address or "",
                "description":   lead.description or "",
                "permit_type":   lead.project_type or "",
                "contractor":    lead.contact_company or lead.contact_name or "",
                "contact_phone": lead.contact_phone or "",
                "contact_email": lead.contact_email or "",
                "value_float":   float(lead.project_value or 0),
                "_agent_key":    getattr(lead.source, "kind", "unknown") if lead.source_id else "unknown",
            }
            register_outcome(lead_dict, label)
        except Exception as e:
            logger.warning(f"[crm] No se pudo registrar outcome para lead {pk}: {e}")

    return HttpResponseRedirect(reverse("crm:lead_detail", args=[pk]))
