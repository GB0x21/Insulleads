"""
Minimal CRM views.

For the full list/edit experience use `/admin/`. These views give a quick
at-a-glance dashboard and a one-click endpoint to label a lead as
positive/negative, which retrains the campaign's GP qualifier.
"""
from django.contrib.auth.decorators import login_required
from django.db.models import Count
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from outreach.ml.qualifier import retrain
from outreach.models import Campaign, Lead


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
    retrain(lead.campaign)
    return HttpResponseRedirect(reverse("crm:lead_detail", args=[pk]))
