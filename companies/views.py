"""
companies/views.py – template (HTML) views for the public website.

Views
-----
HomeView          – landing page with featured companies, sectors, pros/cons
CompanyListView   – paginated list, filter by sector / health label
CompanyDetailView – full company profile page
CompareView       – compare shell (JS fetches chart data)
ScreenerView      – screener shell (JS fetches screener data)
SectorDetailView  – all companies in a sector ranked by health score
"""

import random
import logging

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import OuterRef, Subquery
from django.http import Http404
from django.shortcuts import get_object_or_404, render
from django.views import View

from .models import Company, Sector, MLScore, ProsCons, Document, Peer

logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _annotate_with_latest_score(qs):
    """
    Annotate a Company queryset with the latest overall_score and health_label
    using a correlated subquery so we avoid N+1 queries.
    """
    latest_score_sq = (
        MLScore.objects
        .filter(symbol=OuterRef("symbol"))
        .order_by("-computed_at")
        .values("overall_score")[:1]
    )
    latest_label_sq = (
        MLScore.objects
        .filter(symbol=OuterRef("symbol"))
        .order_by("-computed_at")
        .values("health_label")[:1]
    )
    return qs.annotate(
        latest_overall_score=Subquery(latest_score_sq),
        latest_health_label=Subquery(latest_label_sq),
    )


# ── Views ─────────────────────────────────────────────────────────────────────

class HomeView(View):
    """
    Landing page.

    Context:
    - featured_companies : 6 random companies with latest MLScore pre-fetched
    - sectors            : all Sector objects with company count
    - latest_pros_cons   : latest 10 pros_cons entries (cross-company)
    - health_label_counts: dict of health_label → count for mini dashboard
    """

    template_name = "home.html"

    def get(self, request):
        from django.db.models import Count, Avg

        # All companies with latest score annotation
        all_companies = list(
            _annotate_with_latest_score(
                Company.objects.select_related("sector")
            )
        )

        # 6 highest-scoring companies for the featured section
        scored = [c for c in all_companies if c.latest_overall_score is not None]
        scored.sort(key=lambda c: float(c.latest_overall_score), reverse=True)
        featured = scored[:6] if len(scored) >= 6 else (scored + random.sample(all_companies, 6 - len(scored)))

        # Attach latest P&L metrics (OPM%, D/E) to each featured company
        for c in featured:
            latest_pl = (
                c.profit_loss_records
                .select_related("year")
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )
            latest_bs = (
                c.balance_sheet_records
                .select_related("year")
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )
            c.cached_opm_pct = latest_pl.opm_percentage if latest_pl else None
            c.cached_de_ratio = latest_bs.debt_to_equity if latest_bs else None

        # Sectors with company count and avg health score
        sectors_qs = (
            Sector.objects
            .annotate(company_count=Count("companies"))
            .order_by("sector_name")
        )
        sectors = list(sectors_qs)
        # Add avg_health_score per sector
        for s in sectors:
            avg = (
                MLScore.objects
                .filter(symbol__sector=s)
                .filter(
                    computed_at=Subquery(
                        MLScore.objects
                        .filter(symbol=OuterRef("symbol"))
                        .order_by("-computed_at")
                        .values("computed_at")[:1]
                    )
                )
                .aggregate(Avg("overall_score"))["overall_score__avg"]
            )
            s.avg_health_score = round(float(avg), 1) if avg is not None else None

        # Latest pros/cons across all companies
        latest_pros_cons = list(
            ProsCons.objects
            .select_related("symbol")
            .order_by("-generated_at")[:10]
        )

        # Health label distribution: list of (label, count, text_color, bar_color)
        label_counts_qs = (
            MLScore.objects
            .filter(
                computed_at=Subquery(
                    MLScore.objects
                    .filter(symbol=OuterRef("symbol"))
                    .order_by("-computed_at")
                    .values("computed_at")[:1]
                )
            )
            .values("health_label")
            .annotate(count=Count("health_label"))
        )
        counts = {row["health_label"]: row["count"] for row in label_counts_qs}
        health_distribution = [
            ("EXCELLENT", counts.get("EXCELLENT", 0), "text-emerald-600", "bg-emerald-500"),
            ("GOOD",      counts.get("GOOD",      0), "text-lime-600",    "bg-lime-500"),
            ("AVERAGE",   counts.get("AVERAGE",   0), "text-yellow-600",  "bg-yellow-400"),
            ("WEAK",      counts.get("WEAK",      0), "text-orange-600",  "bg-orange-400"),
            ("POOR",      counts.get("POOR",      0), "text-red-600",     "bg-red-500"),
        ]

        context = {
            "featured_companies":  featured,
            "sectors":             sectors,
            "latest_pros_cons":    latest_pros_cons,
            "health_distribution": health_distribution,
            "total_companies":     len(all_companies),
        }
        return render(request, self.template_name, context)


class CompanyListView(View):
    """
    Paginated company directory.

    Query parameters:
    - sector      : filter by Sector.sector_name (case-insensitive contains)
    - health_label: filter by latest health_label exact match
    - sort        : field to sort by (score_desc, score_asc, name_asc, name_desc)
    - page        : pagination page number
    - q           : search by symbol or company_name
    """

    template_name = "companies/list.html"
    PAGE_SIZE = 24

    def get(self, request):
        qs = _annotate_with_latest_score(
            Company.objects.select_related("sector").order_by("symbol")
        )

        # Search
        q = request.GET.get("q", "").strip()
        if q:
            qs = qs.filter(
                models_Q_company_search(q)
            )

        # Sector filter
        sector_filter = request.GET.get("sector", "").strip()
        if sector_filter:
            qs = qs.filter(sector__sector_name__icontains=sector_filter)

        # Health label filter (post-annotation)
        health_filter = request.GET.get("health_label", "").strip().upper()
        if health_filter:
            qs = qs.filter(latest_health_label=health_filter)

        # Sorting
        sort = request.GET.get("sort", "name_asc")
        sort_map = {
            "score_desc": "-latest_overall_score",
            "score_asc":  "latest_overall_score",
            "name_asc":   "company_name",
            "name_desc":  "-company_name",
        }
        qs = qs.order_by(sort_map.get(sort, "company_name"))

        # Pagination
        paginator  = Paginator(qs, self.PAGE_SIZE)
        page_param = request.GET.get("page", 1)
        try:
            companies = paginator.page(page_param)
        except PageNotAnInteger:
            companies = paginator.page(1)
        except EmptyPage:
            companies = paginator.page(paginator.num_pages)

        from django.db.models import Count
        sectors_qs = (
            Sector.objects
            .annotate(count=Count("companies"))
            .order_by("sector_name")
        )
        # Attach .name attribute so template can use sector.name (template originally written this way)
        sectors = list(sectors_qs)
        for s in sectors:
            s.name = s.sector_name

        health_label_options = [
            ("EXCELLENT", "text-emerald-600", "bg-emerald-100"),
            ("GOOD",      "text-lime-600",    "bg-lime-100"),
            ("AVERAGE",   "text-yellow-600",  "bg-yellow-100"),
            ("WEAK",      "text-orange-600",  "bg-orange-100"),
            ("POOR",      "text-red-600",     "bg-red-100"),
        ]

        context = {
            "companies":              companies,
            "sectors":                sectors,
            "health_label_options":   health_label_options,
            "selected_sectors":       [sector_filter] if sector_filter else [],
            "selected_health_labels": [health_filter] if health_filter else [],
            "current_sort":           sort,
            "search_query":           q,
        }
        return render(request, self.template_name, context)


def models_Q_company_search(q):
    """Return a Q object for symbol OR company_name search."""
    from django.db.models import Q
    return Q(symbol__icontains=q) | Q(company_name__icontains=q)


class CompanyDetailView(View):
    """
    Full company profile page.

    Context:
    - company       : Company instance
    - latest_score  : most recent MLScore
    - all_scores    : last 5 MLScore rows (for sub-score radar chart)
    - pros          : ProsCons where is_pro=True
    - cons          : ProsCons where is_pro=False
    - documents     : Document records ordered by year desc
    - peers         : top 5 Peer entries with peer company data
    - profit_loss   : all P&L rows ordered by year
    - balance_sheet : all BS rows ordered by year
    - cash_flow     : all CF rows ordered by year
    """

    template_name = "companies/detail.html"

    def get(self, request, symbol):
        symbol = symbol.upper()
        company = get_object_or_404(
            Company.objects.select_related("sector"),
            symbol=symbol,
        )

        # Latest and recent ML scores
        latest_score = company.ml_scores.order_by("-computed_at").first()
        all_scores   = list(company.ml_scores.order_by("-computed_at")[:5])

        # Pros / cons
        pros = list(company.pros_cons.filter(is_pro=True).order_by("id"))
        cons = list(company.pros_cons.filter(is_pro=False).order_by("id"))

        # Documents (annual reports)
        documents = list(
            Document.objects
            .filter(symbol=symbol)
            .order_by("-year")
        )

        # Peers – top 5 by rank (fact_peers has composite PK, wrap in try/except)
        try:
            peers = list(
                Peer.objects
                .filter(symbol=company)
                .select_related("peer_symbol", "peer_symbol__sector")
                .order_by("rank")[:5]
            )
        except Exception:
            peers = []

        # Financial time-series data (for client-side chart rendering)
        profit_loss = list(
            company.profit_loss_records
            .select_related("year")
            .order_by("year__sort_order")
        )
        balance_sheet = list(
            company.balance_sheet_records
            .select_related("year")
            .order_by("year__sort_order")
        )
        cash_flow = list(
            company.cash_flow_records
            .select_related("year")
            .order_by("year__sort_order")
        )

        # Latest non-TTM rows for quick stats
        latest_pl = next(
            (pl for pl in reversed(profit_loss) if not pl.year.is_ttm), None
        )
        latest_bs = next(
            (bs for bs in reversed(balance_sheet) if not bs.year.is_ttm), None
        )

        context = {
            "company":       company,
            "latest_score":  latest_score,
            "all_scores":    all_scores,
            "pros":          pros,
            "cons":          cons,
            "documents":     documents,
            "peers":         peers,
            "profit_loss":   profit_loss,
            "balance_sheet": balance_sheet,
            "cash_flow":     cash_flow,
            "latest_pl":     latest_pl,
            "latest_bs":     latest_bs,
        }
        return render(request, self.template_name, context)


class CompareView(View):
    """
    Company comparison shell page.

    The page renders an empty frame; JavaScript calls the Chart.js API
    (api/v1/companies/<symbol>/charts/) for each selected company.

    Context:
    - all_companies : list of (symbol, company_name) for the selector dropdowns
    - preselected   : list of symbols from GET ?symbols=A,B,C (max 4)
    """

    template_name = "companies/compare.html"

    def get(self, request):
        all_companies = list(
            Company.objects
            .values("symbol", "company_name")
            .order_by("company_name")
        )

        raw_symbols = request.GET.get("symbols", "")
        preselected = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()][:4]

        context = {
            "all_companies": all_companies,
            "preselected":   preselected,
        }
        return render(request, self.template_name, context)


class ScreenerView(View):
    """
    Dynamic screener shell page.

    Renders the screener UI shell; all filtering logic runs in the browser
    and calls api/v1/screener/ for results.

    Context:
    - sectors      : all sectors (for filter chips)
    - health_labels: ordered label choices
    """

    template_name = "companies/screener.html"

    def get(self, request):
        sectors      = list(Sector.objects.order_by("sector_name"))
        health_labels = [
            {"label": "EXCELLENT", "color": "#22c55e"},
            {"label": "GOOD",      "color": "#84cc16"},
            {"label": "AVERAGE",   "color": "#eab308"},
            {"label": "WEAK",      "color": "#f97316"},
            {"label": "POOR",      "color": "#ef4444"},
        ]

        context = {
            "sectors":      sectors,
            "health_labels": health_labels,
        }
        return render(request, self.template_name, context)


class SectorDetailView(View):
    """
    All companies in a given sector ranked by latest health score (desc).

    Context:
    - sector    : Sector instance
    - companies : annotated Company queryset, ordered best-score-first
    - avg_score : average overall_score for the sector
    """

    template_name = "companies/sector.html"

    def get(self, request, name):
        sector = get_object_or_404(
            Sector, sector_name__iexact=name
        )

        companies = list(
            _annotate_with_latest_score(
                Company.objects
                .filter(sector=sector)
                .select_related("sector")
            )
            .order_by("-latest_overall_score")
        )

        # Compute sector average score (ignoring companies with no score)
        scored = [c for c in companies if c.latest_overall_score is not None]
        avg_score = (
            round(sum(float(c.latest_overall_score) for c in scored) / len(scored), 1)
            if scored else None
        )

        context = {
            "sector":    sector,
            "companies": companies,
            "avg_score": avg_score,
        }
        return render(request, self.template_name, context)
