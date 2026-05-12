"""
companies/api_views.py – internal JSON API views consumed by Chart.js / AJAX.

Views
-----
CompanyListAPIView   – paginated JSON company list with filters
CompanyChartsAPIView – all chart data for one company
CompanyPeersAPIView  – top-5 peers for one company
ScreenerAPIView      – dynamic Q()-based screener
"""

import logging
from decimal import Decimal

from django.core.cache import cache
from django.db.models import OuterRef, Subquery, Q
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from drf_spectacular.utils import extend_schema, OpenApiParameter

from .models import (
    Company,
    Sector,
    MLScore,
    ProfitLoss,
    BalanceSheet,
    CashFlow,
    Analysis,
    Peer,
    ProsCons,
    Anomaly,
    Forecast,
)
from .serializers import (
    CompanyListSerializer,
    ProfitLossSerializer,
    BalanceSheetSerializer,
    CashFlowSerializer,
    MLScoreSerializer,
    AnalysisSerializer,
    PeerSerializer,
    ProsConsSerializer,
    AnomalySerializer,
    ForecastSerializer,
)

logger = logging.getLogger(__name__)

CHART_CACHE_TTL = 300   # 5 minutes


# ── Helpers ───────────────────────────────────────────────────────────────────

def _annotate_with_latest_score(qs):
    """Annotate a Company queryset with the latest ML score fields."""
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


def _to_float(val):
    """Safely convert Decimal or None to float / None."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ── Views ─────────────────────────────────────────────────────────────────────

@extend_schema(tags=["companies"])
class CompanyListAPIView(APIView):
    """
    GET /api/v1/companies/

    Returns a paginated JSON list of companies.

    Query parameters
    ----------------
    q            : search symbol / company_name (case-insensitive)
    sector       : filter by sector name (case-insensitive contains)
    health_label : exact match (EXCELLENT | GOOD | AVERAGE | WEAK | POOR)
    is_banking   : true | false
    sort         : score_desc (default) | score_asc | name_asc | name_desc
    page         : integer, default 1
    page_size    : integer 1-100, default 20
    """

    @extend_schema(
        parameters=[
            OpenApiParameter("q",            description="Search query"),
            OpenApiParameter("sector",        description="Sector name filter"),
            OpenApiParameter("health_label",  description="Health label filter"),
            OpenApiParameter("is_banking",    description="Banking flag"),
            OpenApiParameter("sort",          description="Sort order"),
            OpenApiParameter("page",          description="Page number"),
            OpenApiParameter("page_size",     description="Page size"),
        ]
    )
    def get(self, request):
        qs = _annotate_with_latest_score(
            Company.objects.select_related("sector")
        )

        # Search
        q = request.query_params.get("q", "").strip()
        if q:
            qs = qs.filter(
                Q(symbol__icontains=q) | Q(company_name__icontains=q)
            )

        # Sector filter
        sector = request.query_params.get("sector", "").strip()
        if sector:
            qs = qs.filter(sector__sector_name__icontains=sector)

        # Health label filter
        health_label = request.query_params.get("health_label", "").strip().upper()
        if health_label:
            qs = qs.filter(latest_health_label=health_label)

        # Banking filter
        is_banking = request.query_params.get("is_banking", "").strip().lower()
        if is_banking == "true":
            qs = qs.filter(is_banking=True)
        elif is_banking == "false":
            qs = qs.filter(is_banking=False)

        # Sorting
        sort = request.query_params.get("sort", "score_desc")
        sort_map = {
            "score_desc": "-latest_overall_score",
            "score_asc":  "latest_overall_score",
            "name_asc":   "company_name",
            "name_desc":  "-company_name",
        }
        qs = qs.order_by(sort_map.get(sort, "-latest_overall_score"))

        # Pagination
        try:
            page      = max(1, int(request.query_params.get("page", 1)))
            page_size = min(100, max(1, int(request.query_params.get("page_size", 20))))
        except (ValueError, TypeError):
            page, page_size = 1, 20

        total = qs.count()
        start = (page - 1) * page_size
        end   = start + page_size
        companies = qs[start:end]

        serializer = CompanyListSerializer(companies, many=True)

        return Response({
            "count":      total,
            "page":       page,
            "page_size":  page_size,
            "num_pages":  max(1, -(-total // page_size)),   # ceiling division
            "results":    serializer.data,
        })


@extend_schema(tags=["charts"])
class CompanyChartsAPIView(APIView):
    """
    GET /api/v1/companies/<symbol>/charts/

    Returns all chart-ready time-series data for a single company.

    Response shape
    --------------
    {
      "symbol": "RELIANCE",
      "company_name": "...",
      "years": ["Mar 2019", "Mar 2020", ...],
      "revenue_trend":    { labels, revenue, expenses, net_profit, opm_pct },
      "balance_sheet":    { labels, equity, borrowings, total_assets },
      "cash_flow":        { labels, operating, investing, financing, fcf },
      "eps_dividend":     { labels, eps, dividend_payout },
      "debt_vs_equity":   { labels, debt_to_equity, equity_ratio },
      "margin_trend":     { labels, npm_pct, opm_pct, return_on_assets },
      "ml_scores":        [ { computed_at, overall_score, ... } ],
      "analysis_cagr":    { "compounded_sales_growth": {"5Y": x, "3Y": y, ...} },
      "forecasts":        [ { forecast_year, predicted_sales, ... } ],
    }
    """

    def get(self, request, symbol):
        symbol = symbol.upper()

        cache_key = f"charts:{symbol}"
        cached    = cache.get(cache_key)
        if cached is not None:
            return Response(cached)

        # Validate company exists
        try:
            company = Company.objects.select_related("sector").get(symbol=symbol)
        except Company.DoesNotExist:
            return Response(
                {"detail": f"Company '{symbol}' not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        # ── P&L data ──────────────────────────────────────────────────────────
        pl_rows = list(
            company.profit_loss_records
            .select_related("year")
            .order_by("year__sort_order")
        )

        # ── Balance sheet data ────────────────────────────────────────────────
        bs_rows = list(
            company.balance_sheet_records
            .select_related("year")
            .order_by("year__sort_order")
        )

        # ── Cash flow data ────────────────────────────────────────────────────
        cf_rows = list(
            company.cash_flow_records
            .select_related("year")
            .order_by("year__sort_order")
        )

        # ── ML scores (last 10 runs) ──────────────────────────────────────────
        ml_scores = list(
            company.ml_scores
            .order_by("-computed_at")[:10]
        )

        # ── Analysis CAGR ─────────────────────────────────────────────────────
        analysis_rows = list(company.analysis_records.all())
        analysis_cagr = {}
        for row in analysis_rows:
            metric = row.metric
            if metric not in analysis_cagr:
                analysis_cagr[metric] = {}
            analysis_cagr[metric][row.period] = _to_float(row.value_pct)

        # ── Forecasts ─────────────────────────────────────────────────────────
        forecasts = list(company.forecasts.order_by("forecast_year"))

        # ── Build chart datasets ──────────────────────────────────────────────
        pl_labels   = [r.year.year_label for r in pl_rows]
        bs_labels   = [r.year.year_label for r in bs_rows]
        cf_labels   = [r.year.year_label for r in cf_rows]

        revenue_trend = {
            "labels":     pl_labels,
            "revenue":    [_to_float(r.sales)           for r in pl_rows],
            "expenses":   [_to_float(r.expenses)        for r in pl_rows],
            "net_profit": [_to_float(r.net_profit)      for r in pl_rows],
            "opm_pct":    [_to_float(r.opm_percentage)  for r in pl_rows],
        }

        balance_sheet_chart = {
            "labels":      bs_labels,
            "equity":      [_to_float(r.equity_capital) for r in bs_rows],
            "reserves":    [_to_float(r.reserves)       for r in bs_rows],
            "borrowings":  [_to_float(r.borrowings)     for r in bs_rows],
            "total_assets":[_to_float(r.total_assets)   for r in bs_rows],
        }

        cash_flow_chart = {
            "labels":    cf_labels,
            "operating": [_to_float(r.operating_activity) for r in cf_rows],
            "investing": [_to_float(r.investing_activity) for r in cf_rows],
            "financing": [_to_float(r.financing_activity) for r in cf_rows],
            "fcf":       [_to_float(r.free_cash_flow)     for r in cf_rows],
        }

        eps_dividend = {
            "labels":          pl_labels,
            "eps":             [_to_float(r.eps)             for r in pl_rows],
            "dividend_payout": [_to_float(r.dividend_payout) for r in pl_rows],
        }

        debt_vs_equity = {
            "labels":       bs_labels,
            "debt_to_equity": [_to_float(r.debt_to_equity) for r in bs_rows],
            "equity_ratio":   [_to_float(r.equity_ratio)   for r in bs_rows],
        }

        margin_trend = {
            "labels":           pl_labels,
            "npm_pct":          [_to_float(r.net_profit_margin_pct) for r in pl_rows],
            "opm_pct":          [_to_float(r.opm_percentage)        for r in pl_rows],
            "return_on_assets": [_to_float(r.return_on_assets_pct)  for r in pl_rows],
        }

        # ── Serialise ML scores ───────────────────────────────────────────────
        ml_score_data = MLScoreSerializer(ml_scores, many=True).data

        # ── Serialise forecasts ───────────────────────────────────────────────
        forecast_data = ForecastSerializer(forecasts, many=True).data

        payload = {
            "symbol":              company.symbol,
            "company_name":        company.company_name,
            "is_banking":          company.is_banking,
            "revenue_trend":       revenue_trend,
            "balance_sheet":       balance_sheet_chart,
            "cash_flow":           cash_flow_chart,
            "eps_dividend":        eps_dividend,
            "debt_vs_equity":      debt_vs_equity,
            "margin_trend":        margin_trend,
            "ml_scores":           ml_score_data,
            "analysis_cagr":       analysis_cagr,
            "forecasts":           forecast_data,
        }

        cache.set(cache_key, payload, CHART_CACHE_TTL)
        return Response(payload)


@extend_schema(tags=["companies"])
class CompanyPeersAPIView(APIView):
    """
    GET /api/v1/companies/<symbol>/peers/

    Returns the top-5 peer companies by similarity rank.
    Each peer entry includes the peer company's latest health label
    and key ratios for comparison tables.
    """

    def get(self, request, symbol):
        symbol = symbol.upper()

        cache_key = f"peers:{symbol}"
        cached    = cache.get(cache_key)
        if cached is not None:
            return Response(cached)

        if not Company.objects.filter(symbol=symbol).exists():
            return Response(
                {"detail": f"Company '{symbol}' not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        peers = list(
            Peer.objects
            .filter(symbol_id=symbol)
            .select_related("peer_symbol", "peer_symbol__sector")
            .prefetch_related("peer_symbol__ml_scores")
            .order_by("rank")[:5]
        )

        peer_data = []
        for peer in peers:
            p = peer.peer_symbol
            latest = p.ml_scores.order_by("-computed_at").first()
            latest_pl = (
                p.profit_loss_records
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )
            latest_bs = (
                p.balance_sheet_records
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )

            peer_data.append({
                "symbol":        p.symbol,
                "company_name":  p.company_name,
                "sector_name":   p.sector.sector_name if p.sector else None,
                "company_logo":  p.company_logo,
                "is_banking":    p.is_banking,
                "similarity":    _to_float(peer.similarity),
                "rank":          peer.rank,
                "health_label":  latest.health_label  if latest else None,
                "overall_score": _to_float(latest.overall_score) if latest else None,
                "opm_pct":       _to_float(latest_pl.opm_percentage) if latest_pl else None,
                "de_ratio":      _to_float(latest_bs.debt_to_equity) if latest_bs else None,
                "eps":           _to_float(latest_pl.eps)            if latest_pl else None,
            })

        payload = {
            "symbol": symbol,
            "peers":  peer_data,
        }
        cache.set(cache_key, payload, CHART_CACHE_TTL)
        return Response(payload)


@extend_schema(tags=["screener"])
class ScreenerAPIView(APIView):
    """
    GET /api/v1/screener/

    Dynamic screener – applies Q() filters and returns a compact company list.

    Filter parameters (all optional, combinable)
    --------------------------------------------
    q              : text search (symbol / name)
    sector         : sector name contains
    health_label   : exact label (EXCELLENT | GOOD | AVERAGE | WEAK | POOR)
    is_banking     : true | false
    min_score      : overall_score >= value  (0-100)
    max_score      : overall_score <= value  (0-100)
    min_opm        : latest opm_percentage >= value
    max_opm        : latest opm_percentage <= value
    min_de         : latest debt_to_equity >= value
    max_de         : latest debt_to_equity <= value
    positive_fcf   : true  – companies whose latest FCF > 0
    sort           : score_desc (default) | score_asc | name_asc
    page           : default 1
    page_size      : default 20, max 100
    """

    def get(self, request):  # noqa: C901 – intentionally complex filter block
        params = request.query_params

        qs = _annotate_with_latest_score(
            Company.objects.select_related("sector")
        )

        # ── Text search ───────────────────────────────────────────────────────
        q = params.get("q", "").strip()
        if q:
            qs = qs.filter(
                Q(symbol__icontains=q) | Q(company_name__icontains=q)
            )

        # ── Sector ────────────────────────────────────────────────────────────
        sector = params.get("sector", "").strip()
        if sector:
            qs = qs.filter(sector__sector_name__icontains=sector)

        # ── Banking flag ──────────────────────────────────────────────────────
        is_banking = params.get("is_banking", "").lower()
        if is_banking == "true":
            qs = qs.filter(is_banking=True)
        elif is_banking == "false":
            qs = qs.filter(is_banking=False)

        # ── Health label ──────────────────────────────────────────────────────
        health_label = params.get("health_label", "").strip().upper()
        if health_label:
            qs = qs.filter(latest_health_label=health_label)

        # ── Score range (uses annotated field) ────────────────────────────────
        try:
            min_score = float(params["min_score"])
            qs = qs.filter(latest_overall_score__gte=min_score)
        except (KeyError, ValueError):
            pass
        try:
            max_score = float(params["max_score"])
            qs = qs.filter(latest_overall_score__lte=max_score)
        except (KeyError, ValueError):
            pass

        # ── OPM range – filter via latest P&L subquery ────────────────────────
        latest_opm_sq = (
            ProfitLoss.objects
            .filter(symbol=OuterRef("symbol"), year__is_ttm=False)
            .order_by("-year__sort_order")
            .values("opm_percentage")[:1]
        )
        qs = qs.annotate(latest_opm=Subquery(latest_opm_sq))

        try:
            min_opm = float(params["min_opm"])
            qs = qs.filter(latest_opm__gte=min_opm)
        except (KeyError, ValueError):
            pass
        try:
            max_opm = float(params["max_opm"])
            qs = qs.filter(latest_opm__lte=max_opm)
        except (KeyError, ValueError):
            pass

        # ── D/E range – filter via latest BS subquery ─────────────────────────
        latest_de_sq = (
            BalanceSheet.objects
            .filter(symbol=OuterRef("symbol"), year__is_ttm=False)
            .order_by("-year__sort_order")
            .values("debt_to_equity")[:1]
        )
        qs = qs.annotate(latest_de=Subquery(latest_de_sq))

        try:
            min_de = float(params["min_de"])
            qs = qs.filter(latest_de__gte=min_de)
        except (KeyError, ValueError):
            pass
        try:
            max_de = float(params["max_de"])
            qs = qs.filter(latest_de__lte=max_de)
        except (KeyError, ValueError):
            pass

        # ── Positive FCF filter ────────────────────────────────────────────────
        if params.get("positive_fcf", "").lower() == "true":
            latest_fcf_sq = (
                CashFlow.objects
                .filter(symbol=OuterRef("symbol"), year__is_ttm=False)
                .order_by("-year__sort_order")
                .values("free_cash_flow")[:1]
            )
            qs = qs.annotate(latest_fcf=Subquery(latest_fcf_sq))
            qs = qs.filter(latest_fcf__gt=0)

        # ── Sorting ───────────────────────────────────────────────────────────
        sort = params.get("sort", "score_desc")
        sort_map = {
            "score_desc": "-latest_overall_score",
            "score_asc":  "latest_overall_score",
            "name_asc":   "company_name",
            "name_desc":  "-company_name",
        }
        qs = qs.order_by(sort_map.get(sort, "-latest_overall_score"))

        # ── Pagination ────────────────────────────────────────────────────────
        try:
            page      = max(1, int(params.get("page", 1)))
            page_size = min(100, max(1, int(params.get("page_size", 20))))
        except (ValueError, TypeError):
            page, page_size = 1, 20

        total = qs.count()
        start = (page - 1) * page_size
        end   = start + page_size
        companies = list(qs[start:end])

        results = []
        for c in companies:
            results.append({
                "symbol":         c.symbol,
                "company_name":   c.company_name,
                "sector_name":    c.sector.sector_name if c.sector else None,
                "is_banking":     c.is_banking,
                "health_label":   c.latest_health_label,
                "overall_score":  _to_float(c.latest_overall_score),
                "opm_pct":        _to_float(getattr(c, "latest_opm",  None)),
                "de_ratio":       _to_float(getattr(c, "latest_de",   None)),
                "company_logo":   c.company_logo,
            })

        return Response({
            "count":     total,
            "page":      page,
            "page_size": page_size,
            "num_pages": max(1, -(-total // page_size)),
            "results":   results,
        })
