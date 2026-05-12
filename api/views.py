"""
Channel-partner API views for the Nifty 100 financial intelligence platform.

All endpoints require HMAC authentication (HMACAuthentication) and enforce
tiered rate limits (TieredRateThrottle).

Endpoints
---------
  GET    /api/v1/companies/<symbol>/full/   — full company dump
  GET    /api/v1/bulk-financials/           — up to 10 companies
  GET    /api/v1/screener/                  — filtered company list
  GET    /api/v1/scores/                    — latest ML scores
  GET    /api/v1/keys/                      — list own API keys
  POST   /api/v1/keys/                      — create API key
  DELETE /api/v1/keys/<key_id>/             — deactivate API key
  GET    /api/v1/webhooks/                  — list webhook subscriptions
  POST   /api/v1/webhooks/                  — create webhook subscription
  DELETE /api/v1/webhooks/<pk>/             — delete webhook subscription
  GET    /api/v1/usage/                     — usage summary last 30 days
"""

import uuid
from datetime import timedelta

import bcrypt
import secrets

from django.core.cache import cache
from django.db.models import Avg, Count, OuterRef, Subquery
from django.db.models.functions import TruncDate
from django.utils import timezone
from drf_spectacular.utils import OpenApiParameter, OpenApiResponse, extend_schema
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from api.authentication import HMACAuthentication
from api.models import APIKey, WebhookSubscription
from api.serializers import (
    APIKeySerializer,
    FullCompanySerializer,
    MLScoreSerializer,
    WebhookSubscriptionSerializer,
)
from api.throttling import TieredRateThrottle
from companies.models import (
    APIUsageLog,
    BalanceSheet,
    Company,
    MLScore,
)


# ---------------------------------------------------------------------------
# Base view: auth + throttle applied to all channel-partner endpoints
# ---------------------------------------------------------------------------


class AuthenticatedAPIView(APIView):
    """Base view that wires HMAC auth and tiered throttle to every subclass."""

    authentication_classes = [HMACAuthentication]
    throttle_classes = [TieredRateThrottle]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prefetched_company_qs():
    """Return a Company queryset with all related tables pre-fetched."""
    return Company.objects.prefetch_related(
        "ml_scores",
        "profit_loss_records__year",
        "balance_sheet_records__year",
        "cash_flow_records__year",
        "analysis_records",
        "pros_cons",
    ).select_related("sector")


# ---------------------------------------------------------------------------
# 1. Full company dump
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Full company data dump",
    description=(
        "Return the complete data package for a single Nifty 100 company: "
        "latest ML health score, last 5 years of P&L / balance sheet / cash flow / "
        "analysis, all pros & cons, and all documents."
    ),
    parameters=[
        OpenApiParameter(
            "symbol",
            str,
            OpenApiParameter.PATH,
            description="NSE ticker symbol, e.g. TCS",
        ),
    ],
    responses={
        200: FullCompanySerializer,
        404: OpenApiResponse(description="Company not found"),
    },
    tags=["Company Data"],
)
class CompanyFullView(AuthenticatedAPIView):
    """GET /api/v1/companies/<symbol>/full/"""

    def get(self, request, symbol: str):  # noqa: ARG002 — request required by DRF dispatch
        try:
            company = _prefetched_company_qs().get(symbol=symbol.upper())
        except Company.DoesNotExist:
            return Response(
                {"error": f"Company '{symbol.upper()}' not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(FullCompanySerializer(company).data)


# ---------------------------------------------------------------------------
# 2. Bulk financials
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Bulk financial data for multiple companies",
    description=(
        "Return full company data for up to 10 symbols in one request. "
        "Pass a comma-separated list: ?symbols=TCS,INFY,WIPRO"
    ),
    parameters=[
        OpenApiParameter(
            "symbols",
            str,
            OpenApiParameter.QUERY,
            description="Comma-separated NSE ticker symbols (max 10)",
            required=True,
        ),
    ],
    responses={
        200: FullCompanySerializer(many=True),
        400: OpenApiResponse(description="Too many symbols or missing parameter"),
    },
    tags=["Company Data"],
)
class BulkFinancialsView(AuthenticatedAPIView):
    """GET /api/v1/bulk-financials/?symbols=TCS,INFY"""

    def get(self, request):
        raw = request.query_params.get("symbols", "").strip()
        if not raw:
            return Response(
                {"error": "Provide a comma-separated ?symbols= list."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
        if len(symbols) > 10:
            return Response(
                {"error": "Maximum 10 symbols per request."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        companies = _prefetched_company_qs().filter(symbol__in=symbols)
        found_symbols = {c.symbol for c in companies}
        missing = [s for s in symbols if s not in found_symbols]

        data = FullCompanySerializer(companies, many=True).data
        return Response(
            {
                "count": len(data),
                "missing_symbols": missing,
                "results": data,
            }
        )


# ---------------------------------------------------------------------------
# 3. Screener
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Screen companies by financial criteria",
    description=(
        "Filter Nifty 100 companies by multiple financial metrics. "
        "All query parameters are optional and combinable."
    ),
    parameters=[
        OpenApiParameter("roe_min", float, OpenApiParameter.QUERY, description="Minimum ROE (%)"),
        OpenApiParameter("roe_max", float, OpenApiParameter.QUERY, description="Maximum ROE (%)"),
        OpenApiParameter(
            "de_max",
            float,
            OpenApiParameter.QUERY,
            description="Maximum Debt/Equity ratio",
        ),
        OpenApiParameter(
            "sales_growth_min",
            float,
            OpenApiParameter.QUERY,
            description="Minimum compounded sales growth (%) — matched against fact_analysis 5Y metric",
        ),
        OpenApiParameter(
            "sector",
            str,
            OpenApiParameter.QUERY,
            description="Sector name (partial, case-insensitive)",
        ),
        OpenApiParameter(
            "health_label",
            str,
            OpenApiParameter.QUERY,
            description="ML health label: Healthy | Watch | Critical",
        ),
        OpenApiParameter(
            "min_score",
            float,
            OpenApiParameter.QUERY,
            description="Minimum ML overall_score (0–100)",
        ),
    ],
    responses={200: FullCompanySerializer(many=True)},
    tags=["Screener"],
)
class ScreenerView(AuthenticatedAPIView):
    """GET /api/v1/screener/"""

    def get(self, request):
        params = request.query_params
        companies = _prefetched_company_qs()

        # Sector filter (case-insensitive partial match on sector.sector_name)
        if sector := params.get("sector"):
            companies = companies.filter(sector__sector_name__icontains=sector)

        # Health label / overall score filters
        health_label = params.get("health_label", "").strip()
        min_score_str = params.get("min_score", "").strip()

        if health_label or min_score_str:
            latest_score_sub = (
                MLScore.objects.filter(symbol=OuterRef("pk"))
                .order_by("-computed_at")
                .values("id")[:1]
            )
            score_filter_qs = MLScore.objects.filter(id__in=Subquery(latest_score_sub))

            if health_label:
                score_filter_qs = score_filter_qs.filter(health_label__iexact=health_label)
            if min_score_str:
                try:
                    score_filter_qs = score_filter_qs.filter(overall_score__gte=float(min_score_str))
                except ValueError:
                    pass

            companies = companies.filter(
                symbol__in=score_filter_qs.values_list("symbol_id", flat=True)
            )

        # ROE filter — matched against Company.roe_percentage (denormalized on dim_company)
        roe_min_str = params.get("roe_min", "").strip()
        roe_max_str = params.get("roe_max", "").strip()
        if roe_min_str:
            try:
                companies = companies.filter(roe_percentage__gte=float(roe_min_str))
            except ValueError:
                pass
        if roe_max_str:
            try:
                companies = companies.filter(roe_percentage__lte=float(roe_max_str))
            except ValueError:
                pass

        # D/E filter — matched against the latest BalanceSheet row per company
        de_max_str = params.get("de_max", "").strip()
        if de_max_str:
            try:
                de_val = float(de_max_str)
                latest_bs_sub = (
                    BalanceSheet.objects.filter(symbol=OuterRef("pk"))
                    .order_by("-year__sort_order")
                    .values("id")[:1]
                )
                de_matching = (
                    BalanceSheet.objects.filter(
                        id__in=Subquery(latest_bs_sub),
                        debt_to_equity__lte=de_val,
                    ).values_list("symbol_id", flat=True)
                )
                companies = companies.filter(symbol__in=de_matching)
            except ValueError:
                pass

        # Sales-growth filter — fact_analysis: metric=compounded_sales_growth, period=5Y
        sales_growth_str = params.get("sales_growth_min", "").strip()
        if sales_growth_str:
            try:
                sg_val = float(sales_growth_str)
                from companies.models import Analysis

                matching_sg = Analysis.objects.filter(
                    metric="compounded_sales_growth",
                    period="5Y",
                    value_pct__gte=sg_val,
                ).values_list("symbol_id", flat=True)
                companies = companies.filter(symbol__in=matching_sg)
            except ValueError:
                pass

        companies = companies.distinct()
        return Response(
            {
                "count": companies.count(),
                "results": FullCompanySerializer(companies, many=True).data,
            }
        )


# ---------------------------------------------------------------------------
# 4. ML scores
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Latest ML health scores",
    description=(
        "Return the most recent ML health score for all companies, or a "
        "filtered subset via ?symbols=."
    ),
    parameters=[
        OpenApiParameter(
            "symbols",
            str,
            OpenApiParameter.QUERY,
            description="Optional comma-separated NSE symbols",
        ),
    ],
    responses={200: MLScoreSerializer(many=True)},
    tags=["ML Scores"],
)
class ScoresView(AuthenticatedAPIView):
    """GET /api/v1/scores/"""

    def get(self, request):
        # Latest score per company via window/subquery
        latest_score_sub = (
            MLScore.objects.filter(symbol=OuterRef("symbol"))
            .order_by("-computed_at")
            .values("id")[:1]
        )
        scores = (
            MLScore.objects.filter(id__in=Subquery(latest_score_sub))
            .select_related("symbol")
            .order_by("symbol_id")
        )

        raw = request.query_params.get("symbols", "").strip()
        if raw:
            symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
            scores = scores.filter(symbol__in=symbols)

        data = []
        for score in scores:
            row = MLScoreSerializer(score).data
            row["symbol"] = score.symbol_id
            row["company_name"] = score.symbol.company_name or ""
            data.append(row)

        return Response({"count": len(data), "results": data})


# ---------------------------------------------------------------------------
# 5. List API keys
# ---------------------------------------------------------------------------


@extend_schema(
    summary="List own API keys",
    description="Return all API keys belonging to the authenticated partner.",
    responses={200: APIKeySerializer(many=True)},
    tags=["API Keys"],
)
class APIKeyListView(AuthenticatedAPIView):
    """GET /api/v1/keys/"""

    def get(self, request):
        partner: ChannelPartner = request.user
        keys = APIKey.objects.filter(partner=partner).order_by("-created_at")
        return Response(APIKeySerializer(keys, many=True).data)


# ---------------------------------------------------------------------------
# 6. Create API key
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Create a new API key",
    description=(
        "Generate a new HMAC key pair for the authenticated partner. "
        "The raw secret is returned **once** and cannot be retrieved again. "
        "Store it securely immediately."
    ),
    responses={
        201: OpenApiResponse(description="Key created — raw_secret shown once"),
        400: OpenApiResponse(description="Validation error"),
    },
    tags=["API Keys"],
)
class APIKeyCreateView(AuthenticatedAPIView):
    """POST /api/v1/keys/"""

    def post(self, request):
        partner: ChannelPartner = request.user

        # 48-byte URL-safe random secret → 64 printable chars
        raw_secret = secrets.token_urlsafe(48)

        # bcrypt hash for at-rest storage (never retrieve the plaintext)
        hashed = bcrypt.hashpw(
            raw_secret.encode("utf-8"), bcrypt.gensalt(rounds=12)
        ).decode("utf-8")

        api_key = APIKey.objects.create(
            partner=partner,
            key_secret_hash=hashed,
        )

        # Cache the raw secret for 10 minutes so the authentication class can
        # verify the first set of signed requests.
        raw_secret_cache_key = f"api:raw_secret:{str(api_key.key_id)}"
        cache.set(raw_secret_cache_key, raw_secret, timeout=600)

        return Response(
            {
                "key_id": str(api_key.key_id),
                "raw_secret": raw_secret,
                "warning": (
                    "This is the only time the secret will be shown. "
                    "Store it securely and use it as your HMAC signing key."
                ),
                "is_active": True,
                "created_at": api_key.created_at.isoformat(),
            },
            status=status.HTTP_201_CREATED,
        )


# ---------------------------------------------------------------------------
# 7. Deactivate an API key
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Deactivate an API key",
    description="Permanently deactivate an API key belonging to the authenticated partner.",
    parameters=[
        OpenApiParameter(
            "key_id",
            str,
            OpenApiParameter.PATH,
            description="UUID key_id of the key to deactivate",
        ),
    ],
    responses={
        204: OpenApiResponse(description="Key deactivated"),
        400: OpenApiResponse(description="Invalid key_id format"),
        404: OpenApiResponse(description="Key not found"),
    },
    tags=["API Keys"],
)
class APIKeyDeactivateView(AuthenticatedAPIView):
    """DELETE /api/v1/keys/<key_id>/"""

    def delete(self, request, key_id: str):
        partner: ChannelPartner = request.user
        try:
            key_uuid = uuid.UUID(str(key_id))
        except ValueError:
            return Response(
                {"error": "Invalid key_id format — expected a UUID."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            api_key = APIKey.objects.get(key_id=key_uuid, partner=partner)
        except APIKey.DoesNotExist:
            return Response(
                {"error": "API key not found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        api_key.is_active = False
        api_key.save(update_fields=["is_active"])

        # Remove the cached raw secret to prevent further authentication.
        cache.delete(f"api:raw_secret:{str(api_key.key_id)}")

        return Response(status=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# 8. List webhook subscriptions
# ---------------------------------------------------------------------------


@extend_schema(
    summary="List webhook subscriptions",
    description="Return all webhook subscriptions registered by the authenticated partner.",
    responses={200: WebhookSubscriptionSerializer(many=True)},
    tags=["Webhooks"],
)
class WebhookListView(AuthenticatedAPIView):
    """GET /api/v1/webhooks/"""

    def get(self, request):
        partner: ChannelPartner = request.user
        subs = (
            WebhookSubscription.objects.filter(partner=partner)
            .prefetch_related("webhook_events")
            .order_by("-created_at")
        )
        return Response(WebhookSubscriptionSerializer(subs, many=True).data)


# ---------------------------------------------------------------------------
# 9. Create webhook subscription
# ---------------------------------------------------------------------------


VALID_EVENT_TYPES = frozenset({"score_updated", "anomaly_flagged", "data_refreshed"})


@extend_schema(
    summary="Create a webhook subscription",
    description=(
        "Register a URL to receive push events. "
        f"Supported event types: score_updated, anomaly_flagged, data_refreshed."
    ),
    request=WebhookSubscriptionSerializer,
    responses={
        201: WebhookSubscriptionSerializer,
        400: OpenApiResponse(description="Validation error"),
    },
    tags=["Webhooks"],
)
class WebhookCreateView(AuthenticatedAPIView):
    """POST /api/v1/webhooks/"""

    def post(self, request):
        partner: ChannelPartner = request.user
        url = request.data.get("url", "").strip()
        events = request.data.get("events", [])

        if not url:
            return Response(
                {"error": "url is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not isinstance(events, list) or not events:
            return Response(
                {"error": "events must be a non-empty list of event-type strings."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        invalid = [e for e in events if e not in VALID_EVENT_TYPES]
        if invalid:
            return Response(
                {
                    "error": (
                        f"Unknown event types: {invalid}. "
                        f"Valid types: {sorted(VALID_EVENT_TYPES)}."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        subscription = WebhookSubscription.objects.create(
            partner=partner,
            url=url,
            events=events,
        )
        return Response(
            WebhookSubscriptionSerializer(subscription).data,
            status=status.HTTP_201_CREATED,
        )


# ---------------------------------------------------------------------------
# 10. Delete webhook subscription
# ---------------------------------------------------------------------------


@extend_schema(
    summary="Delete a webhook subscription",
    description="Remove a webhook subscription by its primary key.",
    parameters=[
        OpenApiParameter(
            "pk",
            int,
            OpenApiParameter.PATH,
            description="Primary key of the subscription",
        ),
    ],
    responses={
        204: OpenApiResponse(description="Subscription deleted"),
        404: OpenApiResponse(description="Subscription not found"),
    },
    tags=["Webhooks"],
)
class WebhookDeleteView(AuthenticatedAPIView):
    """DELETE /api/v1/webhooks/<pk>/"""

    def delete(self, request, pk: int):
        partner: ChannelPartner = request.user
        try:
            sub = WebhookSubscription.objects.get(pk=pk, partner=partner)
        except WebhookSubscription.DoesNotExist:
            return Response(
                {"error": "Webhook subscription not found."},
                status=status.HTTP_404_NOT_FOUND,
            )
        sub.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# 11. Usage summary (last 30 days)
# ---------------------------------------------------------------------------


@extend_schema(
    summary="API usage summary — last 30 days",
    description=(
        "Return per-day call volume, top 10 endpoints, HTTP status breakdown, "
        "and average response time for the authenticated partner's API keys."
    ),
    responses={200: OpenApiResponse(description="Usage summary object")},
    tags=["Usage"],
)
class UsageSummaryView(AuthenticatedAPIView):
    """GET /api/v1/usage/"""

    def get(self, request):
        partner: ChannelPartner = request.user
        since = timezone.now() - timedelta(days=30)

        # Collect api_key_prefix values: first 8 chars of each key_id UUID (hex, no dashes)
        key_prefixes = list(
            APIKey.objects.filter(partner=partner).values_list("key_id", flat=True)
        )
        # api_usage_log stores api_key_prefix = first 12 chars of the key_id str
        prefix_strs = [str(k).replace("-", "")[:12] for k in key_prefixes]

        if not prefix_strs:
            return Response(
                {
                    "total_calls": 0,
                    "period_days": 30,
                    "daily_breakdown": [],
                    "top_endpoints": [],
                    "status_breakdown": [],
                    "avg_response_ms": None,
                }
            )

        logs = APIUsageLog.objects.filter(
            api_key_prefix__in=prefix_strs,
            requested_at__gte=since,
        )

        total_calls = logs.count()

        daily_breakdown = list(
            logs.annotate(day=TruncDate("requested_at"))
            .values("day")
            .annotate(calls=Count("id"))
            .order_by("day")
            .values("day", "calls")
        )
        for row in daily_breakdown:
            row["date"] = str(row.pop("day"))

        top_endpoints = list(
            logs.values("endpoint", "method")
            .annotate(calls=Count("id"))
            .order_by("-calls")[:10]
        )

        status_breakdown = list(
            logs.values("status_code")
            .annotate(calls=Count("id"))
            .order_by("-calls")
        )

        avg_ms = logs.aggregate(avg=Avg("response_time_ms"))["avg"]

        return Response(
            {
                "total_calls": total_calls,
                "period_days": 30,
                "daily_breakdown": daily_breakdown,
                "top_endpoints": top_endpoints,
                "status_breakdown": status_breakdown,
                "avg_response_ms": round(avg_ms, 2) if avg_ms else None,
            }
        )
