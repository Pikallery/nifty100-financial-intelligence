"""
DRF serializers for the Nifty 100 channel-partner API.

All model field names match the actual companies/models.py star schema.

Serializer hierarchy
--------------------
  ChannelPartnerSerializer
  APIKeySerializer          — never exposes key_secret_hash
  WebhookSubscriptionSerializer
  WebhookEventSerializer

  Company financial building blocks:
    ProfitLossSerializer
    BalanceSheetSerializer
    CashFlowSerializer
    AnalysisSerializer
    ProsConsSerializer
    DocumentSerializer
    MLScoreSerializer

  FullCompanySerializer     — Company + latest MLScore + last 5 years of each
                              fact table + all pros/cons + all documents
"""

from rest_framework import serializers

from api.models import APIKey, ChannelPartner, WebhookEvent, WebhookSubscription
from companies.models import (
    Analysis,
    BalanceSheet,
    CashFlow,
    Company,
    Document,
    MLScore,
    ProfitLoss,
    ProsCons,
)


# ---------------------------------------------------------------------------
# Channel-partner management
# ---------------------------------------------------------------------------


class ChannelPartnerSerializer(serializers.ModelSerializer):
    """Public representation of a ChannelPartner record."""

    active_key_count = serializers.SerializerMethodField()

    class Meta:
        model = ChannelPartner
        fields = [
            "id",
            "partner_name",
            "email",
            "tier",
            "is_active",
            "created_at",
            "active_key_count",
        ]
        read_only_fields = ["id", "created_at", "active_key_count"]

    def get_active_key_count(self, obj: ChannelPartner) -> int:
        return obj.api_keys.filter(is_active=True).count()


class APIKeySerializer(serializers.ModelSerializer):
    """
    Serializer for APIKey.

    ``key_secret_hash`` is intentionally excluded; the raw secret is only
    returned once at creation time via a separate ``raw_secret`` field injected
    by the create view.
    """

    partner_name = serializers.CharField(source="partner.partner_name", read_only=True)

    class Meta:
        model = APIKey
        fields = [
            "key_id",
            "partner_name",
            "is_active",
            "created_at",
            "last_used_at",
        ]
        read_only_fields = [
            "key_id",
            "partner_name",
            "created_at",
            "last_used_at",
        ]


class WebhookSubscriptionSerializer(serializers.ModelSerializer):
    """Serializer for a webhook subscription."""

    partner_name = serializers.CharField(source="partner.partner_name", read_only=True)
    delivery_success_rate = serializers.SerializerMethodField()

    class Meta:
        model = WebhookSubscription
        fields = [
            "id",
            "partner_name",
            "url",
            "events",
            "is_active",
            "created_at",
            "delivery_success_rate",
        ]
        read_only_fields = ["id", "partner_name", "created_at", "delivery_success_rate"]

    def get_delivery_success_rate(self, obj: WebhookSubscription) -> float | None:
        qs = obj.webhook_events.all()
        total = qs.count()
        if total == 0:
            return None
        delivered = qs.filter(status=WebhookEvent.DELIVERED).count()
        return round(delivered / total * 100, 2)


class WebhookEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = WebhookEvent
        fields = [
            "id",
            "event_type",
            "payload",
            "status",
            "attempts",
            "last_attempt_at",
            "created_at",
        ]
        read_only_fields = fields


# ---------------------------------------------------------------------------
# Company financial data building blocks
# ---------------------------------------------------------------------------


class ProfitLossSerializer(serializers.ModelSerializer):
    """
    Serialize a ProfitLoss row (fact_profit_loss).

    year_label is denormalized from the related Year dimension for readability.
    """

    year_label = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = ProfitLoss
        fields = [
            "year_label",
            "fiscal_year",
            "sales",
            "expenses",
            "operating_profit",
            "opm_percentage",
            "other_income",
            "interest",
            "depreciation",
            "profit_before_tax",
            "tax_percentage",
            "net_profit",
            "eps",
            "dividend_payout",
            "net_profit_margin_pct",
            "expense_ratio_pct",
            "interest_coverage",
            "asset_turnover",
            "return_on_assets_pct",
        ]


class BalanceSheetSerializer(serializers.ModelSerializer):
    """Serialize a BalanceSheet row (fact_balance_sheet)."""

    year_label = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = BalanceSheet
        fields = [
            "year_label",
            "fiscal_year",
            "equity_capital",
            "reserves",
            "borrowings",
            "other_liabilities",
            "total_liabilities",
            "fixed_assets",
            "cwip",
            "investments",
            "other_assets",
            "total_assets",
            "debt_to_equity",
            "equity_ratio",
        ]


class CashFlowSerializer(serializers.ModelSerializer):
    """Serialize a CashFlow row (fact_cash_flow)."""

    year_label = serializers.CharField(source="year.year_label", read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)

    class Meta:
        model = CashFlow
        fields = [
            "year_label",
            "fiscal_year",
            "operating_activity",
            "investing_activity",
            "financing_activity",
            "net_cash_flow",
            "free_cash_flow",
            "cash_conversion_ratio",
        ]


class AnalysisSerializer(serializers.ModelSerializer):
    """
    Serialize an Analysis row (fact_analysis).

    Each row is one metric / period combination, e.g.
    compounded_sales_growth / 5Y.
    """

    class Meta:
        model = Analysis
        fields = [
            "period",
            "metric",
            "value_pct",
        ]


class ProsConsSerializer(serializers.ModelSerializer):
    """Serialize a ProsCons row (fact_pros_cons)."""

    type = serializers.SerializerMethodField()

    class Meta:
        model = ProsCons
        fields = ["type", "text", "source", "generated_at"]

    def get_type(self, obj: ProsCons) -> str:
        return "PRO" if obj.is_pro else "CON"


class DocumentSerializer(serializers.ModelSerializer):
    """Serialize a Document row (documents table)."""

    class Meta:
        model = Document
        fields = [
            "id",
            "symbol",
            "year",
            "annual_report_url",
        ]


class MLScoreSerializer(serializers.ModelSerializer):
    """Serialize an MLScore row (fact_ml_scores)."""

    class Meta:
        model = MLScore
        fields = [
            "overall_score",
            "profitability_score",
            "growth_score",
            "leverage_score",
            "cashflow_score",
            "dividend_score",
            "trend_score",
            "health_label",
            "computed_at",
        ]


# ---------------------------------------------------------------------------
# Full company dump — used by GET /companies/<symbol>/full/
# ---------------------------------------------------------------------------


class FullCompanySerializer(serializers.ModelSerializer):
    """
    Deep serializer: one Company with every related financial record.

    Nested fields are computed via SerializerMethodField so that the calling
    view can supply pre-fetched querysets and avoid N+1 queries.
    """

    sector_name = serializers.CharField(source="sector.sector_name", read_only=True, default=None)
    latest_score = serializers.SerializerMethodField()
    profit_loss = serializers.SerializerMethodField()
    balance_sheet = serializers.SerializerMethodField()
    cash_flow = serializers.SerializerMethodField()
    analysis = serializers.SerializerMethodField()
    pros_cons = serializers.SerializerMethodField()
    documents = serializers.SerializerMethodField()

    class Meta:
        model = Company
        fields = [
            "symbol",
            "company_name",
            "sector_name",
            "website",
            "nse_profile",
            "bse_profile",
            "face_value",
            "book_value",
            "roce_percentage",
            "roe_percentage",
            "about_company",
            "is_banking",
            # nested
            "latest_score",
            "profit_loss",
            "balance_sheet",
            "cash_flow",
            "analysis",
            "pros_cons",
            "documents",
        ]

    def get_latest_score(self, obj: Company):
        score = obj.ml_scores.order_by("-computed_at").first()
        if score is None:
            return None
        return MLScoreSerializer(score).data

    def get_profit_loss(self, obj: Company):
        # Last 5 fiscal years, ordered by sort_order descending (most recent first)
        records = obj.profit_loss_records.select_related("year").order_by("-year__sort_order")[:5]
        return ProfitLossSerializer(records, many=True).data

    def get_balance_sheet(self, obj: Company):
        records = obj.balance_sheet_records.select_related("year").order_by("-year__sort_order")[:5]
        return BalanceSheetSerializer(records, many=True).data

    def get_cash_flow(self, obj: Company):
        records = obj.cash_flow_records.select_related("year").order_by("-year__sort_order")[:5]
        return CashFlowSerializer(records, many=True).data

    def get_analysis(self, obj: Company):
        records = obj.analysis_records.all()
        return AnalysisSerializer(records, many=True).data

    def get_pros_cons(self, obj: Company):
        records = obj.pros_cons.all()
        return ProsConsSerializer(records, many=True).data

    def get_documents(self, obj: Company):
        # Documents table uses a plain CharField for symbol, so filter directly.
        records = Document.objects.filter(symbol=obj.symbol).order_by("-year")
        return DocumentSerializer(records, many=True).data
