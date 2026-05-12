"""
companies/serializers.py

DRF serializers for the Nifty 100 data models.

Included serializers
--------------------
CompanySerializer          – full company record with nested sector
CompanyListSerializer      – lightweight card/list view (includes latest health label)
ProfitLossSerializer       – full income statement fact row
BalanceSheetSerializer     – full balance sheet fact row
CashFlowSerializer         – full cash flow fact row
MLScoreSerializer          – full ML score row
AnalysisSerializer         – CAGR / ratio analysis row
ProsCons Serializer        – pro / con bullet
DocumentSerializer         – annual report link
PeerSerializer             – peer similarity entry
AnomalySerializer          – anomaly flag entry
ForecastSerializer         – sales forecast entry
"""

from rest_framework import serializers

from .models import (
    Sector,
    HealthLabel,
    Company,
    Year,
    ProfitLoss,
    BalanceSheet,
    CashFlow,
    Analysis,
    MLScore,
    ProsCons,
    Document,
    Peer,
    Anomaly,
    Forecast,
    Cluster,
)


# ── Dimension Serializers ─────────────────────────────────────────────────────

class SectorSerializer(serializers.ModelSerializer):
    class Meta:
        model  = Sector
        fields = ["sector_id", "sector_name", "sector_code"]


class HealthLabelSerializer(serializers.ModelSerializer):
    class Meta:
        model  = HealthLabel
        fields = ["label_id", "label_name", "min_score", "max_score", "color_hex"]


class YearSerializer(serializers.ModelSerializer):
    class Meta:
        model  = Year
        fields = ["year_id", "year_label", "fiscal_year", "is_ttm", "sort_order"]


# ── Company Serializers ───────────────────────────────────────────────────────

class CompanySerializer(serializers.ModelSerializer):
    """
    Full company record, used on the detail page and partner API.
    Includes nested sector and the latest ML score as a flat block.
    """

    sector_name  = serializers.CharField(source="sector.sector_name", read_only=True, default=None)
    sector_code  = serializers.CharField(source="sector.sector_code", read_only=True, default=None)
    latest_score = serializers.SerializerMethodField()

    class Meta:
        model  = Company
        fields = [
            "symbol",
            "company_name",
            "sector_id",
            "sector_name",
            "sector_code",
            "company_logo",
            "website",
            "nse_profile",
            "bse_profile",
            "face_value",
            "book_value",
            "roce_percentage",
            "roe_percentage",
            "about_company",
            "is_banking",
            "latest_score",
        ]

    def get_latest_score(self, obj):
        """Return the latest MLScore as a flat dict, or None."""
        score = obj.ml_scores.order_by("-computed_at").first()
        if score is None:
            return None
        return {
            "overall_score":       score.overall_score,
            "health_label":        score.health_label,
            "profitability_score": score.profitability_score,
            "growth_score":        score.growth_score,
            "leverage_score":      score.leverage_score,
            "cashflow_score":      score.cashflow_score,
            "dividend_score":      score.dividend_score,
            "trend_score":         score.trend_score,
            "computed_at":         score.computed_at,
        }


class CompanyListSerializer(serializers.ModelSerializer):
    """
    Lightweight serializer for company list / screener cards.
    Includes sector info, latest health label and key ratios.
    """

    sector_name   = serializers.CharField(source="sector.sector_name", read_only=True, default=None)
    health_label  = serializers.SerializerMethodField()
    overall_score = serializers.SerializerMethodField()
    opm_pct       = serializers.SerializerMethodField()
    de_ratio      = serializers.SerializerMethodField()

    class Meta:
        model  = Company
        fields = [
            "symbol",
            "company_name",
            "sector_id",
            "sector_name",
            "is_banking",
            "health_label",
            "overall_score",
            "opm_pct",
            "de_ratio",
            "company_logo",
        ]

    # ── helpers ──────────────────────────────────────────────────────────────

    def _latest_score(self, obj):
        """Cache the latest MLScore on the object to avoid repeated queries."""
        if not hasattr(obj, "_cached_ml_score"):
            obj._cached_ml_score = obj.ml_scores.order_by("-computed_at").first()
        return obj._cached_ml_score

    def _latest_pl(self, obj):
        """Return the most recent non-TTM ProfitLoss row."""
        if not hasattr(obj, "_cached_pl"):
            obj._cached_pl = (
                obj.profit_loss_records
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )
        return obj._cached_pl

    def _latest_bs(self, obj):
        """Return the most recent non-TTM BalanceSheet row."""
        if not hasattr(obj, "_cached_bs"):
            obj._cached_bs = (
                obj.balance_sheet_records
                .filter(year__is_ttm=False)
                .order_by("-year__sort_order")
                .first()
            )
        return obj._cached_bs

    def get_health_label(self, obj):
        score = self._latest_score(obj)
        return score.health_label if score else None

    def get_overall_score(self, obj):
        score = self._latest_score(obj)
        return score.overall_score if score else None

    def get_opm_pct(self, obj):
        pl = self._latest_pl(obj)
        return pl.opm_percentage if pl else None

    def get_de_ratio(self, obj):
        bs = self._latest_bs(obj)
        return bs.debt_to_equity if bs else None


# ── Fact Serializers ──────────────────────────────────────────────────────────

class ProfitLossSerializer(serializers.ModelSerializer):
    """Full income statement row with year metadata."""

    year_label  = serializers.CharField(source="year.year_label",  read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)
    is_ttm      = serializers.BooleanField(source="year.is_ttm",   read_only=True)
    sort_order  = serializers.IntegerField(source="year.sort_order", read_only=True)

    class Meta:
        model  = ProfitLoss
        fields = [
            "id",
            "symbol_id",
            "year_id",
            "year_label",
            "fiscal_year",
            "is_ttm",
            "sort_order",
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
            "is_banking",
        ]


class BalanceSheetSerializer(serializers.ModelSerializer):
    """Full balance sheet row with year metadata."""

    year_label  = serializers.CharField(source="year.year_label",   read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)
    is_ttm      = serializers.BooleanField(source="year.is_ttm",    read_only=True)
    sort_order  = serializers.IntegerField(source="year.sort_order", read_only=True)

    class Meta:
        model  = BalanceSheet
        fields = [
            "id",
            "symbol_id",
            "year_id",
            "year_label",
            "fiscal_year",
            "is_ttm",
            "sort_order",
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
    """Full cash flow row with year metadata."""

    year_label  = serializers.CharField(source="year.year_label",   read_only=True)
    fiscal_year = serializers.IntegerField(source="year.fiscal_year", read_only=True)
    is_ttm      = serializers.BooleanField(source="year.is_ttm",    read_only=True)
    sort_order  = serializers.IntegerField(source="year.sort_order", read_only=True)

    class Meta:
        model  = CashFlow
        fields = [
            "id",
            "symbol_id",
            "year_id",
            "year_label",
            "fiscal_year",
            "is_ttm",
            "sort_order",
            "operating_activity",
            "investing_activity",
            "financing_activity",
            "net_cash_flow",
            "free_cash_flow",
            "cash_conversion_ratio",
        ]


class MLScoreSerializer(serializers.ModelSerializer):
    """Full ML score row."""

    class Meta:
        model  = MLScore
        fields = [
            "id",
            "symbol_id",
            "computed_at",
            "overall_score",
            "profitability_score",
            "growth_score",
            "leverage_score",
            "cashflow_score",
            "dividend_score",
            "trend_score",
            "health_label",
        ]


class AnalysisSerializer(serializers.ModelSerializer):
    """CAGR / ratio analysis row."""

    class Meta:
        model  = Analysis
        fields = [
            "id",
            "symbol_id",
            "period",
            "metric",
            "value_pct",
        ]


class ProsConsSerializer(serializers.ModelSerializer):
    """Pros / cons bullet point."""

    class Meta:
        model  = ProsCons
        fields = [
            "id",
            "symbol_id",
            "is_pro",
            "text",
            "source",
            "generated_at",
        ]


class DocumentSerializer(serializers.ModelSerializer):
    """Annual report link."""

    class Meta:
        model  = Document
        fields = [
            "id",
            "symbol",
            "year",
            "annual_report_url",
        ]


class PeerSerializer(serializers.ModelSerializer):
    """
    Peer similarity entry – includes the peer company's name and
    latest health label for display.
    """

    peer_name         = serializers.CharField(
        source="peer_symbol.company_name", read_only=True
    )
    peer_sector_name  = serializers.CharField(
        source="peer_symbol.sector.sector_name", read_only=True, default=None
    )
    peer_health_label = serializers.SerializerMethodField()
    peer_logo         = serializers.CharField(
        source="peer_symbol.company_logo", read_only=True, default=None
    )

    class Meta:
        model  = Peer
        fields = [
            "symbol_id",
            "peer_symbol_id",
            "peer_name",
            "peer_sector_name",
            "peer_health_label",
            "peer_logo",
            "similarity",
            "rank",
        ]

    def get_peer_health_label(self, obj):
        score = obj.peer_symbol.ml_scores.order_by("-computed_at").first()
        return score.health_label if score else None


class AnomalySerializer(serializers.ModelSerializer):
    """Anomaly flag entry."""

    year_label = serializers.CharField(source="year.year_label", read_only=True, default=None)

    class Meta:
        model  = Anomaly
        fields = [
            "id",
            "symbol_id",
            "year_id",
            "year_label",
            "metric",
            "value",
            "z_score",
            "method",
            "severity",
            "reviewed",
            "notes",
            "flagged_at",
        ]


class ForecastSerializer(serializers.ModelSerializer):
    """Sales forecast entry."""

    class Meta:
        model  = Forecast
        fields = [
            "id",
            "symbol_id",
            "forecast_year",
            "predicted_sales",
            "lower_bound",
            "upper_bound",
            "trend_direction",
            "computed_at",
        ]


class ClusterSerializer(serializers.ModelSerializer):
    """PCA cluster assignment."""

    class Meta:
        model  = Cluster
        fields = [
            "symbol_id",
            "cluster_id",
            "cluster_label",
            "pca_x",
            "pca_y",
            "computed_at",
        ]
