"""
companies/models.py

Django ORM models mirroring the nifty100_warehouse star schema.
All db_table values map 1-to-1 to the PostgreSQL DDL in etl/03_load_to_warehouse.py.

Dimension tables : Sector, HealthLabel, Company, Year
Fact tables      : ProfitLoss, BalanceSheet, CashFlow, Analysis,
                   MLScore, ProsCons, Document, Peer, Anomaly,
                   Forecast, Cluster
Support tables   : APIUsageLog
"""

from django.db import models


# ── Dimension Tables ──────────────────────────────────────────────────────────

class Sector(models.Model):
    """dim_sector – lookup table for industry sectors."""

    sector_id   = models.AutoField(primary_key=True)
    sector_name = models.CharField(max_length=100, unique=True)
    sector_code = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        db_table   = "dim_sector"
        ordering   = ["sector_name"]
        verbose_name        = "Sector"
        verbose_name_plural = "Sectors"

    def __str__(self):
        return self.sector_name


class HealthLabel(models.Model):
    """dim_health_label – score thresholds and display colours."""

    label_id   = models.AutoField(primary_key=True)
    label_name = models.CharField(max_length=20, unique=True)
    min_score  = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    max_score  = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    color_hex  = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        db_table   = "dim_health_label"
        ordering   = ["-min_score"]
        verbose_name        = "Health Label"
        verbose_name_plural = "Health Labels"

    def __str__(self):
        return self.label_name


class Company(models.Model):
    """
    dim_company – master record for each Nifty 100 constituent.

    Primary key is the NSE ticker symbol (VARCHAR 20).
    """

    symbol         = models.CharField(max_length=20, primary_key=True)
    company_name   = models.CharField(max_length=200, blank=True, null=True)
    sector         = models.ForeignKey(
        Sector,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        db_column="sector_id",
        related_name="companies",
    )
    company_logo   = models.TextField(blank=True, null=True)
    website        = models.TextField(blank=True, null=True)
    nse_profile    = models.TextField(blank=True, null=True)
    bse_profile    = models.TextField(blank=True, null=True)
    face_value     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    book_value     = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    roce_percentage = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    roe_percentage  = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    about_company  = models.TextField(blank=True, null=True)
    is_banking     = models.BooleanField(default=False)

    class Meta:
        db_table   = "dim_company"
        ordering   = ["symbol"]
        verbose_name        = "Company"
        verbose_name_plural = "Companies"

    def __str__(self):
        return f"{self.symbol} – {self.company_name or ''}"

    def get_latest_ml_score(self):
        """Return the most recently computed MLScore for this company."""
        return self.ml_scores.order_by("-computed_at").first()


class Year(models.Model):
    """dim_year – fiscal year dimension including TTM period."""

    year_id     = models.AutoField(primary_key=True)
    year_label  = models.CharField(max_length=20, unique=True)  # e.g. "Mar 2023", "TTM"
    fiscal_year = models.IntegerField(null=True, blank=True)    # e.g. 2023
    is_ttm      = models.BooleanField(default=False)
    sort_order  = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table  = "dim_year"
        ordering  = ["sort_order"]
        verbose_name        = "Year"
        verbose_name_plural = "Years"

    def __str__(self):
        return self.year_label


# ── Fact Tables ───────────────────────────────────────────────────────────────

class ProfitLoss(models.Model):
    """fact_profit_loss – annual income statement data."""

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="profit_loss_records",
    )
    year   = models.ForeignKey(
        Year,
        on_delete=models.CASCADE,
        db_column="year_id",
        related_name="profit_loss_records",
    )

    # Income statement line items
    sales                 = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    expenses              = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    operating_profit      = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    opm_percentage        = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)
    other_income          = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    interest              = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    depreciation          = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    profit_before_tax     = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    tax_percentage        = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)
    net_profit            = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    eps                   = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    dividend_payout       = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)

    # Derived ratios
    net_profit_margin_pct = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)
    expense_ratio_pct     = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)
    interest_coverage     = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    asset_turnover        = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    return_on_assets_pct  = models.DecimalField(max_digits=8,  decimal_places=2, null=True, blank=True)

    is_banking = models.BooleanField(default=False)

    class Meta:
        db_table        = "fact_profit_loss"
        unique_together = [("symbol", "year")]
        ordering        = ["symbol", "year__sort_order"]
        verbose_name        = "Profit & Loss"
        verbose_name_plural = "Profit & Loss Records"

    def __str__(self):
        return f"{self.symbol_id} P&L {self.year}"


class BalanceSheet(models.Model):
    """fact_balance_sheet – annual balance sheet data."""

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="balance_sheet_records",
    )
    year   = models.ForeignKey(
        Year,
        on_delete=models.CASCADE,
        db_column="year_id",
        related_name="balance_sheet_records",
    )

    # Liabilities side
    equity_capital    = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    reserves          = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    borrowings        = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    other_liabilities = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    total_liabilities = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)

    # Assets side
    fixed_assets  = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    cwip          = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    investments   = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    other_assets  = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    total_assets  = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)

    # Derived ratios
    debt_to_equity = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    equity_ratio   = models.DecimalField(max_digits=8,  decimal_places=4, null=True, blank=True)

    class Meta:
        db_table        = "fact_balance_sheet"
        unique_together = [("symbol", "year")]
        ordering        = ["symbol", "year__sort_order"]
        verbose_name        = "Balance Sheet"
        verbose_name_plural = "Balance Sheet Records"

    def __str__(self):
        return f"{self.symbol_id} BS {self.year}"


class CashFlow(models.Model):
    """fact_cash_flow – annual cash flow statement data."""

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="cash_flow_records",
    )
    year   = models.ForeignKey(
        Year,
        on_delete=models.CASCADE,
        db_column="year_id",
        related_name="cash_flow_records",
    )

    operating_activity    = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    investing_activity    = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    financing_activity    = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    net_cash_flow         = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    free_cash_flow        = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    cash_conversion_ratio = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)

    class Meta:
        db_table        = "fact_cash_flow"
        unique_together = [("symbol", "year")]
        ordering        = ["symbol", "year__sort_order"]
        verbose_name        = "Cash Flow"
        verbose_name_plural = "Cash Flow Records"

    def __str__(self):
        return f"{self.symbol_id} CF {self.year}"


class Analysis(models.Model):
    """
    fact_analysis – CAGR / ratio summaries (one row per symbol + period + metric).

    Typical metrics: compounded_sales_growth, compounded_profit_growth,
                     return_on_equity, stock_price_cagr.
    Typical periods: 10Y, 5Y, 3Y, 1Y, TTM.
    """

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="analysis_records",
    )
    period    = models.CharField(max_length=10)
    metric    = models.CharField(max_length=60)
    value_pct = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)

    class Meta:
        db_table        = "fact_analysis"
        unique_together = [("symbol", "period", "metric")]
        ordering        = ["symbol", "metric", "period"]
        verbose_name        = "Analysis Record"
        verbose_name_plural = "Analysis Records"

    def __str__(self):
        return f"{self.symbol_id} {self.metric} {self.period}"


class MLScore(models.Model):
    """
    fact_ml_scores – ML-computed financial health scores.

    Each scoring run produces one row per company.
    get_latest_by = 'computed_at' allows MLScore.objects.latest().
    """

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="ml_scores",
    )
    computed_at = models.DateTimeField()

    overall_score       = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    profitability_score = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    growth_score        = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    leverage_score      = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    cashflow_score      = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    dividend_score      = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    trend_score         = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    health_label        = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        db_table        = "fact_ml_scores"
        unique_together = [("symbol", "computed_at")]
        ordering        = ["-computed_at"]
        get_latest_by   = "computed_at"
        verbose_name        = "ML Score"
        verbose_name_plural = "ML Scores"

    def __str__(self):
        return f"{self.symbol_id} score={self.overall_score} ({self.health_label})"


class ProsCons(models.Model):
    """fact_pros_cons – curated or auto-generated bullet points per company."""

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="pros_cons",
    )
    is_pro       = models.BooleanField()
    text         = models.TextField()
    source       = models.CharField(max_length=10, default="MANUAL")
    generated_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "fact_pros_cons"
        ordering = ["symbol", "-is_pro", "id"]
        verbose_name        = "Pros / Cons Item"
        verbose_name_plural = "Pros / Cons Items"

    def __str__(self):
        kind = "PRO" if self.is_pro else "CON"
        return f"{self.symbol_id} [{kind}] {self.text[:60]}"


class Document(models.Model):
    """documents – annual report URLs per company per year."""

    id                = models.AutoField(primary_key=True)
    # Intentionally a plain CharField (no FK) – may reference tickers
    # loaded after the documents table was populated.
    symbol            = models.CharField(max_length=20)
    year              = models.IntegerField(null=True, blank=True)
    annual_report_url = models.TextField(blank=True, null=True)

    class Meta:
        db_table        = "documents"
        unique_together = [("symbol", "year")]
        ordering        = ["symbol", "-year"]
        verbose_name        = "Document"
        verbose_name_plural = "Documents"

    def __str__(self):
        return f"{self.symbol} Annual Report {self.year}"


class Peer(models.Model):
    """
    fact_peers – pairwise cosine-similarity scores between companies.

    (symbol, peer_symbol) is the composite primary key.
    """

    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="peer_entries",
    )
    peer_symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="peer_symbol",
        to_field="symbol",
        related_name="referenced_as_peer",
    )
    similarity = models.DecimalField(max_digits=8, decimal_places=6, null=True, blank=True)
    rank       = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table        = "fact_peers"
        unique_together = [("symbol", "peer_symbol")]
        ordering        = ["symbol", "rank"]
        verbose_name        = "Peer"
        verbose_name_plural = "Peers"

    def __str__(self):
        return f"{self.symbol_id} → {self.peer_symbol_id} (rank {self.rank})"


class Anomaly(models.Model):
    """fact_anomalies – z-score flagged anomalies in financial metrics."""

    SEVERITY_CHOICES = [
        ("LOW",      "Low"),
        ("MEDIUM",   "Medium"),
        ("HIGH",     "High"),
        ("CRITICAL", "Critical"),
    ]

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="anomalies",
    )
    year = models.ForeignKey(
        Year,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        db_column="year_id",
        related_name="anomalies",
    )
    metric     = models.CharField(max_length=60, blank=True, null=True)
    value      = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    z_score    = models.DecimalField(max_digits=8,  decimal_places=4, null=True, blank=True)
    method     = models.CharField(max_length=20, blank=True, null=True)
    severity   = models.CharField(max_length=10, choices=SEVERITY_CHOICES, blank=True, null=True)
    reviewed   = models.BooleanField(default=False)
    notes      = models.TextField(blank=True, null=True)
    flagged_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "fact_anomalies"
        ordering = ["-flagged_at", "-z_score"]
        verbose_name        = "Anomaly"
        verbose_name_plural = "Anomalies"

    def __str__(self):
        return f"{self.symbol_id} {self.metric} z={self.z_score} [{self.severity}]"


class Forecast(models.Model):
    """fact_forecasts – time-series sales forecasts per company."""

    id     = models.AutoField(primary_key=True)
    symbol = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        related_name="forecasts",
    )
    forecast_year   = models.IntegerField()
    predicted_sales = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    lower_bound     = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    upper_bound     = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    trend_direction = models.CharField(max_length=10, blank=True, null=True)
    computed_at     = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table        = "fact_forecasts"
        unique_together = [("symbol", "forecast_year")]
        ordering        = ["symbol", "forecast_year"]
        verbose_name        = "Forecast"
        verbose_name_plural = "Forecasts"

    def __str__(self):
        return f"{self.symbol_id} forecast {self.forecast_year}: {self.predicted_sales}"


class Cluster(models.Model):
    """fact_clusters – PCA cluster assignment per company."""

    symbol = models.OneToOneField(
        Company,
        on_delete=models.CASCADE,
        db_column="symbol",
        to_field="symbol",
        primary_key=True,
        related_name="cluster",
    )
    cluster_id    = models.IntegerField(null=True, blank=True)
    cluster_label = models.CharField(max_length=100, blank=True, null=True)
    pca_x         = models.DecimalField(max_digits=12, decimal_places=6, null=True, blank=True)
    pca_y         = models.DecimalField(max_digits=12, decimal_places=6, null=True, blank=True)
    computed_at   = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "fact_clusters"
        verbose_name        = "Cluster Assignment"
        verbose_name_plural = "Cluster Assignments"

    def __str__(self):
        return f"{self.symbol_id} cluster={self.cluster_id} ({self.cluster_label})"


# ── Support / Operational Tables ──────────────────────────────────────────────

class APIUsageLog(models.Model):
    """
    api_usage_log – lightweight request audit log for the partner API.

    Used for rate-limit analytics and partner billing reports.
    """

    METHOD_CHOICES = [
        ("GET",    "GET"),
        ("POST",   "POST"),
        ("PUT",    "PUT"),
        ("PATCH",  "PATCH"),
        ("DELETE", "DELETE"),
    ]

    id              = models.AutoField(primary_key=True)
    api_key_prefix  = models.CharField(max_length=12, blank=True, null=True, db_index=True)
    endpoint        = models.CharField(max_length=255)
    method          = models.CharField(max_length=10, choices=METHOD_CHOICES, default="GET")
    status_code     = models.IntegerField(null=True, blank=True)
    response_time_ms = models.IntegerField(null=True, blank=True)
    ip_address      = models.GenericIPAddressField(null=True, blank=True)
    user_agent      = models.TextField(blank=True, null=True)
    query_params    = models.TextField(blank=True, null=True)  # JSON string
    requested_at    = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "api_usage_log"
        ordering = ["-requested_at"]
        verbose_name        = "API Usage Log"
        verbose_name_plural = "API Usage Logs"

    def __str__(self):
        return f"{self.method} {self.endpoint} {self.status_code} @ {self.requested_at}"
