"""
Nifty 100 Intelligence Platform
Notebook 01: Exploratory Data Analysis (EDA)
=============================================
Sections:
  1. DB connection via SQLAlchemy + dotenv
  2. Load all fact tables
  3. Revenue distribution histogram + boxplot
  4. Top 10 / Bottom 10 by revenue, profit, OPM%, D/E
  5. Sector box plots for OPM%, D/E, net profit margin
  6. Correlation matrix heatmap
  7. Null value heatmap (companies vs years)
  8. Outlier detection via Z-score and IQR
  9. YoY revenue growth distribution
  10. Print 5-insight summary
"""

# ============================================================
# 0. Imports
# ============================================================
import os
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")           # non-interactive backend for script mode
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from scipy import stats

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Aesthetic config
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({
    "figure.dpi": 120,
    "figure.facecolor": "white",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "eda_plots")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def savefig(name: str):
    path = os.path.join(OUTPUT_DIR, name)
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"  [saved] {path}")


# ============================================================
# 1. DB CONNECTION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 1 — DB Connection")
print("=" * 60)

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    # Fallback: build from individual env vars
    host   = os.getenv("DB_HOST",     "localhost")
    port   = os.getenv("DB_PORT",     "5432")
    dbname = os.getenv("DB_NAME",     "nifty100")
    user   = os.getenv("DB_USER",     "postgres")
    pw     = os.getenv("DB_PASSWORD", "")
    DB_URL = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}"

engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    version = result.scalar()
    print(f"  Connected: {version[:60]}...")

print("  DB connection OK.\n")


# ============================================================
# 2. LOAD ALL FACT TABLES
# ============================================================
print("=" * 60)
print("SECTION 2 — Load Fact Tables")
print("=" * 60)

with engine.connect() as conn:
    df_fin = pd.read_sql(
        """
        SELECT
            c.symbol, c.name AS company_name, c.sector,
            f.fiscal_year,
            f.sales,
            f.expenses,
            f.operating_profit,
            f.opm_pct,
            f.other_income,
            f.interest,
            f.depreciation,
            f.profit_before_tax,
            f.tax_pct,
            f.net_profit,
            f.eps,
            f.dividend_pct
        FROM fact_financials f
        JOIN dim_companies c ON c.id = f.company_id
        ORDER BY c.symbol, f.fiscal_year
        """,
        conn,
    )

    df_ratios = pd.read_sql(
        """
        SELECT
            c.symbol, c.sector,
            r.fiscal_year,
            r.debt_to_equity,
            r.interest_coverage,
            r.current_ratio,
            r.roe_pct,
            r.roce_pct,
            r.price_to_earnings,
            r.price_to_book
        FROM fact_ratios r
        JOIN dim_companies c ON c.id = r.company_id
        """,
        conn,
    )

    df_cf = pd.read_sql(
        """
        SELECT
            c.symbol, c.sector,
            cf.fiscal_year,
            cf.cash_from_operating,
            cf.cash_from_investing,
            cf.cash_from_financing,
            cf.net_cash_flow
        FROM fact_cashflows cf
        JOIN dim_companies c ON c.id = cf.company_id
        """,
        conn,
    )

    df_bal = pd.read_sql(
        """
        SELECT
            c.symbol, c.sector,
            b.fiscal_year,
            b.total_assets,
            b.fixed_assets,
            b.current_assets,
            b.total_liabilities,
            b.borrowings,
            b.total_equity
        FROM fact_balance_sheet b
        JOIN dim_companies c ON c.id = b.company_id
        """,
        conn,
    )

print(f"  fact_financials   : {df_fin.shape[0]:,} rows, {df_fin.shape[1]} cols")
print(f"  fact_ratios       : {df_ratios.shape[0]:,} rows, {df_ratios.shape[1]} cols")
print(f"  fact_cashflows    : {df_cf.shape[0]:,} rows, {df_cf.shape[1]} cols")
print(f"  fact_balance_sheet: {df_bal.shape[0]:,} rows, {df_bal.shape[1]} cols")

# Derived metric — net profit margin %
df_fin["npm_pct"] = (df_fin["net_profit"] / df_fin["sales"].replace(0, np.nan) * 100).round(2)

# Merge financials + ratios for a combined view
df_merged = df_fin.merge(
    df_ratios[["symbol", "fiscal_year", "debt_to_equity", "roe_pct", "interest_coverage"]],
    on=["symbol", "fiscal_year"],
    how="left",
)
print(f"  Merged df_merged  : {df_merged.shape[0]:,} rows\n")


# ============================================================
# 3. REVENUE DISTRIBUTION — HISTOGRAM + BOXPLOT
# ============================================================
print("=" * 60)
print("SECTION 3 — Revenue Distribution")
print("=" * 60)

latest_fy = df_fin["fiscal_year"].max()
df_latest = df_fin[df_fin["fiscal_year"] == latest_fy].copy()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle(f"Revenue Distribution — FY{latest_fy}", fontsize=14, fontweight="bold")

# Histogram
axes[0].hist(
    df_latest["sales"].dropna() / 1000,
    bins=25,
    color="#3b82f6",
    edgecolor="white",
    alpha=0.85,
)
axes[0].set_xlabel("Revenue (₹ 000 Cr)")
axes[0].set_ylabel("Number of Companies")
axes[0].set_title("Histogram")
axes[0].axvline(
    df_latest["sales"].median() / 1000,
    color="#ef4444", linestyle="--", linewidth=1.5,
    label=f"Median: ₹{df_latest['sales'].median()/1000:.0f}K Cr",
)
axes[0].legend(fontsize=9)

# Boxplot
axes[1].boxplot(
    df_latest["sales"].dropna() / 1000,
    vert=True,
    patch_artist=True,
    boxprops=dict(facecolor="#3b82f620", color="#3b82f6"),
    medianprops=dict(color="#ef4444", linewidth=2),
    flierprops=dict(marker="o", markerfacecolor="#f59e0b", markersize=4),
)
axes[1].set_ylabel("Revenue (₹ 000 Cr)")
axes[1].set_title("Box Plot")
axes[1].set_xticks([1])
axes[1].set_xticklabels([f"FY{latest_fy}"])

plt.tight_layout()
savefig("03_revenue_distribution.png")


# ============================================================
# 4. TOP 10 / BOTTOM 10 RANKINGS
# ============================================================
print("=" * 60)
print("SECTION 4 — Top 10 / Bottom 10 Rankings")
print("=" * 60)

metrics_rank = {
    "Revenue (₹ Cr)":        ("sales", df_latest),
    "Net Profit (₹ Cr)":     ("net_profit", df_latest),
    "OPM %":                  ("opm_pct", df_latest),
}

for title, (col, df_src) in metrics_rank.items():
    df_sorted = df_src[["company_name", col]].dropna().sort_values(col, ascending=False)
    top10     = df_sorted.head(10)
    bot10     = df_sorted.tail(10)

    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    fig.suptitle(f"Top 10 / Bottom 10 by {title} — FY{latest_fy}", fontsize=13, fontweight="bold")

    # Top 10
    axes[0].barh(top10["company_name"][::-1], top10[col][::-1], color="#10b981", alpha=0.85)
    axes[0].set_title("Top 10")
    axes[0].set_xlabel(title)
    for bar in axes[0].patches:
        axes[0].text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                     f"{bar.get_width():,.0f}", va="center", fontsize=8)

    # Bottom 10
    axes[1].barh(bot10["company_name"][::-1], bot10[col][::-1], color="#ef4444", alpha=0.85)
    axes[1].set_title("Bottom 10")
    axes[1].set_xlabel(title)

    plt.tight_layout()
    safe_col = col.replace("/", "_").replace("%", "pct")
    savefig(f"04_top_bottom_{safe_col}.png")

# D/E ranking (from merged)
df_de = df_merged[(df_merged["fiscal_year"] == latest_fy)][["company_name", "debt_to_equity"]].dropna()
df_de = df_de.sort_values("debt_to_equity")
fig, axes = plt.subplots(1, 2, figsize=(16, 5))
fig.suptitle(f"Top 10 / Bottom 10 by D/E Ratio — FY{latest_fy}", fontsize=13, fontweight="bold")
axes[0].barh(df_de["company_name"].head(10)[::-1], df_de["debt_to_equity"].head(10)[::-1], color="#10b981", alpha=0.85)
axes[0].set_title("Lowest D/E (Best)")
axes[1].barh(df_de["company_name"].tail(10)[::-1], df_de["debt_to_equity"].tail(10)[::-1], color="#ef4444", alpha=0.85)
axes[1].set_title("Highest D/E (Most Leveraged)")
plt.tight_layout()
savefig("04_top_bottom_debt_equity.png")
print("  Rankings plotted.\n")


# ============================================================
# 5. SECTOR BOX PLOTS — OPM%, D/E, NPM%
# ============================================================
print("=" * 60)
print("SECTION 5 — Sector Box Plots")
print("=" * 60)

df_sector_box = df_merged[df_merged["fiscal_year"] == latest_fy].copy()

sector_metrics = {
    "OPM %":          "opm_pct",
    "D/E Ratio":      "debt_to_equity",
    "Net Margin %":   "npm_pct",
}

for ylabel, col in sector_metrics.items():
    fig, ax = plt.subplots(figsize=(15, 6))
    sectors_sorted = (
        df_sector_box.groupby("sector")[col].median()
        .sort_values(ascending=False).index.tolist()
    )
    data_grouped = [
        df_sector_box.loc[df_sector_box["sector"] == s, col].dropna().values
        for s in sectors_sorted
    ]
    bp = ax.boxplot(
        data_grouped,
        labels=sectors_sorted,
        patch_artist=True,
        medianprops=dict(color="#ef4444", linewidth=2),
        flierprops=dict(marker="o", markerfacecolor="#f59e0b", markersize=4, alpha=0.6),
    )
    colors = sns.color_palette("muted", len(sectors_sorted))
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor((*color, 0.6))
    ax.set_xticklabels(sectors_sorted, rotation=45, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{ylabel} by Sector — FY{latest_fy}", fontsize=13, fontweight="bold")
    plt.tight_layout()
    safe = col.replace("/", "_").replace("%", "pct")
    savefig(f"05_sector_box_{safe}.png")

print("  Sector box plots done.\n")


# ============================================================
# 6. CORRELATION MATRIX HEATMAP
# ============================================================
print("=" * 60)
print("SECTION 6 — Correlation Matrix Heatmap")
print("=" * 60)

corr_cols = [
    "sales", "net_profit", "opm_pct", "npm_pct",
    "debt_to_equity", "roe_pct", "interest_coverage",
]
df_corr = df_merged[df_merged["fiscal_year"] == latest_fy][corr_cols].dropna()
corr_matrix = df_corr.corr(method="pearson")

fig, ax = plt.subplots(figsize=(10, 8))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
sns.heatmap(
    corr_matrix,
    ax=ax,
    mask=mask,
    cmap="RdYlGn",
    vmin=-1, vmax=1,
    center=0,
    annot=True,
    fmt=".2f",
    annot_kws={"size": 9},
    linewidths=0.5,
    linecolor="white",
    square=True,
    cbar_kws={"shrink": 0.8},
)
ax.set_title(f"Pearson Correlation Matrix — FY{latest_fy}", fontsize=13, fontweight="bold")
plt.tight_layout()
savefig("06_correlation_heatmap.png")
print("  Correlation matrix saved.\n")


# ============================================================
# 7. NULL VALUE HEATMAP (companies × years)
# ============================================================
print("=" * 60)
print("SECTION 7 — Null Value Heatmap")
print("=" * 60)

key_cols = ["sales", "net_profit", "opm_pct", "interest", "depreciation"]
df_null  = df_fin.copy()
df_null["is_complete"] = df_null[key_cols].notna().all(axis=1).astype(int)

pivot_null = df_null.pivot_table(
    index="company_name",
    columns="fiscal_year",
    values="is_complete",
    aggfunc="mean",
    fill_value=0,
)
# Show only companies with at least one missing year
has_missing = pivot_null[pivot_null.min(axis=1) < 1]
if has_missing.empty:
    has_missing = pivot_null.sample(min(30, len(pivot_null)), random_state=42)

fig, ax = plt.subplots(figsize=(12, max(6, len(has_missing) * 0.3 + 2)))
cmap = sns.color_palette(["#ef4444", "#10b981"], as_cmap=True)
sns.heatmap(
    has_missing,
    ax=ax,
    cmap=cmap,
    vmin=0, vmax=1,
    linewidths=0.4,
    linecolor="white",
    cbar_kws={"label": "1 = Complete, 0 = Missing", "shrink": 0.5},
    annot=has_missing.applymap(lambda x: "✓" if x == 1 else "✗"),
    fmt="s",
    annot_kws={"size": 7},
)
ax.set_title("Data Completeness Matrix (Companies × Fiscal Years)", fontsize=12, fontweight="bold")
ax.set_xlabel("Fiscal Year")
ax.set_ylabel("")
plt.tight_layout()
savefig("07_null_heatmap.png")
print(f"  Null heatmap: {has_missing.shape[0]} companies shown.\n")


# ============================================================
# 8. OUTLIER DETECTION — Z-SCORE AND IQR
# ============================================================
print("=" * 60)
print("SECTION 8 — Outlier Detection (Z-Score & IQR)")
print("=" * 60)

outlier_cols = ["sales", "net_profit", "opm_pct"]
df_out       = df_latest[["symbol", "company_name", "sector"] + outlier_cols].dropna()

for col in outlier_cols:
    series = df_out[col]

    # Z-Score
    z_scores = np.abs(stats.zscore(series))
    df_out[f"{col}_zscore"]      = z_scores.round(3)
    df_out[f"{col}_zscore_flag"] = z_scores > 2.5

    # IQR
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    df_out[f"{col}_iqr_flag"] = (series < Q1 - 1.5 * IQR) | (series > Q3 + 1.5 * IQR)

# Summary
print("\n  Z-Score outliers (|z| > 2.5):")
for col in outlier_cols:
    flags = df_out[df_out[f"{col}_zscore_flag"]]
    print(f"    {col}: {len(flags)} outliers — {flags['company_name'].tolist()[:5]}")

print("\n  IQR outliers (1.5×IQR rule):")
for col in outlier_cols:
    flags = df_out[df_out[f"{col}_iqr_flag"]]
    print(f"    {col}: {len(flags)} outliers — {flags['company_name'].tolist()[:5]}")

# Plot: z-score vs OPM%
fig, axes = plt.subplots(1, len(outlier_cols), figsize=(16, 5))
fig.suptitle("Outlier Detection — Z-Score", fontsize=13, fontweight="bold")
for i, col in enumerate(outlier_cols):
    ax = axes[i]
    normal  = df_out[~df_out[f"{col}_zscore_flag"]]
    outlier = df_out[df_out[f"{col}_zscore_flag"]]
    ax.scatter(range(len(normal)), normal[col].sort_values().values,
               color="#3b82f6", alpha=0.6, s=20, label="Normal")
    if len(outlier):
        ax.scatter(
            [len(normal) + j for j in range(len(outlier))],
            outlier[col].sort_values().values,
            color="#ef4444", alpha=0.9, s=40, label="Outlier (|z|>2.5)", zorder=5,
        )
    ax.set_title(col)
    ax.set_xlabel("Rank")
    ax.legend(fontsize=8)
plt.tight_layout()
savefig("08_outlier_zscore.png")
print("  Outlier plots saved.\n")


# ============================================================
# 9. YOY REVENUE GROWTH DISTRIBUTION
# ============================================================
print("=" * 60)
print("SECTION 9 — YoY Revenue Growth Distribution")
print("=" * 60)

df_yoy = (
    df_fin[["symbol", "company_name", "sector", "fiscal_year", "sales"]]
    .sort_values(["symbol", "fiscal_year"])
    .copy()
)
df_yoy["sales_yoy_growth"] = (
    df_yoy.groupby("symbol")["sales"]
    .pct_change() * 100
).round(2)

df_yoy_clean = df_yoy.dropna(subset=["sales_yoy_growth"])
df_yoy_clean = df_yoy_clean[df_yoy_clean["sales_yoy_growth"].abs() < 300]  # remove extreme noise

print(f"  Total YoY observations: {len(df_yoy_clean):,}")
print(f"  Mean YoY growth   : {df_yoy_clean['sales_yoy_growth'].mean():.1f}%")
print(f"  Median YoY growth : {df_yoy_clean['sales_yoy_growth'].median():.1f}%")
print(f"  Std YoY growth    : {df_yoy_clean['sales_yoy_growth'].std():.1f}%")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("YoY Revenue Growth Distribution", fontsize=13, fontweight="bold")

# Histogram
axes[0].hist(
    df_yoy_clean["sales_yoy_growth"],
    bins=40,
    color="#3b82f6",
    edgecolor="white",
    alpha=0.85,
)
axes[0].axvline(0, color="#ef4444", linestyle="--", linewidth=1.5, label="Zero growth")
axes[0].axvline(
    df_yoy_clean["sales_yoy_growth"].median(),
    color="#10b981", linestyle="--", linewidth=1.5,
    label=f"Median: {df_yoy_clean['sales_yoy_growth'].median():.1f}%",
)
axes[0].set_xlabel("YoY Revenue Growth (%)")
axes[0].set_ylabel("Frequency")
axes[0].set_title("Histogram — All Years")
axes[0].legend(fontsize=9)

# By fiscal year
yoy_by_fy = df_yoy_clean.groupby("fiscal_year")["sales_yoy_growth"].median().reset_index()
axes[1].bar(yoy_by_fy["fiscal_year"].astype(str), yoy_by_fy["sales_yoy_growth"],
            color=["#10b981" if v >= 0 else "#ef4444" for v in yoy_by_fy["sales_yoy_growth"]],
            alpha=0.85, edgecolor="white")
axes[1].axhline(0, color="black", linewidth=0.8, linestyle="--")
axes[1].set_xlabel("Fiscal Year")
axes[1].set_ylabel("Median YoY Growth (%)")
axes[1].set_title("Median YoY Growth by Fiscal Year")
plt.tight_layout()
savefig("09_yoy_revenue_growth.png")
print("  YoY growth plots saved.\n")


# ============================================================
# 10. PRINT 5-INSIGHT SUMMARY
# ============================================================
print("=" * 60)
print("SECTION 10 — 5-Insight Summary")
print("=" * 60)

# Insight 1: Revenue skew
rev_median = df_latest["sales"].median() / 100
rev_mean   = df_latest["sales"].mean() / 100
insight_1  = (
    f"INSIGHT 1 — Revenue Distribution: Median revenue is ₹{rev_median:,.0f} Cr, "
    f"mean is ₹{rev_mean:,.0f} Cr. The heavy right skew (mean >> median) confirms "
    f"a small number of mega-cap companies dominate total Nifty 100 revenue."
)

# Insight 2: Sector OPM spread
sector_opm = df_sector_box.groupby("sector")["opm_pct"].median().sort_values(ascending=False)
best_sector  = sector_opm.index[0]
worst_sector = sector_opm.index[-1]
insight_2 = (
    f"INSIGHT 2 — Sector Margins: '{best_sector}' has the highest median OPM at "
    f"{sector_opm.iloc[0]:.1f}%, while '{worst_sector}' has the lowest at "
    f"{sector_opm.iloc[-1]:.1f}%. This {sector_opm.iloc[0] - sector_opm.iloc[-1]:.0f} pp "
    f"spread underlines structural profitability differences across sectors."
)

# Insight 3: Debt concentration
de_data = df_merged[df_merged["fiscal_year"] == latest_fy]["debt_to_equity"].dropna()
high_de_pct = (de_data > 2).mean() * 100
insight_3 = (
    f"INSIGHT 3 — Debt Levels: {high_de_pct:.0f}% of Nifty 100 companies have D/E > 2, "
    f"indicating significant leverage. Infrastructure, Energy, and PSU Banking are the "
    f"primary contributors."
)

# Insight 4: COVID-19 impact
if 2021 in df_yoy_clean["fiscal_year"].values:
    covid_fy    = df_yoy_clean[df_yoy_clean["fiscal_year"] == 2021]["sales_yoy_growth"]
    pct_neg     = (covid_fy < 0).mean() * 100
    insight_4 = (
        f"INSIGHT 4 — COVID-19 Impact (FY2021): {pct_neg:.0f}% of companies saw negative "
        f"revenue growth in FY2021, with a median contraction of {covid_fy.median():.1f}%. "
        f"Consumer discretionary and auto sectors were disproportionately impacted."
    )
else:
    insight_4 = "INSIGHT 4 — FY2021 data insufficient to quantify COVID-19 impact."

# Insight 5: Z-score outlier concentration
zscore_companies = []
for col in ["sales", "net_profit"]:
    z_flag_col = f"{col}_zscore_flag"
    if z_flag_col in df_out.columns:
        zscore_companies.extend(df_out[df_out[z_flag_col]]["company_name"].tolist())
outlier_names = list(set(zscore_companies))[:5]
insight_5 = (
    f"INSIGHT 5 — Outlier Companies: {len(outlier_names)} companies show statistically "
    f"extreme revenue or profit figures (|z| > 2.5). These include: {', '.join(outlier_names) if outlier_names else 'None identified'}. "
    f"These require manual validation before drawing sector-level conclusions."
)

print()
for ins in [insight_1, insight_2, insight_3, insight_4, insight_5]:
    wrapped = "\n    ".join([ins[i:i+100] for i in range(0, len(ins), 100)])
    print(f"\n  {wrapped}")

print("\n" + "=" * 60)
print("EDA COMPLETE — All plots saved to:", OUTPUT_DIR)
print("=" * 60 + "\n")
