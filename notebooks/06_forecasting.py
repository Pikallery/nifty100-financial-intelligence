"""
Nifty 100 Intelligence Platform
Notebook 06: Revenue Forecasting
==================================
Sections:
  1. DB connection
  2. numpy.polyfit on last 5 years revenue per company, classify UP/FLAT/DOWN
  3. Plot trend lines for top 10 companies
  4. Holt-Winters ExponentialSmoothing for top 20 companies, 1-year forecast + CI
  5. Save to fact_forecasts
  6. Print disclaimer: "Model estimates only. Not financial advice."
"""

# ============================================================
# 0. Imports
# ============================================================
import os
import sys
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 120, "figure.facecolor": "white",
                     "axes.spines.top": False, "axes.spines.right": False})

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "forecast_plots")
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
    host   = os.getenv("DB_HOST",     "localhost")
    port   = os.getenv("DB_PORT",     "5432")
    dbname = os.getenv("DB_NAME",     "nifty100")
    user   = os.getenv("DB_USER",     "postgres")
    pw     = os.getenv("DB_PASSWORD", "")
    DB_URL = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}"

engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

with engine.connect() as conn:
    v = conn.execute(text("SELECT version()")).scalar()
    print(f"  Connected: {v[:60]}...")

print("  DB connection OK.\n")


# ============================================================
# LOAD REVENUE DATA
# ============================================================
print("  Loading revenue data...")

with engine.connect() as conn:
    df = pd.read_sql(
        """
        SELECT
            c.id AS company_id,
            c.symbol, c.name AS company_name, c.sector,
            f.fiscal_year, f.sales
        FROM fact_financials f
        JOIN dim_companies c ON c.id = f.company_id
        WHERE f.sales IS NOT NULL
        ORDER BY c.symbol, f.fiscal_year
        """,
        conn,
    )

print(f"  Loaded {len(df):,} revenue observations for {df['symbol'].nunique()} companies.\n")

LOOKBACK_YEARS = 5
latest_fy      = df["fiscal_year"].max()
trend_start_fy = latest_fy - LOOKBACK_YEARS + 1

df_window = df[df["fiscal_year"] >= trend_start_fy].copy()
print(f"  Trend window: FY{trend_start_fy} – FY{latest_fy}")
print(f"  Companies with {LOOKBACK_YEARS}+ years: {df_window.groupby('symbol').size().ge(LOOKBACK_YEARS).sum()}\n")


# ============================================================
# 2. NUMPY.POLYFIT — TREND CLASSIFICATION
# ============================================================
print("=" * 60)
print("SECTION 2 — Linear Trend (polyfit) + Classification")
print("=" * 60)

FLAT_THRESHOLD_PCT = 5.0   # slope < ±5% of mean revenue = FLAT

trend_records = []

for symbol, grp in df_window.groupby("symbol"):
    grp = grp.sort_values("fiscal_year").dropna(subset=["sales"])
    if len(grp) < 3:
        continue

    years  = grp["fiscal_year"].values
    revenue = grp["sales"].values

    # Normalise years to 0-based for numerical stability
    x = years - years[0]
    coeffs = np.polyfit(x, revenue, deg=1)
    slope  = coeffs[0]          # Rs Cr per year
    intercept = coeffs[1]

    mean_rev  = revenue.mean()
    slope_pct = slope / mean_rev * 100 if mean_rev != 0 else 0

    if slope_pct > FLAT_THRESHOLD_PCT:
        trend = "UP"
    elif slope_pct < -FLAT_THRESHOLD_PCT:
        trend = "DOWN"
    else:
        trend = "FLAT"

    # Forecast next year (1 step ahead)
    next_year     = int(latest_fy) + 1
    x_forecast    = next_year - years[0]
    forecast_rev  = float(coeffs[0] * x_forecast + coeffs[1])

    # Simple confidence interval: ±residual RMSE
    y_fit   = np.polyval(coeffs, x)
    rmse    = float(np.sqrt(np.mean((revenue - y_fit) ** 2)))

    trend_records.append({
        "company_id":        grp["company_id"].iloc[0],
        "symbol":            symbol,
        "company_name":      grp["company_name"].iloc[0],
        "sector":            grp["sector"].iloc[0],
        "slope_cr_per_year": round(float(slope), 2),
        "slope_pct":         round(slope_pct, 2),
        "trend_label":       trend,
        "polyfit_forecast_cr": round(max(0.0, forecast_rev), 2),
        "polyfit_ci_lower":   round(max(0.0, forecast_rev - 1.96 * rmse), 2),
        "polyfit_ci_upper":   round(forecast_rev + 1.96 * rmse, 2),
        "forecast_year":     next_year,
    })

df_trends = pd.DataFrame(trend_records)

# Distribution
print(f"\n  Trend classification ({LOOKBACK_YEARS}-year window):")
for trend, cnt in df_trends["trend_label"].value_counts().items():
    pct = cnt / len(df_trends) * 100
    print(f"    {trend:<6}: {cnt:>3} companies  ({pct:.1f}%)")


# ============================================================
# 3. TREND LINE PLOTS — TOP 10 COMPANIES BY REVENUE
# ============================================================
print("\n" + "=" * 60)
print("SECTION 3 — Trend Lines for Top 10 Companies")
print("=" * 60)

latest_rev = df[df["fiscal_year"] == latest_fy][["symbol", "sales"]].set_index("symbol")["sales"]
top10_symbols = latest_rev.nlargest(10).index.tolist()

trend_colors = {"UP": "#10b981", "FLAT": "#f59e0b", "DOWN": "#ef4444"}

fig, axes = plt.subplots(2, 5, figsize=(22, 9))
axes_flat  = axes.flatten()
fig.suptitle(f"Revenue Trend Lines — Top 10 by Revenue (FY{latest_fy})", fontsize=14, fontweight="bold")

for idx, symbol in enumerate(top10_symbols):
    ax = axes_flat[idx]
    grp = df_window[df_window["symbol"] == symbol].sort_values("fiscal_year")
    if grp.empty:
        ax.set_visible(False)
        continue

    years   = grp["fiscal_year"].values
    revenue = grp["sales"].values / 1e5  # convert to Lakh Cr for readability

    trend_row = df_trends[df_trends["symbol"] == symbol]
    trend_lbl = trend_row["trend_label"].iloc[0] if len(trend_row) else "FLAT"
    color     = trend_colors.get(trend_lbl, "#94a3b8")
    slope_p   = trend_row["slope_pct"].iloc[0] if len(trend_row) else 0

    # Actual data
    ax.plot(years, revenue, "o-", color="#1e3a5f", linewidth=2, markersize=5, label="Actual")

    # Trend line
    x_norm = years - years[0]
    coeffs = np.polyfit(x_norm, revenue, 1)
    x_ext  = np.linspace(0, len(years), 50)
    ax.plot(years[0] + x_ext, np.polyval(coeffs, x_ext),
            "--", color=color, linewidth=1.5, alpha=0.7, label=f"Trend ({trend_lbl})")

    # Forecast point
    if len(trend_row):
        fc_yr    = trend_row["forecast_year"].iloc[0]
        fc_val   = trend_row["polyfit_forecast_cr"].iloc[0] / 1e5
        fc_low   = trend_row["polyfit_ci_lower"].iloc[0] / 1e5
        fc_high  = trend_row["polyfit_ci_upper"].iloc[0] / 1e5
        ax.errorbar(fc_yr, fc_val,
                    yerr=[[fc_val - fc_low], [fc_high - fc_val]],
                    fmt="*", color=color, markersize=10,
                    capsize=4, linewidth=1.5,
                    label=f"FY{fc_yr} forecast")

    ax.set_title(f"{symbol}\n{trend_lbl}  ({slope_p:+.1f}%/yr)", fontsize=9, color=color)
    ax.set_xlabel("FY", fontsize=8)
    ax.set_ylabel("Revenue (Lakh Cr)", fontsize=7)
    ax.tick_params(labelsize=7)
    ax.legend(fontsize=6)

for i in range(len(top10_symbols), len(axes_flat)):
    axes_flat[i].set_visible(False)

plt.tight_layout()
savefig("03_top10_trend_lines.png")


# ============================================================
# 4. HOLT-WINTERS FORECASTING — TOP 20 COMPANIES
# ============================================================
print("=" * 60)
print("SECTION 4 — Holt-Winters Forecast (Top 20 Companies)")
print("=" * 60)

top20_symbols = latest_rev.nlargest(20).index.tolist()
hw_records    = []

FORECAST_STEPS    = 1
SIMULATION_RUNS   = 1000     # Monte Carlo runs for CI via residual bootstrap

fig, axes = plt.subplots(4, 5, figsize=(24, 18))
axes_flat  = axes.flatten()
fig.suptitle(
    f"Holt-Winters Revenue Forecast (1-Year Ahead)\nTop 20 Companies by FY{latest_fy} Revenue",
    fontsize=14, fontweight="bold",
)

for idx, symbol in enumerate(top20_symbols):
    ax = axes_flat[idx]

    grp = (
        df[df["symbol"] == symbol]
        .sort_values("fiscal_year")
        .dropna(subset=["sales"])
        .copy()
    )

    if len(grp) < 3:
        ax.set_visible(False)
        print(f"  [{symbol}] Insufficient data ({len(grp)} points) — skipped")
        continue

    revenue_series = grp["sales"].values
    years          = grp["fiscal_year"].values
    company_id     = int(grp["company_id"].iloc[0])
    forecast_year  = int(years[-1]) + 1

    # ---- Holt-Winters model ----
    try:
        model = ExponentialSmoothing(
            revenue_series,
            trend="add",
            seasonal=None,         # annual data — no seasonality
            damped_trend=True,
            initialization_method="estimated",
        )
        fit   = model.fit(optimized=True, use_brute=True)
        fc    = fit.forecast(FORECAST_STEPS)
        fc_val = float(fc[0])

        # ---- Bootstrap confidence interval ----
        residuals = fit.resid
        rng    = np.random.default_rng(42)
        sims   = np.zeros(SIMULATION_RUNS)
        for r in range(SIMULATION_RUNS):
            resampled = revenue_series + rng.choice(residuals, size=len(revenue_series), replace=True)
            try:
                sim_model = ExponentialSmoothing(
                    resampled,
                    trend="add", seasonal=None, damped_trend=True,
                    initialization_method="estimated",
                )
                sim_fit = sim_model.fit(optimized=True, use_brute=False)
                sims[r] = float(sim_fit.forecast(1)[0])
            except Exception:
                sims[r] = fc_val

        ci_lower = float(np.percentile(sims, 5))
        ci_upper = float(np.percentile(sims, 95))
        ci_lower = max(0.0, ci_lower)

    except Exception as e:
        print(f"  [{symbol}] HW fit failed: {e} — using polyfit fallback")
        x_norm  = np.arange(len(revenue_series))
        coeffs  = np.polyfit(x_norm, revenue_series, 1)
        fc_val  = float(np.polyval(coeffs, len(revenue_series)))
        fc_val  = max(0.0, fc_val)
        rmse    = np.sqrt(np.mean((revenue_series - np.polyval(coeffs, x_norm)) ** 2))
        ci_lower = max(0.0, fc_val - 1.96 * float(rmse))
        ci_upper = fc_val + 1.96 * float(rmse)

    # YoY implied growth
    last_actual = float(revenue_series[-1])
    yoy_growth  = (fc_val / last_actual - 1) * 100 if last_actual else 0

    hw_records.append({
        "company_id":       company_id,
        "symbol":           symbol,
        "company_name":     grp["company_name"].iloc[0],
        "sector":           grp["sector"].iloc[0],
        "forecast_year":    forecast_year,
        "forecast_revenue": round(fc_val, 2),
        "ci_lower_90":      round(ci_lower, 2),
        "ci_upper_90":      round(ci_upper, 2),
        "yoy_growth_pct":   round(yoy_growth, 2),
        "algorithm":        "holt_winters",
    })

    print(f"  [{symbol}] FY{forecast_year} forecast: ₹{fc_val/1000:,.0f}K Cr  "
          f"  CI: [{ci_lower/1000:,.0f}K – {ci_upper/1000:,.0f}K]  "
          f"  YoY: {yoy_growth:+.1f}%")

    # ---- Plot ----
    yr_plot  = list(years) + [forecast_year]
    rev_plot = list(revenue_series / 1000)   # ₹ 000 Cr

    ax.plot(years, np.array(revenue_series) / 1000, "o-",
            color="#1e3a5f", linewidth=2, markersize=4, label="Actual")
    ax.errorbar(
        forecast_year, fc_val / 1000,
        yerr=[[max(0, fc_val / 1000 - ci_lower / 1000)],
              [ci_upper / 1000 - fc_val / 1000]],
        fmt="*", color="#ef4444", markersize=10,
        capsize=4, linewidth=1.5, label=f"FY{forecast_year}\n±90% CI",
    )
    ax.fill_betweenx(
        [ci_lower / 1000, ci_upper / 1000],
        forecast_year - 0.3, forecast_year + 0.3,
        alpha=0.15, color="#ef4444",
    )
    ax.set_title(f"{symbol}\nFY{forecast_year}: ₹{fc_val/1000:,.0f}K Cr ({yoy_growth:+.1f}%)",
                 fontsize=8)
    ax.set_xlabel("FY", fontsize=7)
    ax.set_ylabel("Rev (₹000 Cr)", fontsize=7)
    ax.tick_params(labelsize=6)
    ax.legend(fontsize=6, loc="upper left")

for i in range(len(top20_symbols), len(axes_flat)):
    axes_flat[i].set_visible(False)

plt.tight_layout()
savefig("04_holtwinters_forecast_top20.png")

df_hw = pd.DataFrame(hw_records)
print(f"\n  Holt-Winters forecasts generated: {len(df_hw)} companies")


# ============================================================
# 5. SAVE TO fact_forecasts
# ============================================================
print("\n" + "=" * 60)
print("SECTION 5 — Save to fact_forecasts")
print("=" * 60)

# Combine polyfit + HW records
df_polyfit_save = df_trends[[
    "company_id", "forecast_year",
    "polyfit_forecast_cr", "polyfit_ci_lower", "polyfit_ci_upper",
    "trend_label", "slope_pct",
]].copy()
df_polyfit_save.rename(columns={
    "polyfit_forecast_cr": "forecast_revenue",
    "polyfit_ci_lower":    "ci_lower_90",
    "polyfit_ci_upper":    "ci_upper_90",
}, inplace=True)
df_polyfit_save["algorithm"]   = "polyfit"
df_polyfit_save["computed_at"] = datetime.utcnow()
df_polyfit_save["yoy_growth_pct"] = (
    (df_polyfit_save["forecast_revenue"] /
     df[df["fiscal_year"] == latest_fy].set_index("symbol")["sales"]
     .reindex(df_trends.set_index("company_id")["symbol"].map(
         df_trends.set_index("symbol")["symbol"]).values)
     .values - 1) * 100
)

df_hw_save = df_hw[[
    "company_id", "forecast_year", "forecast_revenue",
    "ci_lower_90", "ci_upper_90", "yoy_growth_pct", "algorithm",
]].copy()
df_hw_save["trend_label"]  = np.where(df_hw_save["yoy_growth_pct"] > 5, "UP",
                               np.where(df_hw_save["yoy_growth_pct"] < -5, "DOWN", "FLAT"))
df_hw_save["slope_pct"]    = df_hw_save["yoy_growth_pct"]
df_hw_save["computed_at"]  = datetime.utcnow()

with engine.begin() as conn:
    conn.execute(text(
        "DELETE FROM fact_forecasts WHERE algorithm IN ('polyfit', 'holt_winters')"
    ))
    df_polyfit_save.to_sql(
        "fact_forecasts", conn, if_exists="append", index=False,
        method="multi", chunksize=200,
    )
    df_hw_save.to_sql(
        "fact_forecasts", conn, if_exists="append", index=False,
        method="multi", chunksize=200,
    )

total_saved = len(df_polyfit_save) + len(df_hw_save)
print(f"  {total_saved} forecast records saved to fact_forecasts.")
print(f"    polyfit     : {len(df_polyfit_save)} records")
print(f"    holt_winters: {len(df_hw_save)} records")


# ============================================================
# 6. DISCLAIMER
# ============================================================
print("\n" + "=" * 60)
disclaimer = (
    "DISCLAIMER\n"
    "==========\n"
    "Model estimates only. Not financial advice.\n\n"
    "The revenue forecasts produced in this notebook are generated using\n"
    "statistical models (linear trend regression and Holt-Winters exponential\n"
    "smoothing) applied to historical financial data from public filings.\n\n"
    "These models assume past trends continue and cannot account for:\n"
    "  - Macroeconomic shocks or policy changes\n"
    "  - Corporate actions (mergers, acquisitions, restructuring)\n"
    "  - Sector disruptions or regulatory changes\n"
    "  - Management strategy shifts\n\n"
    "B100 Intelligence is not a SEBI-registered investment advisor.\n"
    "All forecasts are for informational and educational purposes only.\n"
    "Do not make investment decisions solely based on these projections.\n"
    "Always consult a qualified financial advisor before investing.\n"
)
print(disclaimer)
print("=" * 60)

print("\n" + "=" * 60)
print("FORECASTING COMPLETE — Output plots:", OUTPUT_DIR)
print("=" * 60 + "\n")
