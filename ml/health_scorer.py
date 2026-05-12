"""
ml/health_scorer.py
Computes a 0-100 financial health score for every company.

Sub-dimension weights:
  Profitability  25%   (net profit margin, OPM%, ROA)
  Revenue Growth 20%   (5Y/3Y CAGR + YoY avg)
  Leverage       20%   (D/E, equity ratio)
  Cash Flow      15%   (FCF consistency, cash conversion, operating cash)
  Dividend       10%   (avg payout, paying years)
  Growth Trend   10%   (revenue + profit slope)

Label thresholds: 85+ EXCELLENT | 70+ GOOD | 50+ AVERAGE | 35+ WEAK | 0+ POOR
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

WEIGHTS = {
    "profitability": 0.25,
    "growth":        0.20,
    "leverage":      0.20,
    "cashflow":      0.15,
    "dividend":      0.10,
    "trend":         0.10,
}

LABEL_THRESHOLDS = [(85,"EXCELLENT"),(70,"GOOD"),(50,"AVERAGE"),(35,"WEAK"),(0,"POOR")]

BANKING = {
    "AXISBANK","BANKBARODA","CANBK","HDFCBANK","ICICIBANK",
    "INDUSINDBK","IDFCFIRSTB","KOTAKBANK","PNB","SBIN",
    "UNIONBANK","BANDHANBNK","FEDERALBNK",
}


def get_engine():
    url = (
        f"postgresql://{os.getenv('DB_USER','postgres')}:"
        f"{os.getenv('DB_PASSWORD','Pradyumna123')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','5432')}/"
        f"{os.getenv('DB_NAME','nifty100_warehouse')}"
    )
    return create_engine(url)


def load_data(engine):
    with engine.connect() as conn:
        pl = pd.read_sql("""
            SELECT f.symbol, dy.fiscal_year,
                   f.net_profit_margin_pct, f.opm_percentage,
                   f.return_on_assets_pct, f.interest_coverage,
                   f.eps, f.dividend_payout, f.sales, f.net_profit,
                   f.is_banking
            FROM fact_profit_loss f
            JOIN dim_year dy ON f.year_id = dy.year_id
            WHERE dy.is_ttm = FALSE AND dy.fiscal_year IS NOT NULL
              AND dy.fiscal_year >= (
                  SELECT MAX(fiscal_year)-4 FROM dim_year WHERE is_ttm=FALSE
              )
        """, conn)

        bs = pd.read_sql("""
            SELECT f.symbol, dy.fiscal_year,
                   f.debt_to_equity, f.equity_ratio, f.borrowings
            FROM fact_balance_sheet f
            JOIN dim_year dy ON f.year_id = dy.year_id
            WHERE dy.is_ttm = FALSE AND dy.fiscal_year IS NOT NULL
              AND dy.fiscal_year >= (
                  SELECT MAX(fiscal_year)-4 FROM dim_year WHERE is_ttm=FALSE
              )
        """, conn)

        cf = pd.read_sql("""
            SELECT f.symbol, dy.fiscal_year,
                   f.free_cash_flow, f.cash_conversion_ratio, f.operating_activity
            FROM fact_cash_flow f
            JOIN dim_year dy ON f.year_id = dy.year_id
            WHERE dy.is_ttm = FALSE AND dy.fiscal_year IS NOT NULL
              AND dy.fiscal_year >= (
                  SELECT MAX(fiscal_year)-4 FROM dim_year WHERE is_ttm=FALSE
              )
        """, conn)

        analysis = pd.read_sql(
            "SELECT symbol, period, metric, value_pct FROM fact_analysis", conn
        )
    return pl, bs, cf, analysis


def pct_rank(s):
    return s.rank(pct=True, na_option="keep") * 100

def pct_rank_inv(s):
    return (1 - s.rank(pct=True, na_option="keep")) * 100


def score_profitability(pl):
    # For banking companies, skip OPM% (it's NULL) and weight NPM+ROA higher
    agg = pl.groupby("symbol").agg(
        avg_npm=("net_profit_margin_pct", "mean"),
        avg_opm=("opm_percentage",        "mean"),
        avg_roa=("return_on_assets_pct",  "mean"),
    )
    # Where OPM is NaN (banking), use 50th percentile as neutral
    opm_rank = pct_rank(agg["avg_opm"]).fillna(50)
    s = (
        pct_rank(agg["avg_npm"]) * 0.45 +
        opm_rank                 * 0.25 +
        pct_rank(agg["avg_roa"]) * 0.30
    )
    return s.rename("profitability_score")


def score_growth(pl, analysis):
    an = analysis[analysis["metric"] == "compounded_sales_growth"]
    pivot = an.pivot_table(index="symbol", columns="period", values="value_pct")

    pl_s = pl.sort_values(["symbol","fiscal_year"])
    pl_s["yoy"] = pl_s.groupby("symbol")["sales"].pct_change() * 100
    avg_yoy = pl_s.groupby("symbol")["yoy"].mean()

    score = pd.Series(0.0, index=avg_yoy.index)
    if "5Y" in pivot.columns:
        score = score.add(pct_rank(pivot["5Y"]).fillna(50) * 0.45, fill_value=0)
    if "3Y" in pivot.columns:
        score = score.add(pct_rank(pivot["3Y"]).fillna(50) * 0.25, fill_value=0)
    score = score.add(pct_rank(avg_yoy).fillna(50) * 0.30, fill_value=0)
    return score.rename("growth_score")


def score_leverage(bs):
    agg = bs.groupby("symbol").agg(
        avg_de=("debt_to_equity", "mean"),
        avg_er=("equity_ratio",   "mean"),
    )
    # Debt-free companies (borrowings=0, D/E=0) should score well
    s = (
        pct_rank_inv(agg["avg_de"]).fillna(50) * 0.60 +
        pct_rank(agg["avg_er"]).fillna(50)     * 0.40
    )
    return s.rename("leverage_score")


def score_cashflow(cf):
    agg = cf.groupby("symbol").agg(
        pos_fcf=("free_cash_flow",        lambda x: (x > 0).sum()),
        avg_ccr=("cash_conversion_ratio", "mean"),
        avg_op =("operating_activity",    "mean"),
    )
    s = (
        pct_rank(agg["pos_fcf"]).fillna(50) * 0.40 +
        pct_rank(agg["avg_ccr"]).fillna(50) * 0.35 +
        pct_rank(agg["avg_op"]).fillna(50)  * 0.25
    )
    return s.rename("cashflow_score")


def score_dividend(pl):
    agg = pl.groupby("symbol").agg(
        avg_payout=("dividend_payout", "mean"),
        paying_yrs=("dividend_payout", lambda x: (x > 0).sum()),
    )
    s = (
        pct_rank(agg["avg_payout"]).fillna(50) * 0.50 +
        pct_rank(agg["paying_yrs"]).fillna(50) * 0.50
    )
    return s.rename("dividend_score")


def score_trend(pl):
    def slope(series):
        series = series.dropna()
        if len(series) < 3:
            return np.nan
        return np.polyfit(np.arange(len(series)), series.values, 1)[0]

    pl_s = pl.sort_values(["symbol","fiscal_year"])
    sales_slope  = pl_s.groupby("symbol")["sales"].apply(slope)
    profit_slope = pl_s.groupby("symbol")["net_profit_margin_pct"].apply(slope)
    s = (
        pct_rank(sales_slope).fillna(50)  * 0.50 +
        pct_rank(profit_slope).fillna(50) * 0.50
    )
    return s.rename("trend_score")


def assign_label(score):
    for threshold, label in LABEL_THRESHOLDS:
        if score >= threshold:
            return label
    return "POOR"


def compute_scores(engine=None):
    if engine is None:
        engine = get_engine()
    pl, bs, cf, analysis = load_data(engine)

    sub_scores = [
        score_profitability(pl),
        score_growth(pl, analysis),
        score_leverage(bs),
        score_cashflow(cf),
        score_dividend(pl),
        score_trend(pl),
    ]
    combined = pd.concat(sub_scores, axis=1).fillna(50)

    combined["overall_score"] = (
        combined["profitability_score"] * WEIGHTS["profitability"] +
        combined["growth_score"]        * WEIGHTS["growth"]        +
        combined["leverage_score"]      * WEIGHTS["leverage"]      +
        combined["cashflow_score"]      * WEIGHTS["cashflow"]      +
        combined["dividend_score"]      * WEIGHTS["dividend"]      +
        combined["trend_score"]         * WEIGHTS["trend"]
    ).clip(0, 100).round(2)

    combined["health_label"] = combined["overall_score"].apply(assign_label)
    combined["computed_at"]  = datetime.now(timezone.utc).replace(tzinfo=None)
    return combined.reset_index().rename(columns={"index":"symbol"})


def save_scores(scores, engine=None):
    if engine is None:
        engine = get_engine()
    cols = ["symbol","computed_at","overall_score","profitability_score",
            "growth_score","leverage_score","cashflow_score",
            "dividend_score","trend_score","health_label"]
    scores = scores[cols]
    with engine.begin() as conn:
        for _, row in scores.iterrows():
            conn.execute(text("""
                INSERT INTO fact_ml_scores
                    (symbol,computed_at,overall_score,profitability_score,
                     growth_score,leverage_score,cashflow_score,
                     dividend_score,trend_score,health_label)
                VALUES
                    (:symbol,:computed_at,:overall_score,:profitability_score,
                     :growth_score,:leverage_score,:cashflow_score,
                     :dividend_score,:trend_score,:health_label)
                ON CONFLICT (symbol,computed_at) DO UPDATE SET
                    overall_score=EXCLUDED.overall_score,
                    health_label=EXCLUDED.health_label
            """), row.to_dict())
    print(f"Saved {len(scores)} health scores to fact_ml_scores.")


if __name__ == "__main__":
    engine = get_engine()
    scores = compute_scores(engine)
    print("\nTop 10 companies by health score:")
    print(scores[["symbol","overall_score","health_label"]]
          .sort_values("overall_score", ascending=False)
          .head(10).to_string(index=False))
    print("\nLabel distribution:")
    print(scores["health_label"].value_counts().to_string())
    save_scores(scores, engine)
