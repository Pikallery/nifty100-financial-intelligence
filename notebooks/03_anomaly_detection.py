"""
Nifty 100 Intelligence Platform
Notebook 03: Anomaly Detection
================================
Sections:
  1. DB connection
  2. Z-score per company per year on: sales, net_profit, borrowings, operating_profit (flag |z|>2.5)
  3. Isolation Forest (contamination=0.05) on same metrics
  4. Compare both methods — agreement rate
  5. Cross-reference with known events: Adani 2022-23, COVID FY2021, NBFC crisis FY2019
  6. Save to fact_anomalies
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
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 120, "figure.facecolor": "white",
                     "axes.spines.top": False, "axes.spines.right": False})

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "anomaly_plots")
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
# LOAD DATA
# ============================================================
print("  Loading financial data...")

with engine.connect() as conn:
    df = pd.read_sql(
        """
        SELECT
            c.symbol, c.name AS company_name, c.sector,
            f.fiscal_year,
            f.sales,
            f.net_profit,
            f.operating_profit,
            b.borrowings
        FROM fact_financials f
        JOIN dim_companies c ON c.id = f.company_id
        LEFT JOIN fact_balance_sheet b
            ON b.company_id = f.company_id AND b.fiscal_year = f.fiscal_year
        ORDER BY c.symbol, f.fiscal_year
        """,
        conn,
    )

print(f"  Loaded {len(df):,} rows for {df['symbol'].nunique()} companies.\n")

ANOMALY_METRICS = ["sales", "net_profit", "operating_profit", "borrowings"]


# ============================================================
# 2. Z-SCORE PER COMPANY PER YEAR
# ============================================================
print("=" * 60)
print("SECTION 2 — Z-Score Anomaly Detection")
print("=" * 60)

Z_THRESHOLD = 2.5

df_zscore = df.copy()

# Compute per-metric z-scores across the entire cohort for each fiscal year
for metric in ANOMALY_METRICS:
    col_zscore = f"{metric}_zscore"
    col_flag   = f"{metric}_zscore_flag"

    df_zscore[col_zscore] = np.nan
    df_zscore[col_flag]   = False

    for fy, group in df_zscore.groupby("fiscal_year"):
        valid_mask = group[metric].notna()
        if valid_mask.sum() < 5:
            continue
        z = np.abs(stats.zscore(group.loc[valid_mask, metric]))
        df_zscore.loc[group.index[valid_mask], col_zscore] = z
        df_zscore.loc[group.index[valid_mask], col_flag]   = z > Z_THRESHOLD

# Create a flag_any_zscore column
flag_cols_z = [f"{m}_zscore_flag" for m in ANOMALY_METRICS]
df_zscore["flag_any_zscore"] = df_zscore[flag_cols_z].any(axis=1)

n_zscore = df_zscore["flag_any_zscore"].sum()
print(f"  Z-Score anomalies flagged (|z| > {Z_THRESHOLD}): {n_zscore}")
print(f"  Breakdown per metric:")
for m in ANOMALY_METRICS:
    cnt = df_zscore[f"{m}_zscore_flag"].sum()
    print(f"    {m:<20}: {cnt}")


# ============================================================
# 3. ISOLATION FOREST
# ============================================================
print("\n" + "=" * 60)
print("SECTION 3 — Isolation Forest Anomaly Detection")
print("=" * 60)

CONTAMINATION = 0.05

df_iforest = df.copy()
df_iforest["flag_iforest"] = False

for fy, group in df_iforest.groupby("fiscal_year"):
    valid_rows = group[ANOMALY_METRICS].dropna()
    if len(valid_rows) < 10:
        continue

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(valid_rows.values)

    iso = IsolationForest(
        contamination=CONTAMINATION,
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    preds = iso.fit_predict(X_scaled)  # -1 = anomaly, 1 = normal

    anomaly_indices = valid_rows.index[preds == -1]
    df_iforest.loc[anomaly_indices, "flag_iforest"] = True

n_iforest = df_iforest["flag_iforest"].sum()
print(f"  Isolation Forest anomalies flagged: {n_iforest}")
print(f"  (contamination={CONTAMINATION}, ~{CONTAMINATION*100:.0f}% of each FY cohort)")


# ============================================================
# 4. COMPARE BOTH METHODS — AGREEMENT RATE
# ============================================================
print("\n" + "=" * 60)
print("SECTION 4 — Method Comparison & Agreement")
print("=" * 60)

# Merge both flag columns
df_compare = df.copy()
df_compare["flag_zscore"]  = df_zscore["flag_any_zscore"].values
df_compare["flag_iforest"] = df_iforest["flag_iforest"].values
df_compare["flag_both"]    = df_compare["flag_zscore"] & df_compare["flag_iforest"]
df_compare["flag_either"]  = df_compare["flag_zscore"] | df_compare["flag_iforest"]

total_obs     = len(df_compare)
both_count    = df_compare["flag_both"].sum()
either_count  = df_compare["flag_either"].sum()
only_z_count  = (df_compare["flag_zscore"] & ~df_compare["flag_iforest"]).sum()
only_if_count = (df_compare["flag_iforest"] & ~df_compare["flag_zscore"]).sum()

agreement_rate = both_count / either_count * 100 if either_count > 0 else 0.0

print(f"\n  Total observations           : {total_obs:,}")
print(f"  Z-Score only                 : {only_z_count}")
print(f"  Isolation Forest only        : {only_if_count}")
print(f"  Flagged by BOTH methods      : {both_count}")
print(f"  Flagged by EITHER method     : {either_count}")
print(f"  Agreement rate (both/either) : {agreement_rate:.1f}%")

# Confusion-like grid
labels     = ["Z-Score: No", "Z-Score: Yes"]
ifor_lbls  = ["IForest: No", "IForest: Yes"]
confusion  = pd.crosstab(df_compare["flag_iforest"], df_compare["flag_zscore"],
                          rownames=["IForest"], colnames=["Z-Score"])

print("\n  Method Agreement Matrix:")
print(confusion.to_string())

# Plot: Venn-style bar
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("Anomaly Detection — Method Comparison", fontsize=13, fontweight="bold")

categories  = ["Z-Score Only", "Both Methods", "IForest Only"]
counts      = [only_z_count, both_count, only_if_count]
colors_bar  = ["#3b82f6", "#8b5cf6", "#10b981"]
axes[0].bar(categories, counts, color=colors_bar, alpha=0.85, edgecolor="white", width=0.5)
axes[0].set_ylabel("Count")
axes[0].set_title("Anomalies by Detection Method")
for i, v in enumerate(counts):
    axes[0].text(i, v + 0.5, str(v), ha="center", fontsize=10, fontweight="bold")

# Per fiscal year agreement
fy_agree = (
    df_compare.groupby("fiscal_year")
    .agg(zscore_flagged=("flag_zscore", "sum"),
         iforest_flagged=("flag_iforest", "sum"),
         both_flagged=("flag_both", "sum"))
    .reset_index()
)
x = range(len(fy_agree))
w = 0.28
axes[1].bar([i - w for i in x], fy_agree["zscore_flagged"],  width=w, label="Z-Score",  color="#3b82f6", alpha=0.8)
axes[1].bar([i     for i in x], fy_agree["iforest_flagged"], width=w, label="IForest",  color="#10b981", alpha=0.8)
axes[1].bar([i + w for i in x], fy_agree["both_flagged"],    width=w, label="Both",     color="#8b5cf6", alpha=0.8)
axes[1].set_xticks(list(x))
axes[1].set_xticklabels(fy_agree["fiscal_year"].astype(str), rotation=45)
axes[1].set_title("Anomaly Count by Fiscal Year")
axes[1].legend(fontsize=9)

plt.tight_layout()
savefig("04_method_comparison.png")


# ============================================================
# 5. CROSS-REFERENCE WITH KNOWN EVENTS
# ============================================================
print("\n" + "=" * 60)
print("SECTION 5 — Cross-Reference with Known Market Events")
print("=" * 60)

known_events = [
    {
        "name":    "Adani Controversy",
        "symbol":  "ADANIENT",
        "year":    2023,
        "description": "Hindenburg report triggered stock collapse and debt scrutiny",
        "expected_metric": "borrowings",
    },
    {
        "name":    "COVID-19 Impact",
        "symbol":  None,          # sector-wide
        "year":    2021,
        "description": "Revenue contraction across Consumer Discretionary, Hospitality, Auto",
        "expected_metric": "sales",
    },
    {
        "name":    "NBFC Liquidity Crisis",
        "symbol":  None,
        "year":    2019,
        "description": "IL&FS default triggering NBFC sector borrowing stress",
        "expected_metric": "borrowings",
    },
]

for event in known_events:
    print(f"\n  Event: {event['name']} (FY{event['year']})")
    print(f"  Context: {event['description']}")

    if event["symbol"]:
        rows = df_compare[
            (df_compare["symbol"] == event["symbol"]) &
            (df_compare["fiscal_year"] == event["year"])
        ]
        if rows.empty:
            print(f"    [{event['symbol']}] No data for FY{event['year']}")
        else:
            row = rows.iloc[0]
            z_flag  = row["flag_zscore"]
            if_flag = row["flag_iforest"]
            print(f"    [{event['symbol']}] Z-Score flag: {z_flag} | IForest flag: {if_flag}")
            met = event["expected_metric"]
            if met in row:
                print(f"    {met}: {row[met]:,.0f}")
    else:
        # Sector-wide
        year_data = df_compare[df_compare["fiscal_year"] == event["year"]]
        flagged   = year_data["flag_either"].sum()
        total_fy  = len(year_data)
        pct       = flagged / total_fy * 100 if total_fy else 0
        print(f"    FY{event['year']}: {flagged}/{total_fy} companies flagged ({pct:.1f}%)")
        met = event["expected_metric"]
        col_change = f"{met}_yoy"
        # Quick YoY for this year
        if met in df_compare.columns:
            year_vals  = df_compare[df_compare["fiscal_year"] == event["year"]][met].dropna()
            prev_vals  = df_compare[df_compare["fiscal_year"] == event["year"] - 1][met].dropna()
            if len(year_vals) and len(prev_vals):
                median_chg = (year_vals.median() - prev_vals.median()) / abs(prev_vals.median()) * 100
                print(f"    Median {met} YoY change: {median_chg:+.1f}%")


# ============================================================
# 6. SAVE TO fact_anomalies
# ============================================================
print("\n" + "=" * 60)
print("SECTION 6 — Save Anomalies to fact_anomalies")
print("=" * 60)

# Build records for all flagged observations
anomaly_records = []

for _, row in df_compare.iterrows():
    for metric in ANOMALY_METRICS:
        z_flag  = bool(row.get(f"{metric}_zscore_flag", False)) if metric in df_zscore.columns or True else False
        if_flag = bool(row["flag_iforest"])

        # Determine per-metric z-score flag
        z_flag_col = f"{metric}_zscore_flag"
        z_flag_val = bool(df_zscore.loc[row.name, z_flag_col]) if z_flag_col in df_zscore.columns else False
        z_score_val = df_zscore.loc[row.name, f"{metric}_zscore"] if f"{metric}_zscore" in df_zscore.columns else None

        if not (z_flag_val or if_flag):
            continue

        if z_flag_val and if_flag:
            method = "both"
        elif z_flag_val:
            method = "zscore"
        else:
            method = "iforest"

        anomaly_records.append({
            "symbol":      row["symbol"],
            "fiscal_year": int(row["fiscal_year"]),
            "metric":      metric,
            "value":       float(row[metric]) if pd.notna(row[metric]) else None,
            "z_score":     float(z_score_val) if z_score_val is not None and pd.notna(z_score_val) else None,
            "method":      method,
            "status":      "pending",
            "detected_at": datetime.utcnow(),
        })

df_anomalies = pd.DataFrame(anomaly_records)
print(f"  Prepared {len(df_anomalies)} anomaly records for insertion.")

if not df_anomalies.empty:
    # Lookup company IDs
    with engine.connect() as conn:
        company_ids = pd.read_sql(
            "SELECT id, symbol FROM dim_companies", conn
        ).set_index("symbol")["id"]

    df_anomalies["company_id"] = df_anomalies["symbol"].map(company_ids)
    df_anomalies = df_anomalies.dropna(subset=["company_id"])
    df_anomalies["company_id"] = df_anomalies["company_id"].astype(int)

    # Truncate existing unreviewed records before insert
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_anomalies WHERE status = 'pending'"))
        df_anomalies.drop(columns=["symbol"]).to_sql(
            "fact_anomalies",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )

    print(f"  {len(df_anomalies)} anomaly records saved to fact_anomalies.")
else:
    print("  No anomalies to save.")

print("\n" + "=" * 60)
print("ANOMALY DETECTION COMPLETE")
print("=" * 60 + "\n")
