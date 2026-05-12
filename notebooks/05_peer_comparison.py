"""
Nifty 100 Intelligence Platform
Notebook 05: Peer Comparison
==============================
Sections:
  1. DB connection
  2. Same feature vector as notebook 04
  3. cosine_similarity matrix
  4. Top 5 peers per company
  5. Validate: TCS peers include INFY/WIPRO
  6. Print peer table for 10 companies
  7. Save to fact_peers
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
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 120, "figure.facecolor": "white",
                     "axes.spines.top": False, "axes.spines.right": False})

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "peer_plots")
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
# 2. FEATURE VECTOR (same as notebook 04)
# ============================================================
print("=" * 60)
print("SECTION 2 — Feature Vector Construction")
print("=" * 60)

with engine.connect() as conn:
    df_fin = pd.read_sql(
        """
        SELECT
            c.id AS company_id,
            c.symbol, c.name AS company_name, c.sector,
            f.fiscal_year,
            f.sales,
            f.net_profit,
            f.operating_profit,
            f.opm_pct
        FROM fact_financials f
        JOIN dim_companies c ON c.id = f.company_id
        """,
        conn,
    )

    df_rat = pd.read_sql(
        """
        SELECT c.symbol, r.fiscal_year, r.debt_to_equity, r.interest_coverage
        FROM fact_ratios r
        JOIN dim_companies c ON c.id = r.company_id
        """,
        conn,
    )

    df_cf = pd.read_sql(
        """
        SELECT c.symbol, cf.fiscal_year, cf.cash_from_operating
        FROM fact_cashflows cf
        JOIN dim_companies c ON c.id = cf.company_id
        """,
        conn,
    )

# Merge and derive features
df = df_fin.merge(df_rat[["symbol","fiscal_year","debt_to_equity","interest_coverage"]],
                  on=["symbol","fiscal_year"], how="left")
df = df.merge(df_cf[["symbol","fiscal_year","cash_from_operating"]],
              on=["symbol","fiscal_year"], how="left")

df["npm_pct"]       = (df["net_profit"] / df["sales"].replace(0, np.nan) * 100).round(3)
df["fcf_to_sales"]  = (df["cash_from_operating"] / df["sales"].replace(0, np.nan)).round(4)
df                  = df.sort_values(["symbol", "fiscal_year"])
df["yoy_growth"]    = df.groupby("symbol")["sales"].pct_change() * 100

FEATURE_COLS = [
    "npm_pct", "opm_pct", "debt_to_equity",
    "interest_coverage", "fcf_to_sales", "yoy_growth",
]

df_feat = (
    df.groupby(["company_id", "symbol", "company_name", "sector"])[FEATURE_COLS]
    .mean()
    .reset_index()
)
df_feat_clean = df_feat.dropna(subset=FEATURE_COLS).copy().reset_index(drop=True)

print(f"  Feature matrix: {df_feat_clean.shape[0]} companies × {len(FEATURE_COLS)} features")
print(f"  Features: {FEATURE_COLS}\n")

# Normalise
scaler  = StandardScaler()
X_scaled = scaler.fit_transform(df_feat_clean[FEATURE_COLS].values)

symbols_clean    = df_feat_clean["symbol"].values
company_names    = df_feat_clean["company_name"].values
company_ids      = df_feat_clean["company_id"].values


# ============================================================
# 3. COSINE SIMILARITY MATRIX
# ============================================================
print("=" * 60)
print("SECTION 3 — Cosine Similarity Matrix")
print("=" * 60)

sim_matrix = cosine_similarity(X_scaled)   # shape: (n, n)
n_companies = len(symbols_clean)

print(f"  Similarity matrix shape: {sim_matrix.shape}")
print(f"  Diagonal (self-similarity): mean={np.diag(sim_matrix).mean():.4f}")

# Visualise similarity for a subset (top 30 by alphabetical)
subset_idx = np.argsort(symbols_clean)[:30]
sub_sim    = sim_matrix[np.ix_(subset_idx, subset_idx)]
sub_labels = symbols_clean[subset_idx]

fig, ax = plt.subplots(figsize=(13, 11))
sns.heatmap(
    sub_sim,
    ax=ax,
    cmap="YlOrRd",
    vmin=0, vmax=1,
    xticklabels=sub_labels,
    yticklabels=sub_labels,
    annot=False,
    linewidths=0.2,
    linecolor="#e2e8f0",
    cbar_kws={"label": "Cosine Similarity", "shrink": 0.7},
)
ax.set_title("Financial Profile Cosine Similarity (subset — first 30 companies A–Z)",
             fontsize=12, fontweight="bold")
plt.xticks(rotation=90, fontsize=7)
plt.yticks(rotation=0, fontsize=7)
plt.tight_layout()
savefig("03_cosine_similarity_heatmap.png")
print("  Similarity heatmap saved.\n")


# ============================================================
# 4. TOP 5 PEERS PER COMPANY
# ============================================================
print("=" * 60)
print("SECTION 4 — Top 5 Peers per Company")
print("=" * 60)

TOP_N = 5

peer_records = []
for i, symbol in enumerate(symbols_clean):
    sim_row = sim_matrix[i].copy()
    sim_row[i] = -1.0  # exclude self

    top_n_idx  = np.argsort(sim_row)[::-1][:TOP_N]
    for rank, j in enumerate(top_n_idx, start=1):
        peer_records.append({
            "company_id":      int(company_ids[i]),
            "symbol":          symbol,
            "company_name":    company_names[i],
            "sector":          df_feat_clean.loc[i, "sector"],
            "peer_company_id": int(company_ids[j]),
            "peer_symbol":     symbols_clean[j],
            "peer_name":       company_names[j],
            "peer_sector":     df_feat_clean.loc[j, "sector"],
            "similarity_score": float(round(sim_row[j], 6)),
            "rank":            rank,
        })

df_peers = pd.DataFrame(peer_records)
print(f"  Generated {len(df_peers)} peer relationships ({n_companies} companies × {TOP_N} peers).\n")


# ============================================================
# 5. VALIDATION — TCS PEERS INCLUDE INFY / WIPRO
# ============================================================
print("=" * 60)
print("SECTION 5 — Validation")
print("=" * 60)

validation_targets = {
    "TCS":     ["INFY", "WIPRO", "HCLTECH", "TECHM"],
    "HDFCBANK":["ICICIBANK", "AXISBANK", "KOTAKBANK"],
    "RELIANCE":["ONGC", "BPCL", "IOC"],
}

all_pass = True
for company, expected_peers in validation_targets.items():
    peers_found = df_peers[df_peers["symbol"] == company]["peer_symbol"].tolist()
    hits = [p for p in expected_peers if p in peers_found]
    status = "PASS" if hits else "FAIL"
    if not hits:
        all_pass = False
    print(f"  [{company}] {status}")
    print(f"    Computed peers : {peers_found}")
    print(f"    Expected in    : {expected_peers}")
    print(f"    Matched        : {hits}\n")

if all_pass:
    print("  All validations PASSED.")
else:
    print("  WARNING: Some expected peers not found — review feature vector or data coverage.")


# ============================================================
# 6. PRINT PEER TABLE FOR 10 COMPANIES
# ============================================================
print("\n" + "=" * 60)
print("SECTION 6 — Peer Table for 10 Companies")
print("=" * 60)

SAMPLE_COMPANIES = ["TCS", "HDFCBANK", "RELIANCE", "INFY", "ICICIBANK",
                    "HINDUNILVR", "ITC", "BAJFINANCE", "AXISBANK", "WIPRO"]

for sym in SAMPLE_COMPANIES:
    peers_df = df_peers[df_peers["symbol"] == sym][
        ["rank", "peer_symbol", "peer_name", "peer_sector", "similarity_score"]
    ]
    if peers_df.empty:
        print(f"\n  {sym}: No peers found (data coverage issue)")
        continue
    print(f"\n  {sym} — Top {TOP_N} Peers:")
    print(peers_df.to_string(index=False, float_format="{:.4f}".format))


# ============================================================
# 7. SAVE TO fact_peers
# ============================================================
print("\n" + "=" * 60)
print("SECTION 7 — Save to fact_peers")
print("=" * 60)

df_save = df_peers[[
    "company_id", "peer_company_id", "similarity_score", "rank"
]].copy()
df_save["algorithm"]   = "cosine"
df_save["computed_at"] = datetime.utcnow()

with engine.begin() as conn:
    conn.execute(text("DELETE FROM fact_peers WHERE algorithm = 'cosine'"))
    df_save.to_sql(
        "fact_peers",
        conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

print(f"  {len(df_save)} peer records saved to fact_peers.")

# Also export peer table to CSV for easy review
csv_path = os.path.join(OUTPUT_DIR, "peer_table_export.csv")
df_peers.to_csv(csv_path, index=False)
print(f"  Full peer table exported to: {csv_path}")

print("\n" + "=" * 60)
print("PEER COMPARISON COMPLETE")
print("=" * 60 + "\n")
