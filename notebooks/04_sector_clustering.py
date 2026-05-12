"""
Nifty 100 Intelligence Platform
Notebook 04: Sector Clustering
================================
Sections:
  1. DB connection
  2. Feature vector: avg net_profit_margin, opm_pct, debt_to_equity,
     interest_coverage, free_cash_flow/sales, yoy_growth
  3. StandardScaler normalisation
  4. K-Means elbow (k=2..10)
  5. K-Means with optimal k, assign labels
  6. PCA to 2D scatter plot
  7. DBSCAN comparison
  8. Name clusters descriptively
  9. Save to fact_clusters
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
import matplotlib.cm as cm
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sns.set_theme(style="whitegrid", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 120, "figure.facecolor": "white",
                     "axes.spines.top": False, "axes.spines.right": False})

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "cluster_plots")
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
# 2. BUILD FEATURE VECTOR
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
        ORDER BY c.symbol, f.fiscal_year
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

# Merge
df = df_fin.merge(df_rat[["symbol","fiscal_year","debt_to_equity","interest_coverage"]],
                  on=["symbol","fiscal_year"], how="left")
df = df.merge(df_cf[["symbol","fiscal_year","cash_from_operating"]],
              on=["symbol","fiscal_year"], how="left")

# Derived columns
df["npm_pct"] = (df["net_profit"] / df["sales"].replace(0, np.nan) * 100).round(3)
df["fcf_to_sales"] = (df["cash_from_operating"] / df["sales"].replace(0, np.nan)).round(4)

# YoY revenue growth
df = df.sort_values(["symbol", "fiscal_year"])
df["yoy_growth"] = df.groupby("symbol")["sales"].pct_change() * 100

# Aggregate per company (average over all years)
feature_cols = [
    "npm_pct", "opm_pct", "debt_to_equity",
    "interest_coverage", "fcf_to_sales", "yoy_growth",
]
df_feat = (
    df.groupby(["company_id", "symbol", "company_name", "sector"])[feature_cols]
    .mean()
    .reset_index()
)
df_feat_clean = df_feat.dropna(subset=feature_cols).copy().reset_index(drop=True)

print(f"  Feature matrix: {df_feat_clean.shape[0]} companies × {len(feature_cols)} features")
print(f"  Features: {feature_cols}")
print(f"  Dropped {len(df_feat) - len(df_feat_clean)} companies with incomplete data.\n")


# ============================================================
# 3. STANDARDSCALER NORMALISATION
# ============================================================
print("=" * 60)
print("SECTION 3 — StandardScaler Normalisation")
print("=" * 60)

X_raw = df_feat_clean[feature_cols].values
scaler = StandardScaler()
X = scaler.fit_transform(X_raw)

print(f"  Scaled X: shape={X.shape}, mean~{X.mean():.4f}, std~{X.std():.4f}\n")


# ============================================================
# 4. K-MEANS ELBOW (k=2..10)
# ============================================================
print("=" * 60)
print("SECTION 4 — K-Means Elbow Curve (k=2..10)")
print("=" * 60)

K_RANGE     = range(2, 11)
inertias    = []
silhouettes = []

for k in K_RANGE:
    km = KMeans(n_clusters=k, n_init=20, random_state=42, max_iter=500)
    km.fit(X)
    inertias.append(km.inertia_)
    sil = silhouette_score(X, km.labels_) if k > 1 else 0
    silhouettes.append(sil)
    print(f"  k={k:2d}: inertia={km.inertia_:>10.1f}, silhouette={sil:.4f}")

# Plot elbow
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("K-Means Elbow Analysis", fontsize=13, fontweight="bold")

axes[0].plot(list(K_RANGE), inertias, "o-", color="#3b82f6", linewidth=2, markersize=6)
axes[0].set_xlabel("Number of Clusters (k)")
axes[0].set_ylabel("Inertia (WCSS)")
axes[0].set_title("Elbow Curve")
axes[0].set_xticks(list(K_RANGE))

axes[1].plot(list(K_RANGE), silhouettes, "s-", color="#10b981", linewidth=2, markersize=6)
axes[1].set_xlabel("Number of Clusters (k)")
axes[1].set_ylabel("Silhouette Score")
axes[1].set_title("Silhouette Score")
axes[1].set_xticks(list(K_RANGE))
best_k_idx = silhouettes.index(max(silhouettes))
best_k     = list(K_RANGE)[best_k_idx]
axes[1].axvline(best_k, color="#ef4444", linestyle="--", linewidth=1.5,
                label=f"Best k={best_k}")
axes[1].legend(fontsize=9)

plt.tight_layout()
savefig("04_kmeans_elbow.png")

print(f"\n  Optimal k by silhouette: {best_k}  (score={silhouettes[best_k_idx]:.4f})")


# ============================================================
# 5. K-MEANS WITH OPTIMAL K
# ============================================================
print("\n" + "=" * 60)
print(f"SECTION 5 — K-Means with k={best_k}")
print("=" * 60)

km_final = KMeans(n_clusters=best_k, n_init=30, random_state=42, max_iter=1000)
df_feat_clean["cluster"] = km_final.fit_predict(X)

print(f"\n  Cluster distribution:")
cluster_counts = df_feat_clean["cluster"].value_counts().sort_index()
for c, cnt in cluster_counts.items():
    print(f"    Cluster {c}: {cnt} companies")

# Cluster profiles
cluster_profiles = df_feat_clean.groupby("cluster")[feature_cols].mean()
print(f"\n  Cluster feature means:")
print(cluster_profiles.to_string(float_format="{:.2f}".format))


# ============================================================
# 6. PCA 2D SCATTER PLOT
# ============================================================
print("\n" + "=" * 60)
print("SECTION 6 — PCA 2D Scatter Plot")
print("=" * 60)

pca = PCA(n_components=2, random_state=42)
X_2d = pca.fit_transform(X)

df_feat_clean["pca1"] = X_2d[:, 0]
df_feat_clean["pca2"] = X_2d[:, 1]

var_explained = pca.explained_variance_ratio_
print(f"  PCA variance explained: PC1={var_explained[0]:.1%}, PC2={var_explained[1]:.1%}")

# Plot
cmap = cm.get_cmap("tab10", best_k)
fig, ax = plt.subplots(figsize=(11, 8))

for cluster_id in range(best_k):
    mask = df_feat_clean["cluster"] == cluster_id
    ax.scatter(
        df_feat_clean.loc[mask, "pca1"],
        df_feat_clean.loc[mask, "pca2"],
        color=cmap(cluster_id),
        alpha=0.75,
        s=70,
        label=f"Cluster {cluster_id}",
        edgecolors="white",
        linewidth=0.5,
    )
    # Label a few companies
    sample = df_feat_clean[mask].nlargest(3, "opm_pct")
    for _, row in sample.iterrows():
        ax.annotate(row["symbol"], (row["pca1"], row["pca2"]),
                    fontsize=7, alpha=0.8,
                    xytext=(4, 4), textcoords="offset points")

ax.set_xlabel(f"PC1 ({var_explained[0]:.1%} variance)", fontsize=11)
ax.set_ylabel(f"PC2 ({var_explained[1]:.1%} variance)", fontsize=11)
ax.set_title(f"K-Means Clusters (k={best_k}) — PCA Projection", fontsize=13, fontweight="bold")
ax.legend(loc="upper right", fontsize=9)
plt.tight_layout()
savefig("06_pca_scatter.png")


# ============================================================
# 7. DBSCAN COMPARISON
# ============================================================
print("\n" + "=" * 60)
print("SECTION 7 — DBSCAN Comparison")
print("=" * 60)

dbscan = DBSCAN(eps=1.2, min_samples=4, n_jobs=-1)
db_labels = dbscan.fit_predict(X)
n_db_clusters = len(set(db_labels)) - (1 if -1 in db_labels else 0)
n_noise       = (db_labels == -1).sum()

print(f"  DBSCAN: {n_db_clusters} clusters found, {n_noise} noise points")

fig, ax = plt.subplots(figsize=(10, 7))
unique_labels = sorted(set(db_labels))
colors_db = cm.get_cmap("tab10", max(len(unique_labels), 1))
for label in unique_labels:
    mask   = db_labels == label
    color  = "lightgrey" if label == -1 else colors_db(label)
    marker = "x" if label == -1 else "o"
    lname  = "Noise" if label == -1 else f"Cluster {label}"
    ax.scatter(X_2d[mask, 0], X_2d[mask, 1],
               c=[color], alpha=0.7, s=60 if label != -1 else 30,
               marker=marker, label=lname, edgecolors="white", linewidth=0.3)

ax.set_xlabel(f"PC1 ({var_explained[0]:.1%} variance)")
ax.set_ylabel(f"PC2 ({var_explained[1]:.1%} variance)")
ax.set_title(f"DBSCAN Clusters (eps=1.2, min_samples=4) — PCA Projection",
             fontsize=12, fontweight="bold")
ax.legend(fontsize=8, loc="upper right")
plt.tight_layout()
savefig("07_dbscan.png")


# ============================================================
# 8. NAME CLUSTERS DESCRIPTIVELY
# ============================================================
print("\n" + "=" * 60)
print("SECTION 8 — Descriptive Cluster Names")
print("=" * 60)

# Assign names based on cluster feature profiles
cluster_name_map = {}
for cluster_id, row in cluster_profiles.iterrows():
    npm    = row.get("npm_pct",         0)
    opm    = row.get("opm_pct",         0)
    de     = row.get("debt_to_equity",  0)
    icov   = row.get("interest_coverage", 0)
    fcf    = row.get("fcf_to_sales",    0)
    growth = row.get("yoy_growth",      0)

    if opm > 20 and npm > 15 and de < 1:
        name = "High-Quality Growth"
    elif de > 3 or icov < 2:
        name = "High Leverage / Stress"
    elif growth > 15 and npm > 5:
        name = "High Growth Emerging"
    elif opm < 8 and npm < 3:
        name = "Low-Margin Commodity"
    elif fcf > 0.1 and de < 1.5:
        name = "Cash-Rich Steady"
    else:
        name = f"Balanced Mid-Tier (C{cluster_id})"

    cluster_name_map[cluster_id] = name

df_feat_clean["cluster_name"] = df_feat_clean["cluster"].map(cluster_name_map)

print("\n  Cluster assignments:")
for cid, name in cluster_name_map.items():
    members = df_feat_clean[df_feat_clean["cluster"] == cid]["symbol"].tolist()
    print(f"    Cluster {cid}: '{name}' — {len(members)} companies")
    print(f"      Members (sample): {members[:8]}")


# ============================================================
# 9. SAVE TO fact_clusters
# ============================================================
print("\n" + "=" * 60)
print("SECTION 9 — Save to fact_clusters")
print("=" * 60)

df_save = df_feat_clean[["company_id", "cluster", "cluster_name", "pca1", "pca2"]].copy()
df_save["algorithm"]   = "kmeans"
df_save["k"]           = best_k
df_save["computed_at"] = datetime.utcnow()

with engine.begin() as conn:
    conn.execute(text("DELETE FROM fact_clusters WHERE algorithm = 'kmeans'"))
    df_save.to_sql(
        "fact_clusters",
        conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=200,
    )

print(f"  {len(df_save)} cluster records saved to fact_clusters.")

print("\n" + "=" * 60)
print("SECTOR CLUSTERING COMPLETE")
print("=" * 60 + "\n")
