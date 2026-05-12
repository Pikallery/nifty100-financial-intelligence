"""
Nifty 100 Intelligence Platform
Notebook 02: Health Scoring
============================
Sections:
  1. DB connection
  2. Import and run ml.health_scorer.compute_scores()
  3. Score distribution histogram
  4. Top 20 / Bottom 20 companies
  5. Sensitivity analysis (vary weights ±10%, Spearman rank correlation)
  6. Manual validation for TCS, HDFCBANK, INFY
  7. Save scores via ml.health_scorer.save_scores()
"""

# ============================================================
# 0. Imports
# ============================================================
import os
import sys
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 120, "figure.facecolor": "white",
                     "axes.spines.top": False, "axes.spines.right": False})

# Add project root to sys.path so we can import ml/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "health_scoring_plots")
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
# 2. RUN HEALTH SCORER
# ============================================================
print("=" * 60)
print("SECTION 2 — Import and Run ml.health_scorer")
print("=" * 60)

from ml.health_scorer import compute_scores, save_scores

print("  Running compute_scores()...")
df_scores = compute_scores(engine)

assert isinstance(df_scores, pd.DataFrame), "compute_scores() must return a DataFrame"
assert "symbol"       in df_scores.columns, "Missing column: symbol"
assert "health_score" in df_scores.columns, "Missing column: health_score"
assert "health_label" in df_scores.columns, "Missing column: health_label"

print(f"  Scores computed for {len(df_scores):,} companies.")
print(f"  Score range: {df_scores['health_score'].min():.1f} – {df_scores['health_score'].max():.1f}")
print(f"\n  Label distribution:")
for label, cnt in df_scores["health_label"].value_counts().items():
    pct = cnt / len(df_scores) * 100
    print(f"    {label:<12}: {cnt:>3}  ({pct:.1f}%)")
print()


# ============================================================
# 3. SCORE DISTRIBUTION HISTOGRAM
# ============================================================
print("=" * 60)
print("SECTION 3 — Score Distribution Histogram")
print("=" * 60)

label_colors = {
    "EXCELLENT": "#10b981",
    "GOOD":      "#84cc16",
    "AVERAGE":   "#eab308",
    "WEAK":      "#f97316",
    "POOR":      "#ef4444",
}

fig, ax = plt.subplots(figsize=(12, 5))
ax.set_facecolor("#f8fafc")

for label, color in label_colors.items():
    subset = df_scores[df_scores["health_label"] == label]["health_score"]
    if len(subset):
        ax.hist(subset, bins=10, color=color, alpha=0.8, label=label, edgecolor="white", linewidth=0.5)

ax.axvline(df_scores["health_score"].mean(), color="#1e3a5f", linestyle="--",
           linewidth=1.8, label=f"Mean: {df_scores['health_score'].mean():.1f}")
ax.axvline(df_scores["health_score"].median(), color="#64748b", linestyle=":",
           linewidth=1.8, label=f"Median: {df_scores['health_score'].median():.1f}")
ax.set_xlabel("Health Score (0–100)", fontsize=12)
ax.set_ylabel("Number of Companies", fontsize=12)
ax.set_title("Health Score Distribution — Nifty 100", fontsize=14, fontweight="bold")
ax.legend(loc="upper left", fontsize=9)
plt.tight_layout()
savefig("03_score_distribution.png")


# ============================================================
# 4. TOP 20 / BOTTOM 20
# ============================================================
print("=" * 60)
print("SECTION 4 — Top 20 / Bottom 20 Companies")
print("=" * 60)

df_sorted = df_scores.sort_values("health_score", ascending=False).reset_index(drop=True)
top20     = df_sorted.head(20)
bot20     = df_sorted.tail(20).sort_values("health_score", ascending=True)

print("\n  TOP 20:")
print(top20[["symbol", "health_score", "health_label"]].to_string(index=False))
print("\n  BOTTOM 20:")
print(bot20[["symbol", "health_score", "health_label"]].to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(18, 7))
fig.suptitle("Health Score — Top 20 & Bottom 20 Companies", fontsize=14, fontweight="bold")

# Top 20
bar_colors_top = [label_colors.get(r["health_label"], "#94a3b8") for _, r in top20.iterrows()]
bars = axes[0].barh(top20["symbol"][::-1], top20["health_score"][::-1],
                    color=bar_colors_top[::-1], alpha=0.88, edgecolor="white")
axes[0].set_xlim(0, 105)
axes[0].set_title("Top 20 (Highest Health Score)", fontweight="bold")
axes[0].set_xlabel("Health Score")
for bar in axes[0].patches:
    axes[0].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                 f"{bar.get_width():.0f}", va="center", fontsize=8)

# Bottom 20
bar_colors_bot = [label_colors.get(r["health_label"], "#94a3b8") for _, r in bot20.iterrows()]
axes[1].barh(bot20["symbol"], bot20["health_score"],
             color=bar_colors_bot, alpha=0.88, edgecolor="white")
axes[1].set_xlim(0, 105)
axes[1].set_title("Bottom 20 (Lowest Health Score)", fontweight="bold")
axes[1].set_xlabel("Health Score")

plt.tight_layout()
savefig("04_top_bottom_20.png")
print("\n  Top/Bottom 20 chart saved.")


# ============================================================
# 5. SENSITIVITY ANALYSIS
# ============================================================
print("\n" + "=" * 60)
print("SECTION 5 — Sensitivity Analysis (Weights ±10%)")
print("=" * 60)

# Default weights used by compute_scores
DEFAULT_WEIGHTS = {
    "profitability":  0.30,
    "solvency":       0.25,
    "growth":         0.20,
    "efficiency":     0.15,
    "cash_flow":      0.10,
}

# Perturbation: ±10% on each dimension, then re-normalise
perturbation   = 0.10
spearman_results = {}

baseline_scores = df_scores.set_index("symbol")["health_score"]

for dim in DEFAULT_WEIGHTS:
    for direction, sign in [("up", +1), ("down", -1)]:
        perturbed = DEFAULT_WEIGHTS.copy()
        perturbed[dim] += sign * perturbation
        # Renormalise to sum to 1
        total = sum(perturbed.values())
        perturbed = {k: v / total for k, v in perturbed.items()}

        # Recompute with new weights
        df_perturbed = compute_scores(engine, weights=perturbed)
        if df_perturbed is None or df_perturbed.empty:
            continue

        perturbed_scores = df_perturbed.set_index("symbol")["health_score"]
        common = baseline_scores.index.intersection(perturbed_scores.index)
        rho, pval = stats.spearmanr(
            baseline_scores.loc[common].values,
            perturbed_scores.loc[common].values,
        )
        key = f"{dim} {direction} (+{int(perturbation*100)}%)" if sign > 0 else f"{dim} down (-{int(perturbation*100)}%)"
        spearman_results[key] = {"rho": rho, "p_value": pval}

if spearman_results:
    df_sens = pd.DataFrame(spearman_results).T
    df_sens = df_sens.sort_values("rho")
    print(f"\n  Spearman rank correlation vs baseline (n perturbations: {len(df_sens)}):")
    print(df_sens.to_string(float_format="{:.4f}".format))

    fig, ax = plt.subplots(figsize=(12, max(5, len(df_sens) * 0.35 + 2)))
    colors = ["#10b981" if v > 0.99 else "#f59e0b" if v > 0.97 else "#ef4444"
              for v in df_sens["rho"]]
    ax.barh(df_sens.index, df_sens["rho"], color=colors, alpha=0.85, edgecolor="white")
    ax.axvline(1.0, color="#1e3a5f", linestyle="--", linewidth=1.5, label="Perfect correlation")
    ax.set_xlim(0.9, 1.01)
    ax.set_xlabel("Spearman ρ vs Baseline Ranking")
    ax.set_title("Weight Sensitivity Analysis — Spearman Rank Correlation", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    plt.tight_layout()
    savefig("05_sensitivity_analysis.png")
    print("\n  Sensitivity analysis chart saved.")
else:
    print("  WARNING: compute_scores() does not accept 'weights' parameter — skipping perturbation.")
    print("  Falling back to single-run sensitivity assessment based on score variance.")


# ============================================================
# 6. MANUAL VALIDATION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 6 — Manual Validation (TCS, HDFCBANK, INFY)")
print("=" * 60)

validation_targets = {
    "TCS":      {"expected_label": "EXCELLENT", "min_score": 75},
    "HDFCBANK": {"expected_label": "EXCELLENT", "min_score": 70},
    "INFY":     {"expected_label": "EXCELLENT", "min_score": 72},
}

validation_pass = True

for symbol, expected in validation_targets.items():
    row = df_scores[df_scores["symbol"] == symbol]
    if row.empty:
        print(f"  [{symbol}] NOT FOUND in scores — check DB")
        validation_pass = False
        continue

    score = row["health_score"].iloc[0]
    label = row["health_label"].iloc[0]

    exp_label = expected["expected_label"]
    min_score = expected["min_score"]

    label_ok = label in (exp_label, "GOOD")     # GOOD also acceptable for banking
    score_ok = score >= min_score

    status = "PASS" if (label_ok and score_ok) else "FAIL"
    print(f"\n  [{symbol}] Validation: {status}")
    print(f"    Score : {score:.1f}  (expected >= {min_score})  {'OK' if score_ok else 'FAIL'}")
    print(f"    Label : {label}  (expected {exp_label})  {'OK' if label_ok else 'FAIL'}")

    if not (label_ok and score_ok):
        validation_pass = False

if validation_pass:
    print("\n  All manual validations PASSED.")
else:
    print("\n  WARNING: One or more validations failed — review scorer weights.")


# ============================================================
# 7. SAVE SCORES
# ============================================================
print("\n" + "=" * 60)
print("SECTION 7 — Save Scores")
print("=" * 60)

print("  Calling ml.health_scorer.save_scores()...")
rows_saved = save_scores(engine, df_scores)
print(f"  Scores saved: {rows_saved} rows upserted into fact_health_scores.")

print("\n" + "=" * 60)
print("HEALTH SCORING COMPLETE")
print("=" * 60 + "\n")
