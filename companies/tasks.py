"""
companies/tasks.py – Celery tasks for background data processing.

Tasks
-----
run_etl_pipeline()       – stub task that triggers the ETL scripts
run_health_scoring()     – recompute ML health scores for all companies;
                           invalidate cache for companies whose score changed by > 2
run_anomaly_detection()  – z-score sweep across fact_profit_loss metrics;
                           saves new anomaly rows to fact_anomalies
"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from celery import shared_task
from django.core.cache import cache
from django.db import transaction

logger = logging.getLogger(__name__)

# Metrics to run anomaly detection on
ANOMALY_METRICS = [
    "sales",
    "net_profit",
    "opm_percentage",
    "net_profit_margin_pct",
    "interest_coverage",
    "return_on_assets_pct",
    "eps",
    "dividend_payout",
]

# Z-score thresholds mapped to severity labels
Z_THRESHOLDS = [
    (4.0, "CRITICAL"),
    (3.0, "HIGH"),
    (2.5, "MEDIUM"),
    (2.0, "LOW"),
]


def _get_sqlalchemy_engine():
    """Create a SQLAlchemy engine using Django's DB settings."""
    from sqlalchemy import create_engine
    from django.conf import settings

    db = settings.DATABASES["default"]
    url = (
        f"postgresql://{db['USER']}:{db['PASSWORD']}"
        f"@{db['HOST']}:{db['PORT']}/{db['NAME']}"
    )
    return create_engine(url)


def _severity_for_z(z_abs: float) -> str:
    """Return severity label for a given absolute z-score."""
    for threshold, label in Z_THRESHOLDS:
        if z_abs >= threshold:
            return label
    return "LOW"


# ── Tasks ─────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    name="companies.tasks.run_etl_pipeline",
    max_retries=2,
    default_retry_delay=300,
)
def run_etl_pipeline(self):
    """
    Trigger the ETL pipeline scripts.

    In production the ETL scripts run as standalone Python scripts using
    SQLAlchemy; this task invokes them programmatically so they can be
    scheduled and monitored via Celery.
    """
    logger.info("[ETL] Starting ETL pipeline task.")
    try:
        import subprocess
        import sys

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        etl_dir      = os.path.join(project_root, "etl")
        python_exe   = sys.executable

        scripts = [
            os.path.join(etl_dir, "02_clean_and_transform.py"),
            os.path.join(etl_dir, "03_load_to_warehouse.py"),
        ]

        for script in scripts:
            if not os.path.exists(script):
                logger.warning("[ETL] Script not found, skipping: %s", script)
                continue

            logger.info("[ETL] Running: %s", script)
            result = subprocess.run(
                [python_exe, script],
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minutes max
            )
            if result.returncode != 0:
                logger.error("[ETL] Script failed: %s\nSTDERR: %s", script, result.stderr)
                raise RuntimeError(f"ETL script failed: {script}")
            logger.info("[ETL] Completed: %s\nSTDOUT: %s", script, result.stdout[-500:])

        logger.info("[ETL] Pipeline completed successfully.")
        return {"status": "success"}

    except Exception as exc:
        logger.exception("[ETL] Pipeline task failed: %s", exc)
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    name="companies.tasks.run_health_scoring",
    max_retries=2,
    default_retry_delay=180,
)
def run_health_scoring(self):
    """
    Recompute ML health scores for all companies.

    Steps
    -----
    1. Import ml.health_scorer and compute new scores via compute_scores().
    2. Load the previous latest scores from fact_ml_scores.
    3. Save new scores via save_scores().
    4. For any company whose overall_score changed by more than 2 points,
       invalidate the charts and peers cache keys.

    Returns
    -------
    dict with keys: scored_count, cache_invalidated_count, status
    """
    logger.info("[HealthScoring] Starting health scoring task.")
    try:
        import sys
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

        from ml.health_scorer import compute_scores, save_scores

        engine = _get_sqlalchemy_engine()

        # ── Fetch previous scores for comparison ──────────────────────────────
        from companies.models import MLScore
        from django.db.models import OuterRef, Subquery

        prev_sq = (
            MLScore.objects
            .filter(symbol=OuterRef("symbol"))
            .order_by("-computed_at")
            .values("overall_score")[:1]
        )
        prev_scores = {
            row["symbol_id"]: float(row["prev_score"])
            for row in MLScore.objects.annotate(prev_score=Subquery(prev_sq))
            .values("symbol_id", "prev_score")
            .distinct()
        }

        # ── Compute and save new scores ───────────────────────────────────────
        logger.info("[HealthScoring] Computing scores...")
        new_scores_df = compute_scores(engine)
        save_scores(new_scores_df, engine)
        scored_count = len(new_scores_df)
        logger.info("[HealthScoring] Saved %d scores.", scored_count)

        # ── Cache invalidation for companies with significant score change ─────
        invalidated = 0
        for _, row in new_scores_df.iterrows():
            sym       = row["symbol"]
            new_score = float(row["overall_score"])
            old_score = prev_scores.get(sym)

            if old_score is None or abs(new_score - old_score) > 2.0:
                cache.delete(f"charts:{sym}")
                cache.delete(f"peers:{sym}")
                invalidated += 1
                logger.debug(
                    "[HealthScoring] Cache invalidated for %s "
                    "(old=%.1f, new=%.1f)",
                    sym, old_score or 0, new_score,
                )

        logger.info(
            "[HealthScoring] Done. Scored=%d, CacheInvalidated=%d",
            scored_count, invalidated,
        )
        return {
            "status":                 "success",
            "scored_count":           scored_count,
            "cache_invalidated_count": invalidated,
        }

    except Exception as exc:
        logger.exception("[HealthScoring] Task failed: %s", exc)
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    name="companies.tasks.run_anomaly_detection",
    max_retries=2,
    default_retry_delay=180,
)
def run_anomaly_detection(self):
    """
    Z-score anomaly detection across fact_profit_loss metrics.

    Algorithm
    ---------
    For each metric in ANOMALY_METRICS:
      1. Load all non-TTM values for that metric across all companies and years.
      2. For each company, compute the z-score of each year's value relative
         to that company's own historical mean and std-dev (min 3 data points).
      3. If |z| >= 2.0, write a row to fact_anomalies (or update if already exists
         for that symbol+year_id+metric).
      4. Assign severity: CRITICAL ≥4, HIGH ≥3, MEDIUM ≥2.5, LOW ≥2.

    Returns
    -------
    dict with keys: anomalies_saved, anomalies_skipped, status
    """
    logger.info("[AnomalyDetection] Starting anomaly detection task.")

    try:
        from companies.models import ProfitLoss, Anomaly, Year

        # Load all non-TTM P&L rows as a DataFrame
        pl_qs = (
            ProfitLoss.objects
            .filter(year__is_ttm=False, year__fiscal_year__isnull=False)
            .select_related("year")
            .values(
                "id",
                "symbol_id",
                "year_id",
                "year__fiscal_year",
                *ANOMALY_METRICS,
            )
        )
        pl_df = pd.DataFrame.from_records(pl_qs)

        if pl_df.empty:
            logger.warning("[AnomalyDetection] No P&L data found; skipping.")
            return {"status": "skipped", "reason": "no_data"}

        anomalies_saved    = 0
        anomalies_skipped  = 0

        with transaction.atomic():
            for metric in ANOMALY_METRICS:
                if metric not in pl_df.columns:
                    logger.warning("[AnomalyDetection] Metric %s not in DataFrame.", metric)
                    continue

                metric_df = pl_df[["symbol_id", "year_id", metric]].dropna(subset=[metric])
                metric_df = metric_df.copy()
                metric_df[metric] = pd.to_numeric(metric_df[metric], errors="coerce")
                metric_df = metric_df.dropna(subset=[metric])

                # Group by company and compute z-scores
                for symbol, group in metric_df.groupby("symbol_id"):
                    if len(group) < 3:
                        # Not enough history for meaningful z-scores
                        anomalies_skipped += len(group)
                        continue

                    values = group[metric].values.astype(float)
                    mean   = values.mean()
                    std    = values.std(ddof=1)

                    if std == 0 or np.isnan(std):
                        anomalies_skipped += len(group)
                        continue

                    z_scores = (values - mean) / std

                    for idx, (_, row) in enumerate(group.iterrows()):
                        z = float(z_scores[idx])
                        z_abs = abs(z)

                        if z_abs < 2.0:
                            continue  # Not anomalous

                        severity = _severity_for_z(z_abs)
                        year_id  = int(row["year_id"])
                        value    = float(row[metric])

                        # Upsert the anomaly row
                        obj, created = Anomaly.objects.update_or_create(
                            symbol_id=symbol,
                            year_id=year_id,
                            metric=metric,
                            defaults={
                                "value":    value,
                                "z_score":  round(z, 4),
                                "method":   "zscore",
                                "severity": severity,
                                "reviewed": False,
                            },
                        )
                        anomalies_saved += 1
                        logger.debug(
                            "[AnomalyDetection] %s anomaly: %s %s "
                            "z=%.2f severity=%s (created=%s)",
                            symbol, metric, year_id, z, severity, created,
                        )

        logger.info(
            "[AnomalyDetection] Done. saved=%d skipped=%d",
            anomalies_saved, anomalies_skipped,
        )
        return {
            "status":           "success",
            "anomalies_saved":  anomalies_saved,
            "anomalies_skipped": anomalies_skipped,
        }

    except Exception as exc:
        logger.exception("[AnomalyDetection] Task failed: %s", exc)
        raise self.retry(exc=exc)
