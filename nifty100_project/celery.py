"""
Celery application for nifty100_project.

Beat schedule (IST):
  01:00  run_etl_pipeline          – pull latest data, transform, load
  02:00  run_health_scoring        – recompute ML health scores
  02:30  run_anomaly_detection     – z-score anomaly sweep

Start worker:
  celery -A nifty100_project worker -l info

Start beat (scheduler):
  celery -A nifty100_project beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler
"""

import os

from celery import Celery
from celery.schedules import crontab
from django.conf import settings  # noqa – imported after Django setup

# Tell Celery which Django settings module to use
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nifty100_project.settings")

app = Celery("nifty100_project")

# Read all CELERY_ settings from Django settings
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()


# ── Beat schedule ─────────────────────────────────────────────────────────────
app.conf.beat_schedule = {
    # ETL pipeline: fetch & load fresh financial data (runs at 1 AM IST)
    "run_etl_pipeline_daily": {
        "task": "companies.tasks.run_etl_pipeline",
        "schedule": crontab(hour=1, minute=0),
        "options": {"expires": 3600},
    },
    # Health scoring: recompute ML scores after ETL finishes (runs at 2 AM IST)
    "run_health_scoring_daily": {
        "task": "companies.tasks.run_health_scoring",
        "schedule": crontab(hour=2, minute=0),
        "options": {"expires": 3600},
    },
    # Anomaly detection: z-score sweep across metrics (runs at 2:30 AM IST)
    "run_anomaly_detection_daily": {
        "task": "companies.tasks.run_anomaly_detection",
        "schedule": crontab(hour=2, minute=30),
        "options": {"expires": 3600},
    },
}

app.conf.timezone = "Asia/Kolkata"


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Utility task to verify Celery worker connectivity."""
    print(f"Request: {self.request!r}")
