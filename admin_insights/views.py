"""
admin_insights/views.py — staff-only platform administration dashboard views.

All views require the user to be both authenticated and is_staff=True.
They render server-side HTML templates and provide aggregate metrics
for platform operators.

Views (9 total)
---------------
ExecutiveSummaryView  — KPI cards + sector distribution
HealthMonitorView     — all companies colour-coded by ML health label
AnomaliesView         — anomaly flag list with mark-reviewed action
DataQualityView       — company × fiscal-year P&L coverage matrix
APIManagementView     — channel partners with key count + usage totals
APIAnalyticsView      — daily call volume, top endpoints, P50/P95 latency
WebhooksView          — webhook subscriptions with delivery success rate
BulkImportView        — CSV upload → validate → commit
CeleryMonitorView     — last run status of scheduled tasks
"""

import csv
import io
import logging
import uuid as _uuid
from datetime import timedelta

from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator
from django.db.models import Avg, Count, FloatField, OuterRef, Subquery
from django.db.models.functions import TruncDate
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View

from api.models import APIKey, ChannelPartner, WebhookEvent, WebhookSubscription
from companies.models import (
    Anomaly,
    APIUsageLog,
    Company,
    MLScore,
    ProfitLoss,
    Sector,
    Year,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Access-control mixin
# ---------------------------------------------------------------------------


class StaffRequiredMixin(LoginRequiredMixin, UserPassesTestMixin):
    """Allow only authenticated staff users; return 403 for non-staff."""

    login_url = "/accounts/login/"
    raise_exception = True

    def test_func(self):
        return self.request.user.is_staff

    def handle_no_permission(self):
        if not self.request.user.is_authenticated:
            return redirect(self.login_url)
        return HttpResponseForbidden("Staff access required.")


# ---------------------------------------------------------------------------
# Shared helper: subquery for latest MLScore id per company symbol
# ---------------------------------------------------------------------------


def _latest_score_subquery():
    return (
        MLScore.objects.filter(symbol=OuterRef("symbol"))
        .order_by("-computed_at")
        .values("id")[:1]
    )


# ---------------------------------------------------------------------------
# 1. Executive summary
# ---------------------------------------------------------------------------


class ExecutiveSummaryView(StaffRequiredMixin, View):
    """
    GET /admin-insights/

    KPI cards:
      - total active companies
      - average health score across all companies
      - company count by health label (Healthy / Watch / Critical)
      - count of unreviewed anomalies
      - API calls in last 24 h
      - last ML scoring run timestamp

    Sector distribution: company count per sector.
    """

    template_name = "admin_insights/executive_summary.html"

    def get(self, request):
        total_companies = Company.objects.count()

        latest_scores_qs = MLScore.objects.filter(
            id__in=Subquery(_latest_score_subquery())
        )
        scored_companies = latest_scores_qs.count()

        avg_overall_score = latest_scores_qs.aggregate(avg=Avg("overall_score"))["avg"]
        if avg_overall_score is not None:
            avg_overall_score = round(float(avg_overall_score), 1)

        # Count by health label
        label_counts = list(
            latest_scores_qs
            .values("health_label")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
        for row in label_counts:
            row["pct"] = (
                round(row["count"] / scored_companies * 100, 1)
                if scored_companies
                else 0
            )

        unreviewed_anomalies = Anomaly.objects.filter(reviewed=False).count()

        # Sector distribution
        sector_dist = list(
            Company.objects.values("sector__sector_name")
            .annotate(count=Count("symbol"))
            .order_by("-count")
        )

        since_24h = timezone.now() - timedelta(hours=24)
        recent_api_calls_24h = APIUsageLog.objects.filter(
            requested_at__gte=since_24h
        ).count()

        last_ml = MLScore.objects.order_by("-computed_at").first()
        last_scoring_run = last_ml.computed_at if last_ml else None

        context = {
            "total_companies": total_companies,
            "scored_companies": scored_companies,
            "avg_overall_score": avg_overall_score,
            "label_distribution": label_counts,
            "unreviewed_anomalies": unreviewed_anomalies,
            "sector_distribution": sector_dist,
            "recent_api_calls_24h": recent_api_calls_24h,
            "last_scoring_run": last_scoring_run,
        }
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 2. Health monitor
# ---------------------------------------------------------------------------


class HealthMonitorView(StaffRequiredMixin, View):
    """
    GET /admin-insights/health/

    Table of all companies with their latest ML score and health label,
    colour-coded by label.  Sorted by overall_score ascending (worst first).
    """

    template_name = "admin_insights/health_monitor.html"

    LABEL_CSS = {
        "healthy": "success",
        "watch": "warning",
        "critical": "danger",
    }

    def get(self, request):
        scores = (
            MLScore.objects.filter(id__in=Subquery(_latest_score_subquery()))
            .select_related("symbol", "symbol__sector")
            .order_by("overall_score")
        )

        rows = []
        for score in scores:
            rows.append(
                {
                    "symbol": score.symbol_id,
                    "company_name": score.symbol.company_name or "",
                    "sector": (
                        score.symbol.sector.sector_name
                        if score.symbol.sector
                        else "—"
                    ),
                    "overall_score": score.overall_score,
                    "health_label": score.health_label,
                    "css_class": self.LABEL_CSS.get(
                        (score.health_label or "").lower(), "secondary"
                    ),
                    "computed_at": score.computed_at,
                    "profitability": score.profitability_score,
                    "growth": score.growth_score,
                    "leverage": score.leverage_score,
                    "cashflow": score.cashflow_score,
                }
            )

        context = {"rows": rows, "total": len(rows)}
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 3. Anomalies
# ---------------------------------------------------------------------------


class AnomaliesView(StaffRequiredMixin, View):
    """
    GET  /admin-insights/anomalies/  — paginated anomaly flag list
    POST /admin-insights/anomalies/  — mark one anomaly as reviewed

    Query params:
      reviewed=1      — show reviewed anomalies instead of open ones
      severity=HIGH   — filter by severity level
      page=N          — pagination
    """

    template_name = "admin_insights/anomalies.html"
    PAGE_SIZE = 30

    def get(self, request):
        show_reviewed = request.GET.get("reviewed", "0") == "1"
        severity_filter = request.GET.get("severity", "").strip().upper()

        qs = (
            Anomaly.objects.select_related("symbol", "year")
            .order_by("-flagged_at", "-z_score")
        )
        if not show_reviewed:
            qs = qs.filter(reviewed=False)
        if severity_filter in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            qs = qs.filter(severity=severity_filter)

        paginator = Paginator(qs, self.PAGE_SIZE)
        page_param = request.GET.get("page", 1)
        try:
            anomalies = paginator.page(page_param)
        except (PageNotAnInteger, EmptyPage):
            anomalies = paginator.page(1)

        context = {
            "anomalies": anomalies,
            "show_reviewed": show_reviewed,
            "severity_filter": severity_filter,
            "severities": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            "open_count": Anomaly.objects.filter(reviewed=False).count(),
            "reviewed_count": Anomaly.objects.filter(reviewed=True).count(),
        }
        return render(request, self.template_name, context)

    def post(self, request):
        """Mark a single anomaly as reviewed (form field: anomaly_id)."""
        anomaly_id = request.POST.get("anomaly_id")
        notes = request.POST.get("notes", "").strip()

        try:
            anomaly = Anomaly.objects.get(pk=int(anomaly_id))
        except (Anomaly.DoesNotExist, TypeError, ValueError):
            return redirect("admin_insights:anomalies")

        anomaly.reviewed = True
        if notes:
            anomaly.notes = notes
        anomaly.save(update_fields=["reviewed", "notes"])

        logger.info(
            "[AdminInsights] Anomaly %d marked reviewed by %s.",
            anomaly.pk,
            request.user.username,
        )
        return redirect("admin_insights:anomalies")


# ---------------------------------------------------------------------------
# 4. Data quality
# ---------------------------------------------------------------------------


class DataQualityView(StaffRequiredMixin, View):
    """
    GET /admin-insights/data-quality/

    Company × fiscal-year coverage matrix showing which fact_profit_loss rows
    exist.  Cells are green (present) or red (missing).
    """

    template_name = "admin_insights/data_quality.html"

    def get(self, request):
        # All non-TTM years in chronological order.
        years = list(
            Year.objects.filter(is_ttm=False)
            .order_by("sort_order")
            .values_list("year_id", "year_label", "fiscal_year")
        )

        year_ids = [y[0] for y in years]

        # All companies ordered by symbol.
        companies = list(
            Company.objects.order_by("symbol").values_list("symbol", "company_name")
        )

        # Existing (symbol, year_id) pairs in fact_profit_loss.
        existing = set(
            ProfitLoss.objects.filter(year__in=year_ids).values_list(
                "symbol_id", "year_id"
            )
        )

        # Build the matrix row-by-row.
        matrix = []
        for sym, cname in companies:
            cells = [
                {
                    "year_label": year_label,
                    "present": (sym, year_id) in existing,
                }
                for year_id, year_label, _ in years
            ]
            present_count = sum(c["present"] for c in cells)
            matrix.append(
                {
                    "symbol": sym,
                    "company_name": cname or sym,
                    "cells": cells,
                    "coverage_pct": (
                        round(present_count / len(cells) * 100) if cells else 0
                    ),
                }
            )

        context = {
            "years": years,
            "matrix": matrix,
            "total_companies": len(companies),
            "total_year_slots": len(years),
        }
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 5. API management
# ---------------------------------------------------------------------------


class APIManagementView(StaffRequiredMixin, View):
    """
    GET /admin-insights/api-management/

    Table of all ChannelPartners showing:
      - tier, is_active
      - total and active API key counts
      - total API calls logged (all time)
    """

    template_name = "admin_insights/api_management.html"

    def get(self, request):
        partners = ChannelPartner.objects.prefetch_related("api_keys").order_by(
            "partner_name"
        )

        rows = []
        for partner in partners:
            active_keys = partner.api_keys.filter(is_active=True)
            # APIUsageLog is keyed by the first 12 hex chars of each key_id UUID.
            key_prefixes = [
                str(k.key_id).replace("-", "")[:12] for k in active_keys
            ]
            total_calls = (
                APIUsageLog.objects.filter(api_key_prefix__in=key_prefixes).count()
                if key_prefixes
                else 0
            )
            rows.append(
                {
                    "id": partner.pk,
                    "partner_name": partner.partner_name,
                    "email": partner.email,
                    "tier": partner.tier,
                    "is_active": partner.is_active,
                    "total_keys": partner.api_keys.count(),
                    "active_key_count": active_keys.count(),
                    "total_calls": total_calls,
                    "created_at": partner.created_at,
                }
            )

        context = {
            "partners": rows,
            "total_partners": len(rows),
            "active_partners": sum(1 for r in rows if r["is_active"]),
        }
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 6. API analytics
# ---------------------------------------------------------------------------


class APIAnalyticsView(StaffRequiredMixin, View):
    """
    GET /admin-insights/api-analytics/

    - Daily call volume for the last 30 days (configurable via ?days=N)
    - Top-10 endpoints by call count with average latency
    - HTTP status code distribution
    - P50 and P95 response-time percentiles (computed in Python)
    """

    template_name = "admin_insights/api_analytics.html"

    def get(self, request):
        try:
            days = max(1, min(int(request.GET.get("days", 30)), 365))
        except (TypeError, ValueError):
            days = 30

        since = timezone.now() - timedelta(days=days)
        logs = APIUsageLog.objects.filter(requested_at__gte=since)

        total_calls = logs.count()

        daily_breakdown = list(
            logs.annotate(day=TruncDate("requested_at"))
            .values("day")
            .annotate(calls=Count("id"))
            .order_by("day")
        )
        for row in daily_breakdown:
            row["date"] = str(row.pop("day"))

        top_endpoints = list(
            logs.values("endpoint", "method")
            .annotate(calls=Count("id"), avg_ms=Avg("response_time_ms"))
            .order_by("-calls")[:10]
        )

        status_dist = list(
            logs.values("status_code")
            .annotate(calls=Count("id"))
            .order_by("-calls")
        )

        # P50 / P95 — fetched as a flat list and sliced in Python to avoid
        # relying on database-specific percentile functions.
        response_times = sorted(
            logs.exclude(response_time_ms__isnull=True).values_list(
                "response_time_ms", flat=True
            )
        )
        p50 = p95 = None
        if response_times:
            n = len(response_times)
            p50 = response_times[int(n * 0.50)]
            p95 = response_times[min(int(n * 0.95), n - 1)]

        avg_ms = logs.aggregate(avg=Avg("response_time_ms"))["avg"]

        context = {
            "since": since,
            "days": days,
            "total_calls": total_calls,
            "daily_breakdown": daily_breakdown,
            "top_endpoints": top_endpoints,
            "status_distribution": status_dist,
            "p50_ms": p50,
            "p95_ms": p95,
            "avg_response_ms": round(float(avg_ms), 1) if avg_ms else None,
        }
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 7. Webhooks
# ---------------------------------------------------------------------------


class WebhooksView(StaffRequiredMixin, View):
    """
    GET /admin-insights/webhooks/

    All webhook subscriptions across all partners with per-subscription
    delivery success rate computed over the last 30 days.
    """

    template_name = "admin_insights/webhooks.html"

    def get(self, request):
        since = timezone.now() - timedelta(days=30)
        subscriptions = WebhookSubscription.objects.select_related(
            "partner"
        ).order_by("partner__partner_name")

        rows = []
        for sub in subscriptions:
            recent = sub.webhook_events.filter(created_at__gte=since)
            total = recent.count()
            delivered = recent.filter(status=WebhookEvent.DELIVERED).count()
            failed = recent.filter(status=WebhookEvent.FAILED).count()
            pending = recent.filter(status=WebhookEvent.PENDING).count()
            success_rate = round(delivered / total * 100, 1) if total else None

            rows.append(
                {
                    "id": sub.pk,
                    "partner_name": sub.partner.partner_name,
                    "tier": sub.partner.tier,
                    "url": sub.url,
                    "events": sub.events,
                    "is_active": sub.is_active,
                    "created_at": sub.created_at,
                    "total_events_30d": total,
                    "delivered": delivered,
                    "failed": failed,
                    "pending": pending,
                    "success_rate": success_rate,
                }
            )

        context = {
            "subscriptions": rows,
            "total_subscriptions": len(rows),
            "active_subscriptions": sum(1 for r in rows if r["is_active"]),
        }
        return render(request, self.template_name, context)


# ---------------------------------------------------------------------------
# 8. Bulk import
# ---------------------------------------------------------------------------


REQUIRED_COLUMNS = {"symbol", "company_name"}


class BulkImportView(StaffRequiredMixin, View):
    """
    GET  /admin-insights/bulk-import/   — upload form
    POST /admin-insights/bulk-import/   — two-phase: validate then commit

    Phase 1 (no confirmed param):
      Upload CSV → parse & validate → store rows in session → show preview.
    Phase 2 (confirmed=true + session_key):
      Read validated rows from session → upsert into Company table → show result.
    """

    template_name = "admin_insights/bulk_import.html"

    def get(self, request):
        return render(request, self.template_name, {"phase": "upload"})

    def post(self, request):
        confirmed = request.POST.get("confirmed") == "true"
        session_key = request.POST.get("session_key", "").strip()

        # ── Phase 2: commit ──────────────────────────────────────────────────
        if confirmed and session_key:
            rows = request.session.get(f"bulk_import_{session_key}")
            if not rows:
                return render(
                    request,
                    self.template_name,
                    {
                        "phase": "error",
                        "error": "Import session expired or invalid. Please re-upload.",
                    },
                )
            created, updated = self._commit_rows(rows)
            del request.session[f"bulk_import_{session_key}"]
            return render(
                request,
                self.template_name,
                {"phase": "done", "created": created, "updated": updated},
            )

        # ── Phase 1: validate CSV ────────────────────────────────────────────
        csv_file = request.FILES.get("csv_file")
        if not csv_file:
            return render(
                request,
                self.template_name,
                {"phase": "upload", "error": "Please select a CSV file."},
            )

        try:
            text = csv_file.read().decode("utf-8-sig")
        except UnicodeDecodeError:
            return render(
                request,
                self.template_name,
                {"phase": "upload", "error": "File must be UTF-8 encoded."},
            )

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return render(
                request,
                self.template_name,
                {"phase": "upload", "error": "CSV file is empty or has no headers."},
            )

        headers = {h.strip().lower() for h in reader.fieldnames}
        missing = REQUIRED_COLUMNS - headers
        if missing:
            return render(
                request,
                self.template_name,
                {
                    "phase": "upload",
                    "error": f"Missing required columns: {sorted(missing)}.",
                },
            )

        valid_rows, row_errors = [], []
        for i, raw in enumerate(reader, start=2):
            row = {k.strip().lower(): (v or "").strip() for k, v in raw.items()}
            symbol = row.get("symbol", "").upper()
            company_name = row.get("company_name", "")
            if not symbol:
                row_errors.append(f"Row {i}: symbol is blank.")
                continue
            if not company_name:
                row_errors.append(f"Row {i}: company_name is blank (symbol={symbol}).")
                continue
            valid_rows.append(
                {
                    "symbol": symbol,
                    "company_name": company_name,
                    "sector": row.get("sector", ""),
                    "website": row.get("website", ""),
                    "about_company": row.get("about_company", ""),
                    "face_value": row.get("face_value") or None,
                    "book_value": row.get("book_value") or None,
                    "roce_percentage": row.get("roce_percentage") or None,
                    "roe_percentage": row.get("roe_percentage") or None,
                    "is_banking": row.get("is_banking", "false").lower()
                    in ("true", "1", "yes"),
                }
            )

        if not valid_rows:
            return render(
                request,
                self.template_name,
                {
                    "phase": "upload",
                    "error": "No valid rows found in the uploaded file.",
                    "row_errors": row_errors,
                },
            )

        sk = _uuid.uuid4().hex
        request.session[f"bulk_import_{sk}"] = valid_rows
        request.session.modified = True

        return render(
            request,
            self.template_name,
            {
                "phase": "confirm",
                "valid_rows": valid_rows[:50],
                "total_valid": len(valid_rows),
                "row_errors": row_errors,
                "session_key": sk,
            },
        )

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _resolve_sector(self, sector_name: str):
        if not sector_name:
            return None
        sector, _ = Sector.objects.get_or_create(
            sector_name=sector_name,
            defaults={"sector_name": sector_name},
        )
        return sector

    def _commit_rows(self, rows: list) -> tuple[int, int]:
        created = updated = 0
        for row in rows:
            sector_name = row.pop("sector", "")
            symbol = row.pop("symbol")
            sector = self._resolve_sector(sector_name)
            defaults = {k: v for k, v in row.items() if v not in (None, "")}
            if sector:
                defaults["sector"] = sector
            try:
                _, was_created = Company.objects.update_or_create(
                    symbol=symbol, defaults=defaults
                )
                if was_created:
                    created += 1
                else:
                    updated += 1
            except Exception as exc:
                logger.error("BulkImport: failed to upsert %s: %s", symbol, exc)
        return created, updated


# ---------------------------------------------------------------------------
# 9. Celery monitor
# ---------------------------------------------------------------------------


class CeleryMonitorView(StaffRequiredMixin, View):
    """
    GET /admin-insights/celery/

    Displays the last run status of each periodic task configured in
    django_celery_beat, joined to the most recent TaskResult from
    django_celery_results.  Also surfaces the 20 most recent FAILURE results
    across all task types.
    """

    template_name = "admin_insights/celery_monitor.html"

    def get(self, request):
        rows = []
        recent_failures = []
        errors = []

        # Load periodic task definitions from django_celery_beat.
        try:
            from django_celery_beat.models import PeriodicTask

            periodic_tasks = list(
                PeriodicTask.objects.select_related(
                    "crontab", "interval", "solar", "clocked"
                ).order_by("name")
            )
        except ImportError:
            periodic_tasks = []
            errors.append("django_celery_beat is not installed.")

        # Load the last TaskResult per task name from django_celery_results.
        latest_results: dict = {}
        try:
            from django_celery_results.models import TaskResult

            for task in periodic_tasks:
                result = (
                    TaskResult.objects.filter(task_name=task.task)
                    .order_by("-date_done")
                    .first()
                )
                if result:
                    latest_results[task.task] = result

            recent_failures = list(
                TaskResult.objects.filter(status="FAILURE")
                .order_by("-date_done")[:20]
                .values("task_name", "task_id", "status", "date_done", "traceback")
            )
        except ImportError:
            errors.append("django_celery_results is not installed.")

        for task in periodic_tasks:
            result = latest_results.get(task.task)

            # Build a human-readable schedule string.
            schedule_str = ""
            if getattr(task, "crontab", None):
                c = task.crontab
                schedule_str = (
                    f"{c.minute} {c.hour} {c.day_of_month} "
                    f"{c.month_of_year} {c.day_of_week}"
                )
            elif getattr(task, "interval", None):
                iv = task.interval
                schedule_str = f"every {iv.every} {iv.period}"

            rows.append(
                {
                    "name": task.name,
                    "task": task.task,
                    "schedule": schedule_str,
                    "enabled": task.enabled,
                    "last_run_at": task.last_run_at,
                    "total_run_count": task.total_run_count,
                    "result_status": result.status if result else None,
                    "result_date": result.date_done if result else None,
                    "result_traceback": (
                        (result.traceback or "")[:500] if result else None
                    ),
                }
            )

        context = {
            "rows": rows,
            "recent_failures": recent_failures,
            "errors": errors,
        }
        return render(request, self.template_name, context)
