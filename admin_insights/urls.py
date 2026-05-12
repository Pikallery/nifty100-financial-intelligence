"""
admin_insights/urls.py — staff-only dashboard URL patterns.

All views require staff login and render server-side HTML.

Pattern                    View                    Name
-------------------------  ----------------------  ---------------------------
(empty)                    ExecutiveSummaryView    admin_insights:summary
health/                    HealthMonitorView       admin_insights:health
anomalies/                 AnomaliesView           admin_insights:anomalies
data-quality/              DataQualityView         admin_insights:data-quality
api-management/            APIManagementView       admin_insights:api-management
api-analytics/             APIAnalyticsView        admin_insights:api-analytics
webhooks/                  WebhooksView            admin_insights:webhooks
bulk-import/               BulkImportView          admin_insights:bulk-import
celery/                    CeleryMonitorView       admin_insights:celery
"""

from django.urls import path

from admin_insights.views import (
    AnomaliesView,
    APIAnalyticsView,
    APIManagementView,
    BulkImportView,
    CeleryMonitorView,
    DataQualityView,
    ExecutiveSummaryView,
    HealthMonitorView,
    WebhooksView,
)

app_name = "admin_insights"

urlpatterns = [
    path(
        "",
        ExecutiveSummaryView.as_view(),
        name="summary",
    ),
    path(
        "health/",
        HealthMonitorView.as_view(),
        name="health",
    ),
    path(
        "anomalies/",
        AnomaliesView.as_view(),
        name="anomalies",
    ),
    path(
        "data-quality/",
        DataQualityView.as_view(),
        name="data-quality",
    ),
    path(
        "api-management/",
        APIManagementView.as_view(),
        name="api-management",
    ),
    path(
        "api-analytics/",
        APIAnalyticsView.as_view(),
        name="api-analytics",
    ),
    path(
        "webhooks/",
        WebhooksView.as_view(),
        name="webhooks",
    ),
    path(
        "bulk-import/",
        BulkImportView.as_view(),
        name="bulk-import",
    ),
    path(
        "celery/",
        CeleryMonitorView.as_view(),
        name="celery",
    ),
]
