"""
Root URL configuration for nifty100_project.

Route map:
  admin/                  → Django admin
  /                       → companies.urls         (public website)
  api/v1/                 → companies.api_urls      (internal Chart.js API)
  api/partner/v1/         → api.urls                (channel partner API)
  admin-insights/         → admin_insights.urls
  api/docs/               → Swagger UI (drf-spectacular)
  api/redoc/              → ReDoc (drf-spectacular)
  api/schema/             → OpenAPI schema download
"""

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularSwaggerView,
    SpectacularRedocView,
)

urlpatterns = [
    # ── Django admin ──────────────────────────────────────────────────────────
    path("admin/", admin.site.urls),

    # ── Public website (template views) ──────────────────────────────────────
    path("", include("companies.urls")),

    # ── Internal Chart.js API ─────────────────────────────────────────────────
    path("api/v1/", include("companies.api_urls")),

    # ── Channel partner REST API ──────────────────────────────────────────────
    path("api/partner/v1/", include("api.urls")),

    # ── Admin insights dashboard ──────────────────────────────────────────────
    path("admin-insights/", include("admin_insights.urls")),

    # ── API schema & docs ─────────────────────────────────────────────────────
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "api/docs/",
        SpectacularSwaggerView.as_view(url_name="schema"),
        name="swagger-ui",
    ),
    path(
        "api/redoc/",
        SpectacularRedocView.as_view(url_name="schema"),
        name="redoc",
    ),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
