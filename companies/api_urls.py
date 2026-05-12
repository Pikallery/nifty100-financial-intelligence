"""
companies/api_urls.py – internal Chart.js / AJAX API URL patterns.

All views return JSON and are consumed by the front-end JavaScript
running on the public website templates.

Pattern summary
---------------
api/v1/companies/                           → CompanyListAPIView
api/v1/companies/<symbol>/charts/           → CompanyChartsAPIView
api/v1/companies/<symbol>/peers/            → CompanyPeersAPIView
api/v1/screener/                            → ScreenerAPIView
"""

from django.urls import path
from . import api_views

app_name = "companies_api"

urlpatterns = [
    # Paginated company list with JSON filters (sector, health_label, sort)
    path(
        "companies/",
        api_views.CompanyListAPIView.as_view(),
        name="api_company_list",
    ),

    # All chart-ready time-series data for a single company
    path(
        "companies/<str:symbol>/charts/",
        api_views.CompanyChartsAPIView.as_view(),
        name="api_company_charts",
    ),

    # Top-5 peer companies with latest health scores
    path(
        "companies/<str:symbol>/peers/",
        api_views.CompanyPeersAPIView.as_view(),
        name="api_company_peers",
    ),

    # Dynamic screener – Q() filter engine, returns company list JSON
    path(
        "screener/",
        api_views.ScreenerAPIView.as_view(),
        name="api_screener",
    ),
]
