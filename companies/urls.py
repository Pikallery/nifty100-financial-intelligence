"""
companies/urls.py – public website URL patterns.

All views render HTML templates and are consumed by the browser directly.

Pattern summary
---------------
/                           → HomeView
/companies/                 → CompanyListView
/company/<symbol>/          → CompanyDetailView
/compare/                   → CompareView
/screener/                  → ScreenerView
/sector/<name>/             → SectorDetailView
"""

from django.urls import path
from . import views

app_name = "companies"

urlpatterns = [
    # Home page – featured companies, sector summary, latest pros/cons
    path("", views.HomeView.as_view(), name="home"),

    # Full paginated company list with sector/label filters
    path("companies/", views.CompanyListView.as_view(), name="company_list"),

    # Individual company deep-dive page
    path("company/<str:symbol>/", views.CompanyDetailView.as_view(), name="company_detail"),

    # Compare shell – JS-driven, fetches data from api/v1/
    path("compare/", views.CompareView.as_view(), name="compare"),

    # Screener shell – JS-driven, fetches data from api/v1/screener/
    path("screener/", views.ScreenerView.as_view(), name="screener"),

    # All companies in a given sector, ranked by health score
    path("sector/<str:name>/", views.SectorDetailView.as_view(), name="sector_detail"),
]
