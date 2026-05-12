"""
URL routing for the Nifty 100 channel-partner API.

All routes are mounted under /api/v1/ in the root URLconf.

Pattern                              View                    Name
-----------------------------------  ----------------------  --------------------------
GET  companies/<symbol>/full/        CompanyFullView         api:company-full
GET  bulk-financials/                BulkFinancialsView      api:bulk-financials
GET  screener/                       ScreenerView            api:screener
GET  scores/                         ScoresView              api:scores
GET  keys/                           APIKeyListView          api:key-list
POST keys/                           APIKeyCreateView        api:key-create
DEL  keys/<key_id>/                  APIKeyDeactivateView    api:key-deactivate
GET  webhooks/                       WebhookListView         api:webhook-list
POST webhooks/                       WebhookCreateView       api:webhook-create
DEL  webhooks/<int:pk>/              WebhookDeleteView       api:webhook-delete
GET  usage/                          UsageSummaryView        api:usage-summary
"""

from django.urls import path

from api.views import (
    APIKeyCreateView,
    APIKeyDeactivateView,
    APIKeyListView,
    BulkFinancialsView,
    CompanyFullView,
    ScoresView,
    ScreenerView,
    UsageSummaryView,
    WebhookCreateView,
    WebhookDeleteView,
    WebhookListView,
)

app_name = "api"

urlpatterns = [
    # --- Company data ---
    path(
        "companies/<str:symbol>/full/",
        CompanyFullView.as_view(),
        name="company-full",
    ),
    path(
        "bulk-financials/",
        BulkFinancialsView.as_view(),
        name="bulk-financials",
    ),
    path(
        "screener/",
        ScreenerView.as_view(),
        name="screener",
    ),
    path(
        "scores/",
        ScoresView.as_view(),
        name="scores",
    ),
    # --- API key management ---
    path(
        "keys/",
        APIKeyListView.as_view(),
        name="key-list",
    ),
    path(
        "keys/create/",
        APIKeyCreateView.as_view(),
        name="key-create",
    ),
    path(
        "keys/<str:key_id>/",
        APIKeyDeactivateView.as_view(),
        name="key-deactivate",
    ),
    # --- Webhook management ---
    path(
        "webhooks/",
        WebhookListView.as_view(),
        name="webhook-list",
    ),
    path(
        "webhooks/create/",
        WebhookCreateView.as_view(),
        name="webhook-create",
    ),
    path(
        "webhooks/<int:pk>/",
        WebhookDeleteView.as_view(),
        name="webhook-delete",
    ),
    # --- Usage analytics ---
    path(
        "usage/",
        UsageSummaryView.as_view(),
        name="usage-summary",
    ),
]
