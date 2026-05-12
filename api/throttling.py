"""
Tiered rate-throttle for the Nifty 100 channel-partner API.

Three rate windows are enforced per partner tier:

    BASIC      :  10 / min    100 / hour    500 / day
    PRO        :  60 / min   1000 / hour  10000 / day
    ENTERPRISE : 300 / min  10000 / hour  (no daily cap)

Each window is tracked independently in the Django cache (Redis).
Cache keys are scoped to the ChannelPartner pk so that multiple API keys
belonging to the same partner share the same quota bucket.

When any window is exceeded the view receives a throttled response with:
  - HTTP 429
  - Retry-After header (seconds until the earliest window resets)
  - JSON body describing remaining quota for all three windows
"""

import math
import time

from django.core.cache import cache
from rest_framework.throttling import BaseThrottle

# ---------------------------------------------------------------------------
# Tier configuration
# ---------------------------------------------------------------------------

TIER_LIMITS = {
    "BASIC": {
        "minute": 10,
        "hour": 100,
        "day": 500,
    },
    "PRO": {
        "minute": 60,
        "hour": 1000,
        "day": 10000,
    },
    "ENTERPRISE": {
        "minute": 300,
        "hour": 10000,
        "day": None,  # No daily cap for Enterprise
    },
}

WINDOW_SECONDS = {
    "minute": 60,
    "hour": 3600,
    "day": 86400,
}


class TieredRateThrottle(BaseThrottle):
    """
    DRF throttle class that enforces per-tier rate limits.

    ``request.auth`` must be an ``APIKey`` instance (set by HMACAuthentication);
    ``request.user`` / ``request.auth.partner`` must be a ``ChannelPartner``
    with a ``tier`` attribute.

    Usage in views::

        throttle_classes = [TieredRateThrottle]
    """

    def _partner_and_tier(self, request):
        """Return (partner, tier_string) from the authenticated request."""
        # HMACAuthentication returns (partner, api_key); DRF sets request.user
        # to the first element and request.auth to the second.
        partner = request.user
        if partner is None or not hasattr(partner, "tier"):
            return None, "BASIC"
        return partner, partner.tier.upper()

    def _cache_key(self, partner_pk: int, window: str) -> str:
        return f"throttle:partner:{partner_pk}:{window}"

    def allow_request(self, request, view):
        """
        Returns True if the request is within all applicable rate windows.
        Sets ``self._retry_after`` and ``self._quota_info`` for use in
        ``wait()`` and the throttled response.
        """
        partner, tier = self._partner_and_tier(request)
        if partner is None:
            # Unauthenticated — let authentication handle the rejection.
            return True

        limits = TIER_LIMITS.get(tier, TIER_LIMITS["BASIC"])
        partner_pk = partner.pk
        now = time.time()

        self._retry_after = None
        self._quota_info = {}

        for window, limit in limits.items():
            if limit is None:
                # Enterprise has no daily cap.
                self._quota_info[window] = {"limit": None, "remaining": None, "reset_in": None}
                continue

            window_secs = WINDOW_SECONDS[window]
            cache_key = self._cache_key(partner_pk, window)

            # Atomically increment the counter; initialise to 0 on first use.
            current = cache.get(cache_key)
            if current is None:
                cache.set(cache_key, 1, timeout=window_secs)
                current = 1
            else:
                try:
                    current = cache.incr(cache_key)
                except ValueError:
                    # Key expired between get() and incr() — reset it.
                    cache.set(cache_key, 1, timeout=window_secs)
                    current = 1

            remaining = max(0, limit - current)
            # We cannot know the exact TTL without a second round-trip; use an
            # approximation based on the window size for the Retry-After header.
            reset_in = window_secs  # conservative upper-bound

            self._quota_info[window] = {
                "limit": limit,
                "used": current,
                "remaining": remaining,
                "reset_in_seconds": reset_in,
            }

            if current > limit:
                # This window is exhausted.
                if self._retry_after is None or reset_in < self._retry_after:
                    self._retry_after = reset_in

        return self._retry_after is None

    def wait(self):
        """Return seconds until the earliest window resets, for Retry-After."""
        return self._retry_after

    def throttle_failure_response(self):
        """
        Build the JSON body for the 429 response.

        DRF calls this indirectly via the ThrottledException handler.  Views
        that want the enriched body should override ``get_throttles()`` or use
        a custom exception handler (see api/exceptions.py).
        """
        return {
            "error": "rate_limit_exceeded",
            "message": "You have exceeded your API rate limit.",
            "retry_after_seconds": math.ceil(self._retry_after or 60),
            "quota": self._quota_info,
        }
