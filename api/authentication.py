"""
Custom DRF authentication backend: HMAC-SHA256 request signing.

Every authenticated request must carry four headers:

    X-API-Key-ID   — the key_id UUID identifying the API key
    X-Timestamp    — Unix epoch seconds (UTC) as a string
    X-Nonce        — a random string; deduplicated via cache to prevent replay
    X-Signature    — hex-encoded HMAC-SHA256 of the canonical string:

        METHOD + PATH + TIMESTAMP + hex(SHA256(body))

The server rejects requests whose X-Timestamp is more than 300 seconds old.
"""

import hashlib
import hmac
import logging
import secrets
import time

from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed

from api.models import APIKey, ChannelPartner  # noqa: F401 — ChannelPartner imported for clarity
from companies.models import APIUsageLog  # noqa: F401 — used by the async log task

logger = logging.getLogger(__name__)

# Maximum age of a timestamp before the request is rejected (seconds).
MAX_TIMESTAMP_AGE = 300


def _sha256_hex(data: bytes) -> str:
    """Return the lowercase hex SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def _build_canonical_string(method: str, path: str, timestamp: str, body: bytes) -> str:
    """
    Concatenate the four fields that the client must sign.

    canonical = METHOD + PATH + TIMESTAMP + hex(SHA256(body))
    """
    return method.upper() + path + timestamp + _sha256_hex(body)


class HMACAuthentication(BaseAuthentication):
    """
    DRF authentication class that validates HMAC-signed requests.

    On success  → returns (partner, api_key_instance)
    On failure  → raises AuthenticationFailed
    On missing headers → returns None (allows other authenticators to run)
    """

    def authenticate(self, request):
        key_id = request.headers.get("X-API-Key-ID", "").strip()
        timestamp_str = request.headers.get("X-Timestamp", "").strip()
        nonce = request.headers.get("X-Nonce", "").strip()
        signature = request.headers.get("X-Signature", "").strip()

        # If none of the HMAC headers are present, bail out gracefully so
        # other authentication classes (e.g. SessionAuthentication) can run.
        if not any([key_id, timestamp_str, nonce, signature]):
            return None

        # All four headers are required once any one is present.
        if not all([key_id, timestamp_str, nonce, signature]):
            raise AuthenticationFailed(
                "HMAC authentication requires X-API-Key-ID, X-Timestamp, X-Nonce and X-Signature headers."
            )

        # --- Timestamp freshness check ---
        try:
            request_time = float(timestamp_str)
        except ValueError:
            raise AuthenticationFailed("X-Timestamp must be a Unix epoch number.")

        skew = abs(time.time() - request_time)
        if skew > MAX_TIMESTAMP_AGE:
            raise AuthenticationFailed(
                f"Request timestamp is {int(skew)}s old; maximum allowed skew is {MAX_TIMESTAMP_AGE}s."
            )

        # --- Nonce deduplication (replay protection) ---
        from django.core.cache import cache

        nonce_cache_key = f"api:nonce:{key_id}:{nonce}"
        if cache.get(nonce_cache_key):
            raise AuthenticationFailed("Nonce has already been used; possible replay attack.")
        # Store nonce for 2× the allowed window so it cannot be replayed.
        cache.set(nonce_cache_key, "1", timeout=MAX_TIMESTAMP_AGE * 2)

        # --- Key look-up ---
        try:
            api_key = (
                APIKey.objects.select_related("partner")
                .get(key_id=key_id, is_active=True)
            )
        except (APIKey.DoesNotExist, Exception):
            # Uniform error to prevent enumeration.
            raise AuthenticationFailed("Invalid or inactive API key.")

        partner = api_key.partner
        if not partner.is_active:
            raise AuthenticationFailed("Channel partner account is disabled.")

        # --- Signature verification ---
        # Retrieve the raw body; DRF may have already consumed the stream.
        body: bytes = request.body  # Django caches this on first access.

        canonical = _build_canonical_string(
            request.method,
            request.path,
            timestamp_str,
            body,
        )

        # The secret is stored as a bcrypt hash, but the HMAC key must be the
        # *raw* secret.  At key-creation time the raw secret is stored
        # transiently in the cache for a short window so that the first request
        # can succeed; after that window the secret is unavailable server-side
        # (zero-knowledge model).
        #
        # For production deployments where a KMS or HSM stores the raw secret,
        # replace the cache lookup below with a KMS call.
        raw_secret_cache_key = f"api:raw_secret:{str(api_key.key_id)}"
        raw_secret: str | None = cache.get(raw_secret_cache_key)

        if raw_secret is None:
            # The raw secret is no longer cached; we cannot verify the HMAC.
            # This is the expected steady-state; inform the client to rotate.
            raise AuthenticationFailed(
                "API key secret window has expired. Please rotate your key or contact support."
            )

        expected_mac = hmac.new(
            raw_secret.encode("utf-8"),
            canonical.encode("utf-8"),
            "sha256",
        ).hexdigest()

        if not secrets.compare_digest(expected_mac, signature.lower()):
            raise AuthenticationFailed("HMAC signature mismatch.")

        # --- Async usage logging ---
        # Deliberately non-blocking; import here to avoid circular imports at
        # module load time.
        try:
            from api.tasks import log_api_usage  # noqa: PLC0415

            log_api_usage.delay(
                str(api_key.key_id),
                request.path,
                request.method,
                200,  # Status unknown at auth time; views update via middleware.
                0,
                _get_client_ip(request),
                len(body),
                0,
            )
        except Exception:  # pragma: no cover — Celery may be unavailable in tests
            logger.warning("Could not enqueue log_api_usage task.", exc_info=True)

        return (partner, api_key)

    def authenticate_header(self, request):
        return 'HMAC realm="Nifty100 API"'


def _get_client_ip(request) -> str:
    """Extract the real client IP from X-Forwarded-For or REMOTE_ADDR."""
    xff = request.META.get("HTTP_X_FORWARDED_FOR")
    if xff:
        return xff.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "")
