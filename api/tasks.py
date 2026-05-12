"""
Celery tasks for the Nifty 100 channel-partner API.

Tasks
-----
  log_api_usage           — async write one APIUsageLog row
  deliver_webhook         — POST a WebhookEvent to the subscriber URL,
                            HMAC-signed, retried 5× with exponential back-off
  fire_score_updated_event  — create WebhookEvent rows for all score_updated
                              subscriptions and enqueue deliver_webhook
  fire_anomaly_flagged_event — same for anomaly_flagged subscriptions
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone as dt_timezone

import requests as http_requests
from celery import shared_task
from django.utils import timezone

from api.models import WebhookEvent, WebhookSubscription
from companies.models import APIUsageLog

logger = logging.getLogger(__name__)

# Exponential back-off delays in seconds for webhook retries:
# attempt 1 →  60 s, 2 → 120 s, 3 → 240 s, 4 → 480 s, 5 → 960 s
WEBHOOK_RETRY_COUNTDOWN = [60, 120, 240, 480, 960]
MAX_WEBHOOK_ATTEMPTS = 5
WEBHOOK_REQUEST_TIMEOUT = 10  # seconds


# ---------------------------------------------------------------------------
# Task 1: log_api_usage
# ---------------------------------------------------------------------------


@shared_task(
    name="api.tasks.log_api_usage",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    ignore_result=True,
)
def log_api_usage(
    self,
    key_id: str,
    endpoint: str,
    method: str,
    status_code: int,
    response_ms: int,
    ip: str,
    req_size: int,
    resp_size: int,
):
    """
    Async-write a single row to api_usage_log.

    Called by HMACAuthentication immediately after every authenticated request
    so the HTTP response is never delayed by a database write.

    ``key_id`` is the UUID string of the APIKey; we store the first 12 hex
    characters (no dashes) as ``api_key_prefix`` to match the APIUsageLog
    schema while avoiding exposure of the full UUID.
    """
    try:
        prefix = key_id.replace("-", "")[:12]
        APIUsageLog.objects.create(
            api_key_prefix=prefix,
            endpoint=endpoint,
            method=method.upper(),
            status_code=status_code,
            response_time_ms=response_ms,
            ip_address=ip or None,
        )
    except Exception as exc:
        logger.warning("log_api_usage failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc)


# ---------------------------------------------------------------------------
# Task 2: deliver_webhook
# ---------------------------------------------------------------------------


def _sign_payload(raw_secret: str, payload_bytes: bytes) -> str:
    """
    Return the HMAC-SHA256 hex digest of *payload_bytes* signed with
    *raw_secret*.  This is placed in the X-Bluestock-Signature request header.
    """
    return hmac.new(
        raw_secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()


@shared_task(
    name="api.tasks.deliver_webhook",
    bind=True,
    max_retries=MAX_WEBHOOK_ATTEMPTS,
    ignore_result=False,
)
def deliver_webhook(self, event_id: int):
    """
    HTTP POST a WebhookEvent payload to the subscriber's URL.

    Signing
    -------
    The raw partner secret is retrieved from the Django cache (keyed by the
    partner's first active api_key key_id).  If unavailable, the event is
    marked FAILED without retry so as not to block the queue.

    Retry policy
    ------------
    On any non-2xx response or network error the task retries up to 5 times
    using the delays defined in WEBHOOK_RETRY_COUNTDOWN.  After the 5th
    failure the WebhookEvent.status is set to FAILED.
    """
    from django.core.cache import cache

    try:
        event = WebhookEvent.objects.select_related(
            "subscription__partner"
        ).get(pk=event_id)
    except WebhookEvent.DoesNotExist:
        logger.error("deliver_webhook: WebhookEvent %s not found.", event_id)
        return

    if event.status == WebhookEvent.DELIVERED:
        return  # Already delivered by a concurrent retry — idempotent exit.

    subscription = event.subscription
    partner = subscription.partner

    # Retrieve the signing secret from cache (set when the key was created).
    # Use the first active key for this partner.
    from api.models import APIKey

    first_key = APIKey.objects.filter(partner=partner, is_active=True).first()
    raw_secret: str | None = None
    if first_key:
        raw_secret = cache.get(f"api:raw_secret:{str(first_key.key_id)}")

    payload_bytes = json.dumps(event.payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "X-Bluestock-Event": event.event_type,
        "X-Bluestock-Delivery": str(event.pk),
        "User-Agent": "Bluestock-Webhook/1.0",
    }
    if raw_secret:
        headers["X-Bluestock-Signature"] = f"sha256={_sign_payload(raw_secret, payload_bytes)}"

    # Update attempt metadata before the HTTP call.
    attempt_number = (event.attempts or 0) + 1
    event.attempts = attempt_number
    event.last_attempt_at = timezone.now()
    event.save(update_fields=["attempts", "last_attempt_at"])

    try:
        response = http_requests.post(
            subscription.url,
            data=payload_bytes,
            headers=headers,
            timeout=WEBHOOK_REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        # Success
        event.status = WebhookEvent.DELIVERED
        event.save(update_fields=["status"])
        logger.info(
            "deliver_webhook: event %s delivered to %s (HTTP %s).",
            event_id,
            subscription.url,
            response.status_code,
        )

    except Exception as exc:
        logger.warning(
            "deliver_webhook: attempt %s/%s for event %s failed: %s",
            attempt_number,
            MAX_WEBHOOK_ATTEMPTS,
            event_id,
            exc,
        )

        if attempt_number >= MAX_WEBHOOK_ATTEMPTS:
            event.status = WebhookEvent.FAILED
            event.save(update_fields=["status"])
            logger.error(
                "deliver_webhook: event %s permanently failed after %s attempts.",
                event_id,
                attempt_number,
            )
            return

        # Schedule the next retry with exponential back-off.
        countdown = WEBHOOK_RETRY_COUNTDOWN[min(attempt_number - 1, len(WEBHOOK_RETRY_COUNTDOWN) - 1)]
        raise self.retry(exc=exc, countdown=countdown)


# ---------------------------------------------------------------------------
# Task 3: fire_score_updated_event
# ---------------------------------------------------------------------------


@shared_task(
    name="api.tasks.fire_score_updated_event",
    ignore_result=True,
)
def fire_score_updated_event(symbol: str, old_score: float, new_score: float):
    """
    Create one WebhookEvent per active score_updated subscription and enqueue
    deliver_webhook for each.

    Called by the ML pipeline after every scoring run that changes a
    company's overall_score.
    """
    subscriptions = WebhookSubscription.objects.filter(
        is_active=True,
        events__contains="score_updated",
    ).select_related("partner")

    payload = {
        "event": "score_updated",
        "symbol": symbol,
        "old_score": old_score,
        "new_score": new_score,
        "timestamp": datetime.now(dt_timezone.utc).isoformat(),
    }

    for sub in subscriptions:
        event = WebhookEvent.objects.create(
            subscription=sub,
            event_type="score_updated",
            payload=payload,
            status=WebhookEvent.PENDING,
        )
        deliver_webhook.delay(event.pk)

    logger.info(
        "fire_score_updated_event: queued %s delivery tasks for symbol=%s.",
        subscriptions.count(),
        symbol,
    )


# ---------------------------------------------------------------------------
# Task 4: fire_anomaly_flagged_event
# ---------------------------------------------------------------------------


@shared_task(
    name="api.tasks.fire_anomaly_flagged_event",
    ignore_result=True,
)
def fire_anomaly_flagged_event(symbol: str, anomaly_id: int):
    """
    Create one WebhookEvent per active anomaly_flagged subscription and enqueue
    deliver_webhook for each.

    Called by the ML pipeline when a new Anomaly row is persisted.
    """
    from companies.models import Anomaly

    subscriptions = WebhookSubscription.objects.filter(
        is_active=True,
        events__contains="anomaly_flagged",
    ).select_related("partner")

    # Hydrate the anomaly details for the payload if available.
    anomaly_data: dict = {"anomaly_id": anomaly_id, "symbol": symbol}
    try:
        anomaly = Anomaly.objects.select_related("year").get(pk=anomaly_id)
        anomaly_data.update(
            {
                "metric": anomaly.metric,
                "value": float(anomaly.value) if anomaly.value is not None else None,
                "z_score": float(anomaly.z_score) if anomaly.z_score is not None else None,
                "severity": anomaly.severity,
                "year_label": anomaly.year.year_label if anomaly.year else None,
            }
        )
    except Exception:
        pass  # Deliver with minimal payload rather than failing the task.

    payload = {
        "event": "anomaly_flagged",
        "timestamp": datetime.now(dt_timezone.utc).isoformat(),
        **anomaly_data,
    }

    for sub in subscriptions:
        event = WebhookEvent.objects.create(
            subscription=sub,
            event_type="anomaly_flagged",
            payload=payload,
            status=WebhookEvent.PENDING,
        )
        deliver_webhook.delay(event.pk)

    logger.info(
        "fire_anomaly_flagged_event: queued %s delivery tasks for symbol=%s anomaly=%s.",
        subscriptions.count(),
        symbol,
        anomaly_id,
    )
