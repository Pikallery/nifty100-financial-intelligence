import uuid

from django.db import models


class ChannelPartner(models.Model):
    """Registered external party that consumes the Nifty 100 API."""

    BASIC = "BASIC"
    PRO = "PRO"
    ENTERPRISE = "ENTERPRISE"
    TIER_CHOICES = [
        (BASIC, "Basic"),
        (PRO, "Pro"),
        (ENTERPRISE, "Enterprise"),
    ]

    partner_name = models.CharField(max_length=255)
    email = models.EmailField(unique=True)
    tier = models.CharField(max_length=10, choices=TIER_CHOICES, default=BASIC)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "api_channel_partners"
        ordering = ["partner_name"]

    def __str__(self):
        return f"{self.partner_name} ({self.tier})"


class APIKey(models.Model):
    """
    HMAC credential pair for a ChannelPartner.

    ``id``              — UUID primary key (never exposed in URLs).
    ``key_id``          — UUID shared publicly as the key identifier in headers.
    ``key_secret_hash`` — bcrypt hash of the raw secret; the raw secret is
                          shown exactly once at creation time.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    partner = models.ForeignKey(ChannelPartner, on_delete=models.CASCADE, related_name="api_keys")
    key_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    key_secret_hash = models.CharField(max_length=128)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "api_keys"
        ordering = ["-created_at"]

    def __str__(self):
        return f"APIKey {self.key_id} — {self.partner.partner_name}"


class WebhookSubscription(models.Model):
    """
    A URL endpoint registered by a ChannelPartner to receive push events.

    ``events`` is stored as a JSON list of event-type strings, e.g.
    ["score_updated", "anomaly_flagged"].
    """

    partner = models.ForeignKey(ChannelPartner, on_delete=models.CASCADE, related_name="webhook_subscriptions")
    url = models.URLField(max_length=2000)
    events = models.JSONField(default=list)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "api_webhook_subscriptions"
        ordering = ["-created_at"]

    def __str__(self):
        return f"Webhook {self.partner.partner_name} → {self.url}"


class WebhookEvent(models.Model):
    """
    A single outbound push notification attempt for a WebhookSubscription.

    Delivery is retried by a Celery task up to 5 times with exponential back-off.
    """

    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    STATUS_CHOICES = [
        (PENDING, "Pending"),
        (DELIVERED, "Delivered"),
        (FAILED, "Failed"),
    ]

    subscription = models.ForeignKey(
        WebhookSubscription,
        on_delete=models.CASCADE,
        related_name="webhook_events",
    )
    event_type = models.CharField(max_length=100)
    payload = models.JSONField()
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=PENDING)
    attempts = models.IntegerField(default=0)
    last_attempt_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "api_webhook_events"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "-created_at"], name="webhook_events_status_recent"),
        ]

    def __str__(self):
        return f"WebhookEvent {self.event_type} [{self.status}] sub={self.subscription_id}"
