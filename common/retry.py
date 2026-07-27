"""Retry policies.

The storm host (authorize_payment) uses a policy tuned so retries stay at about
one per second and effectively never exhaust during a demo window. That makes
the retry storm a sustained, billable retry loop that recovers cleanly when the
toggle is turned off, rather than failing the workflows outright.

Everything else uses ordinary sensible defaults.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy

# One retry per second, flat (no backoff), effectively non-exhausting.
STORM_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=1.0,
    maximum_interval=timedelta(seconds=1),
    maximum_attempts=1000,
)

# Ordinary policy for healthy activities.
DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)
