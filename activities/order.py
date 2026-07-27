"""Order-domain activities (order-tq).

authorize_payment is the retry-storm host (S1): it reads the in-process control
cache and fails with a configured probability when the storm is on. Reading the
cache and using randomness inside an activity is non-deterministic, which is
allowed; workflows never do this.
"""
from __future__ import annotations

import random
import time

from temporalio import activity
from temporalio.exceptions import ApplicationError

from control.cache import get_params, is_enabled
from control.schema import RETRY_STORM


@activity.defn
def validate_order(order_id: str, slow: bool) -> str:
    # S5 (activity variant): sleep past a short start-to-close timeout.
    if slow:
        time.sleep(30)
    return "valid"


@activity.defn
def authorize_payment(order_id: str) -> str:
    """Storm host. Raises a RETRYABLE error while the retry storm is enabled.

    The activity's retry policy is flat at ~1/sec and effectively non-exhausting,
    so failing orders retry continuously (billable) and recover instantly when
    the toggle is turned off.
    """
    if is_enabled(RETRY_STORM):
        prob = float(get_params(RETRY_STORM).get("failure_probability", 0.6))
        if random.random() < prob:
            # Retryable (default): keeps retrying, does not fail the workflow.
            raise ApplicationError("payment gateway timeout (injected)")
    return f"auth-{order_id}"


@activity.defn
def send_to_restaurant(order_id: str, should_fail: bool) -> str:
    # S4: a non-retryable terminal failure for a fraction of orders.
    if should_fail:
        raise ApplicationError(
            "restaurant rejected order (injected)", non_retryable=True
        )
    return "sent"


@activity.defn
def poll_restaurant_accept(order_id: str) -> bool:
    # A cheap bounded poll; restaurants accept quickly at baseline.
    return True


@activity.defn
def mark_picked_up(order_id: str) -> str:
    return "picked_up"


@activity.defn
def mark_delivered(order_id: str) -> str:
    return "delivered"
