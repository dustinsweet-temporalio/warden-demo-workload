"""Settlement-domain activities (settlement-tq)."""
from __future__ import annotations

from temporalio import activity


@activity.defn
def capture_payment(order_id: str) -> str:
    return f"captured-{order_id}"


@activity.defn
def compute_payout(order_id: str) -> float:
    return 4.75
