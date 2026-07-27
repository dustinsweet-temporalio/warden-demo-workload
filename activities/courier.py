"""Courier-domain activities (courier-tq)."""
from __future__ import annotations

from temporalio import activity


@activity.defn
def accept_assignment(courier_id: str, order_id: str) -> str:
    return f"{courier_id} accepted {order_id}"
