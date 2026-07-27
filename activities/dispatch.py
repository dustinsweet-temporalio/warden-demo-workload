"""Dispatch-domain activities (dispatch-tq)."""
from __future__ import annotations

from temporalio import activity


@activity.defn
def assign_courier(order_id: str, courier_id: str) -> str:
    return f"{courier_id}->{order_id}"


@activity.defn
def noop_marker(seq: int) -> int:
    """Trivial local activity used only to grow event history quickly (S2).

    Run as a LOCAL activity: it adds marker events to history cheaply without
    much billable-action cost, so S2 grows HISTORY without confounding the APS
    story. Do not call this as a normal activity.
    """
    return seq
