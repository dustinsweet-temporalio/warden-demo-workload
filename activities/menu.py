"""Menu-sync activities (menu-tq)."""
from __future__ import annotations

from temporalio import activity


@activity.defn
def fetch_menu(restaurant_id: str) -> int:
    return 42  # item count


@activity.defn
def refresh_menu(restaurant_id: str) -> str:
    return f"refreshed-{restaurant_id}"


@activity.defn
def publish_menu(restaurant_id: str) -> str:
    return f"published-{restaurant_id}"
