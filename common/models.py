"""Shared, sandbox-safe data models used as workflow inputs and signal payloads.

These are pure dataclasses with no side-effecting imports, so they are safe to
import inside the workflow sandbox (via workflow.unsafe.imports_passed_through).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderInput:
    """Immutable start-time input for an OrderWorkflow.

    All fault behavior an order can exhibit is stamped here by the generator at
    start time, so the workflow stays deterministic (it reacts only to its
    immutable input and to signals).
    """

    order_id: str
    region: str
    restaurant_id: str
    # S4: throw a non-retryable error at send_to_restaurant.
    fail_at_restaurant: bool = False
    # S5 (activity variant): validate_order sleeps past its start-to-close timeout.
    slow_validate: bool = False
    # S5 (workflow variant): sleep past a short workflow execution timeout.
    force_timeout: bool = False
    # Order shape, generated at start time and stamped into start-time search
    # attributes (free) so the visibility demo has business dimensions to slice.
    priority_tier: str = "standard"
    order_value_usd: float = 0.0
    surge_pricing: bool = False
    dietary_tags: list = field(default_factory=list)  # list[str]
    delivery_notes: str = ""
    # Whether the order upserts OrderStage as the saga advances. Each upsert is
    # one billable action, so this is an explicit, presenter-controlled cost
    # (ORDER_STAGE_TRACKING). Immutable start-time input keeps it replay-safe.
    track_stages: bool = True


@dataclass
class CourierAssigned:
    courier_id: str


@dataclass
class OrderReady:
    order_id: str
    order_wf_id: str
    region: str


@dataclass
class CourierAvailable:
    courier_id: str
    region: str


@dataclass
class Degradation:
    """Live degradation signal for a DispatchWorkflow region (S2)."""

    suspend_can: bool = False
    inflate_history: bool = False
    inflate_rate: int = 40


@dataclass
class DispatchState:
    """Carried state for the DispatchWorkflow entity across continue-as-new."""

    region: str
    pending_orders: list = field(default_factory=list)  # list[dict]
    available_couriers: list = field(default_factory=list)  # list[str]
    processed: int = 0
    degradation: Degradation = field(default_factory=Degradation)


@dataclass
class CourierState:
    courier_id: str
    region: str
    assignments: int = 0
    pings: int = 0
    vehicle_type: str = "bike"


@dataclass
class BatchInput:
    batch_id: str
    # "normal" | "fanout_storm" | "sequential_bloat"
    mode: str = "normal"
    children: int = 6
    iterations: int = 700


@dataclass
class CandidateInput:
    batch_id: str
    candidate_id: str


@dataclass
class SettlementInput:
    order_id: str
    # Carried from the parent order so the settlement child can be tagged with
    # the same visibility dimensions at start time.
    region: str = ""
    restaurant_id: str = ""
    courier_id: str = ""


@dataclass
class MenuSyncState:
    restaurant_id: str
    runs: int = 0
