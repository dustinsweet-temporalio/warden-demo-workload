"""Per-task-queue registration: which workflows and activities each worker hosts.

One task queue per workflow type keeps every Warden tile an isolable failure
domain. noop_marker is registered on dispatch-tq and invoked as a local activity.
"""
from __future__ import annotations

from activities import batch as batch_acts
from activities import courier as courier_acts
from activities import dispatch as dispatch_acts
from activities import menu as menu_acts
from activities import order as order_acts
from activities import settlement as settlement_acts
from common.constants import (
    BATCH_TQ,
    CONTROL_TQ,
    COURIER_TQ,
    DISPATCH_TQ,
    MENU_TQ,
    ORDER_TQ,
    SETTLEMENT_TQ,
)
from control.workflow import DemoControlWorkflow
from workflows.batch import BatchAssignmentWorkflow
from workflows.candidate import CandidateEvalWorkflow
from workflows.courier import CourierShiftWorkflow
from workflows.dispatch import DispatchWorkflow
from workflows.menu import MenuSyncWorkflow
from workflows.order import OrderWorkflow
from workflows.settlement import SettlementWorkflow

# queue -> (workflows, activities)
REGISTRY: dict[str, tuple[list, list]] = {
    ORDER_TQ: (
        [OrderWorkflow],
        [
            order_acts.validate_order,
            order_acts.authorize_payment,
            order_acts.send_to_restaurant,
            order_acts.poll_restaurant_accept,
            order_acts.mark_picked_up,
            order_acts.mark_delivered,
        ],
    ),
    DISPATCH_TQ: (
        [DispatchWorkflow],
        [dispatch_acts.assign_courier, dispatch_acts.noop_marker],
    ),
    COURIER_TQ: (
        [CourierShiftWorkflow],
        [courier_acts.accept_assignment],
    ),
    BATCH_TQ: (
        [BatchAssignmentWorkflow, CandidateEvalWorkflow],
        [batch_acts.score_candidate, batch_acts.tiny_eval],
    ),
    SETTLEMENT_TQ: (
        [SettlementWorkflow],
        [settlement_acts.capture_payment, settlement_acts.compute_payout],
    ),
    MENU_TQ: (
        [MenuSyncWorkflow],
        [menu_acts.fetch_menu, menu_acts.refresh_menu, menu_acts.publish_menu],
    ),
    CONTROL_TQ: (
        [DemoControlWorkflow],
        [],
    ),
}
