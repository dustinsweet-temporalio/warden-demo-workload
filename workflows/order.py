"""OrderWorkflow: fixed sequential saga, the APS backbone (order-tq).

Deterministic: all fault behavior comes from the immutable OrderInput (stamped
by the generator) or from signals. The workflow never reads the control cache.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.order import (
        authorize_payment,
        mark_delivered,
        mark_picked_up,
        poll_restaurant_accept,
        send_to_restaurant,
        validate_order,
    )
    from common.constants import SETTLEMENT_TQ, dispatch_id, settlement_id
    from common.models import OrderInput, OrderReady, SettlementInput
    from common.retry import DEFAULT_RETRY, STORM_RETRY


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._courier_id: str | None = None

    @workflow.signal
    async def courier_assigned(self, courier_id: str) -> None:
        self._courier_id = courier_id

    @workflow.query
    def status(self) -> str:
        return "assigned" if self._courier_id else "pending"

    @workflow.run
    async def run(self, inp: OrderInput) -> str:
        # S5 (workflow-timeout variant): when the generator gives this order a
        # short workflow execution timeout, sleeping past it produces a
        # workflow_timeout (feeds elevated_timeout_rate).
        if inp.force_timeout:
            await workflow.sleep(timedelta(seconds=30))

        # 1. validate (S5 activity variant: slow=True sleeps past start-to-close)
        await workflow.execute_activity(
            validate_order,
            args=[inp.order_id, inp.slow_validate],
            start_to_close_timeout=timedelta(seconds=8),
            retry_policy=DEFAULT_RETRY,
        )

        # 2. authorize payment (S1 retry-storm host: aggressive flat retry)
        await workflow.execute_activity(
            authorize_payment,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=STORM_RETRY,
        )

        # 3. send to restaurant (S4: non-retryable terminal failure)
        await workflow.execute_activity(
            send_to_restaurant,
            args=[inp.order_id, inp.fail_at_restaurant],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )

        # 4. await restaurant accept (bounded timer + poll)
        await workflow.sleep(timedelta(seconds=10))
        await workflow.execute_activity(
            poll_restaurant_accept,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )

        # 5. prep timer (models food prep)
        await workflow.sleep(timedelta(seconds=70))

        # 6. request dispatch: signal the region's DispatchWorkflow "order ready"
        try:
            handle = workflow.get_external_workflow_handle(dispatch_id(inp.region))
            await handle.signal(
                "order_ready",
                OrderReady(
                    order_id=inp.order_id,
                    order_wf_id=workflow.info().workflow_id,
                    region=inp.region,
                ),
            )
        except Exception:
            workflow.logger.warning("dispatch signal failed; using fallback courier")

        # 7. await courier assignment, with a timeout fallback so orders always
        #    drain even when the region's dispatch is degraded (S2).
        try:
            await workflow.wait_condition(
                lambda: self._courier_id is not None, timeout=timedelta(seconds=30)
            )
        except asyncio.TimeoutError:
            self._courier_id = f"synthetic-{inp.order_id[:8]}"

        # 8. pickup
        await workflow.execute_activity(
            mark_picked_up,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )

        # 9. transit timer (models drive time)
        await workflow.sleep(timedelta(seconds=60))

        # 10. deliver, then start settlement as an abandoned child
        await workflow.execute_activity(
            mark_delivered,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=DEFAULT_RETRY,
        )
        await workflow.start_child_workflow(
            "SettlementWorkflow",
            SettlementInput(order_id=inp.order_id),
            id=settlement_id(inp.order_id),
            task_queue=SETTLEMENT_TQ,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )

        return "delivered"
