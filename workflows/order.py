"""OrderWorkflow: fixed sequential saga, the APS backbone (order-tq).

Deterministic: all fault behavior comes from the immutable OrderInput (stamped
by the generator) or from signals. The workflow never reads the control cache.

Visibility: the generator stamps the order's business dimensions at start time
(free). This workflow adds only what start time cannot know — the stage the saga
has reached and who is carrying the order — in three batched upserts, each one
billable action. `track_stages` (immutable input) turns them off.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributePair, TypedSearchAttributes

with workflow.unsafe.imports_passed_through():
    from activities.order import (
        authorize_payment,
        mark_delivered,
        mark_picked_up,
        poll_restaurant_accept,
        send_to_restaurant,
        validate_order,
    )
    from common import search_attributes as sa
    from common.constants import SETTLEMENT_TQ, dispatch_id, settlement_id
    from common.models import OrderInput, OrderReady, SettlementInput
    from common.retry import DEFAULT_RETRY, STORM_RETRY

# A delivery promise of 35 minutes from acceptance, so PromisedDeliveryAt is a
# real deadline you can query against ("what is due in the next 10 minutes").
_DELIVERY_PROMISE = timedelta(minutes=35)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._courier_id: str | None = None
        self._track_stages: bool = True

    def _stage(self, stage: str, *extra) -> None:
        """One batched upsert per stage change (one billable action, or none).

        Deterministic: called from fixed points in the saga, with values derived
        from workflow state, so a replay produces the same commands.
        """
        if not self._track_stages:
            return
        workflow.upsert_search_attributes(
            [sa.ORDER_STAGE.value_set(stage), *extra]
        )

    @workflow.signal
    async def courier_assigned(self, courier_id: str) -> None:
        self._courier_id = courier_id

    @workflow.query
    def status(self) -> str:
        return "assigned" if self._courier_id else "pending"

    @workflow.run
    async def run(self, inp: OrderInput) -> str:
        self._track_stages = inp.track_stages

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

        # The restaurant has the order: it is now cooking, and the delivery
        # promise starts from here. workflow.now() is replay-safe.
        self._stage(
            sa.STAGE_PREPARING,
            sa.PROMISED_DELIVERY_AT.value_set(workflow.now() + _DELIVERY_PROMISE),
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
        #    drain even when the region's dispatch is degraded (S2). Orders
        #    visibly pool in this stage while a region is degraded, which is the
        #    List Filter to run during S2.
        self._stage(sa.STAGE_AWAITING_COURIER)
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
        # Who is carrying it is only known now, so it rides along with the
        # in_transit stage change rather than costing its own action.
        self._stage(
            sa.STAGE_IN_TRANSIT, sa.COURIER_ID.value_set(self._courier_id or "unknown")
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
        # No terminal "delivered" upsert: ExecutionStatus = 'Completed' already
        # says that, for free. The settlement child inherits the order's
        # dimensions as start-time attributes (also free).
        await workflow.start_child_workflow(
            "SettlementWorkflow",
            SettlementInput(
                order_id=inp.order_id,
                region=inp.region,
                restaurant_id=inp.restaurant_id,
                courier_id=self._courier_id or "unknown",
            ),
            id=settlement_id(inp.order_id),
            task_queue=SETTLEMENT_TQ,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            search_attributes=TypedSearchAttributes(
                [
                    SearchAttributePair(sa.DELIVERY_REGION, inp.region),
                    SearchAttributePair(sa.RESTAURANT_ID, inp.restaurant_id),
                    SearchAttributePair(sa.COURIER_ID, self._courier_id or "unknown"),
                    SearchAttributePair(sa.ORDER_STAGE, sa.STAGE_SETTLING),
                ]
            ),
        )

        return "delivered"
