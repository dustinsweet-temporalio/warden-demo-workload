"""DispatchWorkflow: long-running per-region entity (dispatch-tq).

Signal-and-react. Matches ready orders to available couriers, runs a short
assign activity, and signals the waiting OrderWorkflow back. Continue-as-new
keeps every running instance well under 2000 events at baseline.

This type hosts S2 (running-history growth) and S6 (terminate-without-CAN).
The set_degradation signal (S2) can suspend continue-as-new and grow history
with cheap local activities. Because the fault arrives as a signal recorded in
history, reacting to it stays deterministic.

Visibility: a region carries DeliveryRegion / FleetStatus / PendingOrderCount.
They are re-stamped for free on every continue-as-new, and upserted mid-run only
when the region's health actually flips (S2 on/off), so a running fleet costs one
action per flip, not one per tick.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributePair, TypedSearchAttributes

with workflow.unsafe.imports_passed_through():
    from activities.dispatch import assign_courier, noop_marker
    from common import search_attributes as sa
    from common.models import CourierAvailable, Degradation, DispatchState, OrderReady

# Continue-as-new thresholds at baseline.
_MAX_PROCESSED = 200
_MAX_HISTORY = 1500


@workflow.defn
class DispatchWorkflow:
    def __init__(self) -> None:
        self._region: str = ""
        self._pending: list[dict] = []
        self._couriers: list[str] = []
        self._processed: int = 0
        self._degr: Degradation = Degradation()
        self._seq: int = 0
        # Last FleetStatus value published to visibility, so we upsert on flips
        # only. Rebuilt on replay from the same signal history, so it is safe.
        self._published_status: str = "healthy"

    @workflow.signal
    async def order_ready(self, ready: OrderReady) -> None:
        self._pending.append(
            {"order_id": ready.order_id, "order_wf_id": ready.order_wf_id}
        )

    @workflow.signal
    async def courier_available(self, avail: CourierAvailable) -> None:
        self._couriers.append(avail.courier_id)

    @workflow.signal
    async def set_degradation(self, degr: Degradation) -> None:
        # S2 actuation. Live, deterministic (signal is recorded in history).
        self._degr = degr

    @workflow.query
    def get_stats(self) -> dict:
        return {
            "region": self._region,
            "processed": self._processed,
            "pending": len(self._pending),
            "couriers": len(self._couriers),
            "history_length": workflow.info().get_current_history_length(),
            "suspend_can": self._degr.suspend_can,
            "inflate_history": self._degr.inflate_history,
        }

    def _status(self) -> str:
        """The region's health as visibility sees it. Degraded == S2 is on."""
        return (
            "degraded"
            if (self._degr.suspend_can or self._degr.inflate_history)
            else "healthy"
        )

    def _publish_status(self) -> None:
        """Upsert only when the region's health actually changed (1 action)."""
        status = self._status()
        if status == self._published_status:
            return
        workflow.upsert_search_attributes(
            [
                sa.FLEET_STATUS.value_set(status),
                sa.PENDING_ORDER_COUNT.value_set(len(self._pending)),
            ]
        )
        self._published_status = status

    @workflow.run
    async def run(self, state: DispatchState) -> None:
        self._region = state.region
        self._pending = list(state.pending_orders)
        self._couriers = list(state.available_couriers)
        self._processed = state.processed
        self._degr = state.degradation or Degradation()
        # This run started with the attributes its starter stamped, which reflect
        # the degradation carried across continue-as-new.
        self._published_status = self._status()

        while True:
            if self._degr.inflate_history:
                # S2: grow history quickly with cheap local activities. Local
                # activities add marker events (history) but little billable
                # cost, so this grows HISTORY without confounding the APS story.
                rate = max(1, int(self._degr.inflate_rate or 40))
                for _ in range(rate):
                    self._seq += 1
                    await workflow.execute_local_activity(
                        noop_marker,
                        self._seq,
                        start_to_close_timeout=timedelta(seconds=5),
                    )
                await workflow.sleep(timedelta(seconds=1))
            else:
                # Wake on work, on a degradation flip, on an unpublished health
                # change, or when history is large enough that we must
                # continue-as-new (so a reversed S2 can shed its accumulated
                # history even with no pending work). Without the health term, a
                # region degraded with inflate_history off would sit in this wait
                # and never publish FleetStatus = 'degraded'.
                await workflow.wait_condition(
                    lambda: bool(self._pending)
                    or self._degr.inflate_history
                    or self._status() != self._published_status
                    or (
                        not self._degr.suspend_can
                        and workflow.info().get_current_history_length() > _MAX_HISTORY
                    )
                )

            # Publish a health flip as soon as we notice it (S2 on or reversed).
            self._publish_status()

            await self._drain()

            if not self._degr.suspend_can and (
                self._processed >= _MAX_PROCESSED
                or workflow.info().get_current_history_length() > _MAX_HISTORY
            ):
                # Re-stamp attributes on the next run for free rather than
                # upserting there.
                workflow.continue_as_new(
                    args=[self._carry()],
                    search_attributes=TypedSearchAttributes(
                        [
                            SearchAttributePair(sa.DELIVERY_REGION, self._region),
                            SearchAttributePair(sa.FLEET_STATUS, self._status()),
                            SearchAttributePair(
                                sa.PENDING_ORDER_COUNT, len(self._pending)
                            ),
                        ]
                    ),
                )

    async def _drain(self) -> None:
        while self._pending:
            order = self._pending.pop(0)
            courier = (
                self._couriers.pop(0)
                if self._couriers
                else f"synthetic-{self._region}-{self._processed}"
            )
            await workflow.execute_activity(
                assign_courier,
                args=[order["order_id"], courier],
                start_to_close_timeout=timedelta(seconds=10),
            )
            # Signal the waiting OrderWorkflow back.
            try:
                handle = workflow.get_external_workflow_handle(order["order_wf_id"])
                await handle.signal("courier_assigned", courier)
            except Exception:
                pass
            self._processed += 1

    def _carry(self) -> DispatchState:
        return DispatchState(
            region=self._region,
            pending_orders=self._pending,
            available_couriers=self._couriers[-50:],
            processed=0,
            degradation=self._degr,
        )
