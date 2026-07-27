"""CourierShiftWorkflow: per-courier entity for one active shift (courier-tq).

Signal-and-react over location pings and assignments. Continue-as-new keeps
history bounded. The generator ends shifts and starts replacements. This is the
queue the worker-down scenario (S7) takes offline.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.courier import accept_assignment
    from common.constants import dispatch_id
    from common.models import CourierAvailable, CourierState

_MAX_HISTORY = 1500


@workflow.defn
class CourierShiftWorkflow:
    def __init__(self) -> None:
        self._courier_id: str = ""
        self._region: str = ""
        self._assignments: list[str] = []
        self._pings: int = 0
        self._count: int = 0
        self._end: bool = False

    @workflow.signal
    async def location_ping(self, lat: float, lon: float) -> None:
        self._pings += 1

    @workflow.signal
    async def assignment(self, order_id: str) -> None:
        self._assignments.append(order_id)

    @workflow.signal
    async def end_shift(self) -> None:
        self._end = True

    @workflow.query
    def get_stats(self) -> dict:
        return {
            "courier_id": self._courier_id,
            "region": self._region,
            "assignments": self._count,
            "pings": self._pings,
        }

    @workflow.run
    async def run(self, state: CourierState) -> str:
        self._courier_id = state.courier_id
        self._region = state.region
        self._count = state.assignments
        self._pings = state.pings

        while True:
            await workflow.wait_condition(
                lambda: self._end
                or bool(self._assignments)
                or workflow.info().get_current_history_length() > _MAX_HISTORY
            )

            if self._end:
                return f"shift-ended:{self._courier_id}:{self._count}"

            while self._assignments:
                order_id = self._assignments.pop(0)
                await workflow.execute_activity(
                    accept_assignment,
                    args=[self._courier_id, order_id],
                    start_to_close_timeout=timedelta(seconds=10),
                )
                self._count += 1
                # Report availability back to the region's dispatch entity.
                try:
                    handle = workflow.get_external_workflow_handle(
                        dispatch_id(self._region)
                    )
                    await handle.signal(
                        "courier_available",
                        CourierAvailable(courier_id=self._courier_id, region=self._region),
                    )
                except Exception:
                    pass

            if workflow.info().get_current_history_length() > _MAX_HISTORY:
                workflow.continue_as_new(
                    args=[
                        CourierState(
                            courier_id=self._courier_id,
                            region=self._region,
                            assignments=self._count,
                            pings=self._pings,
                        )
                    ]
                )
