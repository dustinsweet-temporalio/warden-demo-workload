"""Traffic generator and control relay.

Continuously starts executions at the calibrated baseline (about 8-10 APS),
keeps the entity workflows alive, drives the order/dispatch/courier signal flow,
and relays control decisions from demo-control into the fleet through the three
legal channels:

  - start-time flags: stamped into NEW workflow inputs (workflow_failure,
    timeout_spike, close_history_bloat, fanout_storm),
  - signals: set_degradation to a region's DispatchWorkflow (entity_history),
  - (the retry storm is actuated worker-side via the control cache, so the
    generator does nothing special for it.)

The generator is a Temporal client, so reading control state and using
randomness here is fine; none of this runs inside a workflow.
"""
from __future__ import annotations

import asyncio
import random
import uuid
from datetime import timedelta

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from common.config import Config, load_config
from common.connection import connect
from common.constants import (
    BATCH_TQ,
    CONTROL_TQ,
    COURIER_TQ,
    DISPATCH_TQ,
    MENU_TQ,
    ORDER_TQ,
    REGIONS,
    CONTROL_WORKFLOW_ID,
    courier_id as courier_wf_id,
    dispatch_id,
)
from common.models import (
    BatchInput,
    CourierState,
    Degradation,
    DispatchState,
    MenuSyncState,
    OrderInput,
)
from control.schema import (
    CLOSE_HISTORY_BLOAT,
    ENTITY_HISTORY,
    FANOUT_STORM,
    TIMEOUT_SPIKE,
    WORKFLOW_FAILURE,
)
from control.workflow import DemoControlWorkflow
from workflows.batch import BatchAssignmentWorkflow
from workflows.courier import CourierShiftWorkflow
from workflows.dispatch import DispatchWorkflow
from workflows.menu import MenuSyncWorkflow
from workflows.order import OrderWorkflow

RESTAURANTS = [f"rest-{i}" for i in range(1, 21)]
MENU_RESTAURANTS = RESTAURANTS[:4]


class Generator:
    def __init__(self, client: Client, config: Config) -> None:
        self.client = client
        self.config = config
        self._scenarios: dict = {}
        self._couriers: list[str] = []
        self._courier_seq = 0
        self._degraded_region: str | None = None
        self.orders_started = 0

    # ---- control state ---------------------------------------------------

    async def ensure_control(self) -> None:
        try:
            await self.client.start_workflow(
                DemoControlWorkflow.run,
                id=CONTROL_WORKFLOW_ID,
                task_queue=CONTROL_TQ,
            )
            print("[generator] started demo-control", flush=True)
        except WorkflowAlreadyStartedError:
            pass

    async def refresh_scenarios(self) -> None:
        try:
            handle = self.client.get_workflow_handle(CONTROL_WORKFLOW_ID)
            self._scenarios = await handle.query(DemoControlWorkflow.get_state)
        except Exception:
            pass

    def _enabled(self, name: str) -> bool:
        return bool(self._scenarios.get(name, {}).get("enabled", False))

    def _params(self, name: str) -> dict:
        return dict(self._scenarios.get(name, {}).get("params", {}))

    # ---- entities --------------------------------------------------------

    async def ensure_entities(self) -> None:
        for region in REGIONS:
            try:
                await self.client.start_workflow(
                    DispatchWorkflow.run,
                    DispatchState(region=region),
                    id=dispatch_id(region),
                    task_queue=DISPATCH_TQ,
                )
                print(f"[generator] started dispatch-{region}", flush=True)
            except WorkflowAlreadyStartedError:
                pass

        for restaurant in MENU_RESTAURANTS:
            try:
                await self.client.start_workflow(
                    MenuSyncWorkflow.run,
                    MenuSyncState(restaurant_id=restaurant),
                    id=f"menu-{restaurant}",
                    task_queue=MENU_TQ,
                )
            except WorkflowAlreadyStartedError:
                pass

    # ---- loops -----------------------------------------------------------

    async def order_loop(self) -> None:
        while True:
            region = random.choice(REGIONS)
            restaurant = random.choice(RESTAURANTS)
            order_id = uuid.uuid4().hex[:12]

            fail_at_restaurant = self._enabled(WORKFLOW_FAILURE) and (
                random.random() < float(self._params(WORKFLOW_FAILURE).get("failure_probability", 0.4))
            )
            force_timeout = self._enabled(TIMEOUT_SPIKE) and (
                random.random() < float(self._params(TIMEOUT_SPIKE).get("probability", 0.5))
            )

            kwargs = {}
            if force_timeout:
                # A short execution timeout the order will exceed => workflow_timeout.
                kwargs["execution_timeout"] = timedelta(seconds=10)

            try:
                await self.client.start_workflow(
                    OrderWorkflow.run,
                    OrderInput(
                        order_id=order_id,
                        region=region,
                        restaurant_id=restaurant,
                        fail_at_restaurant=fail_at_restaurant,
                        force_timeout=force_timeout,
                    ),
                    id=f"order-{order_id}",
                    task_queue=ORDER_TQ,
                    **kwargs,
                )
                self.orders_started += 1
            except Exception as exc:  # noqa: BLE001
                print(f"[generator] order start failed: {exc}", flush=True)

            await asyncio.sleep(self._order_delay())

    def _order_delay(self) -> float:
        rate = max(0.01, self.config.order_start_rate_per_sec)
        base = 1.0 / rate
        jitter = self.config.order_start_jitter
        return base * (1.0 + random.uniform(-jitter, jitter))

    async def courier_loop(self) -> None:
        # Bring the fleet up to size.
        while len(self._couriers) < self.config.courier_fleet_size:
            await self._start_courier()

        tick = 0
        while True:
            tick += 1
            # GPS pings to every courier each tick.
            for cid in list(self._couriers):
                await self._safe_signal(
                    courier_wf_id(cid), "location_ping", args=[37.0 + random.random(), -122.0 - random.random()]
                )
            # Occasionally hand a courier an assignment (feeds courier_available
            # back to dispatch, real entity texture).
            if self._couriers and random.random() < 0.5:
                cid = random.choice(self._couriers)
                await self._safe_signal(courier_wf_id(cid), "assignment", args=[uuid.uuid4().hex[:8]])
            # Every ~4 ticks retire the oldest courier and start a replacement.
            if tick % 4 == 0 and self._couriers:
                old = self._couriers.pop(0)
                await self._safe_signal(courier_wf_id(old), "end_shift")
                await self._start_courier()
            await asyncio.sleep(15)

    async def _start_courier(self) -> None:
        self._courier_seq += 1
        cid = f"{self._courier_seq}-{uuid.uuid4().hex[:6]}"
        region = random.choice(REGIONS)
        try:
            await self.client.start_workflow(
                CourierShiftWorkflow.run,
                CourierState(courier_id=cid, region=region),
                id=courier_wf_id(cid),
                task_queue=COURIER_TQ,
            )
            self._couriers.append(cid)
        except WorkflowAlreadyStartedError:
            self._couriers.append(cid)
        except Exception as exc:  # noqa: BLE001
            print(f"[generator] courier start failed: {exc}", flush=True)

    async def batch_loop(self) -> None:
        while True:
            await asyncio.sleep(random.uniform(30, 60))
            batch_id = uuid.uuid4().hex[:10]
            if self._enabled(FANOUT_STORM):
                inp = BatchInput(
                    batch_id=batch_id,
                    mode="fanout_storm",
                    children=int(self._params(FANOUT_STORM).get("children", 400)),
                )
            elif self._enabled(CLOSE_HISTORY_BLOAT):
                inp = BatchInput(
                    batch_id=batch_id,
                    mode="sequential_bloat",
                    iterations=int(self._params(CLOSE_HISTORY_BLOAT).get("iterations", 700)),
                )
            else:
                inp = BatchInput(batch_id=batch_id, mode="normal", children=random.randint(5, 10))
            try:
                await self.client.start_workflow(
                    BatchAssignmentWorkflow.run,
                    inp,
                    id=f"batch-{batch_id}",
                    task_queue=BATCH_TQ,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[generator] batch start failed: {exc}", flush=True)

    async def control_loop(self) -> None:
        """Relay entity_history degradation (S2) to a region's dispatch entity."""
        while True:
            await self.refresh_scenarios()
            if self._enabled(ENTITY_HISTORY):
                params = self._params(ENTITY_HISTORY)
                region = params.get("region", "us-west")
                if self._degraded_region and self._degraded_region != region:
                    await self._reset_region(self._degraded_region)
                await self._safe_signal(
                    dispatch_id(region),
                    "set_degradation",
                    args=[
                        Degradation(
                            suspend_can=bool(params.get("suspend_can", True)),
                            inflate_history=bool(params.get("inflate_history", True)),
                            inflate_rate=int(params.get("inflate_rate", 40)),
                        )
                    ],
                )
                self._degraded_region = region
            elif self._degraded_region:
                await self._reset_region(self._degraded_region)
                self._degraded_region = None
            await asyncio.sleep(3)

    async def entity_keepalive_loop(self) -> None:
        """Restart any dispatch/menu entity that died or was terminated (S6 recovery)."""
        while True:
            await asyncio.sleep(20)
            await self.ensure_entities()

    async def _reset_region(self, region: str) -> None:
        await self._safe_signal(dispatch_id(region), "set_degradation", args=[Degradation()])

    async def _safe_signal(self, wf_id: str, signal: str, args: list | None = None) -> None:
        try:
            handle = self.client.get_workflow_handle(wf_id)
            await handle.signal(signal, args=args or [])
        except Exception:
            pass

    async def run(self) -> None:
        await self.ensure_control()
        await self.refresh_scenarios()
        await self.ensure_entities()
        print("[generator] running", flush=True)
        await asyncio.gather(
            self.order_loop(),
            self.courier_loop(),
            self.batch_loop(),
            self.control_loop(),
            self.entity_keepalive_loop(),
        )


async def main() -> None:
    config = load_config()
    client = await connect(config)
    await Generator(client, config).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
