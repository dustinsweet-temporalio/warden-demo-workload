"""Run a single worker for one task queue.

Usage: python -m workers.run_worker <task-queue>

Each worker process:
  - connects to the observed namespace,
  - starts a background control-cache refresher (worker-side, non-deterministic;
    this is how live activity-side faults read scenario state),
  - hosts exactly that queue's workflows and activities.

One process per queue makes the worker-down scenario (S7) able to take one
team's queue offline without starving the rest of the fleet.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import sys

from temporalio.worker import Worker

from common.config import load_config
from common.connection import connect
from control.cache import run_control_refresher
from workers.registry import REGISTRY


async def main(task_queue: str) -> None:
    if task_queue not in REGISTRY:
        raise SystemExit(f"unknown task queue {task_queue!r}; known: {list(REGISTRY)}")

    config = load_config()
    client = await connect(config)
    workflows, activities = REGISTRY[task_queue]

    # Keep this worker's control cache warm so activity-side faults read fresh
    # scenario state without querying demo-control per activity.
    refresher = asyncio.create_task(
        run_control_refresher(client, config.control_cache_refresh_sec)
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=workflows,
            activities=activities,
            activity_executor=executor,
        )
        print(f"[worker] started on {task_queue}", flush=True)
        try:
            await worker.run()
        finally:
            refresher.cancel()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m workers.run_worker <task-queue>")
    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        pass
