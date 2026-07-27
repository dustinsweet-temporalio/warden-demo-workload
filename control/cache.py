"""In-process control cache (worker-side, non-deterministic; this is allowed).

A background asyncio task in each worker process queries the demo-control
workflow every few seconds and caches the scenario map in a module global.
Activities read the cache synchronously via get_scenario() and never query the
workflow per-activity.

This module imports the Temporal client and MUST NOT be imported inside the
workflow sandbox. Only workers and activities import it.
"""
from __future__ import annotations

import asyncio
from typing import Any

from temporalio.client import Client

from common.constants import CONTROL_WORKFLOW_ID
from control.schema import default_scenarios
from control.workflow import DemoControlWorkflow

# Module-global cache. Reads of a dict reference are safe enough across the
# refresher (event-loop thread) and sync activities (executor threads) for a
# demo; we swap the whole dict atomically on refresh rather than mutating it.
_scenarios: dict[str, dict[str, Any]] = default_scenarios()


def get_scenarios() -> dict[str, dict[str, Any]]:
    return _scenarios


def get_scenario(name: str) -> dict[str, Any]:
    return _scenarios.get(name, {"enabled": False, "params": {}})


def is_enabled(name: str) -> bool:
    return bool(_scenarios.get(name, {}).get("enabled", False))


def get_params(name: str) -> dict[str, Any]:
    return dict(_scenarios.get(name, {}).get("params", {}))


async def run_control_refresher(client: Client, refresh_sec: float) -> None:
    """Continuously refresh the cache from demo-control. Never raises out."""
    global _scenarios
    handle = client.get_workflow_handle(CONTROL_WORKFLOW_ID)
    while True:
        try:
            state = await handle.query(DemoControlWorkflow.get_state)
            if isinstance(state, dict):
                _scenarios = state
        except Exception:
            # demo-control may not be running yet, or a transient error; keep
            # the last known map and try again.
            pass
        await asyncio.sleep(refresh_sec)
