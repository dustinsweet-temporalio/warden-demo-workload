"""Operator control surface: FastAPI backend + static single-page UI.

The dashboard is the single writer/reader of scenario state via demo-control
(signals to write, queries to read). It also owns the worker Supervisor so it
can take one queue offline for the worker-down scenario (S7), and it can
terminate DispatchWorkflow instances for S6.

Run: python -m dashboard.app  (serves on DASHBOARD_HOST:DASHBOARD_PORT)
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from common.config import load_config
from common.connection import connect
from common.constants import (
    ALL_TASK_QUEUES,
    CONTROL_TQ,
    CONTROL_WORKFLOW_ID,
)
from common.search_attributes import ORDER_STAGE_UPSERT_ACTIONS
from control.schema import RETRY_STORM, FANOUT_STORM, default_scenarios
from control.workflow import DemoControlWorkflow

_STATIC = Path(__file__).resolve().parent / "static"


def _build_supervisor():
    """Pick the supervisor by mode.

    - docker: control sibling worker CONTAINERS over the docker socket
      (docker-compose owns their lifecycle; we only change running state).
    - process (default): spawn/kill worker SUBPROCESSES locally.
    """
    mode = os.environ.get("SUPERVISOR_MODE", "process").lower()
    if mode == "docker":
        from workers.docker_supervisor import DockerSupervisor

        return DockerSupervisor(), "docker"
    from workers.supervisor import Supervisor

    return Supervisor(), "process"

# Process-wide handles, initialized on startup.
_state: dict = {"client": None, "supervisor": None, "config": None}


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()
    supervisor, mode = _build_supervisor()
    # In process mode the dashboard owns the worker processes, so it spawns
    # them. In docker mode the containers already exist (compose started them),
    # so start_all is a no-op and we leave them running.
    supervisor.start_all()
    print(f"[dashboard] supervisor mode: {mode}", flush=True)
    client = await connect(config)
    # Idempotently ensure the control workflow is running.
    try:
        await client.start_workflow(
            DemoControlWorkflow.run, id=CONTROL_WORKFLOW_ID, task_queue=CONTROL_TQ
        )
    except WorkflowAlreadyStartedError:
        pass
    _state.update(client=client, supervisor=supervisor, config=config)
    print("[dashboard] up; workers supervised, demo-control ensured", flush=True)
    try:
        yield
    finally:
        supervisor.stop_all()


app = FastAPI(title="Warden demo control surface", lifespan=lifespan)


def _client() -> Client:
    client = _state["client"]
    if client is None:
        raise HTTPException(status_code=503, detail="client not ready")
    return client


def _supervisor() -> Supervisor:
    return _state["supervisor"]


def _control():
    return _client().get_workflow_handle(CONTROL_WORKFLOW_ID)


class ScenarioBody(BaseModel):
    enabled: bool
    params: dict = {}


class TerminateBody(BaseModel):
    count: int = 1


@app.get("/api/scenarios")
async def get_scenarios():
    try:
        return await _control().query(DemoControlWorkflow.get_state)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"demo-control unavailable: {exc}")


@app.post("/api/scenarios/{name}")
async def set_scenario(name: str, body: ScenarioBody):
    await _control().signal(
        DemoControlWorkflow.set_scenario, args=[name, body.enabled, body.params]
    )
    return {"ok": True, "name": name, "enabled": body.enabled, "params": body.params}


@app.post("/api/reset")
async def reset_all():
    await _control().signal(DemoControlWorkflow.reset_all)
    return {"ok": True}


@app.post("/api/actions/terminate-dispatch")
async def terminate_dispatch(body: TerminateBody):
    client = _client()
    query = "WorkflowType = 'DispatchWorkflow' AND ExecutionStatus = 'Running'"
    terminated = []
    async for wf in client.list_workflows(query=query):
        await client.get_workflow_handle(wf.id).terminate("demo S6: terminate without CAN")
        terminated.append(wf.id)
        if len(terminated) >= max(1, body.count):
            break
    return {"ok": True, "terminated": terminated}


@app.post("/api/workers/{queue}/{action}")
async def worker_action(queue: str, action: str):
    if queue not in ALL_TASK_QUEUES:
        raise HTTPException(status_code=404, detail=f"unknown queue {queue}")
    sup = _supervisor()
    if action == "stop":
        sup.stop(queue)
    elif action == "start":
        sup.start(queue)
    elif action == "restart":
        sup.restart(queue)
    else:
        raise HTTPException(status_code=400, detail="action must be stop|start|restart")
    return {"ok": True, "queue": queue, "state": sup.states().get(queue)}


@app.get("/api/health")
async def health():
    sup = _supervisor()
    config = _state["config"]
    scenarios = {}
    try:
        scenarios = await _control().query(DemoControlWorkflow.get_state)
    except Exception:
        scenarios = default_scenarios()

    # Rough APS estimate for presenter confidence (not a metric read).
    # An order is ~10 actions, plus one per mid-run OrderStage upsert and one for
    # its settlement's payout upsert when stage tracking is on.
    actions_per_order = 10.0
    if config.order_stage_tracking:
        actions_per_order += ORDER_STAGE_UPSERT_ACTIONS + 1
    order_aps = config.order_start_rate_per_sec * actions_per_order
    baseline_other = 3.7
    est = order_aps + baseline_other
    if scenarios.get(RETRY_STORM, {}).get("enabled"):
        est += 13.0
    if scenarios.get(FANOUT_STORM, {}).get("enabled"):
        est += 10.0

    return {
        "workers": sup.states(),
        "scenarios": scenarios,
        "estimated_aps": round(est, 1),
    }


# Serve the single-page UI at /.
@app.get("/")
async def index():
    return FileResponse(str(_STATIC / "index.html"))


app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


def main() -> None:
    config = load_config()
    uvicorn.run(app, host=config.dashboard_host, port=config.dashboard_port)


if __name__ == "__main__":
    main()
