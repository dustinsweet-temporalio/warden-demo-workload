# Warden demo workload

A Delivery Service-flavored Temporal application that runs a fleet of canonical
food-delivery workflows continuously against a Temporal Cloud namespace, plus an
operator control surface (web dashboard) that injects and reverses fault
scenarios on demand.

It is the substrate for a Warden demo. By default the fleet sits at a calm,
green baseline (about 8-10 APS). Under operator control it exhibits exactly the
failure modes a platform team fears, each producing a specific, visible Warden
finding. Every scenario is controllable and reversible from the dashboard.

Warden itself is not in this repo. This workload runs in the namespace Warden
watches; Warden observes it entirely from the outside (metrics endpoint plus
read-only visibility).

## What is here

```
common/        connection, config, constants, retry policies, shared models
control/       DemoControlWorkflow + scenario schema + in-process control cache
activities/    activity functions, one module per domain
workflows/     one file per workflow type; workflow classes only
workers/       per-task-queue worker entrypoint + supervisor
generator.py   continuously starts executions; relays control decisions
dashboard/     FastAPI backend + static single-page UI
```

### The fleet (each type is its own Warden tile)

| Type | Task queue | Role |
|---|---|---|
| `OrderWorkflow` | `order-tq` | Fixed saga, the APS backbone (dominant traffic) |
| `DispatchWorkflow` | `dispatch-tq` | Per-region entity, continue-as-new, signal/react |
| `CourierShiftWorkflow` | `courier-tq` | Per-courier entity |
| `BatchAssignmentWorkflow` | `batch-tq` | Surge fan-out with child workflows |
| `CandidateEvalWorkflow` | `batch-tq` | Child of the batch workflow |
| `SettlementWorkflow` | `settlement-tq` | Short child of an order |
| `MenuSyncWorkflow` | `menu-tq` | Scheduled low-volume background job |
| `DemoControlWorkflow` | `control-tq` | Durable scenario state of record (near-idle) |

## Setup

1. Python 3.9+ and a Temporal Cloud namespace with an API key.

2. Install dependencies (a virtualenv is recommended):

   ```
   python -m venv .venv && . .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Copy `.env.example` to `.env` and fill in the connection to the **observed**
   namespace (the one Warden watches):

   ```
   TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233
   TEMPORAL_NAMESPACE=<namespace>.<account>
   TEMPORAL_API_KEY=<api-key>
   ```

4. Confirm connectivity:

   ```
   temporal workflow list --address "$TEMPORAL_ADDRESS" \
     --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" --tls --limit 1 -o json
   ```

5. Wire this same namespace into Warden's own `.env` as the observed namespace
   (`WARDEN_NAMESPACE`, plus `WARDEN_TARGET_ADDRESS` / `WARDEN_TARGET_NAMESPACE`
   / `WARDEN_TARGET_API_KEY` with a read-only identity). The visibility plane is
   required for S2, S3, and the running-history rule.

## Bring-up

### Option A: Docker (recommended, one command)

Everything runs in containers: one worker container per task queue, the
generator, and the dashboard. No Temporal server runs here; the containers
connect to the observed Temporal Cloud namespace using `.env`.

```
cp .env.example .env        # then fill in your Cloud connection

docker compose up -d --build   # bring EVERYTHING up
docker compose down            # tear EVERYTHING down
```

Then open http://127.0.0.1:8800.

Useful:

```
docker compose logs -f generator dashboard   # follow logs
docker compose ps                             # container status
```

**Killing and restarting workers from the UI.** Each task queue is its own
container (`warden-worker-order`, `warden-worker-courier`, etc.). The dashboard
container mounts the docker socket, so the worker-health row has **kill / start**
and **restart** buttons per queue. This is how you run the worker-down scenario
(S7): kill `courier-tq` and watch `no_poller_tasks` appear, then restart it. The
worker containers use `restart: unless-stopped`, so a killed worker stays down
until you start it, while a crashed worker self-heals.

> The socket mount (`/var/run/docker.sock`) is what lets the dashboard control
> sibling containers. It is required for the in-UI kill/restart controls.

### Option B: local Python processes (no Docker)

The dashboard supervises the worker *subprocesses* directly and idempotently
ensures `demo-control` is running:

```
# terminal 1: dashboard — spawns one worker per task queue, ensures demo-control
python -m dashboard.app          # serves http://127.0.0.1:8800

# terminal 2: the traffic generator
python generator.py
```

Or run the fleet without the dashboard (headless warm-up, no S7):

```
python -m workers.supervisor     # runs and self-heals one worker per queue
```

The dashboard chooses how it controls workers via `SUPERVISOR_MODE`
(`docker` in compose, `process` by default for local runs).

**Warm-up.** Let the fleet run at least 10-15 minutes before demoing so Warden's
anomaly baselines warm up (20 windows at a 30s cadence) and Cloud metrics fully
populate. Do not stop the generator between rehearsals.

## The scenarios

Drive these from the dashboard. Five make up the recommended path; the rest are
secondary or optional.

| # | Scenario | Host | Warden finding | Tile |
|---|---|---|---|---|
| S1 | Retry storm on flaky payments (PRIMARY) | OrderWorkflow | `activity_failure_ratio` + `billable_action_count` anomaly | Order red |
| S2 | Running entity history grows unbounded (PRIMARY) | one DispatchWorkflow region | `running_overlong_history` (while running) | Dispatch amber→red |
| S3 | Overlong history on a workflow that closes | BatchAssignmentWorkflow | `overlong_history` (closed) | Batch amber |
| S4 | Workflow failure spike | OrderWorkflow | `workflow_failure_ratio` | Order amber/red |
| S5 | Timeout spike | OrderWorkflow | `elevated_timeout_rate` | Order amber |
| S6 | Terminate without continue-as-new | DispatchWorkflow | `terminate_without_continue_as_new` | Dispatch amber |
| S7 | Worker fleet down, no pollers | `courier-tq` worker | `no_poller_tasks` (namespace) | `(namespace)` |
| S8 | Runaway fan-out APS (OPTIONAL) | BatchAssignmentWorkflow | `billable_action_count` anomaly (+ `resource_exhausted` if throttled) | Batch |

### Recommended narrative (about 12 minutes)

1. Warm up (≥10 min). All tiles green, total APS 8-10, zero open findings.
2. **S2** on one region. The DispatchWorkflow tile climbs amber then red while
   the instance is still running; the drill-down shows the hogging region
   dwarfing its siblings. Reverse it and show it recover.
3. **S1**. The OrderWorkflow tile goes red; total APS jumps to 20-25. Reverse it.
4. **S7** (or S6). A finding appears from a different failure class.
5. **RESET**. Everything returns to green. Close on "you saw all of that before
   it became an incident, on workflows you did not write."

### Notes per scenario

- **S1 retry storm** is actuated live, worker-side: `authorize_payment` reads
  the in-process control cache and fails with the configured probability. Its
  retry policy is flat at ~1/sec and effectively non-exhausting, so orders retry
  continuously (billable) and recover instantly when you toggle it off.
- **S2 running history** is actuated by a signal to the region's dispatch entity
  (`set_degradation`). The region stops calling continue-as-new and grows its
  history with cheap **local** activities, so HISTORY grows without confounding
  the APS story. Reversing it calls continue-as-new on the next tick, shedding
  the accumulated history. With `inflate_rate` ~40 and Warden thresholds 2000
  (warning) / 6000 (critical), the tile goes amber in ~50s and red in ~2.5min.
- **S3 / S4 / S5 / S8** are actuated by **start-time flags** stamped by the
  generator into new executions, so in-flight healthy runs are never corrupted;
  the fault drains as new bad runs appear and stops when you toggle it off.
- **S6 terminate** and **S7 worker-down** are one-shot / process actions on the
  dashboard. The generator restarts terminated dispatch entities; the dashboard
  restarts the stopped worker.
- **S8** only produces `resource_exhausted` if the namespace APS limit is low
  enough to be tripped. Pre-demo, lower the namespace APS limit so a moderate
  fan-out throttles; restore it afterward. Otherwise treat S8 as an
  APS-anomaly-only scenario.

## Reset to green

The dashboard RESET button signals `reset_all`, disabling every scenario. Live
faults stop immediately, new runs are healthy, and degraded entities are
signalled back to healthy and continue-as-new to shed accumulated history. All
findings age out of their 300-second open window and tiles return to green.

For a completely clean slate between full rehearsals, let the fleet drain and
optionally reset Warden's stores (`WARDEN_RESET_STORE=true` on the Warden
worker) to clear old findings and baselines.

## APS budget

Baseline lands ~8-10 APS. Tune with `.env`:

- `ORDER_START_RATE_PER_SEC` (default 0.6) is the main knob. Orders are ~10
  billable actions each, so ~0.6/sec ≈ 6 APS from orders; the entities, batches,
  settlements, and menu sync add the rest.
- `ORDER_START_JITTER` (default 0.2) adds mild natural variation so the anomaly
  detector's baseline is not perfectly flat (which it ignores), while staying
  well under the z=3.5 warning threshold.

The S1 storm adds ~10-15 APS of retries, taking the total to ~20-25.

## Determinism guardrails

- Workflow code never reads the control cache or queries `demo-control`. Only
  workers and activities do.
- All workflow behavior changes come from immutable start-time input or from
  signals (both replay-safe).
- Every side effect is an activity or local activity. Timers use
  `workflow.sleep`.
- Entity workflows (dispatch, courier) and the control workflow call
  continue-as-new to keep their own history bounded, so they never trip a Warden
  finding on themselves. The one exception is S2, which deliberately suspends
  continue-as-new on a single region to demonstrate unbounded running history.

## How the control plane actuates faults

`DemoControlWorkflow` (id `demo-control`) is the durable state of record. The
dashboard writes state only by signalling it and reads state only by querying
it. Running code picks up decisions through exactly one of three legal channels:

| Channel | Who reads control state | Used for |
|---|---|---|
| In-process control cache | Workers/activities (a background task queries `demo-control` every few seconds) | Live activity-side faults (S1) |
| Start-time parameters | The generator stamps flags into new executions | S3, S4, S5, S8 |
| Signals | The generator / dashboard signal specific running workflows | S2 (`set_degradation`), S6 (terminate) |
