# Warden demo workload

> # ⚠️ STOP — THIS COSTS REAL MONEY. NOT INTENDED FOR GENERAL USE. ⚠️
>
> **This is a load-generating workload for testing and demos only. It is designed
> to run continuously and it will bill you accordingly.**
>
> | | |
> |---|---|
> | Sustained rate | **~10 Actions per second** (~11-13 with search attribute stage tracking on) |
> | If left running for a month | **~25-30 million billable Actions** |
> | Rough cost at list Pay-As-You-Go rates | **~$1,000-1,300 per month**, before storage and plan fees |
>
> There is no built-in stop, budget guard, or time limit. It generates load for as
> long as the containers are up, whether or not anyone is watching.
>
> **Do not run this unless you know what you are doing**, and specifically:
>
> - Run it in a **dedicated demo namespace**, never a production one.
> - Know who pays for that namespace, and that they expect this.
> - **`docker compose down` the moment you are done.** Not "later".
> - Do not leave it running overnight, over a weekend, or between demos.
>
> See [Temporal Cloud pricing](https://docs.temporal.io/cloud/pricing) for current
> rates and [Actions](https://docs.temporal.io/cloud/actions) for what counts as a
> billable Action. Your actual bill depends on your account's volume tier and plan.
>

A Delivery Service-flavored Temporal application that runs a fleet of canonical
food-delivery workflows continuously against a Temporal Cloud namespace, plus an
operator control surface (web dashboard) that injects and reverses fault
scenarios on demand.

It is the substrate for a Warden demo. By default the fleet sits at a calm,
green baseline (about 11-13 APS, or 8-10 with search attribute stage tracking
off). Under operator control it exhibits exactly the failure modes a platform
team fears, each producing a specific, visible Warden finding. Every scenario is
controllable and reversible from the dashboard.

Warden itself is not in this repo. This workload runs in the namespace Warden
watches; Warden observes it entirely from the outside (metrics endpoint plus
read-only visibility).

## What is here

```
common/        connection, config, constants, retry policies, shared models,
               custom search attribute vocabulary
control/       DemoControlWorkflow + scenario schema + in-process control cache
activities/    activity functions, one module per domain
workflows/     one file per workflow type; workflow classes only
workers/       per-task-queue worker entrypoint + supervisor
generator.py   continuously starts executions; relays control decisions
dashboard/     FastAPI backend + static single-page UI
scripts/       one-off operator scripts (search attribute registration,
               visibility demo queries)
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

5. Register the custom Search Attributes on the namespace. The fleet sets them,
   so they must exist first: a workflow that sets an unregistered attribute
   fails its workflow task.

   ```
   python -m scripts.register_search_attributes --dry-run   # inspect first
   python -m scripts.register_search_attributes             # register via tcld
   ```

   This needs `tcld` and an API key with write access to the namespace
   (`TEMPORAL_API_KEY` is reused unless `TEMPORAL_CLOUD_API_KEY` is set). You can
   also add them by hand in the Cloud UI under Namespace → Edit → Custom Search
   Attributes. Propagation takes a few seconds and has no SLA, so do this before
   a rehearsal, not during one. See [Visibility demo](#visibility-demo) for what
   each attribute is.

6. Wire this same namespace into Warden's own `.env` as the observed namespace
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

`--build` is not optional after a code change: without it, compose reuses the last
image and you will be demoing stale code.

**The clock starts at `up` and stops at `down`.** Nothing else pauses billing —
closing the dashboard, walking away, or stopping the dashboard container all leave
the generator and workers producing Actions. See the warning at the top.

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

## Visibility demo

The fleet tags itself with one shared vocabulary of custom Search Attributes,
defined once in `common/search_attributes.py` and used by every workflow type, so
a single List Filter cuts across the whole fleet instead of one type at a time.

| Attribute | Type | Set on | Meaning |
|---|---|---|---|
| `DeliveryRegion` | Keyword | order, dispatch, courier, settlement | one of the five regions |
| `RestaurantId` | Keyword | order, settlement, menu sync | `rest-1` .. `rest-20` |
| `CourierId` | Keyword | courier shift, order (in transit), settlement | who is carrying it |
| `OrderStage` | Keyword | order, settlement | `placed`, `preparing`, `awaiting_courier`, `in_transit`, `settling` |
| `PriorityTier` | Keyword | order | `standard` / `plus` / `premium` (premium is ~10%) |
| `OrderValueUsd` | Double | order | basket total, $12-$145 |
| `IsSurgePricing` | Bool | order | ~25% of orders |
| `PromisedDeliveryAt` | Datetime | order | 35 minutes from restaurant acceptance |
| `DietaryTags` | KeywordList | order | e.g. `vegan`, `gluten-free`, `contains-nuts` |
| `DeliveryNotes` | Text | order | free text, word-level searchable |
| `FleetStatus` | Keyword | dispatch | `healthy` / `degraded` — flips with S2 |
| `PendingOrderCount` | Int | dispatch | backlog sampled at the last publish |
| `VehicleType` | Keyword | courier shift | `bike` / `scooter` / `car` |
| `BatchMode` | Keyword | batch + its candidate children | `normal` / `fanout_storm` / `sequential_bloat` |
| `FanoutSize` | Int | batch + its candidate children | children this batch starts |
| `CourierPayoutUsd` | Double | settlement | what the courier earned |

That is 16 attributes: within the Cloud per-type limits (40 Keyword, 20 each of
Int / Double / Bool / Datetime, 5 KeywordList, 5 Text) with room to spare.

### Running the queries

`scripts/visibility_demo.py` pairs the business question with the List Filter
that answers it, prints the count, and shows real executions behind the number:

```
python -m scripts.visibility_demo            # every query
python -m scripts.visibility_demo stage s2   # only queries matching a word
```

Every filter it prints works verbatim in the Web UI search bar and with the CLI:

```
temporal workflow list --address "$TEMPORAL_ADDRESS" \
  --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" --tls \
  -q "OrderStage = 'awaiting_courier' AND ExecutionStatus = 'Running'"
```

Two of them tie the visibility story to the scenarios, which is the pairing worth
demoing: run the query, then point at the Warden tile.

- **During S2**, `FleetStatus = 'degraded'` names the hogging region, and
  `OrderStage = 'awaiting_courier' AND DeliveryRegion = '<that region>'` shows
  the orders pooling behind it while the tile climbs. Reverse S2 and both drain.
- **During S8**, `BatchMode = 'fanout_storm' AND FanoutSize > 100` counts every
  execution the storm created.

### Where values are set, and what it costs

Search Attributes supplied at start time (client start, child start,
continue-as-new) are free. Each in-workflow upsert is one billable action no
matter how many attributes it carries. So:

- The generator stamps everything knowable up front, and re-stamps on
  continue-as-new, so the entity workflows (courier, menu, and a steady dispatch
  region) add no actions at all.
- An order upserts three times, batching values into the stage change that
  reveals them: `preparing` + `PromisedDeliveryAt`, `awaiting_courier`, and
  `in_transit` + `CourierId`. There is no terminal `delivered` stage —
  `ExecutionStatus = 'Completed'` already says that, for free.
- A dispatch region upserts only when its health actually flips, so a running
  region costs one action per S2 toggle, not one per tick.
- A settlement upserts once, for the payout its activity just computed.

That is +4 actions per order (3 stages plus the settlement payout), which the
dashboard's APS estimate accounts for. `ORDER_STAGE_TRACKING=false` turns the
order stage upserts off and reclaims the APS; the start-time attributes stay
either way.

All of it stays replay-safe: upserts happen at fixed points in the saga, from
values already in workflow state or from `workflow.now()`, and the dispatch flip
is driven by a signal recorded in history.

### Rolling this onto a fleet that is already running

The order stage upserts are new commands in the middle of an existing saga, so an
OrderWorkflow that started before the upgrade will hit a nondeterminism error when
its replay reaches the first upsert. Rather than versioning a demo workload, drain
it:

```
docker compose stop generator          # stop starting new orders
                                       # wait ~4 min for in-flight orders to drain
docker compose up -d --build           # bring the fleet up on the new code
```

Any order still running after the drain will fail its workflow task in a loop;
terminate the leftovers:

```
temporal workflow terminate --address "$TEMPORAL_ADDRESS" \
  --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" --tls \
  -q "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running'" --reason "SA upgrade"
```

The entity workflows are safe to leave alone — they emit no new commands on
replay — but a dispatch, courier, or menu instance that predates the upgrade
carries no search attributes until it next calls continue-as-new. To tag them at
once, terminate them with the same command against `DispatchWorkflow` and
`CourierShiftWorkflow`: the generator's keepalive restarts dispatch and menu
entities within 20 seconds, and the courier loop replaces shifts on its own.
Warm up again afterwards, since terminations will move the baseline.

## Reset to green

The dashboard RESET button signals `reset_all`, disabling every scenario. Live
faults stop immediately, new runs are healthy, and degraded entities are
signalled back to healthy and continue-as-new to shed accumulated history. All
findings age out of their 300-second open window and tiles return to green.

For a completely clean slate between full rehearsals, let the fleet drain and
optionally reset Warden's stores (`WARDEN_RESET_STORE=true` on the Warden
worker) to clear old findings and baselines.

## APS budget

Baseline lands ~11-13 APS with search attribute stage tracking on, ~8-10 with it
off. Tune with `.env`:

- `ORDER_START_RATE_PER_SEC` (default 0.6) is the main knob. Orders are ~10
  billable actions each, plus 4 for the search attribute upserts (3 stage
  changes and the settlement payout), so ~0.6/sec ≈ 8.4 APS from orders; the
  entities, batches, and menu sync add the rest.
- `ORDER_STAGE_TRACKING` (default true) turns the order stage upserts off. Set it
  to `false`, or drop `ORDER_START_RATE_PER_SEC` to 0.45, to hold the original
  8-10 APS baseline. Either way the start-time attributes remain, since those are
  free — you lose only the live stage progression.
- `ORDER_START_JITTER` (default 0.2) adds mild natural variation so the anomaly
  detector's baseline is not perfectly flat (which it ignores), while staying
  well under the z=3.5 warning threshold.

The dashboard's estimated-APS readout follows these knobs, so what the presenter
sees matches what is configured.

The S1 storm adds ~10-15 APS of retries, taking the total to ~20-25.

Whatever you choose, keep it fixed across a rehearsal: Warden's anomaly baselines
warm up against the running rate, so changing the search attribute knobs
mid-session moves the baseline out from under the detector.

## Determinism guardrails

- Workflow code never reads the control cache or queries `demo-control`. Only
  workers and activities do.
- All workflow behavior changes come from immutable start-time input or from
  signals (both replay-safe).
- Search attribute upserts happen at fixed points in the code, from values held
  in workflow state or from `workflow.now()`, never from wall-clock time or
  randomness. The dispatch health flip reacts to a signal recorded in history.
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
