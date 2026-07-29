"""Run the visibility demo: a set of List Filters over the live fleet.

Reads only. Each entry pairs the business question a presenter would ask with the
List Filter that answers it, then prints the count and a couple of matching
workflow ids so the audience sees real executions behind the number.

    python -m scripts.visibility_demo              # every query
    python -m scripts.visibility_demo stuck late   # only queries matching a word

The same filters work verbatim in the Web UI search bar and with
`temporal workflow list -q '<filter>'`.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone

from common.connection import connect
from common.config import load_config

# (label, list filter). Ordered as a narrative: the fleet, then the business
# slices, then the scenario-specific queries that make a Warden finding concrete.
# List Filters have no now() function, so time-relative filters are stamped with
# a concrete RFC3339 timestamp at run time.
_now = datetime.now(timezone.utc)
_soon = (_now + timedelta(minutes=10)).isoformat()
_now_iso = _now.isoformat()

QUERIES: list[tuple[str, str]] = [
    (
        "Orders in flight right now",
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running'",
    ),
    (
        "In-flight orders, by stage: awaiting a courier",
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running' "
        "AND OrderStage = 'awaiting_courier'",
    ),
    (
        "In-flight orders, by stage: on the road",
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running' "
        "AND OrderStage = 'in_transit'",
    ),
    (
        "Everything happening in us-west (any workflow type)",
        "DeliveryRegion = 'us-west' AND ExecutionStatus = 'Running'",
    ),
    (
        "High-value premium orders still open",
        "PriorityTier = 'premium' AND OrderValueUsd > 75 "
        "AND ExecutionStatus = 'Running'",
    ),
    (
        "Surge-priced orders",
        "IsSurgePricing = true AND ExecutionStatus = 'Running'",
    ),
    (
        "Allergen-relevant orders (KeywordList membership)",
        "DietaryTags IN ('contains-nuts', 'gluten-free') "
        "AND ExecutionStatus = 'Running'",
    ),
    (
        "Contactless drop-offs (Text, word-level match)",
        "DeliveryNotes = 'door' AND ExecutionStatus = 'Running'",
    ),
    (
        "One restaurant group's orders (Keyword prefix)",
        "RestaurantId STARTS_WITH 'rest-1' AND ExecutionStatus = 'Running'",
    ),
    (
        "Orders already past their delivery promise",
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running' "
        f"AND PromisedDeliveryAt < '{_now_iso}'",
    ),
    (
        "Orders due in the next 10 minutes (Datetime range)",
        "WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running' "
        f"AND PromisedDeliveryAt BETWEEN '{_now_iso}' AND '{_soon}'",
    ),
    (
        "S2: dispatch regions reporting a degraded fleet",
        "WorkflowType = 'DispatchWorkflow' AND FleetStatus = 'degraded'",
    ),
    (
        "S8: workflows created by a fan-out storm",
        "BatchMode = 'fanout_storm' AND FanoutSize > 100",
    ),
    (
        "Courier shifts by vehicle: cars on the road",
        "WorkflowType = 'CourierShiftWorkflow' AND VehicleType = 'car' "
        "AND ExecutionStatus = 'Running'",
    ),
    (
        "Settlements that paid a courier more than $4",
        "WorkflowType = 'SettlementWorkflow' AND CourierPayoutUsd > 4.0",
    ),
]

_SAMPLE_IDS = 2


async def main() -> int:
    terms = [t.lower() for t in sys.argv[1:]]
    queries = [
        (label, q)
        for label, q in QUERIES
        if not terms or any(t in label.lower() or t in q.lower() for t in terms)
    ]
    if not queries:
        print("no queries matched")
        return 1

    client = await connect(load_config())
    for label, query in queries:
        print(f"\n\033[1m{label}\033[0m")
        print(f"  {query}")
        try:
            # count_workflows is the cheap aggregate; list_workflows gives the
            # executions behind it. Neither is a billable action.
            count = await client.count_workflows(query=query)
            print(f"  -> {count.count} matching")
            shown = 0
            async for wf in client.list_workflows(query=query):
                print(f"     {wf.id}  ({wf.workflow_type}, {wf.status.name})")
                shown += 1
                if shown >= _SAMPLE_IDS:
                    break
        except Exception as exc:  # noqa: BLE001
            print(f"  !! {exc}")
    print()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        pass
