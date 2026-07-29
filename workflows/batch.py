"""BatchAssignmentWorkflow: surge fan-out with child workflows (batch-tq).

Healthy: a small bounded fan-out to CandidateEvalWorkflow children, aggregate,
close small. Two fault modes are chosen by immutable start-time input:

  - fanout_storm (S8): fan out to a very large number of children, spiking APS.
  - sequential_bloat (S3): loop over a large candidate list awaiting a tiny
    activity each iteration, accumulate >2000 events, then close (the
    closed-execution overlong-history footgun).
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributePair, TypedSearchAttributes

with workflow.unsafe.imports_passed_through():
    from activities.batch import tiny_eval
    from common import search_attributes as sa
    from common.constants import BATCH_TQ, candidate_id
    from common.models import BatchInput, CandidateInput

# Healthy fan-out is small and bounded regardless of the requested children.
# Public so the generator can stamp the effective FanoutSize at start time.
NORMAL_MAX_CHILDREN = 8


def effective_fanout(inp: BatchInput) -> int:
    """How many children this batch will actually start."""
    if inp.mode == "sequential_bloat":
        return 0
    if inp.mode == "fanout_storm":
        return inp.children
    return min(inp.children, NORMAL_MAX_CHILDREN)


@workflow.defn
class BatchAssignmentWorkflow:
    @workflow.run
    async def run(self, inp: BatchInput) -> dict:
        if inp.mode == "sequential_bloat":
            # S3: no fan-out, no continue-as-new; close with a large history.
            for i in range(inp.iterations):
                await workflow.execute_activity(
                    tiny_eval,
                    i,
                    start_to_close_timeout=timedelta(seconds=10),
                )
            return {"mode": "sequential_bloat", "iterations": inp.iterations}

        # normal or fanout_storm: fan out to children and aggregate scores.
        n = effective_fanout(inp)
        # Children inherit the batch's mode and size as start-time attributes, so
        # one List Filter counts every workflow a storm created (free).
        child_attrs = TypedSearchAttributes(
            [
                SearchAttributePair(sa.BATCH_MODE, inp.mode),
                SearchAttributePair(sa.FANOUT_SIZE, n),
            ]
        )
        tasks = [
            workflow.execute_child_workflow(
                "CandidateEvalWorkflow",
                CandidateInput(batch_id=inp.batch_id, candidate_id=f"c{i}"),
                id=candidate_id(inp.batch_id, i),
                task_queue=BATCH_TQ,
                search_attributes=child_attrs,
            )
            for i in range(n)
        ]
        scores = await asyncio.gather(*tasks)
        winners = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:3]
        return {"mode": inp.mode, "children": n, "winners": winners}
