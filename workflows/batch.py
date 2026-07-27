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

with workflow.unsafe.imports_passed_through():
    from activities.batch import tiny_eval
    from common.constants import BATCH_TQ, candidate_id
    from common.models import BatchInput, CandidateInput

# Healthy fan-out is small and bounded regardless of the requested children.
_NORMAL_MAX_CHILDREN = 8


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
        n = inp.children if inp.mode == "fanout_storm" else min(inp.children, _NORMAL_MAX_CHILDREN)
        tasks = [
            workflow.execute_child_workflow(
                "CandidateEvalWorkflow",
                CandidateInput(batch_id=inp.batch_id, candidate_id=f"c{i}"),
                id=candidate_id(inp.batch_id, i),
                task_queue=BATCH_TQ,
            )
            for i in range(n)
        ]
        scores = await asyncio.gather(*tasks)
        winners = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:3]
        return {"mode": inp.mode, "children": n, "winners": winners}
