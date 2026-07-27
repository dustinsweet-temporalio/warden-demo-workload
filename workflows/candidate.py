"""CandidateEvalWorkflow: short-lived child of BatchAssignmentWorkflow (batch-tq)."""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.batch import score_candidate
    from common.models import CandidateInput


@workflow.defn
class CandidateEvalWorkflow:
    @workflow.run
    async def run(self, inp: CandidateInput) -> float:
        return await workflow.execute_activity(
            score_candidate,
            inp.candidate_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
