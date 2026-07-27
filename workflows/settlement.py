"""SettlementWorkflow: short child of OrderWorkflow (settlement-tq)."""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.settlement import capture_payment, compute_payout
    from common.models import SettlementInput


@workflow.defn
class SettlementWorkflow:
    @workflow.run
    async def run(self, inp: SettlementInput) -> str:
        await workflow.execute_activity(
            capture_payment,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.execute_activity(
            compute_payout,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return "settled"
