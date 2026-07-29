"""SettlementWorkflow: short child of OrderWorkflow (settlement-tq).

Visibility: region / restaurant / courier arrive as start-time attributes from
the parent order. The payout is only known once the activity returns, so it is
the one upsert here (one billable action per settled order).
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.settlement import capture_payment, compute_payout
    from common import search_attributes as sa
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
        payout = await workflow.execute_activity(
            compute_payout,
            inp.order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        workflow.upsert_search_attributes(
            [sa.COURIER_PAYOUT_USD.value_set(float(payout))]
        )
        return "settled"
