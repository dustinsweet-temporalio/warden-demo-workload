"""MenuSyncWorkflow: scheduled low-volume background job (menu-tq).

A cron-style timer loop with continue-as-new: refresh a restaurant's menu, sleep
a few minutes, carry a small state forward. Always-green, low APS.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.menu import fetch_menu, publish_menu, refresh_menu
    from common.models import MenuSyncState


@workflow.defn
class MenuSyncWorkflow:
    @workflow.run
    async def run(self, state: MenuSyncState) -> None:
        await workflow.execute_activity(
            fetch_menu, state.restaurant_id, start_to_close_timeout=timedelta(seconds=15)
        )
        await workflow.execute_activity(
            refresh_menu, state.restaurant_id, start_to_close_timeout=timedelta(seconds=15)
        )
        await workflow.execute_activity(
            publish_menu, state.restaurant_id, start_to_close_timeout=timedelta(seconds=15)
        )
        state.runs += 1
        await workflow.sleep(timedelta(minutes=3))
        workflow.continue_as_new(args=[state])
