"""DemoControlWorkflow: the durable state of record for demo scenarios.

Singleton (id "demo-control") on control-tq. Nearly idle, so its tile is tiny.
It holds the scenario map, applies set_scenario / reset_all signals, answers
get_state / get_scenario queries, and continue-as-news to keep its own history
bounded so it never trips a Warden finding on itself.

Determinism note: this workflow only mutates state in response to signals and
reads nothing external, so it is fully deterministic. Workers and activities
read the scenario map through the in-process control cache, never the workflow.
"""
from __future__ import annotations

from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from control.schema import default_scenarios, merge_scenario

# Bound history: continue-as-new after this many applied signals, or when the
# live history length crosses this threshold, whichever comes first.
_MAX_SIGNALS = 500
_MAX_HISTORY = 1500


@workflow.defn
class DemoControlWorkflow:
    def __init__(self) -> None:
        self._scenarios: dict[str, dict[str, Any]] = default_scenarios()
        self._applied = 0

    @workflow.run
    async def run(self, carried: dict[str, dict[str, Any]] | None = None) -> None:
        if carried:
            # Carry forward the scenario map across continue-as-new. Merge over
            # defaults so newly added scenario keys pick up their defaults.
            merged = default_scenarios()
            merged.update(carried)
            self._scenarios = merged

        while True:
            await workflow.wait_condition(
                lambda: self._applied > 0
                or workflow.info().get_current_history_length() > _MAX_HISTORY
            )

            if (
                self._applied >= _MAX_SIGNALS
                or workflow.info().get_current_history_length() > _MAX_HISTORY
            ):
                workflow.continue_as_new(args=[self._scenarios])

            # Reset the per-run counter after handling; state persists.
            self._applied = 0

    @workflow.signal
    async def set_scenario(self, name: str, enabled: bool, params: dict[str, Any]) -> None:
        merge_scenario(self._scenarios, name, enabled, params or {})
        self._applied += 1

    @workflow.signal
    async def reset_all(self) -> None:
        """Return every scenario to its disabled, healthy default (the RESET button)."""
        self._scenarios = default_scenarios()
        self._applied += 1

    @workflow.signal
    async def record_action(self, name: str, when: str) -> None:
        """Record a one-shot action (terminate, worker-down) for display only."""
        self._scenarios.setdefault("_actions", {"enabled": False, "params": {}})
        self._scenarios["_actions"]["params"][name] = when
        self._applied += 1

    @workflow.query
    def get_state(self) -> dict[str, dict[str, Any]]:
        return self._scenarios

    @workflow.query
    def get_scenario(self, name: str) -> dict[str, Any]:
        return self._scenarios.get(name, {"enabled": False, "params": {}})
