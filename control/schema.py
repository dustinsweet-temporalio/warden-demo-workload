"""Scenario registry: the control-state schema held by DemoControlWorkflow.

Kept pure (plain dicts) so it is safe to import anywhere: the workflow sandbox,
the generator, the dashboard, and the activity-side control cache.

Each scenario is {"enabled": bool, "params": {...}}. Defaults are the disabled,
healthy ("green") state. reset_all() returns every scenario to these defaults.
"""
from __future__ import annotations

from typing import Any

# Scenario keys (Appendix A). Terminate (S6) and worker-down (S7) are one-shot
# or process actions handled by the dashboard, not persistent scenario state,
# but we record their last-invoked time in the map for display.
RETRY_STORM = "retry_storm"
ENTITY_HISTORY = "entity_history"
CLOSE_HISTORY_BLOAT = "close_history_bloat"
WORKFLOW_FAILURE = "workflow_failure"
TIMEOUT_SPIKE = "timeout_spike"
FANOUT_STORM = "fanout_storm"


def default_scenarios() -> dict[str, dict[str, Any]]:
    """The healthy baseline: every scenario disabled with default params."""
    return {
        RETRY_STORM: {"enabled": False, "params": {"failure_probability": 0.6}},
        ENTITY_HISTORY: {
            "enabled": False,
            "params": {
                "region": "us-west",
                "suspend_can": True,
                "inflate_history": True,
                "inflate_rate": 40,
            },
        },
        CLOSE_HISTORY_BLOAT: {"enabled": False, "params": {"iterations": 700}},
        WORKFLOW_FAILURE: {"enabled": False, "params": {"failure_probability": 0.4}},
        TIMEOUT_SPIKE: {"enabled": False, "params": {"mode": "workflow", "probability": 0.5}},
        FANOUT_STORM: {"enabled": False, "params": {"children": 400}},
    }


def merge_scenario(
    state: dict[str, dict[str, Any]], name: str, enabled: bool, params: dict[str, Any]
) -> None:
    """Upsert one scenario into state in place, merging params over defaults."""
    defaults = default_scenarios()
    base = state.get(name) or defaults.get(name) or {"enabled": False, "params": {}}
    merged_params = dict(base.get("params", {}))
    merged_params.update(params or {})
    state[name] = {"enabled": bool(enabled), "params": merged_params}
