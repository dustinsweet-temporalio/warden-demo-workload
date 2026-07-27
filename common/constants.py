"""Fixed topology constants for the demo workload.

Task queue names are fixed by the topology (one queue per workflow type), so
they live here as constants rather than in the environment. Warden groups
metrics by temporal_workflow_type, so every workflow type name here becomes its
own treemap tile.
"""

# One task queue per workflow type. This makes every Warden tile an isolable
# failure domain and lets the worker-down scenario target one queue.
ORDER_TQ = "order-tq"
DISPATCH_TQ = "dispatch-tq"
COURIER_TQ = "courier-tq"
BATCH_TQ = "batch-tq"
SETTLEMENT_TQ = "settlement-tq"
MENU_TQ = "menu-tq"
CONTROL_TQ = "control-tq"

ALL_TASK_QUEUES = [
    ORDER_TQ,
    DISPATCH_TQ,
    COURIER_TQ,
    BATCH_TQ,
    SETTLEMENT_TQ,
    MENU_TQ,
    CONTROL_TQ,
]

# The five regions each get one long-running DispatchWorkflow entity, with a
# stable workflow id so the generator can always signal the right one.
REGIONS = ["us-west", "us-east", "midwest", "south", "northeast"]

# Singleton control workflow.
CONTROL_WORKFLOW_ID = "demo-control"

# Container name for the worker serving a given queue (docker-compose sets these
# via container_name, so the dashboard can stop/start them over the docker
# socket). e.g. "order-tq" -> "warden-worker-order".
def worker_container_name(queue: str) -> str:
    short = queue[:-3] if queue.endswith("-tq") else queue
    return f"warden-worker-{short}"


def dispatch_id(region: str) -> str:
    return f"dispatch-{region}"


def courier_id(courier: str) -> str:
    return f"courier-{courier}"


def settlement_id(order_id: str) -> str:
    return f"settle-{order_id}"


def candidate_id(batch_uuid: str, n: int) -> str:
    return f"cand-{batch_uuid}-{n}"
