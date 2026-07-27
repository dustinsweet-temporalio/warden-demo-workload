"""Docker-backed supervisor: control per-queue worker CONTAINERS.

Used when the workload runs under docker-compose (SUPERVISOR_MODE=docker). The
dashboard container mounts the docker socket, so it can stop, start, and restart
the sibling worker containers by name. This is how the worker-down scenario (S7)
takes one team's queue offline and how the UI kills/restarts any worker.

The containers themselves are created and torn down by docker-compose; this
supervisor only changes their running state, so start_all/stop_all are no-ops.
"""
from __future__ import annotations

import docker

from common.constants import ALL_TASK_QUEUES, worker_container_name


class DockerSupervisor:
    def __init__(self, queues: list[str] | None = None) -> None:
        self._queues = queues or list(ALL_TASK_QUEUES)
        self._client = docker.from_env()

    def _get(self, queue: str):
        try:
            container = self._client.containers.get(worker_container_name(queue))
            container.reload()
            return container
        except Exception:
            return None

    def start(self, queue: str) -> None:
        container = self._get(queue)
        if container is not None and container.status != "running":
            container.start()

    def stop(self, queue: str) -> None:
        container = self._get(queue)
        if container is not None and container.status == "running":
            container.stop(timeout=10)

    def restart(self, queue: str) -> None:
        container = self._get(queue)
        if container is not None:
            container.restart(timeout=10)

    def is_running(self, queue: str) -> bool:
        container = self._get(queue)
        return bool(container is not None and container.status == "running")

    def states(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for queue in self._queues:
            container = self._get(queue)
            out[queue] = container.status if container is not None else "absent"
        return out

    # Containers are managed by docker-compose lifecycle, not by this process.
    def start_all(self) -> None:
        return None

    def stop_all(self) -> None:
        return None
