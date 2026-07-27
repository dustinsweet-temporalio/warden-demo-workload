"""Worker supervisor: one child process per task queue, individually restartable.

Used two ways:
  - `python -m workers.supervisor` runs the whole fleet standalone (headless
    warm-up, or when you do not need the worker-down scenario).
  - The dashboard instantiates a Supervisor and drives stop/start on one queue
    for the worker-down scenario (S7).

Each worker is a subprocess running `python -m workers.run_worker <queue>`,
inheriting this project's environment (and .env via the worker).
"""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

from common.constants import ALL_TASK_QUEUES

_REPO_ROOT = Path(__file__).resolve().parent.parent


class Supervisor:
    def __init__(self, queues: list[str] | None = None) -> None:
        self._queues = queues or list(ALL_TASK_QUEUES)
        self._procs: dict[str, subprocess.Popen] = {}

    def start(self, queue: str) -> None:
        if queue not in self._queues:
            raise ValueError(f"unknown queue {queue!r}")
        existing = self._procs.get(queue)
        if existing and existing.poll() is None:
            return  # already running
        proc = subprocess.Popen(
            [sys.executable, "-m", "workers.run_worker", queue],
            cwd=str(_REPO_ROOT),
        )
        self._procs[queue] = proc

    def stop(self, queue: str) -> None:
        proc = self._procs.get(queue)
        if not proc or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    def restart(self, queue: str) -> None:
        self.stop(queue)
        self.start(queue)

    def start_all(self) -> None:
        for queue in self._queues:
            self.start(queue)

    def stop_all(self) -> None:
        for queue in list(self._procs):
            self.stop(queue)

    def is_running(self, queue: str) -> bool:
        proc = self._procs.get(queue)
        return bool(proc and proc.poll() is None)

    def states(self) -> dict[str, str]:
        return {
            q: ("running" if self.is_running(q) else "stopped") for q in self._queues
        }


def _run_standalone() -> None:
    sup = Supervisor()
    sup.start_all()
    print(f"[supervisor] started workers: {', '.join(sup._queues)}", flush=True)

    stopping = {"flag": False}

    def _handle(_signum, _frame):
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        while not stopping["flag"]:
            # Restart any worker that has died so the fleet self-heals.
            for queue in sup._queues:
                if not sup.is_running(queue):
                    print(f"[supervisor] restarting {queue}", flush=True)
                    sup.start(queue)
            time.sleep(2)
    finally:
        print("[supervisor] stopping all workers", flush=True)
        sup.stop_all()


if __name__ == "__main__":
    _run_standalone()
