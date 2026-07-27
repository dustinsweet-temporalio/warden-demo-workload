"""Batch-assignment activities (batch-tq)."""
from __future__ import annotations

from temporalio import activity


@activity.defn
def score_candidate(candidate_id: str) -> float:
    # Deterministic-enough cheap score derived from the id; no randomness needed.
    return float(sum(ord(c) for c in candidate_id) % 100) / 100.0


@activity.defn
def tiny_eval(seq: int) -> int:
    """One cheap step per iteration for the sequential-bloat batch (S3)."""
    return seq
