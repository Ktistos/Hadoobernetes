"""
Job Master — heartbeat_monitor.py
===================================
Diagnostic and introspection utilities for the heartbeat / dead-man's-switch
system described in design doc §5.4.

Why a separate module?
-----------------------
The state_machine handles all *mutation* of timers (reset, cancel, fire).
This module provides *read-only* views of the current heartbeat state,
useful for:
  • structured logging / metrics
  • debugging during development
  • future admin endpoints (e.g. GET /debug/heartbeats)

Nothing in this module modifies the state machine or the database.
Import it wherever you need visibility into timer state without coupling
to the orchestration logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Avoid circular import — only used for type hints
    from state_machine import JobStateMachine, TaskState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes — snapshots of heartbeat state (immutable, safe to serialise)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TaskHeartbeatSnapshot:
    """Immutable snapshot of a single task's heartbeat state."""
    task_id:          int
    role:             str        # "mapper" | "reducer"
    status:           str        # task_status value
    attempt_number:   int
    has_active_timer: bool
    started_at:       datetime | None
    seconds_running:  float | None   # None if not yet started


@dataclass(frozen=True)
class JobHeartbeatReport:
    """Aggregated heartbeat report for a whole job."""
    job_id:              str
    job_status:          str
    mapper_snapshots:    list[TaskHeartbeatSnapshot]
    reducer_snapshots:   list[TaskHeartbeatSnapshot]
    mappers_running:     int
    mappers_completed:   int
    mappers_failed:      int
    reducers_running:    int
    reducers_completed:  int
    reducers_failed:     int


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def snapshot_task(task: "TaskState", role: str) -> TaskHeartbeatSnapshot:
    """
    Build an immutable snapshot of one TaskState for logging or introspection.
    Does not touch any timer handles.
    """
    now = datetime.now(timezone.utc)

    seconds_running: float | None = None
    if task.started_at is not None:
        started = task.started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        seconds_running = (now - started).total_seconds()

    return TaskHeartbeatSnapshot(
        task_id          = task.task_id,
        role             = role,
        status           = task.status,
        attempt_number   = task.attempt_number,
        has_active_timer = (task.timer_handle is not None and not task.timer_handle.cancelled()),
        started_at       = task.started_at,
        seconds_running  = seconds_running,
    )


def build_report(sm: "JobStateMachine") -> JobHeartbeatReport:
    """
    Produce a full JobHeartbeatReport from a live JobStateMachine.
    Safe to call at any time — read-only.
    """
    from state_machine import (   # local import to avoid circular dependency
        TASK_STATUS_RUNNING,
        TASK_STATUS_COMPLETED,
        TASK_STATUS_FAILED,
    )

    mapper_snapshots = [
        snapshot_task(t, "mapper") for t in sm.map_tasks.values()
    ]
    reducer_snapshots = [
        snapshot_task(t, "reducer") for t in sm.reduce_tasks.values()
    ]

    def count_status(snaps: list[TaskHeartbeatSnapshot], status: str) -> int:
        return sum(1 for s in snaps if s.status == status)

    return JobHeartbeatReport(
        job_id             = sm.job_id,
        job_status         = sm.job.get("status", "unknown"),
        mapper_snapshots   = mapper_snapshots,
        reducer_snapshots  = reducer_snapshots,
        mappers_running    = count_status(mapper_snapshots,   TASK_STATUS_RUNNING),
        mappers_completed  = count_status(mapper_snapshots,   TASK_STATUS_COMPLETED),
        mappers_failed     = count_status(mapper_snapshots,   TASK_STATUS_FAILED),
        reducers_running   = count_status(reducer_snapshots,  TASK_STATUS_RUNNING),
        reducers_completed = count_status(reducer_snapshots,  TASK_STATUS_COMPLETED),
        reducers_failed    = count_status(reducer_snapshots,  TASK_STATUS_FAILED),
    )


def log_report(sm: "JobStateMachine") -> None:
    """
    Write a structured summary of the current heartbeat state to the logger.
    Call this periodically or on phase transitions for observability.
    """
    report = build_report(sm)

    logger.info(
        f"[{report.job_id}] Heartbeat report — job_status={report.job_status} | "
        f"mappers: {report.mappers_running} running / "
        f"{report.mappers_completed} done / "
        f"{report.mappers_failed} failed | "
        f"reducers: {report.reducers_running} running / "
        f"{report.reducers_completed} done / "
        f"{report.reducers_failed} failed"
    )

    # Log any tasks that are running but have no active timer — this is a
    # sign of a bug (timer was cancelled without being rescheduled).
    orphaned = [
        s for s in report.mapper_snapshots + report.reducer_snapshots
        if s.status == "running" and not s.has_active_timer
    ]
    if orphaned:
        logger.warning(
            f"[{report.job_id}] ORPHANED TASKS (running but no timer): "
            + ", ".join(f"{s.role}_{s.task_id}" for s in orphaned)
        )


def log_ping_received(job_id: str, worker_id: str, worker_type: str, status: str) -> None:
    """
    Structured log line for every ping received.
    Call this from handle_ping() before dispatching.
    """
    logger.debug(
        f"[{job_id}] Ping received — worker_id={worker_id}, "
        f"type={worker_type}, status={status}"
    )


def log_timeout(job_id: str, role: str, task_id: int, attempt_number: int, max_retries: int) -> None:
    """
    Structured log line when a dead-man's switch fires.
    """
    logger.warning(
        f"[{job_id}] Timer expired — {role}_{task_id} "
        f"(attempt_number={attempt_number}, max_task_retries={max_retries})"
    )