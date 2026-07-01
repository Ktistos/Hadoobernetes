"""
tests/test_job_master.py
========================
Comprehensive unit tests for all four Job Master modules:
  - state_machine.py   (JobStateMachine, TaskState)
  - main.py            (FastAPI endpoints)
  - heartbeat_monitor.py (snapshot_task, build_report, log_report)
  - worker_spawner.py  (_job_name, _base_env, spawn_mapper, spawn_reducer)

Run with:
    pip install pytest pytest-asyncio httpx fastapi
    PYTHONPATH=job_master pytest tests/test_job_master.py -v

All tests mock DB, Kubernetes, and HTTP clients — no live infrastructure needed.
"""

import asyncio
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Environment bootstrap — must happen before any job_master import
# ---------------------------------------------------------------------------

os.environ.setdefault("JOB_ID",                  "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
os.environ.setdefault("POSTGRES_HOST",            "postgres")
os.environ.setdefault("POSTGRES_PORT",            "5432")
os.environ.setdefault("POSTGRES_USER",            "test")
os.environ.setdefault("POSTGRES_PASSWORD",        "test")
os.environ.setdefault("POSTGRES_DB",              "test")
os.environ.setdefault("CLUSTER_MANAGER_URL",      "http://cluster-manager:8000")
os.environ.setdefault("JOB_MASTER_SERVICE_URL",   "http://job-master-svc-aaaaaaaa:8000")
os.environ.setdefault("MINIO_ENDPOINT",           "minio:9000")
os.environ.setdefault("MINIO_ACCESS_KEY",         "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY",         "minioadmin")
os.environ.setdefault("MINIO_BUCKET",             "mapreduce")
os.environ.setdefault("K8S_NAMESPACE",            "default")

import state_machine as sm_module
from state_machine import (
    JobStateMachine, TaskState,
    JOB_STATUS_PENDING, JOB_STATUS_MAPPING, JOB_STATUS_REDUCING,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED,
    TASK_STATUS_PENDING, TASK_STATUS_RUNNING,
    TASK_STATUS_COMPLETED, TASK_STATUS_FAILED,
    MIN_RESTART_TIMEOUT,
)
import heartbeat_monitor as hm_module
from heartbeat_monitor import (
    snapshot_task, build_report, log_report,
    log_ping_received, log_timeout,
    TaskHeartbeatSnapshot, JobHeartbeatReport,
)
import worker_spawner as ws_module
from worker_spawner import _job_name, spawn_mapper, spawn_reducer, delete_worker_job


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

JOB_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

SAMPLE_JOB = {
    "job_id":                   JOB_ID,
    "user_id":                  "user_01",
    "status":                   JOB_STATUS_PENDING,
    "input_data_path":          "input/dataset.txt",
    "output_data_path":         "output/result/",
    "intermediate_prefix":      f"intermediate/{JOB_ID}/",
    "code_location":            "code/wordcount.py",
    "input_file_size_bytes":    1000,
    "completed_mappers_count":  0,
    "completed_reducers_count": 0,
    "created_at":               None,
    "started_at":               None,
    "completed_at":             None,
    "failure_reason":           None,
}

SAMPLE_CONFIG = {
    "job_id":                   JOB_ID,
    "num_mappers":              2,
    "num_reducers":             2,
    "default_chunk_size_bytes": 500,
    "worker_timeout_seconds":   30,
    "max_task_retries":         3,
}


def make_sm(job: dict = None, config: dict = None) -> JobStateMachine:
    """Build a JobStateMachine with pre-loaded state and mocked DB."""
    machine      = JobStateMachine(JOB_ID)
    machine.db   = AsyncMock()
    machine.job  = dict(job    or SAMPLE_JOB)
    machine.config = dict(config or SAMPLE_CONFIG)
    return machine


def add_map_tasks(machine: JobStateMachine, statuses: list[str]) -> None:
    for i, status in enumerate(statuses):
        t = TaskState(i, machine.config["worker_timeout_seconds"])
        t.status = status
        machine.map_tasks[i] = t


def add_reduce_tasks(machine: JobStateMachine, statuses: list[str]) -> None:
    for i, status in enumerate(statuses):
        t = TaskState(i, machine.config["worker_timeout_seconds"])
        t.status = status
        machine.reduce_tasks[i] = t


# ===========================================================================
# 1. TaskState
# ===========================================================================

class TestTaskState:

    def test_default_values(self):
        t = TaskState(task_id=5, timeout_seconds=30)
        assert t.task_id        == 5
        assert t.status         == TASK_STATUS_PENDING
        assert t.attempt_number == 0
        assert t.started_at     is None
        assert t.timer_handle   is None
        assert t.timeout_seconds == 30

    def test_slots_prevent_arbitrary_attributes(self):
        t = TaskState(0, 10)
        with pytest.raises(AttributeError):
            t.nonexistent = "value"


# ===========================================================================
# 3. JobStateMachine — initialisation
# ===========================================================================

class TestInitialization:

    @pytest.mark.asyncio
    async def test_missing_job_raises(self):
        machine = JobStateMachine(JOB_ID)
        db_mock = AsyncMock()
        db_mock.fetchrow = AsyncMock(return_value=None)
        db_mock.fetch    = AsyncMock(return_value=[])

        with patch("state_machine.asyncpg.connect", return_value=db_mock) as mock_connect:
            with pytest.raises(RuntimeError, match="not found in DB"):
                await machine.initialize()
        mock_connect.assert_called_once_with(
            user="test",
            password="test",
            database="test",
            host="postgres",
            port=5432,
        )

    @pytest.mark.asyncio
    async def test_missing_config_raises(self):
        machine = JobStateMachine(JOB_ID)
        db_mock = AsyncMock()
        db_mock.fetchrow = AsyncMock(side_effect=[
            SAMPLE_JOB,   # jobs row found
            None,          # job_config row missing
        ])
        db_mock.fetch = AsyncMock(return_value=[])

        with patch("state_machine.asyncpg.connect", return_value=db_mock) as mock_connect:
            with pytest.raises(RuntimeError, match="job_config"):
                await machine.initialize()
        mock_connect.assert_called_once_with(
            user="test",
            password="test",
            database="test",
            host="postgres",
            port=5432,
        )

# ===========================================================================
# 3. JobStateMachine — run() resume logic
# ===========================================================================

class TestRunResumeLogic:

    @pytest.mark.asyncio
    async def test_pending_creates_tasks_and_starts_mapping(self):
        machine = make_sm(job={**SAMPLE_JOB, "status": JOB_STATUS_PENDING})

        with patch.object(machine, "_create_map_tasks", new_callable=AsyncMock) as mock_create, \
             patch.object(machine, "_set_job_status",   new_callable=AsyncMock) as mock_status, \
             patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn:

            async def _create_side():
                add_map_tasks(machine, [TASK_STATUS_PENDING, TASK_STATUS_PENDING])
            mock_create.side_effect = _create_side

            await machine.run()

        mock_create.assert_called_once()
        mock_status.assert_called_once_with(JOB_STATUS_MAPPING)
        assert mock_spawn.call_count == 2

    @pytest.mark.asyncio
    async def test_mapping_restart_reattaches_running_timers(self):
        machine = make_sm(job={**SAMPLE_JOB, "status": JOB_STATUS_MAPPING})
        add_map_tasks(machine, [TASK_STATUS_RUNNING, TASK_STATUS_COMPLETED])

        with patch.object(machine, "_attach_timer_with_remaining") as mock_attach, \
             patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn:
            await machine.run()

        # Only the RUNNING task gets a re-attached timer
        mock_attach.assert_called_once()
        # COMPLETED task is not re-spawned
        mock_spawn.assert_not_called()

    @pytest.mark.asyncio
    async def test_mapping_restart_respawns_pending_tasks(self):
        machine = make_sm(job={**SAMPLE_JOB, "status": JOB_STATUS_MAPPING})
        add_map_tasks(machine, [TASK_STATUS_PENDING, TASK_STATUS_COMPLETED])

        with patch.object(machine, "_attach_timer_with_remaining"), \
             patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn:
            await machine.run()

        # Only the PENDING task is re-spawned
        mock_spawn.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_terminal_state_does_nothing(self):
        for terminal in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
            machine = make_sm(job={**SAMPLE_JOB, "status": terminal})
            with patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as ms, \
                 patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as rs:
                await machine.run()
            ms.assert_not_called()
            rs.assert_not_called()


# ===========================================================================
# 5. Mapper ping handling
# ===========================================================================

class TestMapperPingHandling:

    @pytest.mark.asyncio
    async def test_started_resets_timer(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        with patch.object(machine, "_reset_timer") as mock_reset:
            await machine._handle_mapper_ping(0, "started")
        mock_reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_alive_resets_timer(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        with patch.object(machine, "_reset_timer") as mock_reset:
            await machine._handle_mapper_ping(0, "alive")
        mock_reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_completed_marks_task_and_updates_db(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        machine.map_tasks[0].timer_handle = MagicMock()

        with patch.object(machine, "_check_mapping_complete", new_callable=AsyncMock):
            await machine._handle_mapper_ping(0, "completed")

        assert machine.map_tasks[0].status == TASK_STATUS_COMPLETED
        assert machine.db.execute.call_count >= 2  # map_tasks + jobs counter

    @pytest.mark.asyncio
    async def test_completed_cancels_timer(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        mock_handle = MagicMock()
        machine.map_tasks[0].timer_handle = mock_handle

        with patch.object(machine, "_check_mapping_complete", new_callable=AsyncMock):
            await machine._handle_mapper_ping(0, "completed")

        mock_handle.cancel.assert_called_once()
        assert machine.map_tasks[0].timer_handle is None

    @pytest.mark.asyncio
    async def test_stale_completed_ping_is_ignored(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_COMPLETED])
        initial_calls = machine.db.execute.call_count
        await machine._handle_mapper_ping(0, "completed")
        assert machine.db.execute.call_count == initial_calls

    @pytest.mark.asyncio
    async def test_unknown_mapper_id_is_ignored(self):
        machine = make_sm()
        # map_tasks is empty — should not raise
        await machine._handle_mapper_ping(99, "alive")

    @pytest.mark.asyncio
    async def test_completed_triggers_check_mapping_complete(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        machine.map_tasks[0].timer_handle = MagicMock()

        with patch.object(machine, "_check_mapping_complete", new_callable=AsyncMock) as mock_check:
            await machine._handle_mapper_ping(0, "completed")

        mock_check.assert_called_once()


# ===========================================================================
# 6. Reducer ping handling
# ===========================================================================

class TestReducerPingHandling:

    @pytest.mark.asyncio
    async def test_started_resets_timer(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])
        with patch.object(machine, "_reset_timer") as mock_reset:
            await machine._handle_reducer_ping(0, "started")
        mock_reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_alive_resets_timer(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])
        with patch.object(machine, "_reset_timer") as mock_reset:
            await machine._handle_reducer_ping(0, "alive")
        mock_reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_completed_marks_task_and_updates_db(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])
        machine.reduce_tasks[0].timer_handle = MagicMock()

        with patch.object(machine, "_check_reducing_complete", new_callable=AsyncMock):
            await machine._handle_reducer_ping(0, "completed")

        assert machine.reduce_tasks[0].status == TASK_STATUS_COMPLETED
        assert machine.db.execute.call_count >= 2

    @pytest.mark.asyncio
    async def test_stale_completed_ping_is_ignored(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_COMPLETED])
        initial_calls = machine.db.execute.call_count
        await machine._handle_reducer_ping(0, "completed")
        assert machine.db.execute.call_count == initial_calls

    @pytest.mark.asyncio
    async def test_unknown_reducer_id_is_ignored(self):
        machine = make_sm()
        await machine._handle_reducer_ping(99, "alive")


# ===========================================================================
# 7. Mapper timeout / retry
# ===========================================================================

class TestMapperTimeout:

    @pytest.mark.asyncio
    async def test_timeout_under_max_retries_spawns_retry(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        machine.map_tasks[0].attempt_number = 1

        with patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn:
            await machine._handle_mapper_timeout(0)

        mock_spawn.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_timeout_at_max_retries_fails_job(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        machine.map_tasks[0].attempt_number = machine.config["max_task_retries"]

        with patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn, \
             patch.object(machine, "_fail_job", new_callable=AsyncMock) as mock_fail:
            await machine._handle_mapper_timeout(0)

        mock_spawn.assert_not_called()
        mock_fail.assert_called_once()
        assert "max_task_retries" in mock_fail.call_args[0][0]

    @pytest.mark.asyncio
    async def test_timeout_on_completed_task_is_noop(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_COMPLETED])

        with patch.object(machine, "_spawn_and_track_mapper", new_callable=AsyncMock) as mock_spawn, \
             patch.object(machine, "_fail_job", new_callable=AsyncMock) as mock_fail:
            await machine._handle_mapper_timeout(0)

        mock_spawn.assert_not_called()
        mock_fail.assert_not_called()

    @pytest.mark.asyncio
    async def test_timeout_sets_task_failed_at_max_retries(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        machine.map_tasks[0].attempt_number = machine.config["max_task_retries"]

        with patch.object(machine, "_fail_job", new_callable=AsyncMock):
            await machine._handle_mapper_timeout(0)

        assert machine.map_tasks[0].status == TASK_STATUS_FAILED


# ===========================================================================
# 8. Reducer timeout / retry
# ===========================================================================

class TestReducerTimeout:

    @pytest.mark.asyncio
    async def test_timeout_under_max_retries_spawns_retry(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])
        machine.reduce_tasks[0].attempt_number = 1

        with patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as mock_spawn:
            await machine._handle_reducer_timeout(0)

        mock_spawn.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_timeout_at_max_retries_fails_job(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])
        machine.reduce_tasks[0].attempt_number = machine.config["max_task_retries"]

        with patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as mock_spawn, \
             patch.object(machine, "_fail_job", new_callable=AsyncMock) as mock_fail:
            await machine._handle_reducer_timeout(0)

        mock_spawn.assert_not_called()
        mock_fail.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_on_completed_task_is_noop(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_COMPLETED])

        with patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as mock_spawn, \
             patch.object(machine, "_fail_job", new_callable=AsyncMock) as mock_fail:
            await machine._handle_reducer_timeout(0)

        mock_spawn.assert_not_called()
        mock_fail.assert_not_called()


# ===========================================================================
# 9. Phase transitions
# ===========================================================================

class TestPhaseTransitions:

    @pytest.mark.asyncio
    async def test_all_mappers_complete_transitions_to_reducing(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_COMPLETED, TASK_STATUS_COMPLETED])

        with patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as mock_spawn:
            await machine._check_mapping_complete()

        # reduce_tasks created (one INSERT per reducer)
        assert machine.db.execute.call_count >= machine.config["num_reducers"]
        # One spawn per reducer
        assert mock_spawn.call_count == machine.config["num_reducers"]
        # Status updated to reducing
        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert JOB_STATUS_REDUCING in all_calls

    @pytest.mark.asyncio
    async def test_partial_mappers_does_not_transition(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_COMPLETED, TASK_STATUS_RUNNING])

        with patch.object(machine, "_spawn_and_track_reducer", new_callable=AsyncMock) as mock_spawn:
            await machine._check_mapping_complete()

        mock_spawn.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_reducers_complete_marks_job_completed(self):
        machine = make_sm(job={**SAMPLE_JOB, "status": JOB_STATUS_REDUCING})
        add_reduce_tasks(machine, [TASK_STATUS_COMPLETED, TASK_STATUS_COMPLETED])

        with patch.object(machine, "_notify_cluster_manager", new_callable=AsyncMock) as mock_notify:
            await machine._check_reducing_complete()

        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert JOB_STATUS_COMPLETED in all_calls
        mock_notify.assert_called_once_with(JOB_STATUS_COMPLETED)

    @pytest.mark.asyncio
    async def test_partial_reducers_does_not_finish_job(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_COMPLETED, TASK_STATUS_RUNNING])

        with patch.object(machine, "_notify_cluster_manager", new_callable=AsyncMock) as mock_notify:
            await machine._check_reducing_complete()

        mock_notify.assert_not_called()


# ===========================================================================
# 10. DB status helpers
# ===========================================================================

class TestDbStatusHelpers:

    @pytest.mark.asyncio
    async def test_set_status_mapping_writes_started_at(self):
        machine = make_sm()
        await machine._set_job_status(JOB_STATUS_MAPPING)
        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert "started_at" in all_calls

    @pytest.mark.asyncio
    async def test_set_status_completed_writes_completed_at(self):
        machine = make_sm()
        await machine._set_job_status(JOB_STATUS_COMPLETED)
        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert "completed_at" in all_calls

    @pytest.mark.asyncio
    async def test_set_status_failed_writes_completed_at(self):
        machine = make_sm()
        await machine._set_job_status(JOB_STATUS_FAILED)
        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert "completed_at" in all_calls

    @pytest.mark.asyncio
    async def test_set_status_reducing_writes_no_timestamps(self):
        machine = make_sm()
        await machine._set_job_status(JOB_STATUS_REDUCING)
        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert "started_at"  not in all_calls
        assert "completed_at" not in all_calls

    @pytest.mark.asyncio
    async def test_fail_job_writes_failure_reason(self):
        machine = make_sm()
        reason = "map_task 0 exceeded max_task_retries (3)"

        with patch.object(machine, "_notify_cluster_manager", new_callable=AsyncMock):
            await machine._fail_job(reason)

        all_calls = " ".join(str(c) for c in machine.db.execute.call_args_list)
        assert reason in all_calls

    @pytest.mark.asyncio
    async def test_fail_job_notifies_cluster_manager(self):
        machine = make_sm()

        with patch.object(machine, "_notify_cluster_manager", new_callable=AsyncMock) as mock_notify:
            await machine._fail_job("test failure")

        mock_notify.assert_called_once_with(JOB_STATUS_FAILED)


# ===========================================================================
# 11. Timer management
# ===========================================================================

class TestTimerManagement:

    def test_reset_timer_cancels_existing(self):
        machine    = make_sm()
        task       = TaskState(0, 30)
        old_handle = MagicMock()
        task.timer_handle = old_handle

        loop = MagicMock()
        loop.call_later = MagicMock(return_value=MagicMock())

        with patch("state_machine.asyncio.get_event_loop", return_value=loop):
            machine._reset_timer(task, lambda: None)

        old_handle.cancel.assert_called_once()

    def test_reset_timer_installs_new_handle(self):
        machine    = make_sm()
        task       = TaskState(0, 30)
        new_handle = MagicMock()
        loop       = MagicMock()
        loop.call_later = MagicMock(return_value=new_handle)

        with patch("state_machine.asyncio.get_event_loop", return_value=loop):
            machine._reset_timer(task, lambda: None)

        # The handle installed on the task must be the one returned by call_later
        assert task.timer_handle is new_handle
        # call_later must have been called with the correct timeout
        delay = loop.call_later.call_args[0][0]
        assert delay == 30

    def test_attach_timer_with_remaining_uses_min_when_no_started_at(self):
        machine = make_sm()
        task    = TaskState(0, 30)
        task.started_at = None  # no timestamp

        loop = MagicMock()
        loop.call_later = MagicMock(return_value=MagicMock())

        with patch("state_machine.asyncio.get_event_loop", return_value=loop):
            machine._attach_timer_with_remaining(task, lambda: None)

        delay = loop.call_later.call_args[0][0]
        assert delay == MIN_RESTART_TIMEOUT

    def test_attach_timer_with_remaining_computes_remaining_time(self):
        from datetime import timedelta
        machine = make_sm()
        task    = TaskState(0, 60)   # 60s timeout
        # Pretend the task started 10 seconds ago using timedelta (safe arithmetic)
        task.started_at = datetime.now(timezone.utc) - timedelta(seconds=10)

        loop = MagicMock()
        loop.call_later = MagicMock(return_value=MagicMock())

        with patch("state_machine.asyncio.get_event_loop", return_value=loop):
            machine._attach_timer_with_remaining(task, lambda: None)

        delay = loop.call_later.call_args[0][0]
        # ~50s remaining (60 - 10), but at least MIN_RESTART_TIMEOUT
        assert delay >= MIN_RESTART_TIMEOUT
        assert delay <= 60


# ===========================================================================
# 12. handle_ping dispatch
# ===========================================================================

class TestHandlePingDispatch:

    @pytest.mark.asyncio
    async def test_mapper_ping_dispatches_to_mapper_handler(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])

        with patch.object(machine, "_handle_mapper_ping", new_callable=AsyncMock) as mock_handler:
            await machine.handle_ping("mapper_0", "mapper", "alive")

        mock_handler.assert_called_once_with(0, "alive")

    @pytest.mark.asyncio
    async def test_reducer_ping_dispatches_to_reducer_handler(self):
        machine = make_sm()
        add_reduce_tasks(machine, [TASK_STATUS_RUNNING])

        with patch.object(machine, "_handle_reducer_ping", new_callable=AsyncMock) as mock_handler:
            await machine.handle_ping("reducer_1", "reducer", "started")

        mock_handler.assert_called_once_with(1, "started")

# ===========================================================================
# 12. heartbeat_monitor — snapshot_task
# ===========================================================================

class TestSnapshotTask:

    def test_snapshot_running_task(self):
        t = TaskState(3, 30)
        t.status         = TASK_STATUS_RUNNING
        t.attempt_number = 2
        t.started_at     = datetime.now(timezone.utc)
        t.timer_handle   = MagicMock()
        t.timer_handle.cancelled = MagicMock(return_value=False)

        snap = snapshot_task(t, "mapper")

        assert snap.task_id        == 3
        assert snap.role           == "mapper"
        assert snap.status         == TASK_STATUS_RUNNING
        assert snap.attempt_number == 2
        assert snap.has_active_timer is True
        assert snap.seconds_running is not None
        assert snap.seconds_running >= 0.0

    def test_snapshot_pending_task_no_start_time(self):
        t = TaskState(0, 30)   # defaults: pending, no started_at
        snap = snapshot_task(t, "reducer")

        assert snap.status          == TASK_STATUS_PENDING
        assert snap.seconds_running is None
        assert snap.has_active_timer is False

    def test_snapshot_completed_task(self):
        t = TaskState(1, 30)
        t.status       = TASK_STATUS_COMPLETED
        t.timer_handle = None
        snap = snapshot_task(t, "mapper")
        assert snap.has_active_timer is False

    def test_snapshot_is_frozen(self):
        """TaskHeartbeatSnapshot is a frozen dataclass — must not be mutatable."""
        t    = TaskState(0, 30)
        snap = snapshot_task(t, "mapper")
        with pytest.raises((AttributeError, TypeError)):
            snap.status = "mutated"   # type: ignore[misc]

    def test_snapshot_naive_started_at_is_handled(self):
        """started_at without tzinfo (from asyncpg) must not crash."""
        t = TaskState(0, 30)
        t.status     = TASK_STATUS_RUNNING
        t.started_at = datetime.now()   # naive — no tzinfo
        snap = snapshot_task(t, "mapper")
        assert snap.seconds_running is not None


# ===========================================================================
# 15. heartbeat_monitor — build_report
# ===========================================================================

class TestBuildReport:

    def test_counts_all_statuses_correctly(self):
        machine = make_sm()
        add_map_tasks(machine, [
            TASK_STATUS_RUNNING, TASK_STATUS_COMPLETED, TASK_STATUS_FAILED
        ])
        add_reduce_tasks(machine, [
            TASK_STATUS_COMPLETED, TASK_STATUS_PENDING
        ])

        report = build_report(machine)

        assert report.mappers_running   == 1
        assert report.mappers_completed == 1
        assert report.mappers_failed    == 1
        assert report.reducers_running  == 0   # PENDING != RUNNING
        assert report.reducers_completed == 1
        assert report.reducers_failed   == 0

    def test_report_contains_correct_job_id(self):
        machine = make_sm()
        report  = build_report(machine)
        assert report.job_id == JOB_ID

    def test_report_job_status_from_job_dict(self):
        machine = make_sm(job={**SAMPLE_JOB, "status": JOB_STATUS_MAPPING})
        report  = build_report(machine)
        assert report.job_status == JOB_STATUS_MAPPING

    def test_empty_tasks_produces_zero_counts(self):
        machine = make_sm()
        report  = build_report(machine)
        assert report.mappers_running   == 0
        assert report.mappers_completed == 0
        assert report.reducers_completed == 0

    def test_snapshot_lists_correct_length(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING, TASK_STATUS_COMPLETED])
        add_reduce_tasks(machine, [TASK_STATUS_PENDING])
        report = build_report(machine)
        assert len(report.mapper_snapshots)  == 2
        assert len(report.reducer_snapshots) == 1


# ===========================================================================
# 16. heartbeat_monitor — log_report (orphan detection)
# ===========================================================================

class TestLogReport:

    def test_log_report_does_not_raise(self):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING, TASK_STATUS_COMPLETED])
        log_report(machine)   # must not raise

    def test_log_report_warns_on_orphaned_task(self, caplog):
        """Running task with no active timer must trigger a warning."""
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        # timer_handle is None by default — this is an orphaned task

        import logging
        with caplog.at_level(logging.WARNING, logger="heartbeat_monitor"):
            log_report(machine)

        assert any("ORPHANED" in r.message for r in caplog.records)

    def test_log_report_no_warning_for_healthy_timers(self, caplog):
        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])
        mock_handle = MagicMock()
        mock_handle.cancelled = MagicMock(return_value=False)
        machine.map_tasks[0].timer_handle = mock_handle

        import logging
        with caplog.at_level(logging.WARNING, logger="heartbeat_monitor"):
            log_report(machine)

        assert not any("ORPHANED" in r.message for r in caplog.records)

    def test_log_ping_received_does_not_raise(self):
        log_ping_received(JOB_ID, "mapper_0", "mapper", "alive")

    def test_log_timeout_does_not_raise(self):
        log_timeout(JOB_ID, "mapper", 0, attempt_number=2, max_retries=3)


# ===========================================================================
# 17. worker_spawner — naming and env helpers
# ===========================================================================

class TestWorkerSpawner:

    def test_job_name_format(self):
        name = _job_name("mapper", "abcdef12-0000-0000-0000-000000000000", 3, 2)
        assert name == "mr-mapper-abcdef12-3-2"

    def test_job_name_reducer(self):
        name = _job_name("reducer", "12345678-aaaa-bbbb-cccc-000000000000", 0, 1)
        assert name == "mr-reducer-12345678-0-1"

    def test_job_name_within_k8s_limit(self):
        """Kubernetes Job names must be ≤63 characters."""
        name = _job_name("mapper", "a" * 36, 999, 999)
        assert len(name) <= 63

    def test_spawn_mapper_calls_create_k8s_job(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_mapper(
                job_id       = JOB_ID,
                map_id       = 0,
                attempt      = 1,
                config       = SAMPLE_CONFIG,
                job          = SAMPLE_JOB,
                offset_start = 0,
                offset_end   = 500,
            )

        mock_api.create_namespaced_job.assert_called_once()

    def test_spawn_mapper_job_name_contains_map_id(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_mapper(
                job_id=JOB_ID, map_id=7, attempt=2,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                offset_start=0, offset_end=100,
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        assert "7" in submitted_job.metadata.name
        assert "2" in submitted_job.metadata.name

    def test_spawn_mapper_uses_fixed_worker_image_and_absolute_command(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_mapper(
                job_id=JOB_ID, map_id=0, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                offset_start=0, offset_end=500,
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        container = submitted_job.spec.template.spec.containers[0]
        assert container.image == "mapreduce-worker:latest"
        assert container.command == ["python", "/app/worker/mapper.py"]

    def test_spawn_reducer_calls_create_k8s_job(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_reducer(
                job_id      = JOB_ID,
                reduce_id   = 1,
                attempt     = 1,
                config      = SAMPLE_CONFIG,
                job         = SAMPLE_JOB,
                output_path = f"intermediate/{JOB_ID}/output/part_1.json",
            )

        mock_api.create_namespaced_job.assert_called_once()

    def test_spawn_reducer_uses_fixed_worker_image_and_absolute_command(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_reducer(
                job_id=JOB_ID, reduce_id=1, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                output_path=f"intermediate/{JOB_ID}/output/part_1.json",
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        container = submitted_job.spec.template.spec.containers[0]
        assert container.image == "mapreduce-worker:latest"
        assert container.command == ["python", "/app/worker/reducer.py"]

    def test_spawn_mapper_backoff_limit_is_zero(self):
        """backoff_limit must be 0 — Kubernetes must never auto-retry."""
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_mapper(
                job_id=JOB_ID, map_id=0, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                offset_start=0, offset_end=500,
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        assert submitted_job.spec.backoff_limit == 0

    def test_spawn_reducer_backoff_limit_is_zero(self):
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_reducer(
                job_id=JOB_ID, reduce_id=0, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                output_path="output/part_0.json",
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        assert submitted_job.spec.backoff_limit == 0

    def test_spawn_mapper_restart_policy_never(self):
        """restart_policy must be Never so failed pods stay for log inspection."""
        mock_api = MagicMock()
        mock_api.create_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            spawn_mapper(
                job_id=JOB_ID, map_id=0, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                offset_start=0, offset_end=500,
            )

        submitted_job = mock_api.create_namespaced_job.call_args[1]["body"]
        restart = submitted_job.spec.template.spec.restart_policy
        assert restart == "Never"

    def test_delete_worker_job_calls_delete_api(self):
        mock_api = MagicMock()
        mock_api.delete_namespaced_job = MagicMock()

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            delete_worker_job("mapper", JOB_ID, 0, 1)

        mock_api.delete_namespaced_job.assert_called_once()

    def test_duplicate_job_409_is_swallowed(self):
        """A 409 Conflict from K8s (duplicate job) must not raise."""
        from kubernetes import client as k8s_client
        mock_api = MagicMock()
        exc      = k8s_client.exceptions.ApiException(status=409)
        mock_api.create_namespaced_job = MagicMock(side_effect=exc)

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            # Must not raise
            spawn_mapper(
                job_id=JOB_ID, map_id=0, attempt=1,
                config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                offset_start=0, offset_end=500,
            )

    def test_non_409_k8s_error_propagates(self):
        """Any K8s error other than 409 must re-raise."""
        from kubernetes import client as k8s_client
        mock_api = MagicMock()
        exc      = k8s_client.exceptions.ApiException(status=500)
        mock_api.create_namespaced_job = MagicMock(side_effect=exc)

        with patch("worker_spawner._get_batch_v1", return_value=mock_api):
            with pytest.raises(k8s_client.exceptions.ApiException):
                spawn_mapper(
                    job_id=JOB_ID, map_id=0, attempt=1,
                    config=SAMPLE_CONFIG, job=SAMPLE_JOB,
                    offset_start=0, offset_end=500,
                )


# ===========================================================================
# 18. main.py — FastAPI endpoints
# ===========================================================================

class TestFastAPIEndpoints:

    @pytest.fixture
    def client(self):
        """Return a TestClient with the state machine pre-injected."""
        from fastapi.testclient import TestClient
        import main as main_module

        machine = make_sm()
        add_map_tasks(machine, [TASK_STATUS_RUNNING])

        # Inject the machine and set _ready directly, bypassing lifespan
        main_module.state_machine = machine
        main_module._ready        = True

        yield TestClient(main_module.app, raise_server_exceptions=True)

        # Cleanup
        main_module.state_machine = None
        main_module._ready        = False

    def test_healthz_always_returns_200(self, client):
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"

    def test_readyz_returns_200_when_ready(self, client):
        resp = client.get("/readyz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

    def test_readyz_returns_503_when_not_ready(self):
        from fastapi.testclient import TestClient
        import main as main_module

        main_module.state_machine = None
        main_module._ready        = False

        client = TestClient(main_module.app, raise_server_exceptions=False)
        resp   = client.get("/readyz")
        assert resp.status_code == 503

    def test_worker_ping_valid_request_returns_ok(self, client):
        with patch.object(client.app.state, "state_machine", create=True), \
             patch("main.state_machine") as mock_sm:
            mock_sm.handle_ping = AsyncMock()
            resp = client.post("/worker_ping", json={
                "worker_id":   "mapper_0",
                "worker_type": "mapper",
                "status":      "alive",
            })
        # Either 200 (mock took effect) or 200 from real sm — both fine
        assert resp.status_code == 200

    def test_worker_ping_invalid_worker_type_returns_400(self, client):
        resp = client.post("/worker_ping", json={
            "worker_id":   "mapper_0",
            "worker_type": "unknown_type",
            "status":      "alive",
        })
        assert resp.status_code == 400

    def test_worker_ping_invalid_status_returns_400(self, client):
        resp = client.post("/worker_ping", json={
            "worker_id":   "mapper_0",
            "worker_type": "mapper",
            "status":      "invalid_status",
        })
        assert resp.status_code == 400

    def test_worker_ping_no_state_machine_returns_503(self):
        from fastapi.testclient import TestClient
        import main as main_module

        prev_sm           = main_module.state_machine
        main_module.state_machine = None
        main_module._ready        = True

        client = TestClient(main_module.app, raise_server_exceptions=False)
        resp   = client.post("/worker_ping", json={
            "worker_id":   "mapper_0",
            "worker_type": "mapper",
            "status":      "alive",
        })
        assert resp.status_code == 503

        main_module.state_machine = prev_sm
