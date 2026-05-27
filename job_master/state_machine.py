"""
Job Master — state_machine.py
==============================
Core orchestration engine for a single Map-Reduce job.

Design-doc alignment
---------------------
Status strings are taken verbatim from the schema enums defined in §6:

  job_status  : pending | mapping | reducing | completed | failed
  task_status : pending | running | completed | failed

All SQL column names match the ER diagram in §6 exactly.

Design-doc gap patched here
----------------------------
reduce_tasks has no reduce_id column in the ER diagram, but the rest of
the document references reducers by ID (worker_id "reducer_N", output path
"part_{reduce_id}.json", etc.).  We treat this as an oversight and include
  reduce_id  INTEGER NOT NULL
in every reduce_tasks query.  Flag this to your team so the CREATE TABLE
statement in your schema migration includes it.

Fault-tolerance model (§5.4)
------------------------------
Each running task has an asyncio countdown timer acting as a dead-man's
switch.  Workers must send "alive" pings before the timer expires or the
Job Master treats the pod as crashed and spawns a replacement (up to
max_task_retries from job_config).  On Job Master pod restart, in-flight
tasks whose started_at is known are re-attached timers computed from
  remaining = worker_timeout_seconds - (now - started_at).seconds
  (floored at 5 s to give the pod a grace period to reconnect).
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

import asyncpg
import httpx

from worker_spawner import spawn_mapper, spawn_reducer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Status constants — verbatim from design-doc schema enums (§6)
# ---------------------------------------------------------------------------

# job_status enum
JOB_STATUS_PENDING   = "pending"
JOB_STATUS_MAPPING   = "mapping"
JOB_STATUS_REDUCING  = "reducing"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED    = "failed"

# task_status enum
TASK_STATUS_PENDING   = "pending"
TASK_STATUS_RUNNING   = "running"
TASK_STATUS_COMPLETED = "completed"
TASK_STATUS_FAILED    = "failed"

# Minimum remaining timeout when re-attaching a timer after a restart (seconds)
MIN_RESTART_TIMEOUT = 5


# ---------------------------------------------------------------------------
# In-memory task mirror
# ---------------------------------------------------------------------------

class TaskState:
    """
    Lightweight in-memory mirror of a map_tasks / reduce_tasks row.
    Field names match the DB column names from the ER diagram.
    """
    __slots__ = (
        "task_id",
        "status",
        "attempt_number",
        "started_at",
        "timer_handle",
        "timeout_seconds",
    )

    def __init__(self, task_id: int, timeout_seconds: int):
        self.task_id         = task_id
        self.status          = TASK_STATUS_PENDING
        self.attempt_number  = 0
        self.started_at      = None
        self.timer_handle    = None
        self.timeout_seconds = timeout_seconds


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class JobStateMachine:
    def __init__(self, job_id: str):
        self.job_id        = job_id
        self.map_tasks:    dict[int, TaskState] = {}
        self.reduce_tasks: dict[int, TaskState] = {}
        self.db: asyncpg.Connection | None = None
        self.job:    dict = {}
        self.config: dict = {}
        self.lock = asyncio.Lock()

    # -----------------------------------------------------------------------
    # Initialisation
    # -----------------------------------------------------------------------

    async def initialize(self):
        self.db = await asyncpg.connect(
            user=os.environ.get("POSTGRES_USER", "admin"),
            password=os.environ.get("POSTGRES_PASSWORD", "admin"),
            database=os.environ.get("POSTGRES_DB", "mapreduce"),
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", os.environ.get("DB_PORT", "5432"))),
        )

        jobs_row = await self.db.fetchrow(
            """
            SELECT job_id, user_id, status,
                   input_data_path, output_data_path,
                   intermediate_prefix, code_location,
                   input_file_size_bytes,
                   completed_mappers_count, completed_reducers_count,
                   created_at, started_at, completed_at, failure_reason
            FROM jobs WHERE job_id = $1
            """,
            self.job_id,
        )
        if jobs_row is None:
            raise RuntimeError(f"Job {self.job_id} not found in DB")
        self.job = dict(jobs_row)

        config_row = await self.db.fetchrow(
            """
            SELECT job_id, num_mappers, num_reducers,
                   default_chunk_size_bytes,
                   worker_timeout_seconds, max_task_retries
            FROM job_config WHERE job_id = $1
            """,
            self.job_id,
        )
        if config_row is None:
            raise RuntimeError(f"job_config for {self.job_id} not found in DB")
        self.config = dict(config_row)

        map_rows = await self.db.fetch(
            """
            SELECT map_id, status,
                   byte_offset_start, byte_offset_end,
                   attempt_number, created_at, started_at, completed_at
            FROM map_tasks WHERE job_id = $1
            """,
            self.job_id,
        )
        for row in map_rows:
            t = TaskState(row["map_id"], self.config["worker_timeout_seconds"])
            t.status         = row["status"]
            t.attempt_number = row["attempt_number"]
            t.started_at     = row["started_at"]
            self.map_tasks[row["map_id"]] = t

        reduce_rows = await self.db.fetch(
            """
            SELECT reduce_id, status, output_data_path,
                   attempt_number, created_at, started_at, completed_at
            FROM reduce_tasks WHERE job_id = $1
            """,
            self.job_id,
        )
        for row in reduce_rows:
            t = TaskState(row["reduce_id"], self.config["worker_timeout_seconds"])
            t.status         = row["status"]
            t.attempt_number = row["attempt_number"]
            t.started_at     = row["started_at"]
            self.reduce_tasks[row["reduce_id"]] = t

        logger.info(
            f"[{self.job_id}] Initialized — job.status={self.job['status']}, "
            f"map_tasks={len(self.map_tasks)}, reduce_tasks={len(self.reduce_tasks)}"
        )

    # -----------------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------------

    async def run(self):
        current_status = self.job["status"]
        logger.info(f"[{self.job_id}] run() — current job status: {current_status}")

        if current_status == JOB_STATUS_PENDING:
            await self._create_map_tasks()
            await self._set_job_status(JOB_STATUS_MAPPING)
            for map_id in self.map_tasks:
                await self._spawn_and_track_mapper(map_id)

        elif current_status == JOB_STATUS_MAPPING:
            for map_id, task in self.map_tasks.items():
                if task.status == TASK_STATUS_RUNNING:
                    self._attach_timer_with_remaining(
                        task,
                        lambda mid=map_id: asyncio.create_task(self._handle_mapper_timeout(mid)),
                    )
                elif task.status == TASK_STATUS_PENDING:
                    await self._spawn_and_track_mapper(map_id)

        elif current_status == JOB_STATUS_REDUCING:
            for reduce_id, task in self.reduce_tasks.items():
                if task.status == TASK_STATUS_RUNNING:
                    self._attach_timer_with_remaining(
                        task,
                        lambda rid=reduce_id: asyncio.create_task(self._handle_reducer_timeout(rid)),
                    )
                elif task.status == TASK_STATUS_PENDING:
                    await self._spawn_and_track_reducer(reduce_id)

        elif current_status in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
            logger.info(
                f"[{self.job_id}] Already in terminal state '{current_status}'."
            )

    # -----------------------------------------------------------------------
    # Map task creation
    # -----------------------------------------------------------------------

    async def _create_map_tasks(self):
        file_size   = self.job["input_file_size_bytes"]
        num_mappers = self.config["num_mappers"]
        chunk_size  = file_size // num_mappers

        for i in range(num_mappers):
            start = i * chunk_size
            end   = file_size if i == num_mappers - 1 else (i + 1) * chunk_size
            await self.db.execute(
                """
                INSERT INTO map_tasks
                  (job_id, map_id, status, byte_offset_start, byte_offset_end,
                   attempt_number, created_at)
                VALUES ($1, $2, $3, $4, $5, 0, NOW())
                """,
                self.job_id, i, TASK_STATUS_PENDING, start, end,
            )
            self.map_tasks[i] = TaskState(i, self.config["worker_timeout_seconds"])

        logger.info(
            f"[{self.job_id}] Created {num_mappers} map_tasks "
            f"(file_size={file_size}, chunk_size≈{chunk_size})"
        )

    # -----------------------------------------------------------------------
    # Spawning
    # -----------------------------------------------------------------------

    async def _spawn_and_track_mapper(self, map_id: int):
        task = self.map_tasks[map_id]
        task.attempt_number += 1
        task.status          = TASK_STATUS_RUNNING
        task.started_at      = datetime.now(timezone.utc)

        offsets = await self.db.fetchrow(
            "SELECT byte_offset_start, byte_offset_end FROM map_tasks WHERE job_id=$1 AND map_id=$2",
            self.job_id, map_id,
        )
        await self.db.execute(
            "UPDATE map_tasks SET status=$1, attempt_number=$2, started_at=NOW() WHERE job_id=$3 AND map_id=$4",
            TASK_STATUS_RUNNING, task.attempt_number, self.job_id, map_id,
        )
        spawn_mapper(
            job_id=self.job_id, map_id=map_id, attempt=task.attempt_number,
            config=self.config, job=self.job,
            offset_start=offsets["byte_offset_start"],
            offset_end=offsets["byte_offset_end"],
        )

        self._reset_timer(
            task,
            lambda mid=map_id: asyncio.create_task(self._handle_mapper_timeout(mid)),
        )
        logger.info(
            f"[{self.job_id}] Spawned mapper_{map_id} "
            f"(attempt={task.attempt_number}, "
            f"offsets=[{offsets['byte_offset_start']},{offsets['byte_offset_end']}])"
        )

    async def _spawn_and_track_reducer(self, reduce_id: int):
        task = self.reduce_tasks[reduce_id]
        task.attempt_number += 1
        task.status          = TASK_STATUS_RUNNING
        task.started_at      = datetime.now(timezone.utc)

        output_path = f"{self.job['intermediate_prefix']}output/part_{reduce_id}.json"

        await self.db.execute(
            "UPDATE reduce_tasks SET status=$1, attempt_number=$2, output_data_path=$3, started_at=NOW() WHERE job_id=$4 AND reduce_id=$5",
            TASK_STATUS_RUNNING, task.attempt_number, output_path, self.job_id, reduce_id,
        )
        spawn_reducer(
            job_id=self.job_id, reduce_id=reduce_id, attempt=task.attempt_number,
            config=self.config, job=self.job, output_path=output_path,
        )

        self._reset_timer(
            task,
            lambda rid=reduce_id: asyncio.create_task(self._handle_reducer_timeout(rid)),
        )
        logger.info(f"[{self.job_id}] Spawned reducer_{reduce_id} (attempt={task.attempt_number})")

    # -----------------------------------------------------------------------
    # Timer management
    # -----------------------------------------------------------------------

    def _reset_timer(self, task: TaskState, callback):
        if task.timer_handle:
            task.timer_handle.cancel()
        loop = asyncio.get_event_loop()
        task.timer_handle = loop.call_later(task.timeout_seconds, callback)

    def _attach_timer_with_remaining(self, task: TaskState, callback):
        if task.started_at is None:
            remaining = MIN_RESTART_TIMEOUT
        else:
            now     = datetime.now(timezone.utc)
            started = task.started_at
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            elapsed   = (now - started).total_seconds()
            remaining = max(MIN_RESTART_TIMEOUT, task.timeout_seconds - elapsed)

        if task.timer_handle:
            task.timer_handle.cancel()
        loop = asyncio.get_event_loop()
        task.timer_handle = loop.call_later(remaining, callback)
        logger.info(
            f"[{self.job_id}] Re-attached timer for task {task.task_id} "
            f"— {remaining:.1f}s remaining"
        )

    # -----------------------------------------------------------------------
    # Ping handler
    # -----------------------------------------------------------------------

    async def handle_ping(self, worker_id: str, worker_type: str, status: str):
        task_id = int(worker_id.split("_")[-1])
        async with self.lock:
            if worker_type == "mapper":
                await self._handle_mapper_ping(task_id, status)
            else:
                await self._handle_reducer_ping(task_id, status)

    async def _handle_mapper_ping(self, map_id: int, status: str):
        task = self.map_tasks.get(map_id)
        if task is None:
            logger.warning(f"[{self.job_id}] Ping for unknown mapper_{map_id} — ignored")
            return
        if task.status == TASK_STATUS_COMPLETED:
            return

        if status in ("started", "alive"):
            self._reset_timer(
                task,
                lambda mid=map_id: asyncio.create_task(self._handle_mapper_timeout(mid)),
            )
            if status == "started":
                logger.info(f"[{self.job_id}] mapper_{map_id} started (attempt={task.attempt_number})")

        elif status == "completed":
            task.status = TASK_STATUS_COMPLETED
            if task.timer_handle:
                task.timer_handle.cancel()
                task.timer_handle = None
            await self.db.execute(
                "UPDATE map_tasks SET status=$1, completed_at=NOW() WHERE job_id=$2 AND map_id=$3",
                TASK_STATUS_COMPLETED, self.job_id, map_id,
            )
            await self.db.execute(
                "UPDATE jobs SET completed_mappers_count = completed_mappers_count + 1 WHERE job_id=$1",
                self.job_id,
            )
            logger.info(f"[{self.job_id}] mapper_{map_id} completed ✓")
            await self._check_mapping_complete()
        elif status == "failed":
            if task.timer_handle:
                task.timer_handle.cancel()
                task.timer_handle = None
            logger.warning(f"[{self.job_id}] mapper_{map_id} reported failure (attempt={task.attempt_number})")
            if task.attempt_number >= self.config["max_task_retries"]:
                task.status = TASK_STATUS_FAILED
                await self.db.execute(
                    "UPDATE map_tasks SET status=$1 WHERE job_id=$2 AND map_id=$3",
                    TASK_STATUS_FAILED, self.job_id, map_id,
                )
                await self._fail_job(
                    f"map_task {map_id} exceeded max_task_retries ({self.config['max_task_retries']})"
                )
                return
            await self._spawn_and_track_mapper(map_id)

    async def _handle_reducer_ping(self, reduce_id: int, status: str):
        task = self.reduce_tasks.get(reduce_id)
        if task is None:
            logger.warning(f"[{self.job_id}] Ping for unknown reducer_{reduce_id} — ignored")
            return
        if task.status == TASK_STATUS_COMPLETED:
            return

        if status in ("started", "alive"):
            self._reset_timer(
                task,
                lambda rid=reduce_id: asyncio.create_task(self._handle_reducer_timeout(rid)),
            )
            if status == "started":
                logger.info(f"[{self.job_id}] reducer_{reduce_id} started (attempt={task.attempt_number})")

        elif status == "completed":
            task.status = TASK_STATUS_COMPLETED
            if task.timer_handle:
                task.timer_handle.cancel()
                task.timer_handle = None
            await self.db.execute(
                "UPDATE reduce_tasks SET status=$1, completed_at=NOW() WHERE job_id=$2 AND reduce_id=$3",
                TASK_STATUS_COMPLETED, self.job_id, reduce_id,
            )
            await self.db.execute(
                "UPDATE jobs SET completed_reducers_count = completed_reducers_count + 1 WHERE job_id=$1",
                self.job_id,
            )
            logger.info(f"[{self.job_id}] reducer_{reduce_id} completed ✓")
            await self._check_reducing_complete()
        elif status == "failed":
            if task.timer_handle:
                task.timer_handle.cancel()
                task.timer_handle = None
            logger.warning(f"[{self.job_id}] reducer_{reduce_id} reported failure (attempt={task.attempt_number})")
            if task.attempt_number >= self.config["max_task_retries"]:
                task.status = TASK_STATUS_FAILED
                await self.db.execute(
                    "UPDATE reduce_tasks SET status=$1 WHERE job_id=$2 AND reduce_id=$3",
                    TASK_STATUS_FAILED, self.job_id, reduce_id,
                )
                await self._fail_job(
                    f"reduce_task {reduce_id} exceeded max_task_retries ({self.config['max_task_retries']})"
                )
                return
            await self._spawn_and_track_reducer(reduce_id)

    # -----------------------------------------------------------------------
    # Timeout handlers
    # -----------------------------------------------------------------------

    async def _handle_mapper_timeout(self, map_id: int):
        async with self.lock:
            task = self.map_tasks[map_id]
            if task.status == TASK_STATUS_COMPLETED:
                return
            logger.warning(
                f"[{self.job_id}] mapper_{map_id} timed out — "
                f"attempt_number={task.attempt_number}, max_task_retries={self.config['max_task_retries']}"
            )
            if task.attempt_number >= self.config["max_task_retries"]:
                task.status = TASK_STATUS_FAILED
                await self.db.execute(
                    "UPDATE map_tasks SET status=$1 WHERE job_id=$2 AND map_id=$3",
                    TASK_STATUS_FAILED, self.job_id, map_id,
                )
                await self._fail_job(
                    f"map_task {map_id} exceeded max_task_retries ({self.config['max_task_retries']})"
                )
                return
            await self._spawn_and_track_mapper(map_id)

    async def _handle_reducer_timeout(self, reduce_id: int):
        async with self.lock:
            task = self.reduce_tasks[reduce_id]
            if task.status == TASK_STATUS_COMPLETED:
                return
            logger.warning(
                f"[{self.job_id}] reducer_{reduce_id} timed out — "
                f"attempt_number={task.attempt_number}, max_task_retries={self.config['max_task_retries']}"
            )
            if task.attempt_number >= self.config["max_task_retries"]:
                task.status = TASK_STATUS_FAILED
                await self.db.execute(
                    "UPDATE reduce_tasks SET status=$1 WHERE job_id=$2 AND reduce_id=$3",
                    TASK_STATUS_FAILED, self.job_id, reduce_id,
                )
                await self._fail_job(
                    f"reduce_task {reduce_id} exceeded max_task_retries ({self.config['max_task_retries']})"
                )
                return
            await self._spawn_and_track_reducer(reduce_id)

    # -----------------------------------------------------------------------
    # Phase transitions
    # -----------------------------------------------------------------------

    async def _check_mapping_complete(self):
        completed_count = sum(1 for t in self.map_tasks.values() if t.status == TASK_STATUS_COMPLETED)
        if completed_count < self.config["num_mappers"]:
            return

        logger.info(f"[{self.job_id}] All {self.config['num_mappers']} mappers done. Starting reduce phase.")
        await self._set_job_status(JOB_STATUS_REDUCING)

        for i in range(self.config["num_reducers"]):
            output_path = f"{self.job['intermediate_prefix']}output/part_{i}.json"
            await self.db.execute(
                """
                INSERT INTO reduce_tasks
                  (job_id, reduce_id, status, output_data_path, attempt_number, created_at)
                VALUES ($1, $2, $3, $4, 0, NOW())
                """,
                self.job_id, i, TASK_STATUS_PENDING, output_path,
            )
            self.reduce_tasks[i] = TaskState(i, self.config["worker_timeout_seconds"])

        for reduce_id in self.reduce_tasks:
            await self._spawn_and_track_reducer(reduce_id)

    async def _check_reducing_complete(self):
        completed_count = sum(1 for t in self.reduce_tasks.values() if t.status == TASK_STATUS_COMPLETED)
        if completed_count < self.config["num_reducers"]:
            return

        logger.info(f"[{self.job_id}] All {self.config['num_reducers']} reducers done. Job complete.")
        await self._set_job_status(JOB_STATUS_COMPLETED)

        await self._notify_cluster_manager(JOB_STATUS_COMPLETED)

    # -----------------------------------------------------------------------
    # DB helpers
    # -----------------------------------------------------------------------

    async def _set_job_status(self, status: str):
        if status == JOB_STATUS_MAPPING:
            await self.db.execute(
                "UPDATE jobs SET status=$1, started_at=NOW() WHERE job_id=$2",
                status, self.job_id,
            )
        elif status in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
            await self.db.execute(
                "UPDATE jobs SET status=$1, completed_at=NOW() WHERE job_id=$2",
                status, self.job_id,
            )
        else:
            await self.db.execute(
                "UPDATE jobs SET status=$1 WHERE job_id=$2",
                status, self.job_id,
            )
        logger.info(f"[{self.job_id}] jobs.status → {status}")

    async def _fail_job(self, reason: str):
        logger.error(f"[{self.job_id}] Job FAILED: {reason}")
        await self.db.execute(
            "UPDATE jobs SET status=$1, failure_reason=$2, completed_at=NOW() WHERE job_id=$3",
            JOB_STATUS_FAILED, reason, self.job_id,
        )
        await self._notify_cluster_manager(JOB_STATUS_FAILED)

    async def _notify_cluster_manager(self, status: str):
        url     = os.environ["CLUSTER_MANAGER_URL"]
        payload = {"job_id": self.job_id, "status": status}
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(f"{url}/update_job_state/{self.job_id}", json=payload, timeout=10)
                resp.raise_for_status()
                logger.info(f"[{self.job_id}] Notified Cluster Manager: status={status}")
            except Exception as exc:
                logger.error(
                    f"[{self.job_id}] Failed to notify Cluster Manager (status={status}): {exc}"
                )
