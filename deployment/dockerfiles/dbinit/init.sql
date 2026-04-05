CREATE SCHEMA IF NOT EXISTS mapreduce;
CREATE SCHEMA IF NOT EXISTS keycloak;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'job_status' AND n.nspname = 'mapreduce'
    ) THEN
        CREATE TYPE mapreduce.job_status AS ENUM (
            'pending',
            'mapping',
            'reducing',
            'completed',
            'failed',
            'aborted'
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'task_status' AND n.nspname = 'mapreduce'
    ) THEN
        CREATE TYPE mapreduce.task_status AS ENUM (
            'pending',
            'running',
            'completed',
            'failed'
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS mapreduce.jobs (
    job_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    status mapreduce.job_status NOT NULL,
    input_data_path TEXT NOT NULL,
    output_data_path TEXT NOT NULL,
    intermediate_prefix TEXT NOT NULL,
    code_location TEXT NOT NULL,
    input_file_size_bytes BIGINT NOT NULL,
    completed_mappers_count INTEGER NOT NULL DEFAULT 0,
    completed_reducers_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS mapreduce.job_config (
    job_id UUID PRIMARY KEY,
    num_mappers INTEGER NOT NULL,
    num_reducers INTEGER NOT NULL,
    default_chunk_size_bytes BIGINT NOT NULL,
    worker_timeout_seconds INTEGER NOT NULL,
    max_task_retries INTEGER NOT NULL,
    CONSTRAINT fk_job_config_job
        FOREIGN KEY (job_id)
        REFERENCES mapreduce.jobs(job_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mapreduce.map_tasks (
    job_id UUID NOT NULL,
    map_id INTEGER NOT NULL,
    status mapreduce.task_status NOT NULL,
    byte_offset_start BIGINT NOT NULL,
    byte_offset_end BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, map_id),
    CONSTRAINT fk_map_tasks_job
        FOREIGN KEY (job_id)
        REFERENCES mapreduce.jobs(job_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mapreduce.reduce_tasks (
    job_id UUID NOT NULL,
    reduce_id INTEGER NOT NULL,
    status mapreduce.task_status NOT NULL,
    output_data_path TEXT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, reduce_id),
    CONSTRAINT fk_reduce_tasks_job
        FOREIGN KEY (job_id)
        REFERENCES mapreduce.jobs(job_id)
        ON DELETE CASCADE
);

ALTER DATABASE mapreduce SET search_path TO mapreduce, public;
