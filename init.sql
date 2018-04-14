CREATE TYPE RUN_STATUS AS ENUM (
    'RUN_STATUS_PREPARING',
    'RUN_STATUS_PENDING',
    'RUN_STATUS_SUCCESS',
    'RUN_STATUS_ERROR');


CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    pushover_user_key VARCHAR(255) NOT NULL,
    command VARCHAR(255) NOT NULL,
    command_args VARCHAR(255)[] NOT NULL,
    timeout INTEGER NOT NULL, -- microseconds
    schedule INTEGER NOT NULL); -- microseconds


CREATE TABLE runs (
    job_id UUID REFERENCES jobs(id),
    number_ SERIAL NOT NULL,
    status RUN_STATUS NOT NULL,
    started TIMESTAMP(6), -- optional, 6 = microsecond precision = max allowed
    duration INTEGER, -- nanoseconds, optional
    stderr TEXT NOT NULL,
    stdout TEXT NOT NULL,
    err TEXT, -- optional
    pid INTEGER, -- optional
    PRIMARY KEY(job_id, number_));


-- Get the latest run of a given job
CREATE VIEW latest_runs AS
    SELECT job_id, number_, status, started AS last_run_time FROM (
        SELECT
        *,
        -- Select the row number within groups of runs (grouped by job_id). Row
        -- numbers are ordered by the run start time such that the most recent
        -- job is row number 1.
        row_number() OVER (PARTITION BY job_id ORDER BY started DESC) rownum
        FROM runs) AS subquery
    -- Filter for rownum = 1, which represents the latest run from each job
    WHERE rownum = 1;


CREATE VIEW overdue_jobs AS
    SELECT
        id,
        name,
        pushover_user_key,
        command,
        command_args,
        timeout,
        schedule
    FROM jobs LEFT JOIN latest_runs ON jobs.id = latest_runs.job_id
    -- Filter for jobs that were last started more than `jobs.schedule` ago,
    -- but not jobs that are currently in progress or pending (we don't want to
    -- have concurrent runs for the same job)
    WHERE EXTRACT(
        EPOCH FROM (
            TIMEZONE('utc', CURRENT_TIMESTAMP) - latest_runs.last_run_time
        )
    ) * 1000000 >= jobs.schedule -- EPOCH * 1000000 = microseconds
    AND status != 'RUN_STATUS_PENDING'
    AND status != 'RUN_STATUS_PREPARING'
    OR status IS NULL;
