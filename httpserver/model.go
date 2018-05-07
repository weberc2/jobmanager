package main

import (
	"database/sql"

	"github.com/lib/pq"

	. "github.com/weberc2/jobmanager/jobmanager"
)

func jobs(db *sql.DB) ([]Job, error) {
	rows, err := db.Query(`
SELECT id, name, pushover_user_key, command, command_args, timeout, schedule
FROM jobs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID,
			&j.Name,
			&j.PushoverUserKey,
			&j.Command,
			pq.Array(&j.CommandArgs),
			&j.Timeout,
			&j.Schedule,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func runs(db *sql.DB) ([]Run, error) {
	rows, err := db.Query(`
SELECT job_id, number_, status, started, duration, stderr, stdout, err, pid
FROM runs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(
			&r.JobID,
			&r.Number,
			&r.Status,
			&r.Started,
			&r.Duration,
			&r.Stderr,
			&r.Stdout,
			&r.Err,
			&r.PID,
		); err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}

	return runs, nil
}

func runsOfJob(db *sql.DB, jobID JobID) ([]Run, error) {
	rows, err := db.Query(`
SELECT job_id, number_, status, started, duration, stderr, stdout, err, pid
FROM runs WHERE job_id = $1`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(
			&r.JobID,
			&r.Number,
			&r.Status,
			&r.Started,
			&r.Duration,
			&r.Stderr,
			&r.Stdout,
			&r.Err,
			&r.PID,
		); err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}

	return runs, nil
}
