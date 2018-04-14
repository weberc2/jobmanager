package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/gregdel/pushover"
	"github.com/kr/pretty"

	"github.com/lib/pq"
)

func jsonLog(message string, details interface{}) {
	input := struct {
		Message   string      `json:"message"`
		Details   interface{} `json:"details"`
		Timestamp time.Time   `json:"timestamp"`
	}{message, details, time.Now().UTC()}
	data, err := json.MarshalIndent(input, "", "    ")
	if err != nil {
		jsonLog("log_serialize_error", pretty.Sprint(input))
		return
	}
	log.Println(string(data))
}

type JobID string

type Job struct {
	Name            string
	ID              JobID
	PushoverUserKey string
	Command         string
	CommandArgs     []string
	Timeout         time.Duration
	Schedule        time.Duration
}

type RunStatus string

const (
	RunStatusPreparing RunStatus = "RUN_STATUS_PREPARING"
	RunStatusPending   RunStatus = "RUN_STATUS_PENDING"
	RunStatusSuccess   RunStatus = "RUN_STATUS_SUCCESS"
	RunStatusError     RunStatus = "RUN_STATUS_ERROR"
)

type Run struct {
	JobID    JobID
	Number   int
	Status   RunStatus
	Started  time.Time
	Duration time.Duration
	Stderr   string
	Stdout   string
	Err      string
	PID      int
}

func (r Run) Running() bool {
	return r.Status == RunStatusPending || r.Status == RunStatusPreparing
}

type ScheduleStore interface {
	// Overdue returns all of the jobs which are not running and which last
	// started more than ${job.Schedule} ago
	Overdue() ([]Job, error)
}

type PostgresScheduleStore struct {
	DB *sql.DB
}

func (pgss PostgresScheduleStore) Overdue() ([]Job, error) {
	// Get the jobs which are not running and which were last started more than
	// ${job.Schedule} ago.
	rows, err := pgss.DB.Query(`
SELECT
	id,
	name,
	pushover_user_key,
	command,
	command_args,
	timeout,
	schedule
FROM overdue_jobs`)
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
	return jobs, rows.Err()
}

type RunStore interface {
	Create(r Run) (Run, error)
	Update(r Run) (Run, error)
}

type JobInProgressErr JobID

func (jipe JobInProgressErr) Error() string {
	return fmt.Sprintf("Job in progress: %s", string(jipe))
}

type RunNotFoundErr struct {
	JobID     JobID
	RunNumber int
}

func (rnfe RunNotFoundErr) Error() string {
	return fmt.Sprintf(
		"Run not found: job=%s run=%d",
		rnfe.JobID,
		rnfe.RunNumber,
	)
}

type PostgresRunStore struct {
	DB *sql.DB
}

func (pgrs PostgresRunStore) Create(r Run) (Run, error) {
	// Insert the run and get back the run number
	return r, pgrs.DB.QueryRow(`
INSERT INTO runs (job_id, status, started, duration, stderr, stdout, err, pid)
VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING number_`,
		r.JobID,
		r.Status,
		r.Started,
		r.Duration.Round(time.Microsecond),
		r.Stderr,
		r.Stdout,
		r.Err,
		r.PID,
	).Scan(&r.Number)
}

func (pgrs PostgresRunStore) Update(r Run) (Run, error) {
	// TODO: Handle RunNotFoundErr
	_, err := pgrs.DB.Exec(`
UPDATE runs
SET
	status = $3,
	started = $4,
	duration = $5,
	stderr = $6,
	stdout = $7,
	err = $8,
	pid = $9
WHERE job_id = $1 AND number_ = $2`,
		r.JobID,
		r.Number,
		r.Status,
		r.Started,
		r.Duration,
		r.Stderr,
		r.Stdout,
		r.Err,
		r.PID,
	)
	return r, err
}

type MemoryRunStore struct {
	Lock sync.Mutex
	Runs []Run
}

func (mrs *MemoryRunStore) Create(r Run) (Run, error) {
	mrs.Lock.Lock()
	defer mrs.Lock.Unlock()

	// iterate backwards to find the latest run from the same job, if any
	for i := len(mrs.Runs) - 1; i >= 0; i-- {
		// if we got here, we found the latest run for the job
		if mrs.Runs[i].JobID == r.JobID {
			// grab the latest job number and increment it
			r.Number = mrs.Runs[i].Number + 1
			mrs.Runs = append(mrs.Runs, r)
			return r, nil
		}
	}

	// if we got here, we never found a run for the same job, so we set the
	// run's number to zero, add it to the runs, and return it
	r.Number = 0
	mrs.Runs = append(mrs.Runs, r)
	return r, nil
}

func (mrs *MemoryRunStore) Update(r Run) (Run, error) {
	mrs.Lock.Lock()
	defer mrs.Lock.Unlock()
	for i, run := range mrs.Runs {
		if run.JobID == r.JobID && run.Number == r.Number {
			mrs.Runs[i] = r
			return r, nil
		}
	}

	return Run{}, RunNotFoundErr{r.JobID, r.Number}
}

type TimeNow func() time.Time

type NotifyFunc func(
	pushoverUserKey string,
	summary string,
	details interface{},
) error

type StartFunc func(
	stdout io.Writer,
	stderr io.Writer,
	cmd string,
	args ...string,
) (*exec.Cmd, error)

type Runtime struct {
	Scheduler  Scheduler
	Dispatcher Dispatcher
	Notify     NotifyFunc
}

func (rt Runtime) Run() error {
	return rt.Scheduler.Run(func(j Job) {
		run, err := rt.Dispatcher.Dispatch(j)
		if run.Err != "" {
			err = errors.New(run.Err)
		}
		if err != nil {
			// TODO: Retry logic
			summary := fmt.Sprintf("Error running job %s (%s)", j.Name, j.ID)
			if err2 := rt.Notify(
				j.PushoverUserKey,
				summary,
				err.Error(),
			); err2 != nil {
				jsonLog("notify_user_error", map[string]interface{}{
					"message":              "Failed to notify user",
					"pushover_user_key":    j.PushoverUserKey,
					"notification_summary": summary,
					"notification_details": err.Error(),
					"notification_error":   err2.Error(),
				})
			}
		}
	})
}

type schedule struct {
	Job     Job
	LastRun time.Time
}

type MemoryScheduleStore []schedule

func (mss MemoryScheduleStore) Overdue() ([]Job, error) {
	var jobs []Job
	for _, schedule := range mss {
		if time.Since(schedule.LastRun) > schedule.Job.Schedule {
			jobs = append(jobs, schedule.Job)
		}
	}
	return jobs, nil
}

type Scheduler struct {
	Schedules        ScheduleStore
	PollingFrequency time.Duration
}

func (s Scheduler) Run(cb func(Job)) error {
	for range time.Tick(s.PollingFrequency) {
		jobs, err := s.Schedules.Overdue()
		if err != nil {
			return err
		}
		jsonLog("overdue_jobs", jobs)
		for _, job := range jobs {
			go cb(job)
		}
	}

	// shouldn't ever actually get here
	return nil
}

type Dispatcher struct {
	Runs    RunStore
	TimeNow TimeNow
	Start   StartFunc
}

func (d Dispatcher) Dispatch(j Job) (Run, error) {
	jsonLog("running_job", j)
	stdout := bytes.NewBuffer(nil)
	stderr := bytes.NewBuffer(nil)

	// create an initial record for the run
	run, err := d.Runs.Create(Run{
		JobID:   j.ID,
		Status:  RunStatusPreparing,
		Started: d.TimeNow(),
	})
	if err != nil {
		return run, err
	}

	// start the command
	cmd, err := d.Start(stdout, stderr, j.Command, j.CommandArgs...)
	if err != nil {
		jsonLog("start_run_error", err.Error())
		run, err2 := d.Runs.Update(Run{
			JobID:    j.ID,
			Number:   run.Number,
			Status:   RunStatusError,
			Started:  run.Started,
			Duration: time.Since(run.Started),
			Err:      err.Error(),
		})
		if err2 != nil {
			jsonLog("update_run_error", map[string]interface{}{
				"message":    "Failed to update run status to RUN_STATUS_ERROR",
				"details":    err2.Error(),
				"run_job_id": j.ID,
				"run_number": run.Number,
				"run_error":  err.Error(),
			})
			return run, err2
		}
		return run, err
	}

	// update the run's record to 'pending'; this means we've successfully
	// started the process
	run, err = d.Runs.Update(Run{
		JobID:   j.ID,
		Number:  run.Number,
		Status:  RunStatusPending,
		Started: run.Started,
		PID:     cmd.Process.Pid,
	})
	if err != nil {
		var consequence interface{} = "Killed job process"
		if err := cmd.Process.Kill(); err != nil {
			consequence = map[string]interface{}{
				"message": "Failed to kill run process",
				"details": err.Error(),
			}
		}
		jsonLog("update_run_error", map[string]interface{}{
			"message":     "Failed to update run status to RUN_STATUS_PENDING",
			"details":     err.Error(),
			"run_job_id":  j.ID,
			"run_number":  run.Number,
			"run_pid":     cmd.Process.Pid,
			"consequence": consequence,
		})
		return run, err
	}

	// await the command. If there is an error, update the run's record with
	// error
	if err := cmd.Wait(); err != nil {
		return d.Runs.Update(Run{
			JobID:    j.ID,
			Number:   run.Number,
			Err:      err.Error(),
			Started:  run.Started,
			Duration: time.Since(run.Started),
			Status:   RunStatusError,
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
		})
	}

	// if there was no error awaiting the command, update the run's record with
	// success
	return d.Runs.Update(Run{
		JobID:    j.ID,
		Number:   run.Number,
		Started:  run.Started,
		Duration: time.Since(run.Started),
		Status:   RunStatusSuccess,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
	})
}

func NativeStart(
	stdout io.Writer,
	stderr io.Writer,
	cmd string,
	args ...string,
) (*exec.Cmd, error) {
	command := exec.Command(cmd, args...)
	command.Stdout = stdout
	command.Stderr = stderr
	return command, command.Start()
}

type PushoverNotifier struct {
	App *pushover.Pushover
}

func (pn PushoverNotifier) Notify(
	userKey string,
	summary string,
	details interface{},
) error {
	data, err := json.MarshalIndent(details, "", "    ")
	if err != nil {
		data = []byte(pretty.Sprint(details))
	}
	message := fmt.Sprintf("%s - %s", summary, string(data))
	rsp, err := pn.App.SendMessage(
		pushover.NewMessage(message),
		pushover.NewRecipient(userKey),
	)
	if err != nil {
		return err
	}
	jsonLog("notify_user_succeeded", map[string]interface{}{
		"message":               "Successfully notified user",
		"pushover_user_key":     userKey,
		"notification_summary":  summary,
		"notification_details":  details,
		"notification_response": rsp,
	})
	return nil
}

func main() {
	pushoverAPIKey := os.Getenv("PUSHOVER_API_KEY")
	if pushoverAPIKey == "" {
		jsonLog(
			"pushover_api_key_unset",
			"PUSHOVER_API_KEY env var is not set",
		)
		log.Fatal()
	}

	jsonLog("app_starting", nil)
	db, err := sql.Open(
		"postgres",
		"postgresql://localhost:5432/jobmanager?sslmode=disable",
	)
	if err != nil {
		jsonLog("db_open_error", err.Error())
		log.Fatal()
	}
	defer db.Close()

	jsonLog("db_open_succeeded", nil)

	// var runs MemoryRunStore
	runtime := Runtime{
		Dispatcher: Dispatcher{
			TimeNow: func() time.Time { return time.Now().UTC() },
			// Runs:    &runs,
			Runs:  PostgresRunStore{db},
			Start: NativeStart,
		},
		Scheduler: Scheduler{
			PollingFrequency: 30 * time.Second,
			Schedules:        PostgresScheduleStore{db},
		},
		Notify: PushoverNotifier{App: pushover.New(pushoverAPIKey)}.Notify,
	}

	if err := runtime.Run(); err != nil {
		jsonLog("runtime_error", err.Error())
		log.Fatal()
	}
}
