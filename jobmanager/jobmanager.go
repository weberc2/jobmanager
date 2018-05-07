package jobmanager

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/kr/pretty"
)

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

func JSONLog(message string, details interface{}) {
	input := struct {
		Message   string      `json:"message"`
		Details   interface{} `json:"details"`
		Timestamp time.Time   `json:"timestamp"`
	}{message, details, time.Now().UTC()}
	data, err := json.MarshalIndent(input, "", "    ")
	if err != nil {
		JSONLog("log_serialize_error", pretty.Sprint(input))
		return
	}
	log.Println(string(data))
}

func OpenDatabase() (*sql.DB, error) {
	postgresConnectionString := os.Getenv("PG_CONNECTION_STRING")
	if postgresConnectionString == "" {
		postgresConnectionString =
			"postgresql://localhost:5432/jobmanager?sslmode=disable"
	}

	db, err := sql.Open("postgres", postgresConnectionString)
	if err != nil {
		return nil, err
	}

	// Test the database
	rows, err := db.Query("SELECT name FROM jobs")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
	}

	return db, nil
}
