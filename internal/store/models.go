package store

import (
	"time"
)

// JobRunStatus represents the execution state of a job run.
type JobRunStatus string

const (
	JobRunPending   JobRunStatus = "PENDING"
	JobRunRunning   JobRunStatus = "RUNNING"
	JobRunCompleted JobRunStatus = "COMPLETED"
	JobRunFailed    JobRunStatus = "FAILED"
	JobRunCancelled JobRunStatus = "CANCELLED"
)

// StepRunStatus represents the execution state of a step run.
type StepRunStatus string

const (
	StepRunPending   StepRunStatus = "PENDING"
	StepRunRunning   StepRunStatus = "RUNNING"
	StepRunCompleted StepRunStatus = "COMPLETED"
	StepRunFailed    StepRunStatus = "FAILED"
	StepRunSkipped   StepRunStatus = "SKIPPED"
)

// TriggeredBy represents how a job run was triggered.
type TriggeredBy string

const (
	TriggeredByCron   TriggeredBy = "CRON"
	TriggeredByCLI    TriggeredBy = "CLI"
	TriggeredByWebUI  TriggeredBy = "WEB_UI"
	TriggeredByManual TriggeredBy = "MANUAL"
)

// Job represents a registered workflow or job definition.
type Job struct {
	ID               string    `json:"id" db:"id"`
	Name             string    `json:"name" db:"name"`
	Description      string    `json:"description" db:"description"`
	CronExpr         string    `json:"cron_expr" db:"cron_expr"`
	MaxRetries       int       `json:"max_retries" db:"max_retries"`
	RetryDelaySec    int       `json:"retry_delay_sec" db:"retry_delay_sec"`
	TimeoutSec       int       `json:"timeout_sec" db:"timeout_sec"`
	ConcurrencyGroup string    `json:"concurrency_group" db:"concurrency_group"`
	SourceFile       string    `json:"source_file,omitempty" db:"source_file"`
	Enabled          bool      `json:"enabled" db:"enabled"`
	CreatedAt        time.Time `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time `json:"updated_at" db:"updated_at"`
}

// JobStep represents an individual step within a DAG workflow.
type JobStep struct {
	ID         string            `json:"id" db:"id"`
	JobID      string            `json:"job_id" db:"job_id"`
	Name       string            `json:"name" db:"name"`
	Command    string            `json:"command" db:"command"`
	Args       []string          `json:"args" db:"args"`
	WorkingDir string            `json:"working_dir" db:"working_dir"`
	EnvVars    map[string]string `json:"env_vars" db:"env_vars"`
	DependsOn  []string          `json:"depends_on" db:"depends_on"`
	StepOrder  int               `json:"step_order" db:"step_order"`
}

// JobRun represents an overall execution run of a job.
type JobRun struct {
	ID           string       `json:"id" db:"id"`
	JobID        string       `json:"job_id" db:"job_id"`
	Status       JobRunStatus `json:"status" db:"status"`
	TriggeredBy  TriggeredBy  `json:"triggered_by" db:"triggered_by"`
	StartTime    time.Time    `json:"start_time" db:"start_time"`
	EndTime      *time.Time   `json:"end_time,omitempty" db:"end_time"`
	ExitCode     *int         `json:"exit_code,omitempty" db:"exit_code"`
	Attempt      int          `json:"attempt" db:"attempt"`
	ErrorMessage string       `json:"error_message,omitempty" db:"error_message"`
	LogFilePath  string       `json:"log_file_path,omitempty" db:"log_file_path"`
}

// StepRun represents a granular execution record of a single step within a job run.
type StepRun struct {
	ID            string        `json:"id" db:"id"`
	JobRunID      string        `json:"job_run_id" db:"job_run_id"`
	StepID        string        `json:"step_id" db:"step_id"`
	Status        StepRunStatus `json:"status" db:"status"`
	StartTime     time.Time     `json:"start_time" db:"start_time"`
	EndTime       *time.Time    `json:"end_time,omitempty" db:"end_time"`
	ExitCode      *int          `json:"exit_code,omitempty" db:"exit_code"`
	StdoutSnippet string        `json:"stdout_snippet,omitempty" db:"stdout_snippet"`
	StderrSnippet string        `json:"stderr_snippet,omitempty" db:"stderr_snippet"`
}
