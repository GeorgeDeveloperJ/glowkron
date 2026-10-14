package client

import (
	"context"
	"errors"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

var (
	// ErrDaemonOffline is returned when the daemon HTTP/socket connection cannot be established.
	ErrDaemonOffline = errors.New("glowkron daemon is offline")
	// ErrJobNotFound is returned when the specified job identifier does not exist.
	ErrJobNotFound = errors.New("job not found")
	// ErrRunNotFound is returned when the specified run identifier does not exist.
	ErrRunNotFound = errors.New("run not found")
)

// HealthStatus reports runtime statistics and status of the daemon.
type HealthStatus struct {
	Online         bool          `json:"online"`
	Version        string        `json:"version"`
	Pid            int           `json:"pid"`
	Uptime         time.Duration `json:"uptime"`
	RunningRuns    int           `json:"running_runs"`
	MaxConcurrency int           `json:"max_concurrency"`
	WorkflowsDir   string        `json:"workflows_dir"`
}

// Client defines the unified interface for interacting with GlowKron,
// whether through the daemon REST/SSE API or direct SQLite/disk offline access.
type Client interface {
	// Health probes whether the daemon is online.
	Health(ctx context.Context) (*HealthStatus, error)

	// ListJobs returns registered workflow jobs. statusFilter can be "all", "enabled", or "disabled".
	ListJobs(ctx context.Context, statusFilter string) ([]*store.Job, error)

	// GetJob retrieves a specific job by its ID.
	GetJob(ctx context.Context, id string) (*store.Job, error)

	// TriggerJob executes a job immediately. idOrPath can be a job ID or path to a workflow YAML file.
	TriggerJob(ctx context.Context, idOrPath string, async bool) (*store.JobRun, error)

	// ListRuns returns execution history, optionally filtered by jobID and bounded by limit.
	ListRuns(ctx context.Context, jobID string, limit int) ([]*store.JobRun, error)

	// GetRun returns metadata for a single execution run.
	GetRun(ctx context.Context, runID string) (*store.JobRun, error)

	// GetRunSteps returns step executions for a given run ID.
	GetRunSteps(ctx context.Context, runID string) ([]*store.StepRun, error)

	// GetRunLogs reads historical raw log output from disk.
	GetRunLogs(ctx context.Context, runID string) (string, error)

	// StreamRunLogs connects to real-time log streaming for an active run.
	StreamRunLogs(ctx context.Context, runID string) (<-chan string, <-chan error, error)

	// Close terminates any underlying connections or resources.
	Close() error
}
