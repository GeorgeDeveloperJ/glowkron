package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

func TestRecoverOrphanedRuns(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer st.Close()

	ctx := context.Background()

	// Create job and steps
	job := &store.Job{
		ID:       "crash-job",
		Name:     "crash-job",
		Enabled:  true,
		CronExpr: "0 1 * * *",
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	step := &store.JobStep{
		ID:      "s1",
		JobID:   "crash-job",
		Name:    "Step 1",
		Command: "sleep 100",
	}
	if err := st.CreateSteps(ctx, []*store.JobStep{step}); err != nil {
		t.Fatalf("failed to create step: %v", err)
	}

	// Insert an orphaned RUNNING JobRun
	jobRun := &store.JobRun{
		ID:          "run-orphaned-1",
		JobID:       "crash-job",
		Status:      store.JobRunRunning,
		TriggeredBy: store.TriggeredByCron,
		StartTime:   time.Now().Add(-10 * time.Minute),
	}
	if err := st.CreateJobRun(ctx, jobRun); err != nil {
		t.Fatalf("failed to create orphaned job run: %v", err)
	}

	// Insert an orphaned RUNNING StepRun
	stepRun := &store.StepRun{
		ID:        "step-run-orphaned-1",
		JobRunID:  "run-orphaned-1",
		StepID:    "s1",
		Status:    store.StepRunRunning,
		StartTime: time.Now().Add(-10 * time.Minute),
	}
	if err := st.CreateStepRun(ctx, stepRun); err != nil {
		t.Fatalf("failed to create orphaned step run: %v", err)
	}

	// Run recovery
	jobsRecovered, stepsRecovered, err := RecoverOrphanedRuns(ctx, st)
	if err != nil {
		t.Fatalf("unexpected recovery error: %v", err)
	}

	if jobsRecovered != 1 {
		t.Fatalf("expected 1 job recovered, got %d", jobsRecovered)
	}
	if stepsRecovered != 1 {
		t.Fatalf("expected 1 step recovered, got %d", stepsRecovered)
	}

	// Verify states in database
	recJobRun, err := st.GetJobRun(ctx, "run-orphaned-1")
	if err != nil {
		t.Fatalf("failed to get recovered job run: %v", err)
	}
	if recJobRun.Status != store.JobRunFailed {
		t.Fatalf("expected status FAILED, got: %s", recJobRun.Status)
	}
	if recJobRun.ErrorMessage != DefaultCrashRecoveryMessage {
		t.Fatalf("unexpected error message: %s", recJobRun.ErrorMessage)
	}
	if recJobRun.EndTime == nil {
		t.Fatal("expected non-nil end time on recovered job run")
	}

	recStepRuns, err := st.GetStepRunsByJobRunID(ctx, "run-orphaned-1")
	if err != nil || len(recStepRuns) != 1 {
		t.Fatalf("failed to get step runs, len=%d, err=%v", len(recStepRuns), err)
	}
	if recStepRuns[0].Status != store.StepRunFailed {
		t.Fatalf("expected step status FAILED, got: %s", recStepRuns[0].Status)
	}
}

func TestDaemonLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "daemon.db")
	workflowsDir := filepath.Join(tempDir, "workflows")
	logsDir := filepath.Join(tempDir, "logs")

	if err := os.MkdirAll(workflowsDir, 0755); err != nil {
		t.Fatalf("failed to create workflows dir: %v", err)
	}

	// Create a sample workflow file
	sampleYAML := `version: "1.0"
name: daemon-test-job
description: "Daemon test job"
schedule: "0 2 * * *"
steps:
  - id: step-1
    name: "Step 1"
    command: "echo hello"
`
	if err := os.WriteFile(filepath.Join(workflowsDir, "sample.yaml"), []byte(sampleYAML), 0644); err != nil {
		t.Fatalf("failed to write sample yaml: %v", err)
	}

	cfg := Config{
		DBPath:          dbPath,
		WorkflowsDir:    workflowsDir,
		LogsDir:         logsDir,
		MaxConcurrency:  2,
		ShutdownTimeout: 2 * time.Second,
	}

	d, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to initialize daemon: %v", err)
	}

	// Start daemon
	if err := d.Start(); err != nil {
		t.Fatalf("failed to start daemon: %v", err)
	}

	// Verify scheduler and watcher registered sample job
	if !d.Scheduler().HasJob("daemon-test-job") {
		t.Fatal("expected scheduler to have registered daemon-test-job")
	}

	job, err := d.Store().GetJob(context.Background(), "daemon-test-job")
	if err != nil || job == nil {
		t.Fatalf("expected job in database: %v", err)
	}

	// Shutdown daemon cleanly
	if err := d.Shutdown(); err != nil {
		t.Fatalf("unexpected error during shutdown: %v", err)
	}
}
