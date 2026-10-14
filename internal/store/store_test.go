package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func TestMigrationsAndPragmas(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_pragmas.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	// Verify WAL mode
	var journalMode string
	if err := s.DB().QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("expected journal_mode 'wal', got %q", journalMode)
	}

	// Verify foreign keys are enabled
	var foreignKeys int
	if err := s.DB().QueryRow("PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		t.Fatalf("failed to query foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("expected foreign_keys 1, got %d", foreignKeys)
	}

	// Verify tables exist
	tables := []string{"jobs", "job_steps", "job_runs", "step_runs"}
	for _, tbl := range tables {
		var name string
		err := s.DB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", tbl).Scan(&name)
		if err != nil {
			t.Errorf("table %q not created: %v", tbl, err)
		}
	}

	// Verify migrations are idempotent by calling New again on existing db
	s2, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen database idempotently: %v", err)
	}
	_ = s2.Close()
}

func TestJobCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	job := &Job{
		ID:               "job-1",
		Name:             "Daily Backup",
		Description:      "Database backup to S3",
		CronExpr:         "0 2 * * *",
		MaxRetries:       2,
		RetryDelaySec:    15,
		TimeoutSec:       1800,
		ConcurrencyGroup: "backups",
		SourceFile:       "workflows/daily-backup.yaml",
		Enabled:          true,
	}

	// 1. CreateJob
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	// 2. GetJob
	fetched, err := s.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if fetched.Name != job.Name || fetched.CronExpr != job.CronExpr || !fetched.Enabled {
		t.Errorf("GetJob mismatch: got %+v", fetched)
	}

	// 3. ListJobs
	jobs, err := s.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-1" {
		t.Errorf("ListJobs expected 1 job, got %d", len(jobs))
	}

	// 4. UpdateJob
	fetched.Name = "Daily Database Backup Updated"
	fetched.MaxRetries = 3
	fetched.Enabled = false
	if err := s.UpdateJob(ctx, fetched); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	updated, err := s.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob after update failed: %v", err)
	}
	if updated.Name != "Daily Database Backup Updated" || updated.MaxRetries != 3 || updated.Enabled != false {
		t.Errorf("UpdateJob values mismatch: got %+v", updated)
	}

	// 5. DeleteJob
	if err := s.DeleteJob(ctx, "job-1"); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// Verify deletion
	_, err = s.GetJob(ctx, "job-1")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after deletion, got %v", err)
	}

	// Verify not found on operations with non-existent job
	if err := s.UpdateJob(ctx, &Job{ID: "non-existent"}); err != ErrNotFound {
		t.Errorf("expected ErrNotFound on update non-existent, got %v", err)
	}
	if err := s.DeleteJob(ctx, "non-existent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound on delete non-existent, got %v", err)
	}
}

func TestJobStepsCascade(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	job := &Job{
		ID:   "job-dag",
		Name: "DAG Pipeline",
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	steps := []*JobStep{
		{
			ID:         "step-dump",
			JobID:      "job-dag",
			Name:       "Dump DB",
			Command:    "pg_dump",
			Args:       []string{"-U", "postgres", "-f", "/tmp/backup.sql"},
			WorkingDir: "/tmp",
			EnvVars:    map[string]string{"PGPASSWORD": "secret"},
			DependsOn:  []string{},
			StepOrder:  0,
		},
		{
			ID:        "step-compress",
			JobID:     "job-dag",
			Name:      "Compress",
			Command:   "gzip",
			Args:      []string{"-f", "/tmp/backup.sql"},
			DependsOn: []string{"step-dump"},
			StepOrder: 1,
		},
	}

	// 1. CreateSteps
	if err := s.CreateSteps(ctx, steps); err != nil {
		t.Fatalf("CreateSteps failed: %v", err)
	}

	// 2. GetStepsByJobID
	fetchedSteps, err := s.GetStepsByJobID(ctx, "job-dag")
	if err != nil {
		t.Fatalf("GetStepsByJobID failed: %v", err)
	}
	if len(fetchedSteps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(fetchedSteps))
	}
	if fetchedSteps[0].Name != "Dump DB" || len(fetchedSteps[0].Args) != 4 || fetchedSteps[0].EnvVars["PGPASSWORD"] != "secret" {
		t.Errorf("step 0 details mismatch: %+v", fetchedSteps[0])
	}
	if len(fetchedSteps[1].DependsOn) != 1 || fetchedSteps[1].DependsOn[0] != "step-dump" {
		t.Errorf("step 1 dependencies mismatch: %+v", fetchedSteps[1])
	}

	// 3. Test CASCADE deletion when job is deleted
	if err := s.DeleteJob(ctx, "job-dag"); err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// Verify cascading deletion of steps
	remainingSteps, err := s.GetStepsByJobID(ctx, "job-dag")
	if err != nil {
		t.Fatalf("GetStepsByJobID after cascade failed: %v", err)
	}
	if len(remainingSteps) != 0 {
		t.Errorf("expected 0 steps after cascade delete, got %d", len(remainingSteps))
	}
}

func TestJobRunAndStepRunLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	job := &Job{
		ID:   "job-runs-test",
		Name: "Test Run Workflow",
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	step := &JobStep{
		ID:      "step-1",
		JobID:   "job-runs-test",
		Name:    "Echo",
		Command: "echo",
		Args:    []string{"hello"},
	}
	if err := s.CreateSteps(ctx, []*JobStep{step}); err != nil {
		t.Fatalf("CreateSteps failed: %v", err)
	}

	// 1. CreateJobRun in PENDING
	run := &JobRun{
		ID:          "run-1",
		JobID:       "job-runs-test",
		Status:      JobRunPending,
		TriggeredBy: TriggeredByCLI,
		Attempt:     1,
		LogFilePath: "/tmp/logs/run-1.log",
	}
	if err := s.CreateJobRun(ctx, run); err != nil {
		t.Fatalf("CreateJobRun failed: %v", err)
	}

	// 2. CreateStepRun in PENDING
	stepRun := &StepRun{
		ID:       "srun-1",
		JobRunID: "run-1",
		StepID:   "step-1",
		Status:   StepRunPending,
	}
	if err := s.CreateStepRun(ctx, stepRun); err != nil {
		t.Fatalf("CreateStepRun failed: %v", err)
	}

	// 3. Update JobRun to RUNNING
	if err := s.UpdateJobRunStatus(ctx, "run-1", JobRunRunning, nil, ""); err != nil {
		t.Fatalf("UpdateJobRunStatus to RUNNING failed: %v", err)
	}

	// 4. Update StepRun to RUNNING then COMPLETED
	stepRun.Status = StepRunRunning
	if err := s.UpdateStepRun(ctx, stepRun); err != nil {
		t.Fatalf("UpdateStepRun to RUNNING failed: %v", err)
	}

	stepExitCode := 0
	stepRun.Status = StepRunCompleted
	stepRun.ExitCode = &stepExitCode
	stepRun.StdoutSnippet = "hello\n"
	if err := s.UpdateStepRun(ctx, stepRun); err != nil {
		t.Fatalf("UpdateStepRun to COMPLETED failed: %v", err)
	}

	// Verify step run fields
	fetchedSR, err := s.GetStepRun(ctx, "srun-1")
	if err != nil {
		t.Fatalf("GetStepRun failed: %v", err)
	}
	if fetchedSR.Status != StepRunCompleted || *fetchedSR.ExitCode != 0 || fetchedSR.EndTime == nil {
		t.Errorf("StepRun completed state invalid: %+v", fetchedSR)
	}

	stepRunsList, err := s.GetStepRunsByJobRunID(ctx, "run-1")
	if err != nil || len(stepRunsList) != 1 {
		t.Fatalf("GetStepRunsByJobRunID failed: %v (count: %d)", err, len(stepRunsList))
	}

	// 5. Update JobRun to COMPLETED
	jobExitCode := 0
	if err := s.UpdateJobRunStatus(ctx, "run-1", JobRunCompleted, &jobExitCode, ""); err != nil {
		t.Fatalf("UpdateJobRunStatus to COMPLETED failed: %v", err)
	}

	completedRun, err := s.GetJobRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetJobRun failed: %v", err)
	}
	if completedRun.Status != JobRunCompleted || completedRun.EndTime == nil || *completedRun.ExitCode != 0 {
		t.Errorf("JobRun completed state invalid: %+v", completedRun)
	}

	// 6. Test FAILED run status
	run2 := &JobRun{
		ID:          "run-2",
		JobID:       "job-runs-test",
		Status:      JobRunRunning,
		TriggeredBy: TriggeredByCron,
	}
	if err := s.CreateJobRun(ctx, run2); err != nil {
		t.Fatalf("CreateJobRun run-2 failed: %v", err)
	}

	failExitCode := 127
	if err := s.UpdateJobRunStatus(ctx, "run-2", JobRunFailed, &failExitCode, "command not found"); err != nil {
		t.Fatalf("UpdateJobRunStatus to FAILED failed: %v", err)
	}

	failedRun, err := s.GetJobRun(ctx, "run-2")
	if err != nil {
		t.Fatalf("GetJobRun run-2 failed: %v", err)
	}
	if failedRun.Status != JobRunFailed || *failedRun.ExitCode != 127 || failedRun.ErrorMessage != "command not found" {
		t.Errorf("JobRun failed state mismatch: %+v", failedRun)
	}

	// 7. Test ListJobRuns with filter & limit
	runs, err := s.ListJobRuns(ctx, "job-runs-test", 10)
	if err != nil {
		t.Fatalf("ListJobRuns failed: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("expected 2 runs, got %d", len(runs))
	}

	// Test ListJobRuns with limit 1
	runsLimited, err := s.ListJobRuns(ctx, "job-runs-test", 1)
	if err != nil || len(runsLimited) != 1 {
		t.Errorf("expected 1 run with limit 1, got %d", len(runsLimited))
	}
}

func TestConcurrency(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	job := &Job{
		ID:   "concurrent-job",
		Name: "High Concurrency Job",
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*2)

	// Launch concurrent writers
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			runID := fmt.Sprintf("run-concurrent-%d", idx)
			run := &JobRun{
				ID:          runID,
				JobID:       "concurrent-job",
				Status:      JobRunPending,
				TriggeredBy: TriggeredByCLI,
				StartTime:   time.Now().UTC(),
			}
			if err := s.CreateJobRun(ctx, run); err != nil {
				errCh <- err
				return
			}

			code := 0
			if err := s.UpdateJobRunStatus(ctx, runID, JobRunCompleted, &code, ""); err != nil {
				errCh <- err
				return
			}
		}(i)
	}

	// Launch concurrent readers
	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.ListJobs(ctx); err != nil {
				errCh <- err
				return
			}
			if _, err := s.ListJobRuns(ctx, "concurrent-job", 10); err != nil {
				errCh <- err
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrency error encountered: %v", err)
	}
}

func TestWorkflowStepResyncAndSharedStepIDs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// 1. Create two distinct jobs
	jobA := &Job{ID: "job-alpha", Name: "Job Alpha"}
	jobB := &Job{ID: "job-beta", Name: "Job Beta"}
	if err := s.CreateJob(ctx, jobA); err != nil {
		t.Fatalf("CreateJob A failed: %v", err)
	}
	if err := s.CreateJob(ctx, jobB); err != nil {
		t.Fatalf("CreateJob B failed: %v", err)
	}

	// 2. Both jobs define a step with the same ID "step-build"
	stepA := &JobStep{
		ID:      "step-build",
		JobID:   "job-alpha",
		Name:    "Alpha Build",
		Command: "go build",
	}
	stepB := &JobStep{
		ID:      "step-build",
		JobID:   "job-beta",
		Name:    "Beta Build",
		Command: "cargo build",
	}
	if err := s.CreateSteps(ctx, []*JobStep{stepA}); err != nil {
		t.Fatalf("CreateSteps for Job A failed: %v", err)
	}
	if err := s.CreateSteps(ctx, []*JobStep{stepB}); err != nil {
		t.Fatalf("CreateSteps for Job B with shared step ID failed: %v", err)
	}

	// 3. Create execution run history for Job A referencing step-build
	runA := &JobRun{
		ID:          "run-alpha-1",
		JobID:       "job-alpha",
		Status:      JobRunCompleted,
		TriggeredBy: TriggeredByCLI,
	}
	if err := s.CreateJobRun(ctx, runA); err != nil {
		t.Fatalf("CreateJobRun failed: %v", err)
	}
	stepRunA := &StepRun{
		ID:       "srun-alpha-1",
		JobRunID: "run-alpha-1",
		StepID:   "step-build",
		Status:   StepRunCompleted,
	}
	if err := s.CreateStepRun(ctx, stepRunA); err != nil {
		t.Fatalf("CreateStepRun failed: %v", err)
	}

	// 4. Re-sync Job A: update step-build command, add step-test
	stepAUpdated := &JobStep{
		ID:      "step-build",
		JobID:   "job-alpha",
		Name:    "Alpha Build v2",
		Command: "go build -v",
	}
	stepA2 := &JobStep{
		ID:      "step-test",
		JobID:   "job-alpha",
		Name:    "Alpha Test",
		Command: "go test",
	}
	if err := s.CreateSteps(ctx, []*JobStep{stepAUpdated, stepA2}); err != nil {
		t.Fatalf("Re-sync CreateSteps failed on existing run history: %v", err)
	}

	// Verify step-build was updated in place
	stepsA, err := s.GetStepsByJobID(ctx, "job-alpha")
	if err != nil || len(stepsA) != 2 {
		t.Fatalf("GetStepsByJobID failed: %v (count: %d)", err, len(stepsA))
	}
	if stepsA[0].Name != "Alpha Build v2" || stepsA[0].Command != "go build -v" {
		t.Errorf("step-build was not properly upserted: %+v", stepsA[0])
	}

	// Verify historical step_runs are intact
	sr, err := s.GetStepRun(ctx, "srun-alpha-1")
	if err != nil || sr.Status != StepRunCompleted {
		t.Fatalf("historical step run was compromised: %v, %+v", err, sr)
	}

	// 5. Test DeleteStepsNotIn: prune step-test, keep step-build
	if err := s.DeleteStepsNotIn(ctx, "job-alpha", []string{"step-build"}); err != nil {
		t.Fatalf("DeleteStepsNotIn failed: %v", err)
	}
	stepsAfterPrune, err := s.GetStepsByJobID(ctx, "job-alpha")
	if err != nil || len(stepsAfterPrune) != 1 || stepsAfterPrune[0].ID != "step-build" {
		t.Fatalf("Pruning failed: %v, remaining: %+v", err, stepsAfterPrune)
	}
}
