package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

func setupTestEnvironment(t *testing.T, maxConcurrency int) (*Engine, *store.Store, *logger.FileAppender, *logger.LogBroker) {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "engine_test.db")
	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	logDir := filepath.Join(tempDir, "logs")
	appender, err := logger.NewFileAppender(logDir)
	if err != nil {
		t.Fatalf("failed to create test appender: %v", err)
	}

	broker := logger.NewLogBroker(64)
	eng := New(s, appender, broker, maxConcurrency)
	return eng, s, appender, broker
}

func TestExecute_SuccessDAG(t *testing.T) {
	eng, s, appender, broker := setupTestEnvironment(t, 4)
	ctx := context.Background()

	// Diamond DAG:
	//       step-a
	//       /    \
	//   step-b  step-c
	//       \    /
	//       step-d
	wf := &workflow.WorkflowConfig{
		Version: "1",
		Name:    "diamond-dag-workflow",
		Steps: []workflow.StepConfig{
			{
				ID:      "step-a",
				Name:    "Step A",
				Command: "echo",
				Args:    []string{"hello from step-a"},
			},
			{
				ID:        "step-b",
				Name:      "Step B",
				Command:   "echo",
				Args:      []string{"hello from step-b"},
				DependsOn: []string{"step-a"},
			},
			{
				ID:        "step-c",
				Name:      "Step C",
				Command:   "echo",
				Args:      []string{"hello from step-c"},
				DependsOn: []string{"step-a"},
			},
			{
				ID:        "step-d",
				Name:      "Step D",
				Command:   "echo",
				Args:      []string{"hello from step-d"},
				DependsOn: []string{"step-b", "step-c"},
			},
		},
	}

	run, err := eng.ExecuteWorkflow(ctx, wf, store.TriggeredByManual)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed unexpectedly: %v", err)
	}

	if run.Status != store.JobRunCompleted {
		t.Fatalf("expected JobRunCompleted status, got %v", run.Status)
	}
	if run.ExitCode == nil || *run.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %v", run.ExitCode)
	}

	// Verify all step_runs records in SQLite
	stepRuns, err := s.GetStepRunsByJobRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetStepRunsByJobRunID failed: %v", err)
	}
	if len(stepRuns) != 4 {
		t.Fatalf("expected 4 step runs, got %d", len(stepRuns))
	}

	for _, sr := range stepRuns {
		if sr.Status != store.StepRunCompleted {
			t.Errorf("step %s expected COMPLETED, got %v", sr.StepID, sr.Status)
		}
		if sr.ExitCode == nil || *sr.ExitCode != 0 {
			t.Errorf("step %s expected exit code 0, got %v", sr.StepID, sr.ExitCode)
		}
	}

	// Verify raw logs persisted to disk
	rawLogs, err := appender.ReadLogs(run.ID)
	if err != nil {
		t.Fatalf("failed to read disk logs: %v", err)
	}
	logStr := string(rawLogs)
	for _, expected := range []string{"hello from step-a", "hello from step-b", "hello from step-c", "hello from step-d"} {
		if !strings.Contains(logStr, expected) {
			t.Errorf("expected disk log to contain %q, log content:\n%s", expected, logStr)
		}
	}

	// Verify broker closed for run
	if count := broker.ActiveSubscribers(run.ID); count != 0 {
		t.Errorf("expected 0 active subscribers after run finalized, got %d", count)
	}
}

func TestExecute_StepFailureAndSkippedChildren(t *testing.T) {
	eng, s, _, _ := setupTestEnvironment(t, 2)
	ctx := context.Background()

	// Pipeline:
	// step-root (succeeds) -> step-failing (fails) -> step-downstream (skipped) -> step-leaf (skipped)
	wf := &workflow.WorkflowConfig{
		Version: "1",
		Name:    "failure-propagation-workflow",
		Steps: []workflow.StepConfig{
			{
				ID:      "step-root",
				Name:    "Root Step",
				Command: "echo",
				Args:    []string{"root completed"},
			},
			{
				ID:        "step-failing",
				Name:      "Failing Step",
				Command:   "sh",
				Args:      []string{"-c", "exit 42"},
				DependsOn: []string{"step-root"},
			},
			{
				ID:        "step-downstream",
				Name:      "Downstream Step",
				Command:   "echo",
				Args:      []string{"should be skipped"},
				DependsOn: []string{"step-failing"},
			},
			{
				ID:        "step-leaf",
				Name:      "Leaf Step",
				Command:   "echo",
				Args:      []string{"also skipped"},
				DependsOn: []string{"step-downstream"},
			},
		},
	}

	run, err := eng.ExecuteWorkflow(ctx, wf, store.TriggeredByManual)
	if err != nil {
		t.Fatalf("ExecuteWorkflow returned error unexpectedly: %v", err)
	}

	if run.Status != store.JobRunFailed {
		t.Fatalf("expected JobRunFailed, got %v", run.Status)
	}
	if run.ExitCode == nil || *run.ExitCode == 0 {
		t.Fatalf("expected non-zero exit code, got %v", run.ExitCode)
	}

	stepRuns, err := s.GetStepRunsByJobRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetStepRunsByJobRunID failed: %v", err)
	}

	stepStatusMap := make(map[string]store.StepRunStatus)
	exitCodeMap := make(map[string]*int)
	for _, sr := range stepRuns {
		stepStatusMap[sr.StepID] = sr.Status
		exitCodeMap[sr.StepID] = sr.ExitCode
	}

	if stepStatusMap["step-root"] != store.StepRunCompleted {
		t.Errorf("step-root expected COMPLETED, got %v", stepStatusMap["step-root"])
	}
	if stepStatusMap["step-failing"] != store.StepRunFailed {
		t.Errorf("step-failing expected FAILED, got %v", stepStatusMap["step-failing"])
	}
	if exitCodeMap["step-failing"] == nil || *exitCodeMap["step-failing"] != 42 {
		t.Errorf("step-failing expected exit code 42, got %v", exitCodeMap["step-failing"])
	}
	if stepStatusMap["step-downstream"] != store.StepRunSkipped {
		t.Errorf("step-downstream expected SKIPPED, got %v", stepStatusMap["step-downstream"])
	}
	if stepStatusMap["step-leaf"] != store.StepRunSkipped {
		t.Errorf("step-leaf expected SKIPPED, got %v", stepStatusMap["step-leaf"])
	}
}

func TestExecute_RetrySuccess(t *testing.T) {
	eng, s, _, _ := setupTestEnvironment(t, 2)
	ctx := context.Background()

	markerFile := filepath.Join(t.TempDir(), "retry_marker")

	// Script fails on attempt 1 (when marker does not exist, creating it),
	// and succeeds on attempt 2 (when marker exists).
	script := fmt.Sprintf(`if [ ! -f "%s" ]; then touch "%s" && exit 1; else echo "retry succeeded" && exit 0; fi`, markerFile, markerFile)

	wf := &workflow.WorkflowConfig{
		Version:    "1",
		Name:       "retry-success-workflow",
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
		Steps: []workflow.StepConfig{
			{
				ID:      "retry-step",
				Name:    "Retry Step",
				Command: "sh",
				Args:    []string{"-c", script},
			},
		},
	}

	run, err := eng.ExecuteWorkflow(ctx, wf, store.TriggeredByManual)
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}

	if run.Status != store.JobRunCompleted {
		t.Fatalf("expected JobRunCompleted after retry, got %v", run.Status)
	}

	stepRuns, err := s.GetStepRunsByJobRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("failed to fetch step runs: %v", err)
	}
	if len(stepRuns) != 1 {
		t.Fatalf("expected 1 step run, got %d", len(stepRuns))
	}
	if stepRuns[0].Status != store.StepRunCompleted {
		t.Errorf("expected step COMPLETED, got %v", stepRuns[0].Status)
	}
	if !strings.Contains(stepRuns[0].StdoutSnippet, "retry succeeded") {
		t.Errorf("expected stdout snippet to contain 'retry succeeded', got %q", stepRuns[0].StdoutSnippet)
	}
}

func TestExecute_BoundedConcurrency(t *testing.T) {
	// Direct WorkerPool test ensuring concurrent tasks never exceed limit
	poolLimit := 3
	pool := NewWorkerPool(poolLimit)
	var activeCount atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 9; i++ {
		wg.Add(1)
		err := pool.Submit(context.Background(), func() {
			defer wg.Done()
			curr := activeCount.Add(1)
			for {
				peak := maxObserved.Load()
				if curr <= peak || maxObserved.CompareAndSwap(peak, curr) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			activeCount.Add(-1)
		})
		if err != nil {
			t.Fatalf("pool.Submit failed: %v", err)
		}
	}
	wg.Wait()
	pool.Close()

	if maxObserved.Load() > int32(poolLimit) {
		t.Errorf("WorkerPool exceeded limit: peak %d > max %d", maxObserved.Load(), poolLimit)
	}

	// Integration test executing a workflow with maxConcurrency = 2
	eng, _, _, _ := setupTestEnvironment(t, 2)
	wf := &workflow.WorkflowConfig{
		Version: "1",
		Name:    "bounded-concurrency-wf",
		Steps: []workflow.StepConfig{
			{ID: "p1", Name: "Parallel 1", Command: "sh", Args: []string{"-c", "sleep 0.1 && echo p1"}},
			{ID: "p2", Name: "Parallel 2", Command: "sh", Args: []string{"-c", "sleep 0.1 && echo p2"}},
			{ID: "p3", Name: "Parallel 3", Command: "sh", Args: []string{"-c", "sleep 0.1 && echo p3"}},
			{ID: "p4", Name: "Parallel 4", Command: "sh", Args: []string{"-c", "sleep 0.1 && echo p4"}},
		},
	}

	start := time.Now()
	run, err := eng.ExecuteWorkflow(context.Background(), wf, store.TriggeredByManual)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if run.Status != store.JobRunCompleted {
		t.Errorf("expected COMPLETED, got %v", run.Status)
	}

	// 4 steps of 0.1s run at concurrency 2 take at least ~0.2s
	if elapsed < 150*time.Millisecond {
		t.Errorf("execution was suspiciously fast (%v), concurrency might not be bounded", elapsed)
	}
}

func TestExecute_ContextCancellation(t *testing.T) {
	eng, s, _, _ := setupTestEnvironment(t, 2)

	ctx, cancel := context.WithCancel(context.Background())

	wf := &workflow.WorkflowConfig{
		Version: "1",
		Name:    "cancel-workflow",
		Steps: []workflow.StepConfig{
			{
				ID:      "sleep-step",
				Name:    "Sleep Step",
				Command: "sh",
				Args:    []string{"-c", "sleep 10"},
			},
		},
	}

	// Trigger cancellation shortly after workflow starts
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	run, err := eng.ExecuteWorkflow(ctx, wf, store.TriggeredByManual)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("execution took too long to terminate on cancellation: %v", elapsed)
	}

	if run == nil {
		t.Fatal("expected non-nil JobRun returned")
	}

	if run.Status != store.JobRunCancelled && run.Status != store.JobRunFailed {
		t.Errorf("expected CANCELLED or FAILED status, got %v", run.Status)
	}

	// Verify status in store
	persisted, err := s.GetJobRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("failed to retrieve job run: %v", err)
	}
	if persisted.Status != store.JobRunCancelled && persisted.Status != store.JobRunFailed {
		t.Errorf("persisted status mismatch: got %v", persisted.Status)
	}
}

func TestCalculateBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 1 * time.Second

	// Attempt 0/1: around 100ms within [80ms, 120ms]
	b1 := CalculateBackoff(1, base, max)
	if b1 < 80*time.Millisecond || b1 > 120*time.Millisecond {
		t.Errorf("attempt 1 backoff out of expected jitter range: %v", b1)
	}

	// Attempt 2: base * 2 = 200ms within [160ms, 240ms]
	b2 := CalculateBackoff(2, base, max)
	if b2 < 160*time.Millisecond || b2 > 240*time.Millisecond {
		t.Errorf("attempt 2 backoff out of expected jitter range: %v", b2)
	}

	// Attempt 3: base * 4 = 400ms within [320ms, 480ms]
	b3 := CalculateBackoff(3, base, max)
	if b3 < 320*time.Millisecond || b3 > 480*time.Millisecond {
		t.Errorf("attempt 3 backoff out of expected jitter range: %v", b3)
	}

	// Attempt 10: would be 100ms * 512 = 51.2s, capped at max (1s)
	b10 := CalculateBackoff(10, base, max)
	if b10 > max {
		t.Errorf("attempt 10 backoff exceeded maxDelay: %v > %v", b10, max)
	}

	// Zero base delay should return 0
	if zero := CalculateBackoff(1, 0, max); zero != 0 {
		t.Errorf("expected 0 for zero base delay, got %v", zero)
	}
}

func TestTruncateSnippet(t *testing.T) {
	shortStr := "hello"
	if res := truncateSnippet(shortStr, 2048); res != shortStr {
		t.Errorf("expected %q, got %q", shortStr, res)
	}

	longStr := strings.Repeat("a", 3000)
	truncated := truncateSnippet(longStr, 2048)
	if len(truncated) != 2048 {
		t.Errorf("expected length 2048, got %d", len(truncated))
	}
}
