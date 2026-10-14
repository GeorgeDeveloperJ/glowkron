package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/client"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

func setupTestEnvironment(t *testing.T) (string, string, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "glowkron-client-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")
	logsDir := filepath.Join(tempDir, "logs")
	workflowsDir := filepath.Join(tempDir, "workflows")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	if err := os.MkdirAll(workflowsDir, 0755); err != nil {
		t.Fatalf("failed to create workflows dir: %v", err)
	}

	return dbPath, logsDir, workflowsDir
}

func TestOfflineClient_CRUDAndExecution(t *testing.T) {
	dbPath, logsDir, workflowsDir := setupTestEnvironment(t)
	defer os.RemoveAll(filepath.Dir(dbPath))

	// Create a sample workflow file
	wfYAML := `
version: "1.0"
name: offline-echo
description: Test offline workflow execution
steps:
  - id: step-1
    command: echo
    args: ["offline success"]
`
	wfPath := filepath.Join(workflowsDir, "offline-echo.yaml")
	if err := os.WriteFile(wfPath, []byte(wfYAML), 0644); err != nil {
		t.Fatalf("failed to write workflow file: %v", err)
	}

	c, err := client.NewOfflineClient(dbPath, logsDir, workflowsDir)
	if err != nil {
		t.Fatalf("NewOfflineClient error: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// 1. Health
	health, err := c.Health(ctx)
	if err != nil {
		t.Fatalf("Health error: %v", err)
	}
	if health.Online {
		t.Errorf("expected offline health, got online")
	}

	// 2. Trigger workflow in-process
	run, err := c.TriggerJob(ctx, "offline-echo.yaml", false)
	if err != nil {
		t.Fatalf("TriggerJob error: %v", err)
	}
	if run.Status != store.JobRunCompleted {
		t.Errorf("expected status %s, got %s", store.JobRunCompleted, run.Status)
	}

	// 3. GetRun
	fetchedRun, err := c.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun error: %v", err)
	}
	if fetchedRun.ID != run.ID {
		t.Errorf("expected run ID %s, got %s", run.ID, fetchedRun.ID)
	}

	// 4. ListRuns
	runs, err := c.ListRuns(ctx, "offline-echo", 10)
	if err != nil {
		t.Fatalf("ListRuns error: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	// 5. GetRunSteps
	steps, err := c.GetRunSteps(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRunSteps error: %v", err)
	}
	if len(steps) != 1 {
		t.Fatalf("expected 1 step run, got %d", len(steps))
	}
	if steps[0].Status != store.StepRunCompleted {
		t.Errorf("expected step status %s, got %s", store.StepRunCompleted, steps[0].Status)
	}

	// 6. GetRunLogs
	logs, err := c.GetRunLogs(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRunLogs error: %v", err)
	}
	if logs == "" {
		t.Errorf("expected non-empty log output")
	}

	// 7. StreamRunLogs
	logCh, errCh, err := c.StreamRunLogs(ctx, run.ID)
	if err != nil {
		t.Fatalf("StreamRunLogs error: %v", err)
	}
	streamed := ""
	for chunk := range logCh {
		streamed += chunk
	}
	if err := <-errCh; err != nil {
		t.Fatalf("streaming error: %v", err)
	}
	if streamed != logs {
		t.Errorf("streamed log does not match GetRunLogs")
	}
}

func TestHTTPClient_WithMockServer(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(client.HealthStatus{
			Online:         true,
			Version:        "v0.1.0",
			RunningRuns:    0,
			MaxConcurrency: 4,
		})
	})

	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs := []*store.Job{
			{ID: "job-1", Name: "Job One", Enabled: true},
			{ID: "job-2", Name: "Job Two", Enabled: false},
		}
		_ = json.NewEncoder(w).Encode(jobs)
	})

	mux.HandleFunc("/api/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(store.Job{ID: "job-1", Name: "Job One", Enabled: true})
	})

	mux.HandleFunc("/api/jobs/job-1/run", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(store.JobRun{
			ID:          "run-123",
			JobID:       "job-1",
			Status:      store.JobRunCompleted,
			TriggeredBy: store.TriggeredByWebUI,
			StartTime:   time.Now(),
		})
	})

	mux.HandleFunc("/api/runs/run-123/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintf(w, "data: {\"chunk\":\"hello live log\"}\n\n")
		flusher.Flush()
		_, _ = fmt.Fprintf(w, "event: done\ndata: {}\n\n")
		flusher.Flush()
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	c := client.NewHTTPClient(server.URL)
	defer c.Close()

	ctx := context.Background()

	// Health
	health, err := c.Health(ctx)
	if err != nil {
		t.Fatalf("Health error: %v", err)
	}
	if !health.Online || health.Version != "v0.1.0" {
		t.Errorf("unexpected health response: %+v", health)
	}

	// ListJobs
	jobs, err := c.ListJobs(ctx, "all")
	if err != nil {
		t.Fatalf("ListJobs error: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}

	// GetJob
	job, err := c.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob error: %v", err)
	}
	if job.ID != "job-1" {
		t.Errorf("expected job-1, got %s", job.ID)
	}

	// TriggerJob
	run, err := c.TriggerJob(ctx, "job-1", false)
	if err != nil {
		t.Fatalf("TriggerJob error: %v", err)
	}
	if run.ID != "run-123" {
		t.Errorf("expected run-123, got %s", run.ID)
	}

	// StreamRunLogs
	logCh, errCh, err := c.StreamRunLogs(ctx, "run-123")
	if err != nil {
		t.Fatalf("StreamRunLogs error: %v", err)
	}
	var received string
	for chunk := range logCh {
		received += chunk
	}
	if err := <-errCh; err != nil {
		t.Fatalf("streaming error: %v", err)
	}
	if received != "hello live log" {
		t.Errorf("expected 'hello live log', got %q", received)
	}
}

func TestAutoClient_Fallback(t *testing.T) {
	dbPath, logsDir, workflowsDir := setupTestEnvironment(t)
	defer os.RemoveAll(filepath.Dir(dbPath))

	// Point to closed port 127.0.0.1:0 to trigger offline fallback
	c, err := client.NewAutoClient("http://127.0.0.1:0", dbPath, logsDir, workflowsDir)
	if err != nil {
		t.Fatalf("NewAutoClient error: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// Should fall back to offline client cleanly
	health, err := c.Health(ctx)
	if err != nil {
		t.Fatalf("Health error: %v", err)
	}
	if health.Online {
		t.Errorf("expected offline fallback, got online")
	}

	jobs, err := c.ListJobs(ctx, "all")
	if err != nil {
		t.Fatalf("ListJobs error: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs in fresh db, got %d", len(jobs))
	}
}
