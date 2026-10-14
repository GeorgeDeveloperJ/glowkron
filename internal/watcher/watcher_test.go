package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

type mockScheduler struct {
	mu        sync.Mutex
	schedules map[string]string
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{
		schedules: make(map[string]string),
	}
}

func (m *mockScheduler) RegisterOrUpdate(jobID, cronExpr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schedules[jobID] = cronExpr
	return nil
}

func (m *mockScheduler) Unregister(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.schedules, jobID)
	return nil
}

func (m *mockScheduler) get(jobID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.schedules[jobID]
	return val, ok
}

func TestSyncer_SyncFileAndRemove(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer st.Close()

	sched := newMockScheduler()
	syncer := NewSyncer(st, sched)

	yamlContent := `version: "1.0"
name: test-backup
description: "Daily database backup"
schedule: "0 2 * * *"
timeout: 30m
steps:
  - id: dump
    name: "Dump DB"
    command: "echo 'dumping'"
`

	wfPath := filepath.Join(tempDir, "test-backup.yaml")
	if err := os.WriteFile(wfPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write workflow file: %v", err)
	}

	ctx := context.Background()

	// 1. Sync file
	job, err := syncer.SyncFile(ctx, wfPath)
	if err != nil {
		t.Fatalf("failed to sync file: %v", err)
	}
	if job.ID != "test-backup" || job.CronExpr != "0 2 * * *" {
		t.Fatalf("unexpected job synced: %+v", job)
	}

	// Verify steps in store
	steps, err := st.GetStepsByJobID(ctx, "test-backup")
	if err != nil || len(steps) != 1 {
		t.Fatalf("expected 1 step in store, got %d, err: %v", len(steps), err)
	}
	if steps[0].ID != "dump" {
		t.Fatalf("unexpected step ID: %s", steps[0].ID)
	}

	// Verify schedule registered
	cronExpr, ok := sched.get("test-backup")
	if !ok || cronExpr != "0 2 * * *" {
		t.Fatalf("expected schedule 0 2 * * *, got %s", cronExpr)
	}

	// 2. Update file with new schedule and new step
	updatedYAML := `version: "1.0"
name: test-backup
description: "Daily database backup updated"
schedule: "*/10 * * * *"
timeout: 15m
steps:
  - id: dump
    name: "Dump DB"
    command: "echo 'dumping updated'"
  - id: compress
    name: "Compress"
    command: "gzip"
    depends_on: ["dump"]
`
	if err := os.WriteFile(wfPath, []byte(updatedYAML), 0644); err != nil {
		t.Fatalf("failed to update workflow file: %v", err)
	}

	_, err = syncer.SyncFile(ctx, wfPath)
	if err != nil {
		t.Fatalf("failed to sync updated file: %v", err)
	}

	updatedJob, _ := st.GetJob(ctx, "test-backup")
	if updatedJob.CronExpr != "*/10 * * * *" {
		t.Fatalf("expected updated cron expr, got: %s", updatedJob.CronExpr)
	}

	updatedSteps, _ := st.GetStepsByJobID(ctx, "test-backup")
	if len(updatedSteps) != 2 {
		t.Fatalf("expected 2 updated steps, got: %d", len(updatedSteps))
	}

	// 3. Remove file
	err = syncer.RemoveFile(ctx, wfPath)
	if err != nil {
		t.Fatalf("failed to remove file: %v", err)
	}

	disabledJob, _ := st.GetJob(ctx, "test-backup")
	if disabledJob.Enabled {
		t.Fatal("expected job to be disabled after removal")
	}

	_, ok = sched.get("test-backup")
	if ok {
		t.Fatal("expected schedule to be unregistered after removal")
	}
}

func TestWatcher_LiveFileSystemEvents(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer st.Close()

	sched := newMockScheduler()
	syncer := NewSyncer(st, sched)

	workflowsDir := filepath.Join(tempDir, "workflows")
	if err := os.MkdirAll(workflowsDir, 0755); err != nil {
		t.Fatalf("failed to create workflows dir: %v", err)
	}

	// Initial file in directory
	initialYAML := `version: "1.0"
name: initial-job
description: "Initial job"
schedule: "0 1 * * *"
steps:
  - id: s1
    name: "Step 1"
    command: "echo 1"
`
	initialFile := filepath.Join(workflowsDir, "initial.yaml")
	if err := os.WriteFile(initialFile, []byte(initialYAML), 0644); err != nil {
		t.Fatalf("failed to write initial yaml: %v", err)
	}

	watcher, err := New(workflowsDir, syncer, WithDebounce(50*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}

	if err := watcher.Start(); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer watcher.Stop()

	ctx := context.Background()

	// Verify initial sync happened on Start()
	j, err := st.GetJob(ctx, "initial-job")
	if err != nil || j.ID != "initial-job" {
		t.Fatalf("expected initial-job to be synced on startup, err: %v", err)
	}

	// Dynamically create a second workflow file at runtime
	runtimeYAML := `version: "1.0"
name: runtime-job
description: "Runtime created"
schedule: "0 5 * * *"
steps:
  - id: r1
    name: "Run 1"
    command: "echo runtime"
`
	runtimeFile := filepath.Join(workflowsDir, "runtime.yaml")
	if err := os.WriteFile(runtimeFile, []byte(runtimeYAML), 0644); err != nil {
		t.Fatalf("failed to write runtime file: %v", err)
	}

	// Wait for debounce and sync to process
	var runtimeJob *store.Job
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		runtimeJob, _ = st.GetJob(ctx, "runtime-job")
		if runtimeJob != nil {
			break
		}
	}

	if runtimeJob == nil {
		t.Fatal("timed out waiting for runtime-job to be synced by watcher")
	}

	// Verify schedule registered for runtime job
	schedExpr, ok := sched.get("runtime-job")
	if !ok || schedExpr != "0 5 * * *" {
		t.Fatalf("expected runtime schedule registered, got %v", schedExpr)
	}

	// Delete file at runtime
	if err := os.Remove(runtimeFile); err != nil {
		t.Fatalf("failed to remove runtime file: %v", err)
	}

	// Wait for remove event to process
	disabled := false
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		j, _ := st.GetJob(ctx, "runtime-job")
		if j != nil && !j.Enabled {
			disabled = true
			break
		}
	}

	if !disabled {
		t.Fatal("timed out waiting for runtime-job to be disabled after deletion")
	}
}
