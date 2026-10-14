package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/client"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// mockClient implements client.Client for fast, deterministic unit testing of Cobra commands.
type mockClient struct {
	jobs    []*store.Job
	runs    []*store.JobRun
	logs    map[string]string
	trigger func(idOrPath string, async bool) (*store.JobRun, error)
}

func (m *mockClient) Health(ctx context.Context) (*client.HealthStatus, error) {
	return &client.HealthStatus{Online: true, Version: "v0.1.0"}, nil
}

func (m *mockClient) ListJobs(ctx context.Context, statusFilter string) ([]*store.Job, error) {
	return m.jobs, nil
}

func (m *mockClient) GetJob(ctx context.Context, id string) (*store.Job, error) {
	for _, j := range m.jobs {
		if j.ID == id || j.Name == id {
			return j, nil
		}
	}
	return nil, client.ErrJobNotFound
}

func (m *mockClient) TriggerJob(ctx context.Context, idOrPath string, async bool) (*store.JobRun, error) {
	if m.trigger != nil {
		return m.trigger(idOrPath, async)
	}
	zero := 0
	return &store.JobRun{
		ID:          "run-test-1",
		JobID:       idOrPath,
		Status:      store.JobRunCompleted,
		TriggeredBy: store.TriggeredByCLI,
		StartTime:   time.Now(),
		ExitCode:    &zero,
	}, nil
}

func (m *mockClient) ListRuns(ctx context.Context, jobID string, limit int) ([]*store.JobRun, error) {
	return m.runs, nil
}

func (m *mockClient) GetRun(ctx context.Context, runID string) (*store.JobRun, error) {
	for _, r := range m.runs {
		if r.ID == runID {
			return r, nil
		}
	}
	return nil, client.ErrRunNotFound
}

func (m *mockClient) GetRunSteps(ctx context.Context, runID string) ([]*store.StepRun, error) {
	return nil, nil
}

func (m *mockClient) GetRunLogs(ctx context.Context, runID string) (string, error) {
	if l, ok := m.logs[runID]; ok {
		return l, nil
	}
	return "", client.ErrRunNotFound
}

func (m *mockClient) StreamRunLogs(ctx context.Context, runID string) (<-chan string, <-chan error, error) {
	logCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		defer close(logCh)
		defer close(errCh)
		if l, ok := m.logs[runID]; ok {
			logCh <- l
		}
	}()
	return logCh, errCh, nil
}

func (m *mockClient) Close() error {
	return nil
}

func TestCLI_RootHelp(t *testing.T) {
	ctx := &CLIContext{Port: 8080}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing --help: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "GlowKron provides cron scheduling") {
		t.Errorf("expected root help message, got %q", out)
	}
}

func TestCLI_ListCommand(t *testing.T) {
	mock := &mockClient{
		jobs: []*store.Job{
			{Name: "daily-backup", Enabled: true, CronExpr: "0 2 * * *", Description: "Nightly backup"},
			{Name: "data-sync", Enabled: false, CronExpr: "*/10 * * * *", Description: "Sync data"},
		},
	}

	ctx := &CLIContext{Client: mock}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"list"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing list: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "daily-backup") || !strings.Contains(out, "ENABLED") {
		t.Errorf("expected daily-backup in list output, got %q", out)
	}
	if !strings.Contains(out, "data-sync") || !strings.Contains(out, "DISABLED") {
		t.Errorf("expected data-sync in list output, got %q", out)
	}
}

func TestCLI_RunsCommand(t *testing.T) {
	zero := 0
	mock := &mockClient{
		runs: []*store.JobRun{
			{
				ID:          "run-100",
				JobID:       "daily-backup",
				Status:      store.JobRunCompleted,
				TriggeredBy: store.TriggeredByCron,
				StartTime:   time.Now().Add(-5 * time.Minute),
				ExitCode:    &zero,
			},
		},
	}

	ctx := &CLIContext{Client: mock}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"runs"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing runs: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "run-100") || !strings.Contains(out, "COMPLETED") {
		t.Errorf("expected run-100 in runs output, got %q", out)
	}
}

func TestCLI_RunCommand(t *testing.T) {
	mock := &mockClient{}
	ctx := &CLIContext{Client: mock}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"run", "daily-backup"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing run: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "run-test-1") || !strings.Contains(out, "COMPLETED") {
		t.Errorf("expected run output, got %q", out)
	}
}

func TestCLI_LogsCommand(t *testing.T) {
	mock := &mockClient{
		logs: map[string]string{
			"run-100": "Step 1 executed successfully\nStep 2 done\n",
		},
	}
	ctx := &CLIContext{Client: mock}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"logs", "run-100"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing logs: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Step 1 executed successfully") {
		t.Errorf("expected log output, got %q", out)
	}
}

func TestCLI_ValidateCommand_ValidAndCyclic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "glowkron-validate-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	validYAML := `
version: "1.0"
name: valid-workflow
steps:
  - id: step-a
    command: echo
    args: ["A"]
  - id: step-b
    command: echo
    args: ["B"]
    depends_on: ["step-a"]
`
	validPath := filepath.Join(tempDir, "valid.yaml")
	if err := os.WriteFile(validPath, []byte(validYAML), 0644); err != nil {
		t.Fatalf("failed to write valid yaml: %v", err)
	}

	cyclicYAML := `
version: "1.0"
name: cyclic-workflow
steps:
  - id: step-a
    command: echo
    depends_on: ["step-b"]
  - id: step-b
    command: echo
    depends_on: ["step-a"]
`
	cyclicPath := filepath.Join(tempDir, "cyclic.yaml")
	if err := os.WriteFile(cyclicPath, []byte(cyclicYAML), 0644); err != nil {
		t.Fatalf("failed to write cyclic yaml: %v", err)
	}

	ctx := &CLIContext{}
	cmd := NewRootCmd(ctx)

	// 1. Valid test
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"validate", validPath})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("validate failed on valid workflow: %v", err)
	}
	if !strings.Contains(buf.String(), "Stage 1: [step-a]") || !strings.Contains(buf.String(), "Stage 2: [step-b]") {
		t.Errorf("expected stages in validate output, got %q", buf.String())
	}

	// 2. Cyclic test
	buf.Reset()
	cmd.SetArgs([]string{"validate", cyclicPath})
	err = cmd.Execute()
	if err == nil {
		t.Errorf("expected error validating cyclic workflow, got nil")
	}
	if !strings.Contains(fmt.Sprint(err), "cycle") {
		t.Errorf("expected cycle error, got %v", err)
	}
}

func TestCLI_Version(t *testing.T) {
	ctx := &CLIContext{}
	cmd := NewRootCmd(ctx)

	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error executing --version: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "glowkron version") {
		t.Errorf("expected version output, got %q", out)
	}
}
