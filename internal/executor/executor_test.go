package executor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/GeorgeDeveloperJ/glowkron/internal/executor"
)

func TestRun_SimpleEcho(t *testing.T) {
	ctx := context.Background()
	cfg := executor.ProcessConfig{
		Command: "echo",
		Args:    []string{"hello glowkron"},
	}

	res, err := executor.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", res.ExitCode)
	}

	got := strings.TrimSpace(res.Stdout)
	want := "hello glowkron"
	if got != want {
		t.Errorf("stdout mismatch: got %q, want %q", got, want)
	}

	if res.Duration < 0 {
		t.Errorf("expected positive duration, got %v", res.Duration)
	}
}

func TestRun_NonZeroExit(t *testing.T) {
	ctx := context.Background()
	cfg := executor.ProcessConfig{
		Command: "sh",
		Args:    []string{"-c", "exit 42"},
	}

	res, err := executor.Run(ctx, cfg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 42 {
		t.Errorf("expected exit code 42, got %d", res.ExitCode)
	}
}
