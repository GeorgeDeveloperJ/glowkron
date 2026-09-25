package executor_test

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestRun_TimeoutCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cfg := executor.ProcessConfig{
		Command: "sleep",
		Args:    []string{"1"},
	}

	start := time.Now()
	res, err := executor.Run(ctx, cfg)
	elapsed := time.Since(start)

	if res.Duration >= 500*time.Millisecond {
		t.Errorf("command took %v, expected it to be killed around 50ms", elapsed)
	}

	if err == nil {
		t.Errorf("expeted error due to timeout, got nil")
	}
}
