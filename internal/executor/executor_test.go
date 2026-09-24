package executor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/GeorgeDeveloperJ/glowkron/internal/executor"
)

func TestRun_SimpleEcho(t *testing.T) {
	ctx := context.Background()
	var cfg executor.ProcessConfig

	cfg.Command = "echo"
	cfg.Args = []string{"hello glowkron"}

	res, err := executor.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res == nil {
		t.Fatalf("expected non-nil result")
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
