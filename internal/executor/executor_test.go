package executor_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
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
		t.Errorf("expected error due to timeout, got nil")
	}
}

func isProcessAlive(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "State:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				state := fields[1]
				if state == "Z" || state == "X" {
					return false
				}
				return true
			}
		}
	}
	return false
}

func TestRun_ProcessGroupKill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cfg := executor.ProcessConfig{
		Command: "sh",
		Args:    []string{"-c", "sleep 30 & echo $! && wait"},
	}

	res, _ := executor.Run(ctx, cfg)

	pidStr := strings.TrimSpace(res.Stdout)
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		t.Fatalf("failed to parse child PID %q: %v", pidStr, err)
	}

	alive := true
	for i := 0; i < 10; i++ {
		time.Sleep(50 * time.Millisecond)
		if !isProcessAlive(pid) {
			alive = false
			break
		}
	}
	if alive {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Errorf("child process %d is still actively running! Process group kill failed", pid)
	}
}

func TestRun_ConfigTimeout(t *testing.T) {
	ctx := context.Background()
	cfg := executor.ProcessConfig{
		Command: "sleep",
		Args:    []string{"1"},
		Timeout: 50 * time.Millisecond,
	}

	start := time.Now()
	_, err := executor.Run(ctx, cfg)
	elapsed := time.Since(start)

	if elapsed >= 500*time.Millisecond {
		t.Errorf("command took %v, expected cfg.Timeout to kill it around 50ms", elapsed)
	}
	if err == nil {
		t.Errorf("expected timeout error from cfg.Timeout, got nil")
	}
}

func TestRun_BoundedBufferAndCustomWriter(t *testing.T) {
	ctx := context.Background()
	var customStream bytes.Buffer

	cfg := executor.ProcessConfig{
		Command:   "sh",
		Args:      []string{"-c", "echo 'line1' && echo 'line2' && echo 'line3'"},
		Stdout:    &customStream,
		MaxBuffer: 10, // only keep last 10 bytes in ExecutionResult.Stdout
	}

	res, err := executor.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Custom stream receives the entire stdout
	fullStream := customStream.String()
	if !strings.Contains(fullStream, "line1") || !strings.Contains(fullStream, "line3") {
		t.Errorf("custom stream did not receive full output, got %q", fullStream)
	}

	// ExecutionResult.Stdout is bounded by MaxBuffer (10 bytes)
	if len(res.Stdout) > 10 {
		t.Errorf("expected res.Stdout len <= 10, got %d (%q)", len(res.Stdout), res.Stdout)
	}
	if !strings.HasSuffix(res.Stdout, "line3\n") {
		t.Errorf("expected res.Stdout to contain tail 'line3\\n', got %q", res.Stdout)
	}
}
