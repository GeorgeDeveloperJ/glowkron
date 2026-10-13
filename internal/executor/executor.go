// Package executor runs the scripts, provides outputs and handles errors
// Using buffers and time for I/O and duration
package executor

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type ProcessConfig struct {
	Command   string
	Args      []string
	Env       []string
	Dir       string
	Timeout   time.Duration
	Stdout    io.Writer // Optional destination for full stdout stream
	Stderr    io.Writer // Optional destination for full stderr stream
	MaxBuffer int       // Max in-memory capture bytes in ExecutionResult (default 64KB)
}

type ExecutionResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
}

// boundedBuffer captures up to limit bytes in memory, keeping the tail on overflow.
type boundedBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func newBoundedBuffer(limit int) *boundedBuffer {
	if limit <= 0 {
		limit = 64 * 1024 // 64KB default
	}
	return &boundedBuffer{
		buf:   make([]byte, 0, limit),
		limit: limit,
	}
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(p)
	if n >= b.limit {
		b.buf = append(b.buf[:0], p[n-b.limit:]...)
		return n, nil
	}

	space := b.limit - len(b.buf)
	if n > space {
		drop := n - space
		copy(b.buf, b.buf[drop:])
		b.buf = b.buf[:len(b.buf)-drop]
	}
	b.buf = append(b.buf, p...)
	return n, nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

func Run(ctx context.Context, cfg ProcessConfig) (*ExecutionResult, error) {
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	start := time.Now()
	cmd := exec.CommandContext(ctx, cfg.Command, cfg.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	if cfg.Dir != "" {
		cmd.Dir = cfg.Dir
	}
	if len(cfg.Env) > 0 {
		cmd.Env = append(os.Environ(), cfg.Env...)
	}

	limit := cfg.MaxBuffer
	if limit <= 0 {
		limit = 64 * 1024
	}

	stdoutBuf := newBoundedBuffer(limit)
	stderrBuf := newBoundedBuffer(limit)

	if cfg.Stdout != nil {
		cmd.Stdout = io.MultiWriter(cfg.Stdout, stdoutBuf)
	} else {
		cmd.Stdout = stdoutBuf
	}

	if cfg.Stderr != nil {
		cmd.Stderr = io.MultiWriter(cfg.Stderr, stderrBuf)
	} else {
		cmd.Stderr = stderrBuf
	}

	cmd.WaitDelay = 1 * time.Second

	cmdErr := cmd.Run()

	duration := time.Since(start)

	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()

	exitCode := 0

	if ctx.Err() != nil {
		return &ExecutionResult{ExitCode: -1, Stdout: stdout, Stderr: stderr, Duration: duration}, ctx.Err()
	}

	if cmdErr != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](cmdErr); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, cmdErr
		}
	}

	return &ExecutionResult{ExitCode: exitCode, Stdout: stdout, Stderr: stderr, Duration: duration}, nil
}
