package executor

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"time"
)

type ProcessConfig struct {
	Command string
	Args    []string
	Env     []string
	Dir     string
	Timeout time.Duration
}

type ExecutionResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
}

func Run(ctx context.Context, cfg ProcessConfig) (*ExecutionResult, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, cfg.Command, cfg.Args...)

	if cfg.Dir != "" {
		cmd.Dir = cfg.Dir
	}
	if len(cfg.Env) > 0 {
		cmd.Env = cfg.Env
	}

	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	cmdErr := cmd.Run()

	duration := time.Since(start)

	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()

	exitCode := 0

	if cmdErr != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](cmdErr); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, cmdErr
		}
	}

	return &ExecutionResult{ExitCode: exitCode, Stdout: stdout, Stderr: stderr, Duration: duration}, nil
}
