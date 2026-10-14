package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/executor"
	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/notifier"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// Notifier defines the interface for dispatching webhook events.
type Notifier interface {
	Dispatch(event notifier.Event) bool
}

// Engine orchestrates end-to-end DAG workflow execution using a bounded worker pool.
type Engine struct {
	store          *store.Store
	appender       *logger.FileAppender
	broker         *logger.LogBroker
	notifier       Notifier
	maxConcurrency int
}

// New creates an Engine instance configured with store persistence, file appender,
// pub/sub broker, and maximum concurrency limit.
func New(store *store.Store, appender *logger.FileAppender, broker *logger.LogBroker, maxConcurrency int) *Engine {
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}
	return &Engine{
		store:          store,
		appender:       appender,
		broker:         broker,
		maxConcurrency: maxConcurrency,
	}
}

// SetNotifier configures an optional webhook alert dispatcher.
func (e *Engine) SetNotifier(n Notifier) {
	e.notifier = n
}

// ExecuteWorkflow executes a declarative DAG workflow:
// - Validates and resolves the DAG topological execution stages.
// - Ensures Job and JobStep entities exist in SQLite to satisfy foreign keys.
// - Creates and records JobRun in SQLite with RUNNING status.
// - Opens a disk log writer (~/.glowkron/logs/<runID>.log) and initializes broker pub/sub.
// - Dispatches steps wave-by-wave through DAG stages and bounded WorkerPool.
// - Pipes stdout/stderr streams to disk file and broker, saving up to 2KB snippets to SQLite step_runs.
// - Implements retries with exponential backoff and jitter if steps fail.
// - Marks downstream steps as SKIPPED if prerequisites fail.
// - Finalizes JobRun status (COMPLETED, FAILED, or CANCELLED) and cleans up broker resources.
func (e *Engine) ExecuteWorkflow(ctx context.Context, wf *workflow.WorkflowConfig, triggeredBy store.TriggeredBy) (*store.JobRun, error) {
	if wf == nil {
		return nil, errors.New("workflow configuration cannot be nil")
	}
	if err := workflow.Validate(wf); err != nil {
		return nil, fmt.Errorf("workflow validation failed: %w", err)
	}

	plan, graph, err := dag.BuildAndResolve(wf.Steps)
	if err != nil {
		return nil, fmt.Errorf("dag resolution failed: %w", err)
	}

	// 1. Ensure Job and JobStep entities exist in SQLite
	job, err := e.ensureJobAndSteps(ctx, wf)
	if err != nil {
		return nil, err
	}

	// 2. Setup workflow timeout if configured
	execCtx := ctx
	if wf.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, wf.Timeout)
		defer cancel()
	}

	// 3. Create initial JobRun record in store
	runID := uuid.NewString()
	logPath, err := e.appender.LogFilePath(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to determine log file path: %w", err)
	}

	jobRun := &store.JobRun{
		ID:          runID,
		JobID:       job.ID,
		Status:      store.JobRunRunning,
		TriggeredBy: triggeredBy,
		StartTime:   time.Now().UTC(),
		Attempt:     1,
		LogFilePath: logPath,
	}
	if err := e.store.CreateJobRun(execCtx, jobRun); err != nil {
		return nil, fmt.Errorf("failed to create job run in store: %w", err)
	}

	// 4. Open disk log file writer and schedule broker channel cleanup
	logWriter, err := e.appender.OpenWriter(runID)
	if err != nil {
		failCode := 1
		_ = e.store.UpdateJobRunStatus(execCtx, runID, store.JobRunFailed, &failCode, err.Error())
		return nil, fmt.Errorf("failed to open disk log writer: %w", err)
	}
	defer logWriter.Close()
	defer e.broker.CloseRun(runID)

	// 5. Initialize in-memory StateMachine and insert PENDING step_runs in store
	sm := NewStateMachine(wf.Steps, graph)
	stepRunMap := make(map[string]*store.StepRun, len(wf.Steps))

	for _, s := range wf.Steps {
		sr := &store.StepRun{
			ID:        uuid.NewString(),
			JobRunID:  runID,
			StepID:    s.ID,
			Status:    store.StepRunPending,
			StartTime: time.Now().UTC(),
		}
		if err := e.store.CreateStepRun(execCtx, sr); err != nil {
			failCode := 1
			_ = e.store.UpdateJobRunStatus(execCtx, runID, store.JobRunFailed, &failCode, err.Error())
			return nil, fmt.Errorf("failed to create step run %s: %w", s.ID, err)
		}
		sm.SetStepRunID(s.ID, sr.ID)
		stepRunMap[s.ID] = sr
	}

	// 6. Execute DAG stages sequentially; steps in each stage run concurrently bounded by worker pool
	pool := NewWorkerPool(e.maxConcurrency)
	defer pool.Close()

	stepConfigMap := make(map[string]workflow.StepConfig, len(wf.Steps))
	for _, s := range wf.Steps {
		stepConfigMap[s.ID] = s
	}

	for _, stageStepIDs := range plan.Stages {
		if execCtx.Err() != nil {
			break
		}

		var stageWg sync.WaitGroup
		for _, stepID := range stageStepIDs {
			stepCfg := stepConfigMap[stepID]
			sr := stepRunMap[stepID]

			// Skip step if any dependency failed or was skipped
			if sm.ShouldSkip(stepID) {
				skipReason := "Prerequisite step failed or skipped"
				sm.SetSkipped(stepID, skipReason)
				sr.Status = store.StepRunSkipped
				now := time.Now().UTC()
				sr.EndTime = &now
				_ = e.store.UpdateStepRun(execCtx, sr)
				continue
			}

			// Step is runnable: acquire bounded slot and execute
			stageWg.Add(1)
			go func(sCfg workflow.StepConfig, sRun *store.StepRun) {
				defer stageWg.Done()

				if err := pool.Acquire(execCtx); err != nil {
					failCode := -1
					errMsg := err.Error()
					sm.SetFailed(sCfg.ID, failCode, errMsg, "", "")
					sRun.Status = store.StepRunFailed
					sRun.ExitCode = &failCode
					now := time.Now().UTC()
					sRun.EndTime = &now
					_ = e.store.UpdateStepRun(execCtx, sRun)
					return
				}
				defer pool.Release()

				e.executeStep(execCtx, runID, wf, sCfg, sRun, sm, logWriter)
			}(stepCfg, sr)
		}
		stageWg.Wait()
	}

	// 7. Finalize JobRun status in SQLite
	var finalStatus store.JobRunStatus
	var finalExitCode *int
	var finalErrMsg string

	if execCtx.Err() != nil {
		if errors.Is(execCtx.Err(), context.Canceled) {
			finalStatus = store.JobRunCancelled
		} else {
			finalStatus = store.JobRunFailed
		}
		code := -1
		finalExitCode = &code
		finalErrMsg = execCtx.Err().Error()
	} else if sm.HasFailures() {
		finalStatus = store.JobRunFailed
		code := 1
		finalExitCode = &code
		failedList := sm.GetFailedSteps()
		finalErrMsg = fmt.Sprintf("workflow execution failed: steps %v failed", failedList)
	} else if sm.AllCompleted() {
		finalStatus = store.JobRunCompleted
		code := 0
		finalExitCode = &code
		finalErrMsg = ""
	} else {
		finalStatus = store.JobRunFailed
		code := 1
		finalExitCode = &code
		finalErrMsg = "workflow did not complete all steps"
	}

	if err := e.store.UpdateJobRunStatus(context.Background(), runID, finalStatus, finalExitCode, finalErrMsg); err != nil {
		return nil, fmt.Errorf("failed to finalize job run status: %w", err)
	}

	updatedRun, err := e.store.GetJobRun(context.Background(), runID)
	if err != nil {
		return nil, fmt.Errorf("failed to reload finalized job run: %w", err)
	}

	if e.notifier != nil && wf.Notifications.WebhookURL != "" {
		codeVal := 0
		if finalExitCode != nil {
			codeVal = *finalExitCode
		}
		duration := time.Duration(0)
		if updatedRun.EndTime != nil {
			duration = updatedRun.EndTime.Sub(updatedRun.StartTime)
		}
		eventType := notifier.EventWorkflowCompleted
		if finalStatus == store.JobRunFailed || finalStatus == store.JobRunCancelled {
			eventType = notifier.EventWorkflowFailed
		}
		e.notifier.Dispatch(notifier.Event{
			Type:         eventType,
			Timestamp:    time.Now().UTC(),
			JobID:        wf.Name,
			JobName:      wf.Name,
			RunID:        runID,
			Status:       finalStatus,
			ExitCode:     codeVal,
			Duration:     duration,
			ErrorMessage: finalErrMsg,
			WebhookURL:   wf.Notifications.WebhookURL,
			Config:       wf.Notifications,
		})
	}

	if execCtx.Err() != nil {
		return updatedRun, execCtx.Err()
	}

	return updatedRun, nil
}

func (e *Engine) executeStep(
	ctx context.Context,
	runID string,
	wf *workflow.WorkflowConfig,
	stepCfg workflow.StepConfig,
	stepRun *store.StepRun,
	sm *StateMachine,
	logWriter io.Writer,
) {
	sm.SetRunning(stepCfg.ID)
	stepRun.Status = store.StepRunRunning
	_ = e.store.UpdateStepRun(ctx, stepRun)

	maxRetries := wf.MaxRetries
	retryDelay := wf.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 10 * time.Second
	}

	var lastResult *executor.ExecutionResult
	var lastErr error
	attempt := 0

	for {
		attempt++

		stepWriter := &stepStreamWriter{
			runID:  runID,
			stepID: stepCfg.ID,
			writer: logWriter,
			broker: e.broker,
		}

		procCfg := executor.ProcessConfig{
			Command:   stepCfg.Command,
			Args:      stepCfg.Args,
			Dir:       stepCfg.WorkingDir,
			Timeout:   stepCfg.Timeout,
			Stdout:    stepWriter,
			Stderr:    stepWriter,
			MaxBuffer: 64 * 1024,
		}

		if len(stepCfg.Env) > 0 {
			envList := make([]string, 0, len(stepCfg.Env))
			for k, v := range stepCfg.Env {
				envList = append(envList, fmt.Sprintf("%s=%s", k, v))
			}
			procCfg.Env = envList
		}

		res, err := executor.Run(ctx, procCfg)
		lastResult = res
		lastErr = err

		// Successful run
		if err == nil && res != nil && res.ExitCode == 0 {
			stdoutSnip := truncateSnippet(res.Stdout, 2048)
			stderrSnip := truncateSnippet(res.Stderr, 2048)
			sm.SetCompleted(stepCfg.ID, 0, stdoutSnip, stderrSnip)

			stepRun.Status = store.StepRunCompleted
			zero := 0
			stepRun.ExitCode = &zero
			stepRun.StdoutSnippet = stdoutSnip
			stepRun.StderrSnippet = stderrSnip
			now := time.Now().UTC()
			stepRun.EndTime = &now
			_ = e.store.UpdateStepRun(ctx, stepRun)
			return
		}

		// Retries on failure
		if attempt <= maxRetries && ctx.Err() == nil {
			backoff := CalculateBackoff(attempt, retryDelay, 5*time.Minute)
			select {
			case <-ctx.Done():
				break
			case <-time.After(backoff):
				continue
			}
		}

		break
	}

	// Finalize failed step
	exitCode := 1
	stdoutStr := ""
	stderrStr := ""
	if lastResult != nil {
		exitCode = lastResult.ExitCode
		stdoutStr = lastResult.Stdout
		stderrStr = lastResult.Stderr
	}

	errMsg := ""
	if lastErr != nil {
		errMsg = lastErr.Error()
	} else if exitCode != 0 {
		errMsg = fmt.Sprintf("command exited with code %d", exitCode)
	}

	stdoutSnip := truncateSnippet(stdoutStr, 2048)
	stderrSnip := truncateSnippet(stderrStr, 2048)

	sm.SetFailed(stepCfg.ID, exitCode, errMsg, stdoutSnip, stderrSnip)

	stepRun.Status = store.StepRunFailed
	stepRun.ExitCode = &exitCode
	stepRun.StdoutSnippet = stdoutSnip
	stepRun.StderrSnippet = stderrSnip
	now := time.Now().UTC()
	stepRun.EndTime = &now
	_ = e.store.UpdateStepRun(ctx, stepRun)
}

func (e *Engine) ensureJobAndSteps(ctx context.Context, wf *workflow.WorkflowConfig) (*store.Job, error) {
	jobID := wf.Name
	job, err := e.store.GetJob(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) {
		timeoutSec := 3600
		if wf.Timeout > 0 {
			timeoutSec = int(wf.Timeout.Seconds())
		}
		retryDelaySec := 10
		if wf.RetryDelay > 0 {
			retryDelaySec = int(wf.RetryDelay.Seconds())
		}
		concurrencyGroup := wf.ConcurrencyGroup
		if concurrencyGroup == "" {
			concurrencyGroup = "default"
		}

		job = &store.Job{
			ID:               jobID,
			Name:             wf.Name,
			Description:      wf.Description,
			CronExpr:         wf.Schedule,
			MaxRetries:       wf.MaxRetries,
			RetryDelaySec:    retryDelaySec,
			TimeoutSec:       timeoutSec,
			ConcurrencyGroup: concurrencyGroup,
			SourceFile:       wf.SourceFile,
			Enabled:          true,
		}
		if err := e.store.CreateJob(ctx, job); err != nil {
			if j, getErr := e.store.GetJob(ctx, jobID); getErr == nil {
				job = j
			} else {
				return nil, fmt.Errorf("failed to create job in store: %w", err)
			}
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to query job from store: %w", err)
	}

	existingSteps, err := e.store.GetStepsByJobID(ctx, job.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to query job steps from store: %w", err)
	}
	stepMap := make(map[string]bool, len(existingSteps))
	for _, s := range existingSteps {
		stepMap[s.ID] = true
	}

	var newSteps []*store.JobStep
	for i, s := range wf.Steps {
		if !stepMap[s.ID] {
			newSteps = append(newSteps, &store.JobStep{
				ID:         s.ID,
				JobID:      job.ID,
				Name:       s.Name,
				Command:    s.Command,
				Args:       s.Args,
				WorkingDir: s.WorkingDir,
				EnvVars:    s.Env,
				DependsOn:  s.DependsOn,
				StepOrder:  i,
			})
		}
	}
	if len(newSteps) > 0 {
		if err := e.store.CreateSteps(ctx, newSteps); err != nil {
			return nil, fmt.Errorf("failed to persist job steps: %w", err)
		}
	}

	return job, nil
}

type stepStreamWriter struct {
	runID  string
	stepID string
	writer io.Writer
	broker *logger.LogBroker
}

func (w *stepStreamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, err := w.writer.Write(p)
	if w.broker != nil {
		w.broker.Publish(w.runID, p)
	}
	return n, err
}

func truncateSnippet(s string, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = 2048
	}
	if len(s) <= maxBytes {
		return s
	}
	return s[len(s)-maxBytes:]
}
