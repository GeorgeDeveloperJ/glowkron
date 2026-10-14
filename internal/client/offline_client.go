package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/engine"
	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// OfflineClient provides direct access to the SQLite database, workflow parser,
// and disk logs without requiring the GlowKron daemon process to be running.
type OfflineClient struct {
	store        *store.Store
	logsDir      string
	workflowsDir string
}

// NewOfflineClient initializes an OfflineClient instance.
func NewOfflineClient(dbPath, logsDir, workflowsDir string) (*OfflineClient, error) {
	s, err := store.New(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite store at %s: %w", dbPath, err)
	}

	return &OfflineClient{
		store:        s,
		logsDir:      logsDir,
		workflowsDir: workflowsDir,
	}, nil
}

// Health reports that the daemon is offline.
func (c *OfflineClient) Health(ctx context.Context) (*HealthStatus, error) {
	return &HealthStatus{
		Online:       false,
		WorkflowsDir: c.workflowsDir,
	}, nil
}

// ListJobs queries registered jobs directly from the SQLite store.
func (c *OfflineClient) ListJobs(ctx context.Context, statusFilter string) ([]*store.Job, error) {
	jobs, err := c.store.ListJobs(ctx)
	if err != nil {
		return nil, err
	}

	filter := strings.ToLower(statusFilter)
	if filter == "" || filter == "all" {
		return jobs, nil
	}

	var filtered []*store.Job
	for _, j := range jobs {
		if filter == "enabled" && j.Enabled {
			filtered = append(filtered, j)
		} else if filter == "disabled" && !j.Enabled {
			filtered = append(filtered, j)
		}
	}
	return filtered, nil
}

// GetJob queries a specific job from SQLite.
func (c *OfflineClient) GetJob(ctx context.Context, id string) (*store.Job, error) {
	j, err := c.store.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, ErrJobNotFound
	}
	return j, nil
}

// TriggerJob executes a workflow locally in-process without requiring a background daemon.
func (c *OfflineClient) TriggerJob(ctx context.Context, idOrPath string, async bool) (*store.JobRun, error) {
	var wf *workflow.WorkflowConfig
	var err error

	// Check if idOrPath is an existing file or a path in workflowsDir
	filePath := idOrPath
	if _, statErr := os.Stat(filePath); statErr != nil {
		candidate := filepath.Join(c.workflowsDir, idOrPath)
		if _, statErr2 := os.Stat(candidate); statErr2 == nil {
			filePath = candidate
		} else if !strings.HasSuffix(candidate, ".yaml") && !strings.HasSuffix(candidate, ".yml") {
			candidateYaml := candidate + ".yaml"
			if _, statErr3 := os.Stat(candidateYaml); statErr3 == nil {
				filePath = candidateYaml
			}
		}
	}

	if _, statErr := os.Stat(filePath); statErr == nil {
		wf, err = workflow.ParseFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse workflow file %s: %w", filePath, err)
		}
	} else {
		// Attempt to load job and steps from SQLite store
		job, err := c.store.GetJob(ctx, idOrPath)
		if err != nil || job == nil {
			return nil, fmt.Errorf("workflow file or job %q not found: %w", idOrPath, ErrJobNotFound)
		}
		steps, err := c.store.GetStepsByJobID(ctx, job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load steps for job %s: %w", job.ID, err)
		}

		wf = &workflow.WorkflowConfig{
			Name:        job.Name,
			Description: job.Description,
		}
		for _, s := range steps {
			wf.Steps = append(wf.Steps, workflow.StepConfig{
				ID:        s.ID,
				Name:      s.Name,
				Command:   s.Command,
				DependsOn: s.DependsOn,
			})
		}
	}

	appender, err := logger.NewFileAppender(c.logsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize log appender: %w", err)
	}
	broker := logger.NewLogBroker()
	eng := engine.New(c.store, appender, broker, 4)

	if async {
		runID := fmt.Sprintf("run-%s-%d", wf.Name, time.Now().UnixNano())
		runRecord := &store.JobRun{
			ID:          runID,
			JobID:       wf.Name,
			Status:      store.JobRunPending,
			TriggeredBy: store.TriggeredByCLI,
			StartTime:   time.Now(),
		}
		if err := c.store.CreateJobRun(ctx, runRecord); err != nil {
			return nil, err
		}

		go func() {
			_, _ = eng.ExecuteWorkflow(context.Background(), wf, store.TriggeredByCLI)
		}()
		return runRecord, nil
	}

	return eng.ExecuteWorkflow(ctx, wf, store.TriggeredByCLI)
}

// ListRuns queries historical executions from SQLite.
func (c *OfflineClient) ListRuns(ctx context.Context, jobID string, limit int) ([]*store.JobRun, error) {
	return c.store.ListJobRuns(ctx, jobID, limit)
}

// GetRun queries a single execution run from SQLite.
func (c *OfflineClient) GetRun(ctx context.Context, runID string) (*store.JobRun, error) {
	run, err := c.store.GetJobRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, ErrRunNotFound
	}
	return run, nil
}

// GetRunSteps retrieves step executions from SQLite.
func (c *OfflineClient) GetRunSteps(ctx context.Context, runID string) ([]*store.StepRun, error) {
	return c.store.GetStepRunsByJobRunID(ctx, runID)
}

// GetRunLogs reads the raw log file from the logs directory.
func (c *OfflineClient) GetRunLogs(ctx context.Context, runID string) (string, error) {
	logPath := filepath.Join(c.logsDir, runID+".log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrRunNotFound
		}
		return "", err
	}
	return string(data), nil
}

// StreamRunLogs for offline mode streams the current contents of the log file and closes.
func (c *OfflineClient) StreamRunLogs(ctx context.Context, runID string) (<-chan string, <-chan error, error) {
	content, err := c.GetRunLogs(ctx, runID)
	if err != nil {
		return nil, nil, err
	}

	logCh := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(logCh)
		defer close(errCh)
		if content != "" {
			logCh <- content
		}
	}()

	return logCh, errCh, nil
}

// Close closes the SQLite store connection.
func (c *OfflineClient) Close() error {
	return c.store.Close()
}
