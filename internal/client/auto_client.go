package client

import (
	"context"
	"errors"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// AutoClient transparently selects between HTTPClient (when the daemon is online)
// and OfflineClient (when the daemon is offline), giving CLI users seamless operation.
type AutoClient struct {
	http    *HTTPClient
	offline *OfflineClient
}

// NewAutoClient initializes an AutoClient wrapping an HTTP endpoint and offline filesystem fallback.
func NewAutoClient(baseURL, dbPath, logsDir, workflowsDir string) (*AutoClient, error) {
	off, err := NewOfflineClient(dbPath, logsDir, workflowsDir)
	if err != nil {
		return nil, err
	}

	return &AutoClient{
		http:    NewHTTPClient(baseURL),
		offline: off,
	}, nil
}

// isOnline quickly probes if the daemon is online (within 200ms).
func (c *AutoClient) isOnline(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	status, err := c.http.Health(probeCtx)
	return err == nil && status != nil && status.Online
}

func (c *AutoClient) Health(ctx context.Context) (*HealthStatus, error) {
	status, err := c.http.Health(ctx)
	if err == nil {
		return status, nil
	}
	return c.offline.Health(ctx)
}

func (c *AutoClient) ListJobs(ctx context.Context, statusFilter string) ([]*store.Job, error) {
	if c.isOnline(ctx) {
		jobs, err := c.http.ListJobs(ctx, statusFilter)
		if err == nil {
			return jobs, nil
		}
	}
	return c.offline.ListJobs(ctx, statusFilter)
}

func (c *AutoClient) GetJob(ctx context.Context, id string) (*store.Job, error) {
	if c.isOnline(ctx) {
		job, err := c.http.GetJob(ctx, id)
		if err == nil {
			return job, nil
		}
		if errors.Is(err, ErrJobNotFound) {
			return nil, err
		}
	}
	return c.offline.GetJob(ctx, id)
}

func (c *AutoClient) TriggerJob(ctx context.Context, idOrPath string, async bool) (*store.JobRun, error) {
	if c.isOnline(ctx) {
		run, err := c.http.TriggerJob(ctx, idOrPath, async)
		if err == nil {
			return run, nil
		}
	}
	return c.offline.TriggerJob(ctx, idOrPath, async)
}

func (c *AutoClient) ListRuns(ctx context.Context, jobID string, limit int) ([]*store.JobRun, error) {
	if c.isOnline(ctx) {
		runs, err := c.http.ListRuns(ctx, jobID, limit)
		if err == nil {
			return runs, nil
		}
	}
	return c.offline.ListRuns(ctx, jobID, limit)
}

func (c *AutoClient) GetRun(ctx context.Context, runID string) (*store.JobRun, error) {
	if c.isOnline(ctx) {
		run, err := c.http.GetRun(ctx, runID)
		if err == nil {
			return run, nil
		}
		if errors.Is(err, ErrRunNotFound) {
			return nil, err
		}
	}
	return c.offline.GetRun(ctx, runID)
}

func (c *AutoClient) GetRunSteps(ctx context.Context, runID string) ([]*store.StepRun, error) {
	if c.isOnline(ctx) {
		steps, err := c.http.GetRunSteps(ctx, runID)
		if err == nil {
			return steps, nil
		}
	}
	return c.offline.GetRunSteps(ctx, runID)
}

func (c *AutoClient) GetRunLogs(ctx context.Context, runID string) (string, error) {
	if c.isOnline(ctx) {
		logs, err := c.http.GetRunLogs(ctx, runID)
		if err == nil {
			return logs, nil
		}
	}
	return c.offline.GetRunLogs(ctx, runID)
}

func (c *AutoClient) StreamRunLogs(ctx context.Context, runID string) (<-chan string, <-chan error, error) {
	if c.isOnline(ctx) {
		logCh, errCh, err := c.http.StreamRunLogs(ctx, runID)
		if err == nil {
			return logCh, errCh, nil
		}
	}
	return c.offline.StreamRunLogs(ctx, runID)
}

func (c *AutoClient) Close() error {
	_ = c.http.Close()
	return c.offline.Close()
}
