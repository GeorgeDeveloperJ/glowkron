package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// HTTPClient connects to the GlowKron daemon HTTP API.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPClient creates a client pointing to the daemon HTTP address (e.g. "http://127.0.0.1:8080").
func NewHTTPClient(baseURL string) *HTTPClient {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Health checks if the daemon is online and queries runtime metadata.
func (c *HTTPClient) Health(ctx context.Context) (*HealthStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/health", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected health status code: %d", resp.StatusCode)
	}

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode health response: %w", err)
	}
	status.Online = true
	return &status, nil
}

// ListJobs fetches registered workflows from the daemon.
func (c *HTTPClient) ListJobs(ctx context.Context, statusFilter string) ([]*store.Job, error) {
	endpoint := c.baseURL + "/api/jobs"
	if statusFilter != "" {
		endpoint += "?status=" + url.QueryEscape(statusFilter)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list jobs: HTTP %d", resp.StatusCode)
	}

	var jobs []*store.Job
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetJob queries a specific job definition from the daemon.
func (c *HTTPClient) GetJob(ctx context.Context, id string) (*store.Job, error) {
	endpoint := fmt.Sprintf("%s/api/jobs/%s", c.baseURL, url.PathEscape(id))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrJobNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get job %s: HTTP %d", id, resp.StatusCode)
	}

	var job store.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// TriggerJob requests the daemon to execute a workflow run.
func (c *HTTPClient) TriggerJob(ctx context.Context, idOrPath string, async bool) (*store.JobRun, error) {
	endpoint := fmt.Sprintf("%s/api/jobs/%s/run", c.baseURL, url.PathEscape(idOrPath))
	if async {
		endpoint += "?async=true"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrJobNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("trigger failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var run store.JobRun
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns lists execution history from the daemon.
func (c *HTTPClient) ListRuns(ctx context.Context, jobID string, limit int) ([]*store.JobRun, error) {
	vals := url.Values{}
	if jobID != "" {
		vals.Set("job_id", jobID)
	}
	if limit > 0 {
		vals.Set("limit", strconv.Itoa(limit))
	}

	endpoint := c.baseURL + "/api/runs"
	if len(vals) > 0 {
		endpoint += "?" + vals.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list runs: HTTP %d", resp.StatusCode)
	}

	var runs []*store.JobRun
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		return nil, err
	}
	return runs, nil
}

// GetRun queries a specific run record from the daemon.
func (c *HTTPClient) GetRun(ctx context.Context, runID string) (*store.JobRun, error) {
	endpoint := fmt.Sprintf("%s/api/runs/%s", c.baseURL, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrRunNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get run %s: HTTP %d", runID, resp.StatusCode)
	}

	var run store.JobRun
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		return nil, err
	}
	return &run, nil
}

// GetRunSteps queries step executions from the daemon.
func (c *HTTPClient) GetRunSteps(ctx context.Context, runID string) ([]*store.StepRun, error) {
	endpoint := fmt.Sprintf("%s/api/runs/%s/steps", c.baseURL, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get steps for run %s: HTTP %d", runID, resp.StatusCode)
	}

	var steps []*store.StepRun
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, err
	}
	return steps, nil
}

// GetRunLogs reads completed or buffered logs from the daemon.
func (c *HTTPClient) GetRunLogs(ctx context.Context, runID string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/runs/%s/logs", c.baseURL, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrRunNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get logs for run %s: HTTP %d", runID, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// StreamRunLogs subscribes to Server-Sent Events (SSE) from the daemon.
func (c *HTTPClient) StreamRunLogs(ctx context.Context, runID string) (<-chan string, <-chan error, error) {
	endpoint := fmt.Sprintf("%s/api/runs/%s/stream", c.baseURL, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrDaemonOffline, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, nil, ErrRunNotFound
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("SSE connection failed: HTTP %d", resp.StatusCode)
	}

	logCh := make(chan string, 100)
	errCh := make(chan error, 1)

	go func() {
		defer resp.Body.Close()
		defer close(logCh)
		defer close(errCh)

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				chunk := strings.TrimPrefix(line, "data: ")
				var payload struct {
					Chunk string `json:"chunk"`
				}
				if err := json.Unmarshal([]byte(chunk), &payload); err == nil && payload.Chunk != "" {
					logCh <- payload.Chunk
				} else {
					logCh <- chunk + "\n"
				}
			} else if strings.HasPrefix(line, "event: done") {
				return
			}
		}
		if err := scanner.Err(); err != nil && err != context.Canceled {
			errCh <- err
		}
	}()

	return logCh, errCh, nil
}

// Close closes any idle HTTP keepalive connections.
func (c *HTTPClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
