package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/engine"
	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/server"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

func setupTestServer(t *testing.T) (*server.Server, *store.Store, *logger.FileAppender, *logger.LogBroker, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "glowkron-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "server_test.db")
	logsDir := filepath.Join(tempDir, "logs")
	workflowsDir := filepath.Join(tempDir, "workflows")

	_ = os.MkdirAll(logsDir, 0755)
	_ = os.MkdirAll(workflowsDir, 0755)

	s, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	appender, err := logger.NewFileAppender(logsDir)
	if err != nil {
		t.Fatalf("failed to create file appender: %v", err)
	}

	broker := logger.NewLogBroker()
	eng := engine.New(s, appender, broker, 4)

	cfg := server.Config{
		Port:         8080,
		WorkflowsDir: workflowsDir,
	}

	srv := server.New(cfg, s, eng, appender, broker)
	return srv, s, appender, broker, tempDir
}

func TestServer_HealthAndUI(t *testing.T) {
	srv, s, _, _, tempDir := setupTestServer(t)
	defer os.RemoveAll(tempDir)
	defer s.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Test /api/health
	res, err := http.Get(ts.URL + "/api/health")
	if err != nil {
		t.Fatalf("GET /api/health error: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", res.StatusCode)
	}
	var health map[string]any
	if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if health["online"] != true {
		t.Errorf("expected online true, got %v", health["online"])
	}

	// 2. Test / (Embedded HTML Dashboard)
	resHTML, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET / error: %v", err)
	}
	defer resHTML.Body.Close()

	if resHTML.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for index.html, got %d", resHTML.StatusCode)
	}
	htmlBody, _ := io.ReadAll(resHTML.Body)
	if !strings.Contains(string(htmlBody), "GlowKron") {
		t.Errorf("index.html did not contain 'GlowKron'")
	}

	// 3. Test /app.css
	resCSS, err := http.Get(ts.URL + "/app.css")
	if err != nil {
		t.Fatalf("GET /app.css error: %v", err)
	}
	defer resCSS.Body.Close()
	if resCSS.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for app.css, got %d", resCSS.StatusCode)
	}

	// 4. Test /app.js
	resJS, err := http.Get(ts.URL + "/app.js")
	if err != nil {
		t.Fatalf("GET /app.js error: %v", err)
	}
	defer resJS.Body.Close()
	if resJS.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for app.js, got %d", resJS.StatusCode)
	}
}

func TestServer_REST_JobsAndRuns(t *testing.T) {
	srv, s, _, _, tempDir := setupTestServer(t)
	defer os.RemoveAll(tempDir)
	defer s.Close()

	ctx := context.Background()

	// Create test job and steps in store
	job := &store.Job{
		ID:          "build-pipeline",
		Name:        "build-pipeline",
		Description: "Build and test pipeline",
		Enabled:     true,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob error: %v", err)
	}

	step := &store.JobStep{
		ID:        "step-compile",
		JobID:     "build-pipeline",
		Name:      "Compile Binary",
		Command:   "echo",
		Args:      []string{"compiled successfully"},
		StepOrder: 1,
	}
	if err := s.CreateSteps(ctx, []*store.JobStep{step}); err != nil {
		t.Fatalf("CreateSteps error: %v", err)
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. GET /api/jobs
	resJobs, err := http.Get(ts.URL + "/api/jobs")
	if err != nil {
		t.Fatalf("GET /api/jobs error: %v", err)
	}
	defer resJobs.Body.Close()

	var jobsList []*store.Job
	if err := json.NewDecoder(resJobs.Body).Decode(&jobsList); err != nil {
		t.Fatalf("decode jobs error: %v", err)
	}
	if len(jobsList) != 1 || jobsList[0].ID != "build-pipeline" {
		t.Errorf("unexpected jobs list: %+v", jobsList)
	}

	// 2. GET /api/jobs/build-pipeline
	resJobDetail, err := http.Get(ts.URL + "/api/jobs/build-pipeline")
	if err != nil {
		t.Fatalf("GET /api/jobs/build-pipeline error: %v", err)
	}
	defer resJobDetail.Body.Close()

	var jobDetail server.JobDetailResponse
	if err := json.NewDecoder(resJobDetail.Body).Decode(&jobDetail); err != nil {
		t.Fatalf("decode job detail error: %v", err)
	}
	if jobDetail.ID != "build-pipeline" || len(jobDetail.Steps) != 1 {
		t.Errorf("unexpected job detail: %+v", jobDetail)
	}

	// 3. POST /api/jobs/build-pipeline/run
	resTrigger, err := http.Post(ts.URL+"/api/jobs/build-pipeline/run", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST trigger error: %v", err)
	}
	defer resTrigger.Body.Close()

	if resTrigger.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from trigger, got %d", resTrigger.StatusCode)
	}
	var runRecord store.JobRun
	if err := json.NewDecoder(resTrigger.Body).Decode(&runRecord); err != nil {
		t.Fatalf("decode run record error: %v", err)
	}
	if runRecord.Status != store.JobRunCompleted {
		t.Errorf("expected COMPLETED run, got %s", runRecord.Status)
	}

	// 4. GET /api/runs
	resRuns, err := http.Get(ts.URL + "/api/runs")
	if err != nil {
		t.Fatalf("GET /api/runs error: %v", err)
	}
	defer resRuns.Body.Close()

	var runsList []*store.JobRun
	if err := json.NewDecoder(resRuns.Body).Decode(&runsList); err != nil {
		t.Fatalf("decode runs list error: %v", err)
	}
	if len(runsList) != 1 || runsList[0].ID != runRecord.ID {
		t.Errorf("unexpected runs list: %+v", runsList)
	}

	// 5. GET /api/runs/{id}
	resGetRun, err := http.Get(ts.URL + "/api/runs/" + runRecord.ID)
	if err != nil {
		t.Fatalf("GET /api/runs/{id} error: %v", err)
	}
	defer resGetRun.Body.Close()

	var fetchedRun store.JobRun
	if err := json.NewDecoder(resGetRun.Body).Decode(&fetchedRun); err != nil {
		t.Fatalf("decode fetched run error: %v", err)
	}
	if fetchedRun.ID != runRecord.ID {
		t.Errorf("expected run ID %s, got %s", runRecord.ID, fetchedRun.ID)
	}

	// 6. GET /api/runs/{id}/steps
	resSteps, err := http.Get(ts.URL + "/api/runs/" + runRecord.ID + "/steps")
	if err != nil {
		t.Fatalf("GET steps error: %v", err)
	}
	defer resSteps.Body.Close()

	var stepRuns []*store.StepRun
	if err := json.NewDecoder(resSteps.Body).Decode(&stepRuns); err != nil {
		t.Fatalf("decode step runs error: %v", err)
	}
	if len(stepRuns) != 1 || stepRuns[0].Status != store.StepRunCompleted {
		t.Errorf("unexpected step runs: %+v", stepRuns)
	}

	// 7. GET /api/runs/{id}/logs
	resLogs, err := http.Get(ts.URL + "/api/runs/" + runRecord.ID + "/logs")
	if err != nil {
		t.Fatalf("GET logs error: %v", err)
	}
	defer resLogs.Body.Close()

	logBody, _ := io.ReadAll(resLogs.Body)
	if !strings.Contains(string(logBody), "compiled successfully") {
		t.Errorf("expected log output containing 'compiled successfully', got %q", string(logBody))
	}

	// 8. Edge cases: 404 Not Found for non-existent resources
	edgeTests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/jobs/non-existent"},
		{http.MethodPost, "/api/jobs/non-existent/run"},
		{http.MethodGet, "/api/runs/non-existent"},
		{http.MethodGet, "/api/runs/non-existent/steps"},
		{http.MethodGet, "/api/runs/non-existent/logs"},
	}

	for _, tc := range edgeTests {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s error: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 Not Found for %s %s, got %d", tc.method, tc.path, resp.StatusCode)
		}
	}
}

func TestServer_SSE_Stream(t *testing.T) {
	srv, s, appender, broker, tempDir := setupTestServer(t)
	defer os.RemoveAll(tempDir)
	defer s.Close()

	runID := "stream-test-run-1"

	// Write existing disk log
	writer, err := appender.OpenWriter(runID)
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}
	_, _ = writer.Write([]byte("initial line on disk\n"))
	_ = writer.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/runs/"+runID+"/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest error: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("SSE GET error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", resp.StatusCode)
	}

	// Publish live chunk via broker in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(runID, []byte("live chunk from broker\n"))
		time.Sleep(50 * time.Millisecond)
		broker.CloseRun(runID)
	}()

	scanner := bufio.NewScanner(resp.Body)
	var events []string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			chunk := strings.TrimPrefix(line, "data: ")
			events = append(events, chunk)
		} else if strings.HasPrefix(line, "event: done") {
			events = append(events, "done")
			break
		}
	}

	joined := strings.Join(events, " ")
	if !strings.Contains(joined, "initial line on disk") {
		t.Errorf("expected SSE to deliver initial disk log, got %v", events)
	}
	if !strings.Contains(joined, "live chunk from broker") {
		t.Errorf("expected SSE to deliver live broker chunk, got %v", events)
	}
	if !strings.Contains(joined, "done") {
		t.Errorf("expected SSE terminal done event, got %v", events)
	}
}
