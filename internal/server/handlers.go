package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

type JobDetailResponse struct {
	*store.Job
	Steps []*store.JobStep `json:"steps"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"online":        true,
		"version":       "v0.1.0",
		"workflows_dir": s.workflowsDir,
		"time":          time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	statusFilter := strings.ToLower(r.URL.Query().Get("status"))

	jobs, err := s.store.ListJobs(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	if statusFilter == "" || statusFilter == "all" {
		_ = json.NewEncoder(w).Encode(jobs)
		return
	}

	var filtered []*store.Job
	for _, j := range jobs {
		if statusFilter == "enabled" && j.Enabled {
			filtered = append(filtered, j)
		} else if statusFilter == "disabled" && !j.Enabled {
			filtered = append(filtered, j)
		}
	}
	_ = json.NewEncoder(w).Encode(filtered)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID := r.PathValue("id")

	job, err := s.store.GetJob(r.Context(), jobID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && job == nil) {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	steps, _ := s.store.GetStepsByJobID(r.Context(), job.ID)
	resp := JobDetailResponse{
		Job:   job,
		Steps: steps,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleTriggerJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID := r.PathValue("id")
	async := r.URL.Query().Get("async") == "true"

	job, err := s.store.GetJob(r.Context(), jobID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && job == nil) {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	// Build WorkflowConfig from source file or stored steps
	var wf *workflow.WorkflowConfig
	if job.SourceFile != "" {
		if _, statErr := os.Stat(job.SourceFile); statErr == nil {
			wf, _ = workflow.ParseFile(job.SourceFile)
		}
	}

	if wf == nil {
		steps, err := s.store.GetStepsByJobID(r.Context(), job.ID)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}

		wf = &workflow.WorkflowConfig{
			Version:     "1",
			Name:        job.Name,
			Description: job.Description,
		}
		for _, st := range steps {
			wf.Steps = append(wf.Steps, workflow.StepConfig{
				ID:         st.ID,
				Name:       st.Name,
				Command:    st.Command,
				Args:       st.Args,
				WorkingDir: st.WorkingDir,
				Env:        st.EnvVars,
				DependsOn:  st.DependsOn,
			})
		}
	}

	if async {
		runID := fmt.Sprintf("run-%s-%d", wf.Name, time.Now().UnixNano())
		runRecord := &store.JobRun{
			ID:          runID,
			JobID:       wf.Name,
			Status:      store.JobRunPending,
			TriggeredBy: store.TriggeredByWebUI,
			StartTime:   time.Now(),
		}
		if err := s.store.CreateJobRun(r.Context(), runRecord); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}

		go func() {
			_, _ = s.engine.ExecuteWorkflow(context.Background(), wf, store.TriggeredByWebUI)
		}()

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(runRecord)
		return
	}

	run, err := s.engine.ExecuteWorkflow(r.Context(), wf, store.TriggeredByWebUI)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(run)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobID := r.URL.Query().Get("job_id")
	limitStr := r.URL.Query().Get("limit")

	limit := 30
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	runs, err := s.store.ListJobRuns(r.Context(), jobID, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(runs)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	runID := r.PathValue("id")

	run, err := s.store.GetJobRun(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && run == nil) {
		http.Error(w, `{"error":"run not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(run)
}

func (s *Server) handleGetStepRuns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	runID := r.PathValue("id")

	run, err := s.store.GetJobRun(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && run == nil) {
		http.Error(w, `{"error":"run not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	steps, err := s.store.GetStepRunsByJobRunID(r.Context(), runID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(steps)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")

	logPath := filepath.Join(s.appender.BaseDir(), runID+".log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "log file not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Subscribe to in-memory broker
	_, subCh, cancel := s.broker.Subscribe(runID)
	defer cancel()

	// If disk log already exists, flush historical content
	logPath := filepath.Join(s.appender.BaseDir(), runID+".log")
	if data, err := os.ReadFile(logPath); err == nil && len(data) > 0 {
		payload, _ := json.Marshal(map[string]string{"chunk": string(data)})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	notify := r.Context().Done()

	for {
		select {
		case <-notify:
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case chunk, ok := <-subCh:
			if !ok {
				// Run completed and broker channel closed
				_, _ = fmt.Fprintf(w, "event: done\ndata: {}\n\n")
				flusher.Flush()
				return
			}
			payload, _ := json.Marshal(map[string]string{"chunk": string(chunk)})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}
