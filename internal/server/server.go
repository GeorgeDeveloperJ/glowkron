package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/engine"
	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/server/ui"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// Server coordinates the HTTP REST API, SSE real-time log streamer, and embedded dashboard.
type Server struct {
	httpServer   *http.Server
	store        *store.Store
	engine       *engine.Engine
	appender     *logger.FileAppender
	broker       *logger.LogBroker
	workflowsDir string
	port         int
}

// Config configures the Server instance.
type Config struct {
	Port         int
	WorkflowsDir string
}

// New creates and configures the HTTP Server.
func New(cfg Config, s *store.Store, eng *engine.Engine, appender *logger.FileAppender, broker *logger.LogBroker) *Server {
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}

	srv := &Server{
		store:        s,
		engine:       eng,
		appender:     appender,
		broker:       broker,
		workflowsDir: cfg.WorkflowsDir,
		port:         cfg.Port,
	}

	mux := http.NewServeMux()

	// REST API v1
	mux.HandleFunc("GET /api/health", srv.handleHealth)
	mux.HandleFunc("GET /api/jobs", srv.handleListJobs)
	mux.HandleFunc("GET /api/jobs/{id}", srv.handleGetJob)
	mux.HandleFunc("POST /api/jobs/{id}/run", srv.handleTriggerJob)
	mux.HandleFunc("GET /api/runs", srv.handleListRuns)
	mux.HandleFunc("GET /api/runs/{id}", srv.handleGetRun)
	mux.HandleFunc("GET /api/runs/{id}/steps", srv.handleGetStepRuns)
	mux.HandleFunc("GET /api/runs/{id}/logs", srv.handleGetLogs)
	mux.HandleFunc("GET /api/runs/{id}/stream", srv.handleSSEStream)

	// Embedded Static UI Dashboard
	uiHandler := ui.Handler()
	mux.Handle("/", uiHandler)

	// Wrap with CORS and logging middleware
	handler := withCORS(mux)

	srv.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // SSE streaming requires unbounded write timeout
		IdleTimeout:  60 * time.Second,
	}

	return srv
}

// Handler returns the underlying http.Handler for testing with httptest.Server.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// Start runs the HTTP listener. Blocks until server closes or errors.
func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully terminates the HTTP server and active connections.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
