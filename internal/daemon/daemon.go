package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/engine"
	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
	"github.com/GeorgeDeveloperJ/glowkron/internal/notifier"
	"github.com/GeorgeDeveloperJ/glowkron/internal/scheduler"
	"github.com/GeorgeDeveloperJ/glowkron/internal/server"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/watcher"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// Config configures daemon execution options.
type Config struct {
	DBPath          string
	WorkflowsDir    string
	LogsDir         string
	MaxConcurrency  int
	ShutdownTimeout time.Duration
	Port            int
}

// Daemon coordinates all subsystem services, lifecycle transitions, and graceful termination.
type Daemon struct {
	mu           sync.Mutex
	cfg          Config
	store        *store.Store
	appender     *logger.FileAppender
	broker       *logger.LogBroker
	engine       *engine.Engine
	scheduler    *scheduler.Scheduler
	watcher      *watcher.Watcher
	syncer       *watcher.Syncer
	dispatcher   *notifier.Dispatcher
	server       *server.Server
	coordinator  *ShutdownCoordinator
	wg           sync.WaitGroup
	serverWg     sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	running      bool
}

// New initializes the daemon and its subsystem services.
func New(cfg Config) (*Daemon, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("DBPath is required")
	}
	if cfg.WorkflowsDir == "" {
		cfg.WorkflowsDir = "./workflows"
	}
	if cfg.LogsDir == "" {
		cfg.LogsDir = "./logs"
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 10
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	st, err := store.New(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("init store: %w", err)
	}

	appender, err := logger.NewFileAppender(cfg.LogsDir)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init log appender: %w", err)
	}

	broker := logger.NewLogBroker()
	eng := engine.New(st, appender, broker, cfg.MaxConcurrency)
	disp := notifier.NewDispatcher()
	eng.SetNotifier(disp)

	ctx, cancel := context.WithCancel(context.Background())

	d := &Daemon{
		cfg:         cfg,
		store:       st,
		appender:    appender,
		broker:      broker,
		engine:      eng,
		dispatcher:  disp,
		coordinator: NewShutdownCoordinator(),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Trigger callback for scheduled jobs
	triggerFunc := func(triggerCtx context.Context, jobID string) error {
		job, err := d.store.GetJob(triggerCtx, jobID)
		if err != nil {
			return fmt.Errorf("fetch job %s: %w", jobID, err)
		}
		if !job.Enabled {
			return nil
		}

		// Read source file or parse workflow from job definition
		wf, err := workflow.ParseFile(job.SourceFile)
		if err != nil {
			return fmt.Errorf("parse workflow %s from %s: %w", jobID, job.SourceFile, err)
		}

		// Dispatch workflow execution via engine
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			_, _ = d.engine.ExecuteWorkflow(d.ctx, wf, store.TriggeredByCron)
		}()
		return nil
	}

	d.scheduler = scheduler.New(triggerFunc)
	d.syncer = watcher.NewSyncer(d.store, d.scheduler)

	wt, err := watcher.New(cfg.WorkflowsDir, d.syncer)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("init watcher: %w", err)
	}
	d.watcher = wt

	port := cfg.Port
	if port <= 0 {
		port = 8080
	}
	d.server = server.New(server.Config{
		Port:         port,
		WorkflowsDir: cfg.WorkflowsDir,
	}, d.store, d.engine, d.appender, d.broker)

	return d, nil
}

// Start boots the daemon, performs crash recovery, and starts services.
func (d *Daemon) Start(ctxs ...context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return nil
	}

	// 1. Startup recovery: clean up orphaned RUNNING runs
	_, _, err := RecoverOrphanedRuns(d.ctx, d.store)
	if err != nil {
		return fmt.Errorf("startup crash recovery: %w", err)
	}

	// 2. Start scheduler
	d.scheduler.Start()

	// 3. Start directory watcher and initial workflow synchronization
	if err := d.watcher.Start(); err != nil {
		d.scheduler.Stop()
		return fmt.Errorf("start watcher: %w", err)
	}

	// 4. Start HTTP server in a managed background goroutine
	if d.server != nil {
		d.serverWg.Add(1)
		go func() {
			defer d.serverWg.Done()
			if err := d.server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// Server terminated unexpectedly
			}
		}()
	}

	if len(ctxs) > 0 && ctxs[0] != nil {
		go func() {
			select {
			case <-ctxs[0].Done():
				_ = d.Shutdown()
			case <-d.ctx.Done():
			}
		}()
	}

	d.running = true
	return nil
}

// Run blocks until an OS termination signal is caught, then conducts graceful shutdown.
func (d *Daemon) Run() error {
	if err := d.Start(); err != nil {
		return err
	}

	// Wait for OS signal (SIGINT / SIGTERM)
	d.coordinator.Wait()

	// Perform graceful shutdown
	return d.Shutdown()
}

// Shutdown halts services gracefully within the configured grace period.
func (d *Daemon) Shutdown(ctxs ...context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}
	d.running = false

	var shutdownCtx context.Context
	if len(ctxs) > 0 && ctxs[0] != nil {
		shutdownCtx = ctxs[0]
	} else {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(context.Background(), d.cfg.ShutdownTimeout)
		defer cancel()
	}

	// Cancel context to stop triggers and abort active process groups
	d.cancel()

	// 1. Gracefully shutdown HTTP server first for clean drain
	if d.server != nil {
		_ = d.server.Shutdown(shutdownCtx)
		d.serverWg.Wait()
	}

	// 2. Stop directory watcher
	_ = d.watcher.Stop()

	// 3. Stop cron scheduler and wait for in-flight ticks
	schedCtx := d.scheduler.Stop()
	select {
	case <-schedCtx.Done():
	case <-time.After(5 * time.Second):
	}

	// 4. Wait for in-flight workflows to drain within configured grace period
	drainCh := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(drainCh)
	}()

	select {
	case <-drainCh:
	case <-shutdownCtx.Done():
	}

	// 5. Clean up signal coordinator
	d.coordinator.Close()

	// 6. Close webhook dispatcher
	if d.dispatcher != nil {
		_ = d.dispatcher.Close()
	}

	// 7. Close database connection
	_ = d.store.Close()

	return nil
}

// Store returns the underlying store instance.
func (d *Daemon) Store() *store.Store {
	return d.store
}

// Scheduler returns the underlying scheduler instance.
func (d *Daemon) Scheduler() *scheduler.Scheduler {
	return d.scheduler
}

// Server returns the underlying HTTP server instance.
func (d *Daemon) Server() *server.Server {
	return d.server
}
