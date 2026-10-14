package watcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors a workflows directory for YAML file events and triggers synchronization.
type Watcher struct {
	mu            sync.Mutex
	dir           string
	syncer        *Syncer
	fswatcher     *fsnotify.Watcher
	debounceDur   time.Duration
	pendingMu     sync.Mutex
	pendingTimers map[string]*time.Timer
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

// Option configures Watcher parameters.
type Option func(*Watcher)

// WithDebounce sets the debounce duration for filesystem events (default 150ms).
func WithDebounce(d time.Duration) Option {
	return func(w *Watcher) {
		w.debounceDur = d
	}
}

// New creates a new Watcher for the given directory using the provided Syncer.
func New(dir string, syncer *Syncer, opts ...Option) (*Watcher, error) {
	if dir == "" {
		return nil, fmt.Errorf("watcher directory cannot be empty")
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("ensure workflow directory exists: %w", err)
	}

	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Watcher{
		dir:           dir,
		syncer:        syncer,
		fswatcher:     fsw,
		debounceDur:   150 * time.Millisecond,
		pendingTimers: make(map[string]*time.Timer),
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Start scans the directory once and starts watching for runtime file events.
func (w *Watcher) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Initial directory sync
	if _, err := w.syncer.SyncDirectory(w.ctx, w.dir); err != nil {
		return fmt.Errorf("initial workflow sync: %w", err)
	}

	// Add directory to fsnotify
	if err := w.fswatcher.Add(w.dir); err != nil {
		return fmt.Errorf("watch directory %s: %w", w.dir, err)
	}

	go w.eventLoop()
	return nil
}

// Stop stops watching and cleans up resources.
func (w *Watcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.cancel()

	// Cancel all pending timers
	w.pendingMu.Lock()
	for _, timer := range w.pendingTimers {
		timer.Stop()
	}
	w.pendingTimers = make(map[string]*time.Timer)
	w.pendingMu.Unlock()

	err := w.fswatcher.Close()
	select {
	case <-w.done:
	default:
	}
	return err
}

func (w *Watcher) eventLoop() {
	defer close(w.done)

	for {
		select {
		case <-w.ctx.Done():
			return
		case err, ok := <-w.fswatcher.Errors:
			if !ok {
				return
			}
			_ = err // log or drop
		case event, ok := <-w.fswatcher.Events:
			if !ok {
				return
			}
			w.handleEvent(event)
		}
	}
}

func (w *Watcher) handleEvent(event fsnotify.Event) {
	ext := strings.ToLower(filepath.Ext(event.Name))
	if ext != ".yaml" && ext != ".yml" {
		return
	}

	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	// If there's an existing timer for this file, stop it (debounce)
	if timer, exists := w.pendingTimers[event.Name]; exists {
		timer.Stop()
	}

	filePath := event.Name
	op := event.Op

	w.pendingTimers[filePath] = time.AfterFunc(w.debounceDur, func() {
		w.pendingMu.Lock()
		delete(w.pendingTimers, filePath)
		w.pendingMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if op&(fsnotify.Remove|fsnotify.Rename) != 0 {
			// Check if file still exists on disk
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				_ = w.syncer.RemoveFile(ctx, filePath)
				return
			}
		}

		if op&(fsnotify.Create|fsnotify.Write) != 0 {
			if _, err := os.Stat(filePath); err == nil {
				_, _ = w.syncer.SyncFile(ctx, filePath)
			}
		}
	})
}
