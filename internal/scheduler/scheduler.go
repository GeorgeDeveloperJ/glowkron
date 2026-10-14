package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	// ErrJobNotFound is returned when attempting to operate on an unregistered job ID.
	ErrJobNotFound = errors.New("job not found in scheduler")
	// ErrJobAlreadyExists is returned when attempting to register an already existing job ID without update.
	ErrJobAlreadyExists = errors.New("job already registered in scheduler")
	// ErrInvalidCronExpression is returned when parsing a cron expression fails.
	ErrInvalidCronExpression = errors.New("invalid cron expression")
)

// TriggerFunc is invoked whenever a scheduled cron job fires.
// jobID identifies the workflow job that should be executed.
type TriggerFunc func(ctx context.Context, jobID string) error

// EntryInfo holds public snapshot metadata about a scheduled job.
type EntryInfo struct {
	JobID      string    `json:"job_id"`
	Schedule   string    `json:"schedule"`
	EntryID    int       `json:"entry_id"`
	NextRunAt  time.Time `json:"next_run_at"`
	PrevRunAt  time.Time `json:"prev_run_at"`
	Registered time.Time `json:"registered_at"`
}

type jobEntry struct {
	jobID      string
	schedule   string
	parsed     cron.Schedule
	entryID    cron.EntryID
	registered time.Time
}

// Scheduler wraps robfig/cron/v3 with thread-safe job registry and dynamic management.
type Scheduler struct {
	mu      sync.RWMutex
	cron    *cron.Cron
	parser  cron.ScheduleParser
	runner  TriggerFunc
	entries map[string]*jobEntry
	running bool
}

// Option configures Scheduler options.
type Option func(*Scheduler)

// WithParser sets a custom ScheduleParser.
func WithParser(parser cron.ScheduleParser) Option {
	return func(s *Scheduler) {
		s.parser = parser
	}
}

// New creates a new Scheduler using standard 5-field cron parsing by default.
func New(runner TriggerFunc, opts ...Option) *Scheduler {
	// Standard 5-field cron parser: minute, hour, dom, month, dow with descriptors (@daily, etc.)
	defaultParser := cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	s := &Scheduler{
		parser:  defaultParser,
		runner:  runner,
		entries: make(map[string]*jobEntry),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.cron = cron.New(cron.WithParser(s.parser), cron.WithChain(cron.Recover(cron.DefaultLogger)))
	return s
}

// Start begins the cron scheduler in a background goroutine.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		s.cron.Start()
		s.running = true
	}
}

// Stop halts the cron scheduler and returns a context that completes when running jobs finish.
func (s *Scheduler) Stop() context.Context {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	s.running = false
	c := s.cron
	s.mu.Unlock()

	return c.Stop()
}

// IsRunning reports whether the scheduler is currently active.
func (s *Scheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// Register schedules a job with the specified standard cron expression.
// Returns ErrJobAlreadyExists if the job is already scheduled.
func (s *Scheduler) Register(jobID, cronExpr string) error {
	return s.registerInternal(jobID, cronExpr, false)
}

// RegisterOrUpdate schedules a job or updates its schedule if already present.
func (s *Scheduler) RegisterOrUpdate(jobID, cronExpr string) error {
	return s.registerInternal(jobID, cronExpr, true)
}

func (s *Scheduler) registerInternal(jobID, cronExpr string, allowUpdate bool) error {
	if jobID == "" {
		return errors.New("job ID cannot be empty")
	}

	// Validate expression upfront using the configured parser
	sched, err := s.parser.Parse(cronExpr)
	if err != nil {
		return fmt.Errorf("%w: %s (%v)", ErrInvalidCronExpression, cronExpr, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.entries[jobID]
	if exists {
		if !allowUpdate {
			return fmt.Errorf("%w: %s", ErrJobAlreadyExists, jobID)
		}
		// Remove previous cron entry
		s.cron.Remove(existing.entryID)
	}

	// Wrapper job action calling trigger func
	targetJobID := jobID
	entryID := s.cron.Schedule(sched, cron.FuncJob(func() {
		if s.runner != nil {
			_ = s.runner(context.Background(), targetJobID)
		}
	}))

	s.entries[jobID] = &jobEntry{
		jobID:      targetJobID,
		schedule:   cronExpr,
		parsed:     sched,
		entryID:    entryID,
		registered: time.Now(),
	}

	return nil
}

// Unregister removes a job from the scheduler.
// Returns ErrJobNotFound if the job is not registered.
func (s *Scheduler) Unregister(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.entries[jobID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}

	s.cron.Remove(entry.entryID)
	delete(s.entries, jobID)
	return nil
}

// HasJob checks if a job is currently scheduled.
func (s *Scheduler) HasJob(jobID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.entries[jobID]
	return exists
}

// GetEntry returns runtime metadata about a registered job schedule.
func (s *Scheduler) GetEntry(jobID string) (EntryInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.entries[jobID]
	if !exists {
		return EntryInfo{}, false
	}

	cronEntry := s.cron.Entry(entry.entryID)
	nextRun := cronEntry.Next
	if nextRun.IsZero() && entry.parsed != nil {
		nextRun = entry.parsed.Next(time.Now())
	}

	return EntryInfo{
		JobID:      entry.jobID,
		Schedule:   entry.schedule,
		EntryID:    int(entry.entryID),
		NextRunAt:  nextRun,
		PrevRunAt:  cronEntry.Prev,
		Registered: entry.registered,
	}, true
}

// ListEntries returns a sorted list of all active registered job schedules.
func (s *Scheduler) ListEntries() []EntryInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	results := make([]EntryInfo, 0, len(s.entries))
	for _, entry := range s.entries {
		cronEntry := s.cron.Entry(entry.entryID)
		nextRun := cronEntry.Next
		if nextRun.IsZero() && entry.parsed != nil {
			nextRun = entry.parsed.Next(now)
		}

		results = append(results, EntryInfo{
			JobID:      entry.jobID,
			Schedule:   entry.schedule,
			EntryID:    int(entry.entryID),
			NextRunAt:  nextRun,
			PrevRunAt:  cronEntry.Prev,
			Registered: entry.registered,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].JobID < results[j].JobID
	})

	return results
}

// Clear unregisters all scheduled jobs.
func (s *Scheduler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.entries {
		s.cron.Remove(entry.entryID)
	}
	s.entries = make(map[string]*jobEntry)
}
