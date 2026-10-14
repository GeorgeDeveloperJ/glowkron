package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func TestStandardCronParsing(t *testing.T) {
	s := New(nil)

	validCases := []string{
		"* * * * *",
		"*/5 * * * *",
		"0 2 * * *",
		"30 4 1,15 * 5",
		"@hourly",
		"@daily",
		"@midnight",
		"@weekly",
		"@monthly",
		"@yearly",
		"@annually",
	}

	for _, expr := range validCases {
		t.Run(expr, func(t *testing.T) {
			err := s.Register("job-"+expr, expr)
			if err != nil {
				t.Fatalf("expected expression %q to parse successfully, got: %v", expr, err)
			}
			if !s.HasJob("job-" + expr) {
				t.Fatalf("expected job %q to exist", "job-"+expr)
			}
		})
	}
}

func TestInvalidCronExpressions(t *testing.T) {
	s := New(nil)

	invalidCases := []string{
		"",
		"invalid",
		"* * *",       // too few fields
		"* * * * * *", // 6 fields when standard 5 expected
		"60 * * * *",  // invalid minute
		"* 25 * * *",  // invalid hour
		"* * 32 * *",  // invalid dom
		"* * * 13 *",  // invalid month
		"* * * * 8",   // invalid dow
		"bad @descriptor",
	}

	for _, expr := range invalidCases {
		t.Run(expr, func(t *testing.T) {
			err := s.Register("bad-job", expr)
			if err == nil {
				t.Fatalf("expected error for invalid expression %q, got nil", expr)
			}
			if !errors.Is(err, ErrInvalidCronExpression) && expr != "" {
				t.Fatalf("expected ErrInvalidCronExpression, got: %v", err)
			}
		})
	}
}

func TestEmptyJobID(t *testing.T) {
	s := New(nil)
	err := s.Register("", "* * * * *")
	if err == nil {
		t.Fatal("expected error registering empty job ID, got nil")
	}
}

func TestRegisterAndUnregister(t *testing.T) {
	s := New(nil)

	// Register job
	err := s.Register("backup", "0 2 * * *")
	if err != nil {
		t.Fatalf("unexpected register error: %v", err)
	}

	// Verify existence and entry details
	if !s.HasJob("backup") {
		t.Fatal("expected HasJob to return true")
	}
	info, ok := s.GetEntry("backup")
	if !ok {
		t.Fatal("expected GetEntry to find job")
	}
	if info.JobID != "backup" || info.Schedule != "0 2 * * *" {
		t.Fatalf("unexpected entry info: %+v", info)
	}
	if info.NextRunAt.IsZero() {
		t.Fatal("expected next run time to be non-zero")
	}

	// Attempt duplicate register without update
	err = s.Register("backup", "*/5 * * * *")
	if !errors.Is(err, ErrJobAlreadyExists) {
		t.Fatalf("expected ErrJobAlreadyExists, got: %v", err)
	}

	// Unregister
	err = s.Unregister("backup")
	if err != nil {
		t.Fatalf("unexpected unregister error: %v", err)
	}
	if s.HasJob("backup") {
		t.Fatal("expected HasJob to return false after unregister")
	}

	// Unregister non-existent
	err = s.Unregister("backup")
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got: %v", err)
	}

	_, ok = s.GetEntry("backup")
	if ok {
		t.Fatal("expected GetEntry to return false for unregistered job")
	}
}

func TestRegisterOrUpdate(t *testing.T) {
	s := New(nil)

	err := s.RegisterOrUpdate("sync", "@hourly")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry1, _ := s.GetEntry("sync")

	// Update schedule
	err = s.RegisterOrUpdate("sync", "@daily")
	if err != nil {
		t.Fatalf("unexpected error on update: %v", err)
	}
	entry2, _ := s.GetEntry("sync")

	if entry2.Schedule != "@daily" {
		t.Fatalf("expected updated schedule @daily, got: %s", entry2.Schedule)
	}
	if entry1.EntryID == entry2.EntryID {
		t.Fatal("expected new cron EntryID upon rescheduling")
	}
}

func TestListEntriesAndClear(t *testing.T) {
	s := New(nil)

	jobs := []string{"job-c", "job-a", "job-b"}
	for _, j := range jobs {
		if err := s.Register(j, "0 * * * *"); err != nil {
			t.Fatalf("failed to register %s: %v", j, err)
		}
	}

	entries := s.ListEntries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got: %d", len(entries))
	}

	// Verify sorting by JobID
	if entries[0].JobID != "job-a" || entries[1].JobID != "job-b" || entries[2].JobID != "job-c" {
		t.Fatalf("entries not sorted by JobID: %+v", entries)
	}

	s.Clear()
	if len(s.ListEntries()) != 0 {
		t.Fatal("expected 0 entries after Clear")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New(nil)
	s.Start()
	defer s.Stop()

	var wg sync.WaitGroup
	const goroutines = 20

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		jobID := fmt.Sprintf("job-%d", i)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < 15; j++ {
				_ = s.RegisterOrUpdate(id, "*/10 * * * *")
				_ = s.HasJob(id)
				_, _ = s.GetEntry(id)
				_ = s.ListEntries()
				_ = s.Unregister(id)
			}
		}(jobID)
	}

	wg.Wait()
}

func TestTriggerExecutionWithSecondsParser(t *testing.T) {
	// Use custom seconds parser so we can verify live firing in milliseconds
	secParser := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	var triggerCount atomic.Int32
	var lastJobID atomic.Pointer[string]

	runner := func(ctx context.Context, jobID string) error {
		triggerCount.Add(1)
		lastJobID.Store(&jobID)
		return nil
	}

	s := New(runner, WithParser(secParser))
	err := s.Register("fast-tick", "*/1 * * * * *") // every 1 second
	if err != nil {
		t.Fatalf("failed to register second schedule: %v", err)
	}

	s.Start()
	if !s.IsRunning() {
		t.Fatal("expected scheduler to be running")
	}

	// Wait up to 2.5 seconds for trigger to fire
	timeout := time.After(2500 * time.Millisecond)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	fired := false
	for !fired {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for scheduled trigger to fire")
		case <-ticker.C:
			if triggerCount.Load() > 0 {
				fired = true
			}
		}
	}

	stopCtx := s.Stop()
	select {
	case <-stopCtx.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Stop() context")
	}

	if s.IsRunning() {
		t.Fatal("expected scheduler to not be running after Stop")
	}

	if lastJobID.Load() == nil || *lastJobID.Load() != "fast-tick" {
		t.Fatalf("expected last triggered job to be 'fast-tick', got: %v", lastJobID.Load())
	}
}
