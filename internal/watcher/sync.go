package watcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// SchedulerInterface defines the minimal scheduler contract needed for syncing.
type SchedulerInterface interface {
	RegisterOrUpdate(jobID, cronExpr string) error
	Unregister(jobID string) error
}

// Syncer synchronizes workflow YAML definitions on disk with the SQLite store and scheduler.
type Syncer struct {
	store     *store.Store
	scheduler SchedulerInterface
}

// NewSyncer creates a new Syncer.
func NewSyncer(s *store.Store, sched SchedulerInterface) *Syncer {
	return &Syncer{
		store:     s,
		scheduler: sched,
	}
}

// SyncFile parses a single YAML workflow file and upserts it into the SQLite store and scheduler.
func (s *Syncer) SyncFile(ctx context.Context, filePath string) (*store.Job, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read workflow file: %w", err)
	}

	wf, err := workflow.ParseBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parse workflow: %w", err)
	}

	jobID := wf.Name
	timeoutSec := int(wf.Timeout.Seconds())
	retryDelaySec := int(wf.RetryDelay.Seconds())

	job := &store.Job{
		ID:            jobID,
		Name:          wf.Name,
		Description:   wf.Description,
		CronExpr:      wf.Schedule,
		MaxRetries:    wf.MaxRetries,
		RetryDelaySec: retryDelaySec,
		TimeoutSec:    timeoutSec,
		SourceFile:    filePath,
		Enabled:       true,
		UpdatedAt:     time.Now().UTC(),
	}

	// Upsert job in store
	existing, err := s.store.GetJob(ctx, jobID)
	if err != nil && !strings.Contains(err.Error(), "record not found") {
		return nil, fmt.Errorf("check existing job: %w", err)
	}

	if existing == nil {
		job.CreatedAt = time.Now().UTC()
		if err := s.store.CreateJob(ctx, job); err != nil {
			return nil, fmt.Errorf("create job: %w", err)
		}
	} else {
		job.CreatedAt = existing.CreatedAt
		if err := s.store.UpdateJob(ctx, job); err != nil {
			return nil, fmt.Errorf("update job: %w", err)
		}
	}

	// Map and upsert steps
	storeSteps := make([]*store.JobStep, 0, len(wf.Steps))
	stepIDs := make([]string, 0, len(wf.Steps))
	for idx, step := range wf.Steps {
		storeSteps = append(storeSteps, &store.JobStep{
			ID:         step.ID,
			JobID:      jobID,
			Name:       step.Name,
			Command:    step.Command,
			Args:       step.Args,
			WorkingDir: step.WorkingDir,
			EnvVars:    step.Env,
			DependsOn:  step.DependsOn,
			StepOrder:  idx,
		})
		stepIDs = append(stepIDs, step.ID)
	}

	if len(storeSteps) > 0 {
		if err := s.store.CreateSteps(ctx, storeSteps); err != nil {
			return nil, fmt.Errorf("upsert job steps: %w", err)
		}
	}
	// Prune steps that were removed from the updated workflow definition
	if err := s.store.DeleteStepsNotIn(ctx, jobID, stepIDs); err != nil {
		return nil, fmt.Errorf("prune removed job steps: %w", err)
	}

	// Synchronize with Scheduler if schedule is non-empty
	if s.scheduler != nil {
		if wf.Schedule != "" {
			if err := s.scheduler.RegisterOrUpdate(jobID, wf.Schedule); err != nil {
				return nil, fmt.Errorf("register schedule: %w", err)
			}
		} else {
			_ = s.scheduler.Unregister(jobID)
		}
	}

	return job, nil
}

// RemoveFile handles the removal of a workflow file, disabling or removing its job and unregistering its schedule.
func (s *Syncer) RemoveFile(ctx context.Context, filePath string) error {
	jobs, err := s.store.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("list jobs: %w", err)
	}

	absTarget, _ := filepath.Abs(filePath)

	for _, j := range jobs {
		absSource, _ := filepath.Abs(j.SourceFile)
		if absSource == absTarget || j.SourceFile == filePath {
			// Unregister schedule first
			if s.scheduler != nil {
				_ = s.scheduler.Unregister(j.ID)
			}
			// Mark disabled or delete job
			j.Enabled = false
			_ = s.store.UpdateJob(ctx, j)
		}
	}
	return nil
}

// SyncDirectory scans the entire directory for .yaml and .yml files and syncs each one.
func (s *Syncer) SyncDirectory(ctx context.Context, dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read directory %s: %w", dir, err)
	}

	synced := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext == ".yaml" || ext == ".yml" {
			filePath := filepath.Join(dir, entry.Name())
			if _, err := s.SyncFile(ctx, filePath); err != nil {
				return synced, fmt.Errorf("sync file %s: %w", filePath, err)
			}
			synced++
		}
	}
	return synced, nil
}
