package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

const (
	// DefaultCrashRecoveryMessage is assigned to orphaned runs recovered after abnormal daemon exit.
	DefaultCrashRecoveryMessage = "Aborted due to unexpected daemon termination"
)

// RecoverOrphanedRuns inspects the database on startup and marks any in-flight
// JobRuns and StepRuns that were left in RUNNING status as FAILED.
func RecoverOrphanedRuns(ctx context.Context, st *store.Store) (int, int, error) {
	if st == nil {
		return 0, 0, fmt.Errorf("store cannot be nil")
	}

	db := st.DB()
	nowStr := time.Now().UTC().Format(time.RFC3339)

	// 1. Recover orphaned StepRuns (status -> FAILED, exit_code -> 1, end_time -> now)
	failExitCode := 1
	stepQuery := `UPDATE step_runs 
		SET status = ?, exit_code = ?, end_time = COALESCE(end_time, ?)
		WHERE status = ?`

	stepRes, err := db.ExecContext(ctx, stepQuery,
		string(store.StepRunFailed),
		failExitCode,
		nowStr,
		string(store.StepRunRunning),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("recover orphaned step runs: %w", err)
	}

	stepsRecovered, _ := stepRes.RowsAffected()

	// 2. Recover orphaned JobRuns
	jobQuery := `UPDATE job_runs 
		SET status = ?, exit_code = ?, error_message = ?, end_time = COALESCE(end_time, ?)
		WHERE status = ?`

	jobRes, err := db.ExecContext(ctx, jobQuery,
		string(store.JobRunFailed),
		failExitCode,
		DefaultCrashRecoveryMessage,
		nowStr,
		string(store.JobRunRunning),
	)
	if err != nil {
		return int(stepsRecovered), 0, fmt.Errorf("recover orphaned job runs: %w", err)
	}

	jobsRecovered, _ := jobRes.RowsAffected()

	return int(jobsRecovered), int(stepsRecovered), nil
}
