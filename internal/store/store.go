package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var (
	// ErrNotFound indicates that a requested entity was not found in the database.
	ErrNotFound = errors.New("record not found")
)

// Store encapsulates SQLite database access with WAL mode and transaction safety.
type Store struct {
	db *sql.DB
}

// DB returns the underlying sql.DB instance.
func (s *Store) DB() *sql.DB {
	return s.db
}

// New initializes an embedded SQLite connection, enforces pragmas, and runs schema migrations.
func New(dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, errors.New("dbPath cannot be empty")
	}

	// Create directory if not memory db and dir does not exist
	if dbPath != ":memory:" && !strings.HasPrefix(dbPath, "file::memory:") {
		dir := filepath.Dir(dbPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create database directory: %w", err)
			}
		}
	}

	dsn := buildDSN(dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Connection pool tuning for WAL mode
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	ctx := context.Background()

	// Apply critical pragmas directly
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set PRAGMA journal_mode=WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set PRAGMA busy_timeout=5000: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set PRAGMA foreign_keys=ON: %w", err)
	}

	s := &Store{db: db}

	// Apply idempotent schema migrations
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run database migrations: %w", err)
	}

	return s, nil
}

// Close gracefully closes the database connection.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// buildDSN appends WAL, foreign keys, busy timeout, and time handling parameters.
func buildDSN(dbPath string) string {
	pragmas := []string{
		"_pragma=busy_timeout(5000)",
		"_pragma=journal_mode(WAL)",
		"_pragma=foreign_keys(ON)",
		"_texttotime=true",
		"_timezone=UTC",
	}
	delim := "?"
	if strings.Contains(dbPath, "?") {
		delim = "&"
	}
	return dbPath + delim + strings.Join(pragmas, "&")
}

// migrate creates database tables and indexes idempotently.
func (s *Store) migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		description TEXT,
		cron_expr TEXT,
		max_retries INTEGER DEFAULT 0,
		retry_delay_sec INTEGER DEFAULT 10,
		timeout_sec INTEGER DEFAULT 3600,
		concurrency_group TEXT DEFAULT 'default',
		source_file TEXT,
		enabled BOOLEAN DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS job_steps (
		id TEXT NOT NULL,
		job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		command TEXT NOT NULL,
		args TEXT,
		working_dir TEXT,
		env_vars TEXT,
		depends_on TEXT,
		step_order INTEGER DEFAULT 0,
		PRIMARY KEY (job_id, id)
	);

	CREATE TABLE IF NOT EXISTS job_runs (
		id TEXT PRIMARY KEY,
		job_id TEXT NOT NULL REFERENCES jobs(id),
		status TEXT NOT NULL,
		triggered_by TEXT NOT NULL,
		start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
		end_time DATETIME,
		exit_code INTEGER,
		attempt INTEGER DEFAULT 1,
		error_message TEXT,
		log_file_path TEXT
	);

	CREATE TABLE IF NOT EXISTS step_runs (
		id TEXT PRIMARY KEY,
		job_run_id TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
		step_id TEXT NOT NULL,
		status TEXT NOT NULL,
		start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
		end_time DATETIME,
		exit_code INTEGER,
		stdout_snippet TEXT,
		stderr_snippet TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_job_steps_job_id ON job_steps(job_id);
	CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
	CREATE INDEX IF NOT EXISTS idx_step_runs_job_run_id ON step_runs(job_run_id);
	`

	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	return s.upgradeSchema(ctx)
}

// upgradeSchema checks for legacy schemas and safely migrates them.
func (s *Store) upgradeSchema(ctx context.Context) error {
	// Inspect job_steps table structure to see if primary key is single-column (legacy)
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(job_steps)")
	if err != nil {
		return err
	}
	defer rows.Close()

	pkCount := 0
	hasJobSteps := false
	for rows.Next() {
		hasJobSteps = true
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err == nil {
			if pk > 0 {
				pkCount++
			}
		}
	}
	_ = rows.Close()

	if hasJobSteps && pkCount == 1 {
		// Old schema detected with single PK on id. Migrate to composite PK (job_id, id)
		migration := `
		PRAGMA foreign_keys = OFF;

		CREATE TABLE IF NOT EXISTS job_steps_migrated (
			id TEXT NOT NULL,
			job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			command TEXT NOT NULL,
			args TEXT,
			working_dir TEXT,
			env_vars TEXT,
			depends_on TEXT,
			step_order INTEGER DEFAULT 0,
			PRIMARY KEY (job_id, id)
		);
		INSERT OR REPLACE INTO job_steps_migrated SELECT * FROM job_steps;
		DROP TABLE job_steps;
		ALTER TABLE job_steps_migrated RENAME TO job_steps;
		CREATE INDEX IF NOT EXISTS idx_job_steps_job_id ON job_steps(job_id);

		CREATE TABLE IF NOT EXISTS step_runs_migrated (
			id TEXT PRIMARY KEY,
			job_run_id TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
			step_id TEXT NOT NULL,
			status TEXT NOT NULL,
			start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
			end_time DATETIME,
			exit_code INTEGER,
			stdout_snippet TEXT,
			stderr_snippet TEXT
		);
		INSERT OR IGNORE INTO step_runs_migrated SELECT * FROM step_runs;
		DROP TABLE step_runs;
		ALTER TABLE step_runs_migrated RENAME TO step_runs;
		CREATE INDEX IF NOT EXISTS idx_step_runs_job_run_id ON step_runs(job_run_id);

		PRAGMA foreign_keys = ON;
		`
		if _, err := s.db.ExecContext(ctx, migration); err != nil {
			return fmt.Errorf("upgrade schema: %w", err)
		}
	}
	return nil
}

// --- Jobs CRUD ---

// CreateJob inserts a new Job definition.
func (s *Store) CreateJob(ctx context.Context, job *Job) error {
	if job == nil {
		return errors.New("job cannot be nil")
	}
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	if job.ConcurrencyGroup == "" {
		job.ConcurrencyGroup = "default"
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}

	query := `INSERT INTO jobs (
		id, name, description, cron_expr, max_retries, retry_delay_sec,
		timeout_sec, concurrency_group, source_file, enabled, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		job.ID,
		job.Name,
		job.Description,
		job.CronExpr,
		job.MaxRetries,
		job.RetryDelaySec,
		job.TimeoutSec,
		job.ConcurrencyGroup,
		job.SourceFile,
		job.Enabled,
		job.CreatedAt.Format(time.RFC3339),
		job.UpdatedAt.Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("create job %s: %w", job.ID, err)
	}
	return nil
}

// GetJob retrieves a Job by its ID.
func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	query := `SELECT 
		id, name, coalesce(description, ''), coalesce(cron_expr, ''),
		max_retries, retry_delay_sec, timeout_sec, concurrency_group,
		coalesce(source_file, ''), enabled, created_at, updated_at
	FROM jobs WHERE id = ?`

	var job Job
	var createdAtRaw, updatedAtRaw any
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&job.ID,
		&job.Name,
		&job.Description,
		&job.CronExpr,
		&job.MaxRetries,
		&job.RetryDelaySec,
		&job.TimeoutSec,
		&job.ConcurrencyGroup,
		&job.SourceFile,
		&job.Enabled,
		&createdAtRaw,
		&updatedAtRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, err)
	}

	job.CreatedAt, _ = parseTimeValue(createdAtRaw)
	job.UpdatedAt, _ = parseTimeValue(updatedAtRaw)
	return &job, nil
}

// ListJobs retrieves all registered jobs ordered by name.
func (s *Store) ListJobs(ctx context.Context) ([]*Job, error) {
	query := `SELECT 
		id, name, coalesce(description, ''), coalesce(cron_expr, ''),
		max_retries, retry_delay_sec, timeout_sec, concurrency_group,
		coalesce(source_file, ''), enabled, created_at, updated_at
	FROM jobs ORDER BY name ASC`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		var job Job
		var createdAtRaw, updatedAtRaw any
		if err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.Description,
			&job.CronExpr,
			&job.MaxRetries,
			&job.RetryDelaySec,
			&job.TimeoutSec,
			&job.ConcurrencyGroup,
			&job.SourceFile,
			&job.Enabled,
			&createdAtRaw,
			&updatedAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		job.CreatedAt, _ = parseTimeValue(createdAtRaw)
		job.UpdatedAt, _ = parseTimeValue(updatedAtRaw)
		jobs = append(jobs, &job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return jobs, nil
}

// UpdateJob updates an existing Job definition.
func (s *Store) UpdateJob(ctx context.Context, job *Job) error {
	if job == nil {
		return errors.New("job cannot be nil")
	}
	job.UpdatedAt = time.Now().UTC()

	query := `UPDATE jobs SET 
		name = ?, description = ?, cron_expr = ?, max_retries = ?,
		retry_delay_sec = ?, timeout_sec = ?, concurrency_group = ?,
		source_file = ?, enabled = ?, updated_at = ?
	WHERE id = ?`

	res, err := s.db.ExecContext(ctx, query,
		job.Name,
		job.Description,
		job.CronExpr,
		job.MaxRetries,
		job.RetryDelaySec,
		job.TimeoutSec,
		job.ConcurrencyGroup,
		job.SourceFile,
		job.Enabled,
		job.UpdatedAt.Format(time.RFC3339),
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("update job %s: %w", job.ID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteJob removes a Job by its ID.
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM jobs WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete job %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// --- JobSteps CRUD ---

// CreateSteps inserts a collection of steps transactionally.
func (s *Store) CreateSteps(ctx context.Context, steps []*JobStep) error {
	if len(steps) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT INTO job_steps (
		id, job_id, name, command, args, working_dir, env_vars, depends_on, step_order
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT (job_id, id) DO UPDATE SET
		name = excluded.name,
		command = excluded.command,
		args = excluded.args,
		working_dir = excluded.working_dir,
		env_vars = excluded.env_vars,
		depends_on = excluded.depends_on,
		step_order = excluded.step_order`)
	if err != nil {
		return fmt.Errorf("prepare insert step: %w", err)
	}
	defer stmt.Close()

	for _, step := range steps {
		if step.ID == "" {
			step.ID = uuid.NewString()
		}

		argsJSON, err := json.Marshal(step.Args)
		if err != nil || step.Args == nil {
			argsJSON = []byte("[]")
		}

		envVarsJSON, err := json.Marshal(step.EnvVars)
		if err != nil || step.EnvVars == nil {
			envVarsJSON = []byte("{}")
		}

		dependsOnJSON, err := json.Marshal(step.DependsOn)
		if err != nil || step.DependsOn == nil {
			dependsOnJSON = []byte("[]")
		}

		_, err = stmt.ExecContext(ctx,
			step.ID,
			step.JobID,
			step.Name,
			step.Command,
			string(argsJSON),
			step.WorkingDir,
			string(envVarsJSON),
			string(dependsOnJSON),
			step.StepOrder,
		)
		if err != nil {
			return fmt.Errorf("insert step %s: %w", step.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// GetStepsByJobID retrieves all steps belonging to a job, ordered by step_order.
func (s *Store) GetStepsByJobID(ctx context.Context, jobID string) ([]*JobStep, error) {
	query := `SELECT 
		id, job_id, name, command, coalesce(args, '[]'), coalesce(working_dir, ''),
		coalesce(env_vars, '{}'), coalesce(depends_on, '[]'), step_order
	FROM job_steps WHERE job_id = ? ORDER BY step_order ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("get steps for job %s: %w", jobID, err)
	}
	defer rows.Close()

	steps := make([]*JobStep, 0)
	for rows.Next() {
		var step JobStep
		var argsStr, envVarsStr, dependsOnStr string
		if err := rows.Scan(
			&step.ID,
			&step.JobID,
			&step.Name,
			&step.Command,
			&argsStr,
			&step.WorkingDir,
			&envVarsStr,
			&dependsOnStr,
			&step.StepOrder,
		); err != nil {
			return nil, fmt.Errorf("scan step: %w", err)
		}

		if err := json.Unmarshal([]byte(argsStr), &step.Args); err != nil {
			step.Args = []string{}
		}
		if err := json.Unmarshal([]byte(envVarsStr), &step.EnvVars); err != nil {
			step.EnvVars = make(map[string]string)
		}
		if err := json.Unmarshal([]byte(dependsOnStr), &step.DependsOn); err != nil {
			step.DependsOn = []string{}
		}

		steps = append(steps, &step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return steps, nil
}

// DeleteStepsByJobID deletes all steps belonging to a specific job.
func (s *Store) DeleteStepsByJobID(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM job_steps WHERE job_id = ?", jobID)
	if err != nil {
		return fmt.Errorf("delete steps for job %s: %w", jobID, err)
	}
	return nil
}

// DeleteStepsNotIn deletes any steps for a job whose IDs are not in the provided keepIDs list.
func (s *Store) DeleteStepsNotIn(ctx context.Context, jobID string, keepIDs []string) error {
	if len(keepIDs) == 0 {
		return s.DeleteStepsByJobID(ctx, jobID)
	}
	placeholders := make([]string, len(keepIDs))
	args := make([]interface{}, 0, len(keepIDs)+1)
	args = append(args, jobID)
	for i, id := range keepIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	query := fmt.Sprintf("DELETE FROM job_steps WHERE job_id = ? AND id NOT IN (%s)", strings.Join(placeholders, ","))
	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete steps not in list for job %s: %w", jobID, err)
	}
	return nil
}

// --- JobRuns CRUD ---

// CreateJobRun records the start of a job run.
func (s *Store) CreateJobRun(ctx context.Context, run *JobRun) error {
	if run == nil {
		return errors.New("job run cannot be nil")
	}
	if run.ID == "" {
		run.ID = uuid.NewString()
	}
	if run.Status == "" {
		run.Status = JobRunPending
	}
	if run.Attempt <= 0 {
		run.Attempt = 1
	}
	if run.StartTime.IsZero() {
		run.StartTime = time.Now().UTC()
	}

	var endTimeStr *string
	if run.EndTime != nil {
		formatted := run.EndTime.UTC().Format(time.RFC3339)
		endTimeStr = &formatted
	}

	query := `INSERT INTO job_runs (
		id, job_id, status, triggered_by, start_time, end_time,
		exit_code, attempt, error_message, log_file_path
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		run.ID,
		run.JobID,
		string(run.Status),
		string(run.TriggeredBy),
		run.StartTime.Format(time.RFC3339),
		endTimeStr,
		run.ExitCode,
		run.Attempt,
		run.ErrorMessage,
		run.LogFilePath,
	)
	if err != nil {
		return fmt.Errorf("create job run %s: %w", run.ID, err)
	}
	return nil
}

// GetJobRun fetches a JobRun by its ID.
func (s *Store) GetJobRun(ctx context.Context, id string) (*JobRun, error) {
	query := `SELECT 
		id, job_id, status, triggered_by, start_time, end_time,
		exit_code, attempt, coalesce(error_message, ''), coalesce(log_file_path, '')
	FROM job_runs WHERE id = ?`

	var run JobRun
	var startTimeRaw, endTimeRaw any
	var statusStr, triggeredByStr string
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&run.ID,
		&run.JobID,
		&statusStr,
		&triggeredByStr,
		&startTimeRaw,
		&endTimeRaw,
		&run.ExitCode,
		&run.Attempt,
		&run.ErrorMessage,
		&run.LogFilePath,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job run %s: %w", id, err)
	}

	run.Status = JobRunStatus(statusStr)
	run.TriggeredBy = TriggeredBy(triggeredByStr)
	run.StartTime, _ = parseTimeValue(startTimeRaw)
	run.EndTime, _ = parseNullableTimeValue(endTimeRaw)
	return &run, nil
}

// UpdateJobRunStatus updates execution status, exit code, and error message, populating end_time if terminal.
func (s *Store) UpdateJobRunStatus(ctx context.Context, id string, status JobRunStatus, exitCode *int, errMsg string) error {
	var query string
	var args []any

	if status == JobRunCompleted || status == JobRunFailed || status == JobRunCancelled {
		nowStr := time.Now().UTC().Format(time.RFC3339)
		query = `UPDATE job_runs SET 
			status = ?, exit_code = ?, error_message = ?, 
			end_time = COALESCE(end_time, ?) 
		WHERE id = ?`
		args = []any{string(status), exitCode, errMsg, nowStr, id}
	} else {
		query = `UPDATE job_runs SET 
			status = ?, exit_code = ?, error_message = ? 
		WHERE id = ?`
		args = []any{string(status), exitCode, errMsg, id}
	}

	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update job run %s status: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// ListJobRuns returns historical runs, optionally filtered by jobID and limited.
func (s *Store) ListJobRuns(ctx context.Context, jobID string, limit int) ([]*JobRun, error) {
	var query string
	var args []any

	if jobID != "" {
		if limit > 0 {
			query = `SELECT 
				id, job_id, status, triggered_by, start_time, end_time,
				exit_code, attempt, coalesce(error_message, ''), coalesce(log_file_path, '')
			FROM job_runs WHERE job_id = ? ORDER BY start_time DESC, id DESC LIMIT ?`
			args = []any{jobID, limit}
		} else {
			query = `SELECT 
				id, job_id, status, triggered_by, start_time, end_time,
				exit_code, attempt, coalesce(error_message, ''), coalesce(log_file_path, '')
			FROM job_runs WHERE job_id = ? ORDER BY start_time DESC, id DESC`
			args = []any{jobID}
		}
	} else {
		if limit > 0 {
			query = `SELECT 
				id, job_id, status, triggered_by, start_time, end_time,
				exit_code, attempt, coalesce(error_message, ''), coalesce(log_file_path, '')
			FROM job_runs ORDER BY start_time DESC, id DESC LIMIT ?`
			args = []any{limit}
		} else {
			query = `SELECT 
				id, job_id, status, triggered_by, start_time, end_time,
				exit_code, attempt, coalesce(error_message, ''), coalesce(log_file_path, '')
			FROM job_runs ORDER BY start_time DESC, id DESC`
		}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list job runs: %w", err)
	}
	defer rows.Close()

	runs := make([]*JobRun, 0)
	for rows.Next() {
		var run JobRun
		var startTimeRaw, endTimeRaw any
		var statusStr, triggeredByStr string
		if err := rows.Scan(
			&run.ID,
			&run.JobID,
			&statusStr,
			&triggeredByStr,
			&startTimeRaw,
			&endTimeRaw,
			&run.ExitCode,
			&run.Attempt,
			&run.ErrorMessage,
			&run.LogFilePath,
		); err != nil {
			return nil, fmt.Errorf("scan job run: %w", err)
		}
		run.Status = JobRunStatus(statusStr)
		run.TriggeredBy = TriggeredBy(triggeredByStr)
		run.StartTime, _ = parseTimeValue(startTimeRaw)
		run.EndTime, _ = parseNullableTimeValue(endTimeRaw)
		runs = append(runs, &run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return runs, nil
}

// --- StepRuns CRUD ---

// CreateStepRun records execution of an individual workflow step.
func (s *Store) CreateStepRun(ctx context.Context, stepRun *StepRun) error {
	if stepRun == nil {
		return errors.New("step run cannot be nil")
	}
	if stepRun.ID == "" {
		stepRun.ID = uuid.NewString()
	}
	if stepRun.Status == "" {
		stepRun.Status = StepRunPending
	}
	if stepRun.StartTime.IsZero() {
		stepRun.StartTime = time.Now().UTC()
	}

	var endTimeStr *string
	if stepRun.EndTime != nil {
		formatted := stepRun.EndTime.UTC().Format(time.RFC3339)
		endTimeStr = &formatted
	}

	query := `INSERT INTO step_runs (
		id, job_run_id, step_id, status, start_time, end_time,
		exit_code, stdout_snippet, stderr_snippet
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		stepRun.ID,
		stepRun.JobRunID,
		stepRun.StepID,
		string(stepRun.Status),
		stepRun.StartTime.Format(time.RFC3339),
		endTimeStr,
		stepRun.ExitCode,
		stepRun.StdoutSnippet,
		stepRun.StderrSnippet,
	)
	if err != nil {
		return fmt.Errorf("create step run %s: %w", stepRun.ID, err)
	}
	return nil
}

// UpdateStepRun updates step execution state, setting end_time when terminal.
func (s *Store) UpdateStepRun(ctx context.Context, stepRun *StepRun) error {
	if stepRun == nil {
		return errors.New("step run cannot be nil")
	}

	if stepRun.EndTime == nil && (stepRun.Status == StepRunCompleted || stepRun.Status == StepRunFailed || stepRun.Status == StepRunSkipped) {
		now := time.Now().UTC()
		stepRun.EndTime = &now
	}

	var endTimeStr *string
	if stepRun.EndTime != nil {
		formatted := stepRun.EndTime.UTC().Format(time.RFC3339)
		endTimeStr = &formatted
	}

	query := `UPDATE step_runs SET 
		status = ?, end_time = ?, exit_code = ?, stdout_snippet = ?, stderr_snippet = ?
	WHERE id = ?`

	res, err := s.db.ExecContext(ctx, query,
		string(stepRun.Status),
		endTimeStr,
		stepRun.ExitCode,
		stepRun.StdoutSnippet,
		stepRun.StderrSnippet,
		stepRun.ID,
	)
	if err != nil {
		return fmt.Errorf("update step run %s: %w", stepRun.ID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// GetStepRun fetches an individual StepRun by ID.
func (s *Store) GetStepRun(ctx context.Context, id string) (*StepRun, error) {
	query := `SELECT 
		id, job_run_id, step_id, status, start_time, end_time,
		exit_code, coalesce(stdout_snippet, ''), coalesce(stderr_snippet, '')
	FROM step_runs WHERE id = ?`

	var sr StepRun
	var startTimeRaw, endTimeRaw any
	var statusStr string
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&sr.ID,
		&sr.JobRunID,
		&sr.StepID,
		&statusStr,
		&startTimeRaw,
		&endTimeRaw,
		&sr.ExitCode,
		&sr.StdoutSnippet,
		&sr.StderrSnippet,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get step run %s: %w", id, err)
	}
	sr.Status = StepRunStatus(statusStr)
	sr.StartTime, _ = parseTimeValue(startTimeRaw)
	sr.EndTime, _ = parseNullableTimeValue(endTimeRaw)
	return &sr, nil
}

// GetStepRunsByJobRunID retrieves all step executions for a job run.
func (s *Store) GetStepRunsByJobRunID(ctx context.Context, jobRunID string) ([]*StepRun, error) {
	query := `SELECT 
		id, job_run_id, step_id, status, start_time, end_time,
		exit_code, coalesce(stdout_snippet, ''), coalesce(stderr_snippet, '')
	FROM step_runs WHERE job_run_id = ? ORDER BY start_time ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, jobRunID)
	if err != nil {
		return nil, fmt.Errorf("get step runs for run %s: %w", jobRunID, err)
	}
	defer rows.Close()

	stepRuns := make([]*StepRun, 0)
	for rows.Next() {
		var sr StepRun
		var startTimeRaw, endTimeRaw any
		var statusStr string
		if err := rows.Scan(
			&sr.ID,
			&sr.JobRunID,
			&sr.StepID,
			&statusStr,
			&startTimeRaw,
			&endTimeRaw,
			&sr.ExitCode,
			&sr.StdoutSnippet,
			&sr.StderrSnippet,
		); err != nil {
			return nil, fmt.Errorf("scan step run: %w", err)
		}
		sr.Status = StepRunStatus(statusStr)
		sr.StartTime, _ = parseTimeValue(startTimeRaw)
		sr.EndTime, _ = parseNullableTimeValue(endTimeRaw)
		stepRuns = append(stepRuns, &sr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return stepRuns, nil
}

// --- Helpers for robust SQLite Datetime Parsing ---

func parseTimeValue(v any) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}
	switch t := v.(type) {
	case time.Time:
		return t.UTC(), nil
	case string:
		if t == "" {
			return time.Time{}, nil
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse time string: %q", t)
	case []byte:
		return parseTimeValue(string(t))
	case int64:
		return time.Unix(t, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unexpected time type: %T", v)
	}
}

func parseNullableTimeValue(v any) (*time.Time, error) {
	if v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok && s == "" {
		return nil, nil
	}
	t, err := parseTimeValue(v)
	if err != nil {
		return nil, err
	}
	if t.IsZero() {
		return nil, nil
	}
	return &t, nil
}
