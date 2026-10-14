# GlowKron — Master Project Tasklist

> **Source Spec:** [`project-specs/glowkron-setup.md`](file:///home/george_santana/Projects/glowkron/project-specs/glowkron-setup.md)  
> **Architecture Style:** Single-Binary, Zero-CGo, Pure Go Embedded Background Job & DAG Workflow Orchestrator  
> **Target Path:** `project-tasks/glowkron-tasklist.md`  
> **Status:** In Progress (Phase 1 active)

---

## Overall Roadmap Status

- [x] **Phase 1: Core Subprocess Executor & State Store** (4 / 4 complete)
  - [x] Task 1.1: Repository Setup & Entrypoint Skeleton
  - [x] Task 1.2: Subprocess Execution with Process Groups & Timeout
  - [x] Task 1.3: SQLite Storage with WAL Mode, Migrations & CRUD
  - [x] Task 1.4: Disk-Backed Run Log Files & In-Memory Pub/Sub Broker
- [x] **Phase 2: DAG Workflow Engine & Worker Pool** (3 / 3 complete)
  - [x] Task 2.1: Workflow YAML Parser & Schema Validator
  - [x] Task 2.2: DAG Dependency Resolver & Topological Sort
  - [x] Task 2.3: Bounded Worker Pool & Exponential Backoff Engine
- [ ] **Phase 3: Cron Scheduler & Daemon Lifecycle** (2 / 3 complete)
  - [x] Task 3.1: Cron Scheduling Integration (`robfig/cron/v3`)
  - [x] Task 3.2: Directory Watcher & Workflow Auto-Sync
  - [ ] Task 3.3: Daemon Lifecycle, Graceful Shutdown & Crash Recovery
- [ ] **Phase 4: CLI Interface (`spf13/cobra`)** (0 / 2 complete)
  - [ ] Task 4.1: Cobra CLI Command Tree
  - [ ] Task 4.2: Daemon Client & Offline SQLite Fallback
- [ ] **Phase 5: Webhook Notifier** (0 / 1 complete)
  - [ ] Task 5.1: Multi-Platform Webhook Dispatcher
- [ ] **Phase 6: Embedded Web Dashboard & SSE Log Streaming** (0 / 2 complete)
  - [ ] Task 6.1: REST API & SSE Real-time Streaming Handler
  - [ ] Task 6.2: Embedded Single-Page Web Dashboard (`//go:embed`)

---

## Phase 1: Core Subprocess Executor & State Store

### [x] Task 1.1: Repository Setup & Entrypoint Skeleton
- **Spec Reference & Quoted Requirements:**
  > "1.1 (Completed): Module initialization, repository structure, and basic process execution (`internal/executor`)."
  > "Single-Binary Zero-Dependency Distribution: Compiles into one self-contained Go executable (`glowkron`). Zero external runtime dependencies—no Redis, no Postgres, no Docker daemon, and zero CGo (uses `modernc.org/sqlite` pure Go)."
- **Description:** Initialize repository structure, Go module (`github.com/GeorgeDeveloperJ/glowkron`), `.gitignore`, entrypoint binary skeleton in `cmd/glowkron/main.go`, and documentation guidelines.
- **Acceptance Criteria:**
  - Go module initialized and compiles without error.
  - Clean project directory structure with `cmd/` and `internal/`.
  - Main binary builds and outputs startup banner.
- **Files Created/Edited:**
  - `go.mod`
  - `.gitignore`
  - `cmd/glowkron/main.go`
  - `README.md`
  - `AGENTS.md`
- **Verification / Test Command:**
  ```bash
  go build ./cmd/glowkron
  go run ./cmd/glowkron
  ```

---

### [x] Task 1.2: Subprocess Execution with Process Groups & Timeout
- **Spec Reference & Quoted Requirements:**
  > "1.2 (Completed): Process group isolation (`Setpgid`), timeout handling, and zombie prevention."
  > "Subprocess Integrity & Process Group Isolation: Subprocesses run via `os/exec.CommandContext` with `Setpgid: true`. On timeout or cancellation, the entire process group is terminated with `syscall.SIGKILL` to prevent orphaned zombie processes."
- **Description:** Implement `internal/executor/executor.go` executing commands with `os/exec.CommandContext`, setting `SysProcAttr: &syscall.SysProcAttr{Setpgid: true}`, attaching process group SIGKILL on cancellation, and capturing stdout, stderr, exit codes, and durations.
- **Acceptance Criteria:**
  - `ProcessConfig` struct defines command, args, environment, working directory, and timeout duration.
  - `ExecutionResult` captures exit code, stdout, stderr, and elapsed duration.
  - Process group termination kills child background processes spawned by shell scripts without leaving orphaned zombie processes.
  - Context cancellation and config timeout properly kill runaway processes.
- **Files Created/Edited:**
  - `internal/executor/executor.go`
  - `internal/executor/executor_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/executor
  ```

---

### [x] Task 1.3: SQLite Storage with WAL Mode, Schema Migrations, and CRUD
- **Spec Reference & Quoted Requirements:**
  > "1.3 (Next): Embedded SQLite store (`internal/store`):
  > - Pure Go SQLite driver (`modernc.org/sqlite`).
  > - Connection manager with WAL mode, busy timeout, and connection pooling.
  > - Schema migrations creating `jobs`, `job_steps`, `job_runs`, `step_runs`.
  > - CRUD operations for jobs, runs, and step executions.
  > - Comprehensive unit test suite (`store_test.go`)."
  > "Storage Durability & WAL Mode: Embedded SQLite database (`~/.glowkron/glowkron.db`) initialized with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`), foreign keys enabled, and transactional state updates."
- **Description:** Implement pure Go SQLite persistence under `internal/store` using `modernc.org/sqlite`. Establish database connection manager with WAL mode (`PRAGMA journal_mode=WAL;`), busy timeout (`PRAGMA busy_timeout=5000;`), and foreign key enforcement (`PRAGMA foreign_keys=ON;`). Implement idempotent table migrations (`jobs`, `job_steps`, `job_runs`, `step_runs`) and complete CRUD operations with transaction support.
- **Acceptance Criteria:**
  - Zero-CGo SQLite connection using `modernc.org/sqlite`.
  - Database bootstrap applies WAL mode, foreign keys, and idempotent schema migrations matching Section 3 of the specification.
  - Full CRUD operations implemented:
    - Jobs: `CreateJob`, `GetJob`, `ListJobs`, `UpdateJob`, `DeleteJob`.
    - Job Steps: `CreateSteps`, `GetStepsByJobID`, `DeleteStepsByJobID`.
    - Job Runs: `CreateJobRun`, `GetJobRun`, `UpdateJobRunStatus`, `ListJobRuns`.
    - Step Runs: `CreateStepRun`, `UpdateStepRun`, `GetStepRunsByJobRunID`.
  - Transactional safety for state updates.
  - Comprehensive unit tests in `internal/store/store_test.go` covering migrations, foreign key cascades, and concurrent reads/writes without database locked errors.
- **Files to Create/Edit:**
  - `internal/store/store.go`
  - `internal/store/models.go`
  - `internal/store/migrations.go`
  - `internal/store/store_test.go`
  - `go.mod` (add `modernc.org/sqlite`)
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/store
  ```

---

### [x] Task 1.4: Disk-Backed Run Log Files & In-Memory Pub/Sub Broker
- **Spec Reference & Quoted Requirements:**
  > "1.4: Raw Log File Manager (`internal/logger`):
  > - Thread-safe disk file appender for run logs under `~/.glowkron/logs/<run_id>.log`.
  > - Ring-buffer in-memory pub/sub broker for broadcasting live log chunks."
  > "Real-time Logging & Storage Decoupling: Live logs are streamed over an in-memory pub/sub broker to Server-Sent Events (SSE) and CLI followers. Full raw output is written to disk files (`~/.glowkron/logs/<run-id>.log`), while SQLite holds execution metadata, exit codes, and summary snippets (preventing DB bloat and lock contention)."
- **Description:** Implement `internal/logger` to decouple raw high-volume log data from the database. Build a thread-safe disk file appender writing raw command outputs to `~/.glowkron/logs/<run-id>.log` and an in-memory pub/sub broker utilizing channels/ring-buffers to broadcast live log chunks to concurrent subscribers (SSE server, CLI log followers).
- **Acceptance Criteria:**
  - `LogWriter` appends stdout/stderr lines concurrently and thread-safely to `<run_id>.log` files on disk.
  - `LogBroker` allows dynamic subscription by `run_id`, distributing chunks to active subscribers without blocking log producers.
  - Auto-cleanup of subscriber channels when subscriptions close or runs terminate.
  - Helper to read existing log files from disk for completed or running tasks.
  - Unit tests covering concurrent writes, buffer overflow handling, slow reader isolation, and file retrieval.
- **Files to Create/Edit:**
  - `internal/logger/file_appender.go`
  - `internal/logger/pubsub_broker.go`
  - `internal/logger/logger_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/logger
  ```

---

## Phase 2: DAG Workflow Engine & Worker Pool

### [x] Task 2.1: Workflow YAML Parser & Schema Validator
- **Spec Reference & Quoted Requirements:**
  > "2.1: Workflow Parser & Validator (`internal/workflow`):
  > - YAML deserialization into typed structs.
  > - Schema validation (command presence, timeout parsing, duplicate IDs)."
  > "Hybrid Workflow Configuration: Workflows are defined in declarative YAML files in a configured directory (e.g., `~/.glowkron/workflows/` or `./workflows/`) and auto-synced into SQLite upon daemon launch or reload..."
- **Description:** Implement `internal/workflow` to parse and validate declarative workflow YAML files matching the contract in Section 4 of the specification.
- **Acceptance Criteria:**
  - Struct definitions for `WorkflowConfig`, `StepConfig`, and `NotificationConfig` with accurate YAML and JSON tags.
  - Time string parsing (e.g. `1800`, `15s`, `300s`) into Go `time.Duration`.
  - Schema validation rules:
    - Workflow `name` must be non-empty and slug-safe.
    - Each step must have a unique `id` within the workflow.
    - Each step must specify a non-empty `command`.
    - Step `depends_on` references must resolve to existing step IDs within the same workflow.
  - Informative validation error messages identifying specific offending step IDs or fields.
  - Unit tests covering valid sample workflows (e.g. `daily-backup.yaml`) and invalid scenarios (missing commands, duplicate IDs, invalid duration strings).
- **Files to Create/Edit:**
  - `internal/workflow/parser.go`
  - `internal/workflow/types.go`
  - `internal/workflow/validator.go`
  - `internal/workflow/workflow_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/workflow
  ```

---

### [x] Task 2.2: DAG Dependency Resolver & Topological Sort
- **Spec Reference & Quoted Requirements:**
  > "2.2: DAG Dependency Resolver (`internal/dag`):
  > - Directed Acyclic Graph validation using Kahn's Algorithm / DFS.
  > - Cycle detection returning clear cycle path errors.
  > - Topological sort calculating parallel execution stages (independent steps run concurrently, dependent steps await parent completion)."
- **Description:** Implement `internal/dag` for building directed acyclic dependency graphs from parsed workflow steps. Use Kahn's algorithm or DFS to detect circular dependencies and perform topological sorting that groups executable steps into parallel execution waves/stages.
- **Acceptance Criteria:**
  - `Graph` data structure representing steps as nodes and dependencies as directed edges.
  - Cycle detection identifying and returning circular dependency chains (e.g., `stepA -> stepB -> stepC -> stepA`).
  - Topological sort calculates execution stages:
    - Stage 0 contains all root steps with zero dependencies (runnable immediately and in parallel).
    - Subsequent stages contain steps whose dependencies are fully satisfied in earlier stages.
  - Unit tests verifying single-step, linear sequential pipelines, branching parallel DAGs, and cycle detection errors.
- **Files to Create/Edit:**
  - `internal/dag/graph.go`
  - `internal/dag/resolver.go`
  - `internal/dag/dag_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/dag
  ```

---

### [x] Task 2.3: Bounded Worker Pool & Execution Runner
- **Spec Reference & Quoted Requirements:**
  > "2.3: Worker Pool & Execution Runner (`internal/engine`):
  > - Bounded concurrency worker pool using Go channels.
  > - Step state machine (`PENDING` -> `RUNNING` -> `COMPLETED`/`FAILED`/`SKIPPED`).
  > - Exponential backoff retry engine with jitter."
- **Description:** Implement `internal/engine` to coordinate end-to-end execution of workflow DAGs. Execute steps according to topological stages using a bounded concurrency worker pool, updating SQLite run records and step states, and applying exponential backoff retries on failure.
- **Acceptance Criteria:**
  - Worker pool with configurable bounded concurrency (e.g. max parallel goroutines).
  - Step state machine transitioning through `PENDING` -> `RUNNING` -> `COMPLETED` / `FAILED` / `SKIPPED`.
  - If a parent step fails, dependent child steps are marked as `SKIPPED`.
  - Exponential backoff retry loop with randomized jitter when step or job specifies `max_retries` and `retry_delay`.
  - Truncated stdout/stderr snippets (last 2KB) saved to SQLite `step_runs`, full streams piped to disk log appender and pub/sub broker.
  - Unit and integration tests covering successful DAG runs, partial step failures with skipped descendants, retry backoff behavior, and concurrency bounds.
- **Files to Create/Edit:**
  - `internal/engine/engine.go`
  - `internal/engine/worker_pool.go`
  - `internal/engine/retry.go`
  - `internal/engine/state_machine.go`
  - `internal/engine/engine_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/engine
  ```

---

## Phase 3: Cron Scheduler & Daemon Lifecycle

### [x] Task 3.1: Cron Scheduling Integration (`robfig/cron/v3`)
- **Spec Reference & Quoted Requirements:**
  > "3.1: Cron Scheduler (`internal/scheduler`):
  > - Integration with `robfig/cron/v3`.
  > - Dynamic job schedule registration and hot reloading."
- **Description:** Implement `internal/scheduler` integrating `robfig/cron/v3` (with standard 5-field cron syntax parser and optional seconds if configured). Manage active cron schedules, register recurring jobs, and provide dynamic add/update/remove capabilities for hot reloading without restarting the daemon.
- **Acceptance Criteria:**
  - Accurate parsing and scheduling of standard cron expressions (e.g., `0 2 * * *`, `*/5 * * * *`).
  - Thread-safe registry mapping `job_id` to active `cron.EntryID`.
  - Dynamic update method to reschedule or unregister jobs on the fly.
  - Execution callback triggers workflow runs via the execution engine with trigger type `'CRON'`.
  - Unit tests validating schedule calculations, registration, dynamic unregistration, and execution triggers.
- **Files to Create/Edit:**
  - `internal/scheduler/scheduler.go`
  - `internal/scheduler/scheduler_test.go`
  - `go.mod` (add `github.com/robfig/cron/v3`)
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/scheduler
  ```

---

### [x] Task 3.2: Directory Watcher & Workflow Auto-Sync
- **Spec Reference & Quoted Requirements:**
  > "3.2: Directory Watcher:
  > - Scans and watches `workflows/*.yaml` for additions, modifications, and deletions.
  > - Synchronizes changes to SQLite `jobs` and `job_steps`."
  > "Hybrid Workflow Configuration: Workflows are defined in declarative YAML files in a configured directory (e.g., `~/.glowkron/workflows/` or `./workflows/`) and auto-synced into SQLite upon daemon launch or reload..."
- **Description:** Implement directory scanning and filesystem watching (`fsnotify` or polling watcher) for `workflows/*.yaml`. Automatically synchronize changes into SQLite `jobs` and `job_steps`, updating the scheduler upon file additions, modifications, or deletions.
- **Acceptance Criteria:**
  - Scans workflow directory on startup and upserts jobs and steps into SQLite.
  - Detects runtime file events (create, write, remove, rename) with debounce to avoid partial reads.
  - Updates SQLite records and updates cron scheduler entry accordingly.
  - Removes or disables jobs whose source YAML files have been deleted.
  - Unit tests verifying initial scan synchronization and file modification events.
- **Files to Create/Edit:**
  - `internal/watcher/watcher.go`
  - `internal/watcher/sync.go`
  - `internal/watcher/watcher_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/watcher
  ```

---

### [ ] Task 3.3: Daemon Lifecycle, Graceful Shutdown & Crash Recovery
- **Spec Reference & Quoted Requirements:**
  > "3.3: Daemon Lifecycle & Graceful Shutdown:
  > - Listens for `SIGINT` / `SIGTERM`.
  > - Drains worker pool gracefully; aborts running processes cleanly via process group cancellation.
  > - Startup recovery: Marks orphaned `RUNNING` jobs from prior crash as `FAILED` or schedules for retry."
- **Description:** Implement daemon lifecycle coordinator handling clean startup, signal trapping (`SIGINT`, `SIGTERM`), graceful draining, active process tree termination, and crash recovery.
- **Acceptance Criteria:**
  - Startup recovery routine detects any `job_runs` or `step_runs` left in `RUNNING` status from prior crash or kill, updating status to `FAILED` with error message `"Aborted due to unexpected daemon termination"`.
  - OS signal handling traps `SIGINT` and `SIGTERM`.
  - Graceful shutdown stops new schedule triggers, drains in-flight worker pool tasks within a configurable grace period, and forcibly kills remaining subprocess process groups via `SIGKILL`.
  - Closes SQLite connections and log appenders cleanly.
  - Unit and integration tests verifying startup crash recovery and signal-triggered shutdown hooks.
- **Files to Create/Edit:**
  - `internal/daemon/daemon.go`
  - `internal/daemon/recovery.go`
  - `internal/daemon/shutdown.go`
  - `internal/daemon/daemon_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/daemon
-```

---

## Phase 4: CLI Interface (`spf13/cobra`)

### [ ] Task 4.1: Cobra CLI Command Tree
- **Spec Reference & Quoted Requirements:**
  > "4.1: CLI Command Tree (`cmd/glowkron/`):
  > - `glowkron daemon [--config <path>] [--port <8080>]`
  > - `glowkron run <job-id-or-file> [--async]`
  > - `glowkron list [--status <all|enabled>]`
  > - `glowkron runs [--job <id>] [--limit <20>]`
  > - `glowkron logs <run-id> [-f / --follow]`
  > - `glowkron validate <workflow.yaml>`"
- **Description:** Build the command-line interface using `github.com/spf13/cobra` covering all user-facing subcommands, flag parsing, and formatted table outputs.
- **Acceptance Criteria:**
  - CLI root command with persistent flags (`--config`, `--db`).
  - `glowkron daemon [--config <path>] [--port <8080>]`: Starts the orchestrator daemon.
  - `glowkron run <job-id-or-file> [--async]`: Dispatches an ad-hoc or scheduled job run.
  - `glowkron list [--status <all|enabled>]`: Displays tabular list of workflows, schedules, and status.
  - `glowkron runs [--job <id>] [--limit <20>]`: Lists execution run history.
  - `glowkron logs <run-id> [-f / --follow]`: Prints or live-streams logs for a run.
  - `glowkron validate <workflow.yaml>`: Validates syntax, schemas, and checks for DAG cycles offline.
  - Unit tests verifying argument parsing and command flags.
- **Files to Create/Edit:**
  - `cmd/glowkron/root.go`
  - `cmd/glowkron/daemon.go`
  - `cmd/glowkron/run.go`
  - `cmd/glowkron/list.go`
  - `cmd/glowkron/runs.go`
  - `cmd/glowkron/logs.go`
  - `cmd/glowkron/validate.go`
  - `cmd/glowkron/cmd_test.go`
  - `go.mod` (add `github.com/spf13/cobra`)
- **Verification / Test Command:**
  ```bash
  go test -v ./cmd/glowkron
  go run ./cmd/glowkron --help
  ```

---

### [ ] Task 4.2: Daemon Client & Offline SQLite Fallback
- **Spec Reference & Quoted Requirements:**
  > "4.2: Daemon Client:
  > - HTTP client querying local daemon API when running.
  > - Offline fallback reading SQLite database directly if daemon is not running."
- **Description:** Implement `internal/client` providing a unified client interface. Queries daemon HTTP REST endpoints when the daemon is running; automatically falls back to reading SQLite and local logs directly when the daemon is inactive.
- **Acceptance Criteria:**
  - Client checks daemon health on `http://127.0.0.1:8080/api/health`.
  - When daemon is online: dispatches commands (`run`, `list`, `runs`, `logs`) via HTTP REST / SSE.
  - When daemon is offline:
    - Queries SQLite directly for `list` and `runs`.
    - Reads log files directly from `~/.glowkron/logs/<run_id>.log` for `logs`.
    - Validates workflows locally for `validate`.
  - Clean error messaging if an action requiring a running daemon (e.g. ad-hoc run while daemon is offline) is invoked without fallback execution capability.
  - Unit tests mocking HTTP daemon and testing SQLite fallback queries.
- **Files to Create/Edit:**
  - `internal/client/client.go`
  - `internal/client/http_client.go`
  - `internal/client/offline_client.go`
  - `internal/client/client_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/client
  ```

---

## Phase 5: Webhook Notifier

### [ ] Task 5.1: Multi-Platform Webhook Dispatcher
- **Spec Reference & Quoted Requirements:**
  > "5.1: Webhook Dispatcher (`internal/notifier`):
  > - Dispatches HTTP POST notifications with formatted JSON payloads.
  > - Pre-built adapters for:
  >   - Google Chat (Card v2 formatting with job status, error snippet, execution time).
  >   - Discord (Embed formatting with color-coded success/failure states).
  >   - Telegram (Markdown formatted text).
  >   - Generic Webhook (Standardized JSON event payload).
  > - Configurable dispatch rules: `on_failure`, `on_retry_exhausted`, `on_success`."
- **Description:** Implement `internal/notifier` to dispatch alert notifications upon workflow completion or failure. Support channel-specific payloads for Google Chat, Discord, Telegram, and generic JSON webhooks based on workflow notification rules.
- **Acceptance Criteria:**
  - Formatters and adapters:
    - Google Chat Card v2 adapter with status badges, execution duration, and truncated error snippet.
    - Discord Webhook Embed adapter with color (green for success, red for failure) and field details.
    - Telegram Bot API adapter with Markdown-formatted alert message.
    - Generic Webhook adapter with structured JSON payload (`job_id`, `run_id`, `status`, `exit_code`, `duration_ms`, `error`).
  - Dispatch filtering according to rules: `on_failure`, `on_retry_exhausted`, `on_success`.
  - Non-blocking asynchronous dispatch with timeout (e.g. 10s) and retry to avoid delaying workflow runners.
  - Unit tests with `httptest.Server` verifying payload structure, headers, and dispatch logic for all 4 adapters.
- **Files to Create/Edit:**
  - `internal/notifier/dispatcher.go`
  - `internal/notifier/google_chat.go`
  - `internal/notifier/discord.go`
  - `internal/notifier/telegram.go`
  - `internal/notifier/generic.go`
  - `internal/notifier/notifier_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/notifier
  ```

---

## Phase 6: Embedded Web Dashboard & SSE Log Streaming

### [ ] Task 6.1: REST API & SSE Real-time Streaming Handler
- **Spec Reference & Quoted Requirements:**
  > "6.1: REST API & SSE Handler (`internal/server`):
  > - Endpoints:
  >   - `GET /api/jobs`: List all registered workflows.
  >   - `POST /api/jobs/:id/run`: Trigger workflow run.
  >   - `GET /api/runs`: List historical runs with status filters.
  >   - `GET /api/runs/:id`: Get run details, step states, and exit codes.
  >   - `GET /api/runs/:id/logs`: Full run log download.
  >   - `GET /api/runs/:id/stream`: SSE real-time log chunk and step status stream."
  > "Unified Daemon API & Embedded Web Dashboard: The daemon serves a lightweight REST API + SSE endpoint on `http://127.0.0.1:8080`."
- **Description:** Implement `internal/server` HTTP router providing REST endpoints and Server-Sent Events (SSE) streaming for real-time log tailing and live step status transitions.
- **Acceptance Criteria:**
  - Standard library `net/http` router with CORS configuration for local dashboard access.
  - Complete implementation of the 6 core API endpoints:
    - `GET /api/jobs`: JSON list of all registered jobs and configuration metadata.
    - `POST /api/jobs/{id}/run`: Dispatches an immediate ad-hoc run, returning `run_id`.
    - `GET /api/runs`: Lists runs with pagination and query filters (`job_id`, `status`).
    - `GET /api/runs/{id}`: Detailed run payload including all `step_runs`.
    - `GET /api/runs/{id}/logs`: Serves the raw log file text from disk.
    - `GET /api/runs/{id}/stream`: Establishes SSE stream subscribing to pub/sub log broker and step state changes.
  - Unit and integration tests using `net/http/httptest` covering each endpoint and verifying SSE event formatting.
- **Files to Create/Edit:**
  - `internal/server/server.go`
  - `internal/server/handlers_jobs.go`
  - `internal/server/handlers_runs.go`
  - `internal/server/handlers_sse.go`
  - `internal/server/server_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/server
  ```

---

### [ ] Task 6.2: Embedded Single-Page Web Dashboard (`//go:embed`)
- **Spec Reference & Quoted Requirements:**
  > "6.2: Embedded Single-Page Web Dashboard (`internal/server/ui`):
  > - Self-contained HTML/CSS/JS served via `//go:embed`.
  > - Visual DAG workflow viewer showing step status colors (Grey: pending, Blue: running, Green: completed, Red: failed).
  > - Real-time live log console with auto-scroll and terminal dark mode theme.
  > - Zero external CDN dependencies; single binary offline readiness."
  > "The Web UI is embedded directly into the Go binary using `//go:embed` with modern, clean vanilla JS / CSS (no external node_modules or CDN dependencies)."
- **Description:** Build and embed a modern, zero-dependency vanilla HTML/CSS/JavaScript single-page application into `internal/server/ui` using Go's `//go:embed`. Serves the dashboard directly from the single binary executable with interactive DAG visualization and real-time SSE log tailing.
- **Acceptance Criteria:**
  - All assets (HTML, CSS, JS, SVGs/icons) embedded directly into Go binary with zero external CDN requests (`node_modules` or web links).
  - Responsive dark-mode dashboard UI:
    - Workflow overview list with execution buttons and schedule chips.
    - Interactive visual DAG viewer displaying nodes and dependency arrows with real-time state coloring:
      - Grey: `PENDING`
      - Blue: `RUNNING`
      - Green: `COMPLETED`
      - Red: `FAILED`
      - Amber / Muted: `SKIPPED`
    - Live terminal log console connecting to `/api/runs/{id}/stream` with automatic scroll-to-bottom toggle.
    - Historical runs table with filters, search, and direct links to inspect previous executions.
  - Unit tests verifying embedded filesystem serves `index.html` and static assets with correct MIME types.
- **Files to Create/Edit:**
  - `internal/server/ui/embed.go`
  - `internal/server/ui/static/index.html`
  - `internal/server/ui/static/app.css`
  - `internal/server/ui/static/app.js`
  - `internal/server/ui/embed_test.go`
- **Verification / Test Command:**
  ```bash
  go test -v ./internal/server/ui
  ```

---

## Task Maintenance & Quality Gates Protocol

1. **State Preservation:** Every task begins as `[ ]` and transitions to `[x]` ONLY after automated unit tests pass green and git commit is prepared.
2. **Commit Standard:** Every micro-task requires a conventional commit: `feat(...)`, `test(...)`, `fix(...)`, `refactor(...)`.
3. **Quality Gates:**
   - Zero compiler warnings or lint errors.
   - All tests passing with `go test -v ./...`.
   - Process group isolation and database WAL mode integrity preserved across all phases.
