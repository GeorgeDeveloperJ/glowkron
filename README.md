# GlowKron ⚡

> Single-binary background job and DAG workflow orchestrator written in Go.

GlowKron automates personal developer tasks—backups, web scrapers, data pipelines, and maintenance scripts—with cron scheduling, multi-step dependency graphs (DAG), automatic retries with exponential backoff, and embedded SQLite persistence.

---

## ✨ Features

- **Single Binary & Zero Dependencies:** Compiles into a single self-contained binary. No external Redis, Postgres, or Docker daemon required.
- **DAG Workflow Engine:** Chain tasks with step dependencies and failure handling.
- **Cron & Ad-hoc Scheduling:** Full cron expression support (`robfig/cron/v3`) plus manual triggers via CLI or Web UI.
- **Embedded Persistence:** Pure Go SQLite (`modernc.org/sqlite`) with WAL mode for durable job history and execution logs.
- **Process Safety:** Built with process group management (`Setpgid`) to ensure child processes never orphan on timeout or cancellation.
- **Built-in Web Dashboard:** Embedded UI with real-time log streaming via Server-Sent Events (SSE).

---

## 🛠️ Tech Stack

- **Language:** Go 1.22+
- **Process Management:** `os/exec` with process group isolation
- **Storage:** Embedded SQLite (Pure Go / Zero-CGo)
- **CLI:** Cobra

---

## 🚀 Quick Start

### Build & Run

```bash
go run ./cmd/glowkron
```

### Run Tests

``` bash
go test -v ./...
```
