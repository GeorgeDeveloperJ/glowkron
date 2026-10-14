# GlowKron ⚡

> **Single-binary background job and DAG workflow orchestrator written in Go.**

GlowKron automates developer tasks—backups, web scrapers, data pipelines, and maintenance scripts—with cron scheduling, multi-step dependency graphs (DAG), automatic retries with exponential backoff, real-time log streaming, and embedded WAL SQLite durability.

---

## ✨ Features

- **Single Binary & Zero Dependencies:** Compiles into one self-contained Go executable (`glowkron`). Zero external runtime dependencies—no Redis, no Postgres, no Docker daemon, and zero CGo (uses `modernc.org/sqlite` pure Go).
- **DAG Workflow Engine:** Multi-step dependency resolution via Kahn's algorithm, cycle detection, and step state transitions (`PENDING` ➔ `RUNNING` ➔ `COMPLETED` / `FAILED` / `SKIPPED`).
- **Cron & Ad-hoc Scheduling:** Standard 5-field cron syntax (`robfig/cron/v3`) with dynamic registration and hot-reloading from `workflows/*.yaml`.
- **Embedded Web Dashboard:** Zero-CDN, air-gapped embedded SPA (`//go:embed`) served at `http://127.0.0.1:8080` with dark-mode terminal aesthetics, topological DAG visualizer, and Server-Sent Events (SSE) log streaming.
- **Process Group Isolation:** Subprocesses execute in dedicated process groups (`Setpgid: true`) with graceful `SIGTERM` followed by `SIGKILL`—guaranteeing zero zombie processes.
- **Multi-Platform Webhooks:** Asynchronous alert dispatcher for Google Chat (Card v2), Discord (Rich Embeds), Telegram (Markdown), and generic JSON.
- **Offline CLI Fallback:** CLI commands query SQLite and execute workflows locally in-process even when the background daemon is not running.

---

## 🚀 Quick Start (60 Seconds)

### 1. Build or Install

```bash
# Build binary
make build

# Or install globally into $GOPATH/bin
make install
```

### 2. Initialize Workspace & Starter Workflow

```bash
glowkron init
```
This bootstraps `~/.glowkron/`, creates the `workflows/` and `logs/` directories, and scaffolds a validated 3-step starter DAG workflow (`~/.glowkron/workflows/sample-pipeline.yaml`).

### 3. Validate & Execute

```bash
# Validate syntax and check for cyclic dependencies
glowkron validate ~/.glowkron/workflows/sample-pipeline.yaml

# Run the workflow immediately and stream logs
glowkron run sample-pipeline --follow
```

### 4. Launch the Daemon & Web Dashboard

```bash
glowkron daemon --port 8080
```
Open **[http://localhost:8080](http://localhost:8080)** in your browser to view the interactive DAG visualizer and real-time SSE log tailing.

---

## 📖 CLI Commands Reference

| Command | Description | Example |
| :--- | :--- | :--- |
| `glowkron init` | Scaffold directories and sample workflow | `glowkron init [--force] [--with-config]` |
| `glowkron daemon` | Start the background daemon & Web UI | `glowkron daemon --port 8080` |
| `glowkron list` | List registered workflows & cron schedules | `glowkron list` |
| `glowkron run <job>` | Trigger an ad-hoc run | `glowkron run sample-pipeline --follow` |
| `glowkron runs` | View execution history with status filters | `glowkron runs --status FAILED --limit 20` |
| `glowkron logs <id>` | View or follow logs for a run | `glowkron logs <run-id> -f` |
| `glowkron validate <file>` | Verify DAG graph and cycle detection | `glowkron validate workflows/backup.yaml` |

---

## 📝 Workflow Specification (`workflows/*.yaml`)

Drop YAML files into `~/.glowkron/workflows/` (or `./workflows/`). GlowKron hot-reloads them automatically:

```yaml
version: "1.0"
name: database-backup
description: "Nightly backup with compression and upload"
cron: "0 2 * * *"         # Daily at 2:00 AM UTC
max_retries: 2
retry_delay: 5s

notifications:
  on_failure: true
  webhook_url: "https://discord.com/api/webhooks/..."

steps:
  - id: dump
    name: "Dump Database"
    command: pg_dump
    args: ["-Fc", "mydb", "-f", "/tmp/backup.dump"]

  - id: compress
    name: "Compress Archive"
    command: gzip
    args: ["-9", "/tmp/backup.dump"]
    depends_on: ["dump"]

  - id: upload
    name: "Upload to S3"
    command: aws
    args: ["s3", "cp", "/tmp/backup.dump.gz", "s3://my-backups/"]
    depends_on: ["compress"]
```

---

## 🛠️ Developer Ergonomics (`Makefile`)

```bash
make help        # Show all available commands
make build       # Compile zero-CGo static binary to ./glowkron
make test        # Run standard test suite
make test-race   # Run race detector across all packages (go test -race)
make vet         # Run go vet static analysis
make fmt         # Run gofmt formatting check
make clean       # Remove build artifacts and test cache
```

---

## 🐧 Production Deployment (`systemd`)

Install GlowKron as a persistent background user service:

```bash
# 1. Install service unit to ~/.config/systemd/user/glowkron.service
./deploy/systemd/install.sh

# 2. Enable and start the service
systemctl --user enable --now glowkron

# 3. Check status and logs
systemctl --user status glowkron
journalctl --user -u glowkron -f

# 4. (Optional) Keep service running after logout
loginctl enable-linger $USER
```

---

## 📄 License

MIT License. See [LICENSE](file:///home/george_santana/Projects/glowkron/LICENSE) for details.
