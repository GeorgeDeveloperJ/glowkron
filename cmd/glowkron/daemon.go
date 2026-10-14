package main

import (
	"fmt"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/config"
	"github.com/GeorgeDeveloperJ/glowkron/internal/daemon"
	"github.com/spf13/cobra"
)

func newDaemonCmd(cliCtx *CLIContext) *cobra.Command {
	var maxConcurrency int

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Start the GlowKron background orchestrator daemon",
		Long: `Starts the central orchestrator coordinator, including embedded SQLite in WAL mode,
cron scheduler, filesystem watcher for declarative YAML workflows, and bounded worker pool.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths, err := config.ResolvePaths(cliCtx.HomeDir)
			if err != nil {
				return fmt.Errorf("failed to resolve paths: %w", err)
			}
			if err := paths.EnsureDirs(); err != nil {
				return fmt.Errorf("failed to initialize directories: %w", err)
			}

			dbPath := cliCtx.DBFile
			if dbPath == "" {
				dbPath = paths.DBFile
			}

			cmd.Printf("Starting GlowKron Daemon...\n")
			cmd.Printf("  Home:      %s\n", paths.HomeDir)
			cmd.Printf("  Database:  %s\n", dbPath)
			cmd.Printf("  Workflows: %s\n", paths.WorkflowsDir)
			cmd.Printf("  Logs:      %s\n", paths.LogsDir)
			cmd.Printf("  Port:      %d\n", cliCtx.Port)

			cfg := daemon.Config{
				DBPath:          dbPath,
				WorkflowsDir:    paths.WorkflowsDir,
				LogsDir:         paths.LogsDir,
				MaxConcurrency:  maxConcurrency,
				ShutdownTimeout: 15 * time.Second,
				Port:            cliCtx.Port,
			}

			d, err := daemon.New(cfg)
			if err != nil {
				return fmt.Errorf("failed to initialize daemon: %w", err)
			}

			return d.Run()
		},
	}

	cmd.Flags().IntVar(&maxConcurrency, "concurrency", config.DefaultConcurrency, "Max parallel worker goroutines")

	return cmd
}
