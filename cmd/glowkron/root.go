package main

import (
	"fmt"
	"os"

	"github.com/GeorgeDeveloperJ/glowkron/internal/client"
	"github.com/GeorgeDeveloperJ/glowkron/internal/config"
	"github.com/spf13/cobra"
)

// CLIContext encapsulates persistent flags, resolved paths, and the client transport.
type CLIContext struct {
	HomeDir      string
	ConfigFile   string
	DBFile       string
	Port         int
	WorkflowsDir string
	LogsDir      string
	Client       client.Client
}

// initClient initializes the AutoClient (HTTP with offline fallback) if not already provided (e.g. in tests).
func (c *CLIContext) initClient() error {
	if c.Client != nil {
		return nil
	}

	paths, err := config.ResolvePaths(c.HomeDir)
	if err != nil {
		return fmt.Errorf("failed to resolve configuration paths: %w", err)
	}
	if err := paths.EnsureDirs(); err != nil {
		return fmt.Errorf("failed to initialize directories: %w", err)
	}

	if c.DBFile == "" {
		c.DBFile = paths.DBFile
	}
	if c.WorkflowsDir == "" {
		c.WorkflowsDir = paths.WorkflowsDir
	}
	if c.LogsDir == "" {
		c.LogsDir = paths.LogsDir
	}

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", c.Port)
	cl, err := client.NewAutoClient(baseURL, c.DBFile, c.LogsDir, c.WorkflowsDir)
	if err != nil {
		return fmt.Errorf("failed to initialize client: %w", err)
	}
	c.Client = cl
	return nil
}

// NewRootCmd constructs the GlowKron Cobra command tree.
func NewRootCmd(cliCtx *CLIContext) *cobra.Command {
	if cliCtx == nil {
		cliCtx = &CLIContext{
			Port: config.DefaultPort,
		}
	}

	rootCmd := &cobra.Command{
		Use:   "glowkron",
		Short: "GlowKron is a single-binary background job & DAG workflow orchestrator",
		Long: `GlowKron provides cron scheduling, multi-step dependency graphs (DAG),
automatic retries with exponential backoff, real-time logging, and WAL SQLite durability.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Persistent global flags
	rootCmd.PersistentFlags().StringVar(&cliCtx.HomeDir, "home", "", "GlowKron home directory (default is $HOME/.glowkron)")
	rootCmd.PersistentFlags().StringVar(&cliCtx.ConfigFile, "config", "", "Path to configuration file")
	rootCmd.PersistentFlags().StringVar(&cliCtx.DBFile, "db", "", "Path to SQLite database file")
	rootCmd.PersistentFlags().IntVar(&cliCtx.Port, "port", config.DefaultPort, "Daemon API port")

	// Register subcommands
	rootCmd.AddCommand(newDaemonCmd(cliCtx))
	rootCmd.AddCommand(newRunCmd(cliCtx))
	rootCmd.AddCommand(newListCmd(cliCtx))
	rootCmd.AddCommand(newRunsCmd(cliCtx))
	rootCmd.AddCommand(newLogsCmd(cliCtx))
	rootCmd.AddCommand(newValidateCmd(cliCtx))

	return rootCmd
}

func Execute() {
	ctx := &CLIContext{Port: config.DefaultPort}
	rootCmd := NewRootCmd(ctx)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
