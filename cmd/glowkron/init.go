package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/GeorgeDeveloperJ/glowkron/internal/config"
	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
	"github.com/spf13/cobra"
)

const sampleWorkflowTemplate = `version: "1.0"
name: sample-pipeline
description: "Starter 3-step DAG workflow pipeline"
steps:
  - id: step-1
    name: "Fetch Source Data"
    command: echo
    args: ["Fetched latest dataset"]

  - id: step-2
    name: "Transform & Process"
    command: echo
    args: ["Data transformed successfully"]
    depends_on: ["step-1"]

  - id: step-3
    name: "Publish Summary"
    command: echo
    args: ["Pipeline execution complete"]
    depends_on: ["step-2"]
`

const configTemplate = `# GlowKron Configuration
# Documentation: https://github.com/GeorgeDeveloperJ/glowkron

# Port for the daemon HTTP API server
port: %d

# Directory where declarative workflow YAML definitions are stored
workflows_dir: %s

# SQLite database file path (WAL mode durability)
db_path: %s

# Logging level: debug, info, warn, error
log_level: info
`

func newInitCmd(cliCtx *CLIContext) *cobra.Command {
	var force bool
	var withConfig bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize GlowKron directory structure and sample workflow",
		Long: `Initializes the GlowKron local workspace environment.
Creates the home directory, workflows directory, logs directory, and scaffolds
a verified starter 3-step DAG workflow (sample-pipeline.yaml). Optionally
creates a default configuration file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// 1. Resolve paths using CLIContext or default
			paths, err := config.ResolvePaths(cliCtx.HomeDir)
			if err != nil {
				return fmt.Errorf("failed to resolve configuration paths: %w", err)
			}

			// 2. Ensure home, workflows, and logs directories exist
			if err := paths.EnsureDirs(); err != nil {
				return fmt.Errorf("failed to initialize directories: %w", err)
			}

			// 3. Validate template YAML schema and DAG dependencies
			wf, err := workflow.ParseBytes([]byte(sampleWorkflowTemplate))
			if err != nil {
				return fmt.Errorf("starter workflow template validation failed: %w", err)
			}
			if _, _, err := dag.BuildAndResolve(wf.Steps); err != nil {
				return fmt.Errorf("starter workflow DAG resolution failed: %w", err)
			}

			// 4. Scaffold sample-pipeline.yaml
			sampleFile := filepath.Join(paths.WorkflowsDir, "sample-pipeline.yaml")
			if _, err := os.Stat(sampleFile); err == nil {
				if !force {
					cmd.Printf("Notice: starter workflow already exists at %s (use --force to overwrite)\n", sampleFile)
				} else {
					if err := os.WriteFile(sampleFile, []byte(sampleWorkflowTemplate), 0644); err != nil {
						return fmt.Errorf("failed to overwrite starter workflow: %w", err)
					}
					cmd.Printf("Overwrote starter workflow: %s\n", sampleFile)
				}
			} else if os.IsNotExist(err) {
				if err := os.WriteFile(sampleFile, []byte(sampleWorkflowTemplate), 0644); err != nil {
					return fmt.Errorf("failed to create starter workflow: %w", err)
				}
				cmd.Printf("Created starter workflow: %s\n", sampleFile)
			} else {
				return fmt.Errorf("failed to check starter workflow: %w", err)
			}

			// 5. Generate default config.yaml if requested
			if withConfig {
				configFile := paths.ConfigFile
				content := fmt.Sprintf(configTemplate, config.DefaultPort, paths.WorkflowsDir, paths.DBFile)
				if _, err := os.Stat(configFile); err == nil {
					if !force {
						cmd.Printf("Notice: configuration file already exists at %s (use --force to overwrite)\n", configFile)
					} else {
						if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
							return fmt.Errorf("failed to overwrite configuration file: %w", err)
						}
						cmd.Printf("Overwrote configuration file: %s\n", configFile)
					}
				} else if os.IsNotExist(err) {
					if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
						return fmt.Errorf("failed to create configuration file: %w", err)
					}
					cmd.Printf("Created configuration file: %s\n", configFile)
				} else {
					return fmt.Errorf("failed to check configuration file: %w", err)
				}
			}

			// 6. Print stylish banner with next steps
			cmd.Println()
			cmd.Println("╔══════════════════════════════════════════════════════════════════╗")
			cmd.Println("║               ✨ GlowKron Initialized Successfully               ║")
			cmd.Println("╚══════════════════════════════════════════════════════════════════╝")
			cmd.Println()
			cmd.Println("GlowKron workspace is ready:")
			cmd.Printf("  • Home:      %s\n", paths.HomeDir)
			cmd.Printf("  • Workflows: %s\n", paths.WorkflowsDir)
			cmd.Printf("  • Logs:      %s\n", paths.LogsDir)
			if withConfig {
				cmd.Printf("  • Config:    %s\n", paths.ConfigFile)
			}
			cmd.Printf("  • Starter:   %s\n", sampleFile)
			cmd.Println()
			cmd.Println("Next steps:")
			cmd.Println("  • glowkron list")
			cmd.Println("  • glowkron run sample-pipeline --follow")
			cmd.Println("  • glowkron daemon (visit http://localhost:8080)")
			cmd.Println()

			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Overwrite existing starter workflow if it already exists")
	cmd.Flags().BoolVar(&withConfig, "with-config", false, "Generate a default config.yaml in the home directory")

	return cmd
}
