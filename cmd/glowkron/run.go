package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func newRunCmd(cliCtx *CLIContext) *cobra.Command {
	var async bool

	cmd := &cobra.Command{
		Use:   "run <job-id-or-file>",
		Short: "Trigger an immediate run of a workflow job",
		Long: `Executes a registered workflow job by its ID or directly from a workflow YAML file.
When the daemon is running, execution is coordinated by the daemon.
When offline, the workflow is executed locally in-process.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cliCtx.initClient(); err != nil {
				return err
			}

			jobTarget := args[0]
			cmd.Printf("Dispatching run for: %s (async=%v)...\n", jobTarget, async)

			start := time.Now()
			run, err := cliCtx.Client.TriggerJob(cmd.Context(), jobTarget, async)
			if err != nil {
				return fmt.Errorf("failed to execute workflow %s: %w", jobTarget, err)
			}

			duration := time.Since(start).Round(time.Millisecond)

			cmd.Printf("\n--- Execution Summary ---\n")
			cmd.Printf("Run ID:       %s\n", run.ID)
			cmd.Printf("Job:          %s\n", run.JobID)
			cmd.Printf("Status:       %s\n", run.Status)
			cmd.Printf("Triggered By: %s\n", run.TriggeredBy)
			if run.ExitCode != nil {
				cmd.Printf("Exit Code:    %d\n", *run.ExitCode)
			}
			if !async {
				cmd.Printf("Duration:     %v\n", duration)
			}
			if run.ErrorMessage != "" {
				cmd.Printf("Error:        %s\n", run.ErrorMessage)
			}
			cmd.Printf("Logs:         glowkron logs %s\n", run.ID)

			return nil
		},
	}

	cmd.Flags().BoolVarP(&async, "async", "a", false, "Trigger run asynchronously in the background")

	return cmd
}
