package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newLogsCmd(cliCtx *CLIContext) *cobra.Command {
	var follow bool

	cmd := &cobra.Command{
		Use:   "logs <run-id>",
		Short: "View or follow logs for an execution run",
		Long: `Fetches execution logs for the specified run ID.
With -f/--follow, live log output is streamed in real time via Server-Sent Events.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cliCtx.initClient(); err != nil {
				return err
			}

			runID := args[0]

			if follow {
				logCh, errCh, err := cliCtx.Client.StreamRunLogs(cmd.Context(), runID)
				if err != nil {
					return fmt.Errorf("failed to stream logs for run %s: %w", runID, err)
				}

				for chunk := range logCh {
					cmd.Print(chunk)
				}
				if err := <-errCh; err != nil {
					return fmt.Errorf("log stream error: %w", err)
				}
				return nil
			}

			logs, err := cliCtx.Client.GetRunLogs(cmd.Context(), runID)
			if err != nil {
				return fmt.Errorf("failed to read logs for run %s: %w", runID, err)
			}

			if logs == "" {
				cmd.Printf("(no log output recorded for run %s)\n", runID)
				return nil
			}

			cmd.Print(logs)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow log stream in real time")

	return cmd
}
