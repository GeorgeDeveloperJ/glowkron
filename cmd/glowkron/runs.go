package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

func newRunsCmd(cliCtx *CLIContext) *cobra.Command {
	var jobID string
	var limit int

	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List historical workflow execution runs",
		Long:  `Displays historical execution runs recorded in SQLite, including run ID, status, start time, and exit code.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cliCtx.initClient(); err != nil {
				return err
			}

			runs, err := cliCtx.Client.ListRuns(cmd.Context(), jobID, limit)
			if err != nil {
				return fmt.Errorf("failed to list runs: %w", err)
			}

			if len(runs) == 0 {
				cmd.Printf("No execution runs found.\n")
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "RUN ID\tJOB\tSTATUS\tTRIGGER\tSTARTED AT\tDURATION\tEXIT CODE")
			for _, r := range runs {
				exitStr := "-"
				if r.ExitCode != nil {
					exitStr = fmt.Sprintf("%d", *r.ExitCode)
				}
				durStr := "-"
				if r.EndTime != nil {
					durStr = r.EndTime.Sub(r.StartTime).Round(time.Millisecond).String()
				}
				startedStr := r.StartTime.Local().Format("2006-01-02 15:04:05")

				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					r.ID,
					r.JobID,
					r.Status,
					r.TriggeredBy,
					startedStr,
					durStr,
					exitStr,
				)
			}
			return w.Flush()
		},
	}

	cmd.Flags().StringVarP(&jobID, "job", "j", "", "Filter runs by workflow job name or ID")
	cmd.Flags().IntVarP(&limit, "limit", "n", 20, "Maximum number of runs to display")

	return cmd
}
