package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newListCmd(cliCtx *CLIContext) *cobra.Command {
	var statusFilter string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered workflows and their schedules",
		Long:  `Displays all registered jobs in SQLite, their active cron schedule expressions, and status.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cliCtx.initClient(); err != nil {
				return err
			}

			jobs, err := cliCtx.Client.ListJobs(cmd.Context(), statusFilter)
			if err != nil {
				return fmt.Errorf("failed to list workflows: %w", err)
			}

			if len(jobs) == 0 {
				cmd.Printf("No workflows found (filter: %s).\n", statusFilter)
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NAME\tSTATUS\tCRON SCHEDULE\tMAX RETRIES\tTIMEOUT\tDESCRIPTION")
			for _, j := range jobs {
				statusStr := "ENABLED"
				if !j.Enabled {
					statusStr = "DISABLED"
				}
				cronStr := j.CronExpr
				if cronStr == "" {
					cronStr = "(manual)"
				}
				timeoutStr := fmt.Sprintf("%ds", j.TimeoutSec)
				if j.TimeoutSec == 0 {
					timeoutStr = "-"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
					j.Name,
					statusStr,
					cronStr,
					j.MaxRetries,
					timeoutStr,
					j.Description,
				)
			}
			return w.Flush()
		},
	}

	cmd.Flags().StringVarP(&statusFilter, "status", "s", "all", "Filter jobs by status: all, enabled, disabled")

	return cmd
}
