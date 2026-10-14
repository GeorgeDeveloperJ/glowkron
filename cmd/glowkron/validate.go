package main

import (
	"fmt"

	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
	"github.com/spf13/cobra"
)

func newValidateCmd(cliCtx *CLIContext) *cobra.Command {
	return &cobra.Command{
		Use:   "validate <workflow.yaml>",
		Short: "Validate a workflow YAML file and verify DAG dependency graph",
		Long: `Performs offline validation of a declarative workflow file:
- Validates YAML syntax and required fields
- Validates step commands, timeouts, and dependency identifiers
- Resolves Directed Acyclic Graph (DAG) and detects circular dependencies`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			cmd.Printf("Validating workflow file: %s\n", filePath)

			// 1. Parse and validate YAML schema
			wf, err := workflow.ParseFile(filePath)
			if err != nil {
				return fmt.Errorf("schema validation failed:\n  %w", err)
			}
			cmd.Printf("✓ YAML syntax and schema valid (workflow: %q, steps: %d)\n", wf.Name, len(wf.Steps))

			// 2. Validate DAG and detect cycles
			plan, _, err := dag.BuildAndResolve(wf.Steps)
			if err != nil {
				return fmt.Errorf("DAG dependency resolution failed:\n  %w", err)
			}
			cmd.Printf("✓ DAG resolution valid: zero cycles detected\n")

			// 3. Display execution stages
			cmd.Printf("\nResolved %d Parallel Execution Stage(s):\n", plan.StageCount())
			for i, stage := range plan.Stages {
				cmd.Printf("  Stage %d: %v\n", i+1, stage)
			}

			cmd.Printf("\nResult: Workflow %q is valid and ready for execution.\n", wf.Name)
			return nil
		},
	}
}
