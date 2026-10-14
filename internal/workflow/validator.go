package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// validSlugRegex validates slug identifiers (alphanumeric, hyphens, underscores).
var validSlugRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// ValidationError represents a specific schema or semantic validation failure.
type ValidationError struct {
	Field   string `json:"field,omitempty"`
	StepID  string `json:"step_id,omitempty"`
	Message string `json:"message"`
}

// Error formats the ValidationError into a descriptive string.
func (e ValidationError) Error() string {
	if e.StepID != "" && e.Field != "" {
		return fmt.Sprintf("validation error in step %q (field %q): %s", e.StepID, e.Field, e.Message)
	}
	if e.StepID != "" {
		return fmt.Sprintf("validation error in step %q: %s", e.StepID, e.Message)
	}
	if e.Field != "" {
		return fmt.Sprintf("validation error in field %q: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

// ValidationErrors is an aggregate collection of ValidationError.
type ValidationErrors []ValidationError

// Error formats the aggregate errors into a clear, multiline message.
func (ve ValidationErrors) Error() string {
	if len(ve) == 0 {
		return "validation error"
	}
	if len(ve) == 1 {
		return ve[0].Error()
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d validation errors:\n", len(ve)))
	for i, err := range ve {
		b.WriteString(fmt.Sprintf("  %d. %s\n", i+1, err.Error()))
	}
	return strings.TrimRight(b.String(), "\n")
}

// HasField returns true if any validation error relates to the given field name.
func (ve ValidationErrors) HasField(field string) bool {
	for _, e := range ve {
		if e.Field == field {
			return true
		}
	}
	return false
}

// HasStep returns true if any validation error relates to the given step ID.
func (ve ValidationErrors) HasStep(stepID string) bool {
	for _, e := range ve {
		if e.StepID == stepID {
			return true
		}
	}
	return false
}

// Validate inspects a WorkflowConfig against schema and semantic integrity rules:
//   - Workflow config is non-nil
//   - Version is non-empty
//   - Name is non-empty and a valid slug/identifier
//   - Steps list is non-empty
//   - Step IDs are unique within the workflow and valid slugs
//   - Step commands are non-empty
//   - Non-negative timeouts, retry counts, and retry delays
//   - Step depends_on references resolve to existing steps in the workflow
//   - Self-dependency is rejected
func Validate(wf *WorkflowConfig) error {
	if wf == nil {
		return errors.New("workflow config cannot be nil")
	}

	var errs ValidationErrors

	// 1. Version validation
	if strings.TrimSpace(wf.Version) == "" {
		errs = append(errs, ValidationError{
			Field:   "version",
			Message: "workflow version cannot be empty",
		})
	}

	// 2. Name validation
	trimmedName := strings.TrimSpace(wf.Name)
	if trimmedName == "" {
		errs = append(errs, ValidationError{
			Field:   "name",
			Message: "workflow name cannot be empty",
		})
	} else if !validSlugRegex.MatchString(wf.Name) {
		errs = append(errs, ValidationError{
			Field:   "name",
			Message: fmt.Sprintf("workflow name %q must be a valid identifier/slug (alphanumeric, hyphens, underscores)", wf.Name),
		})
	}

	// 3. Workflow-level duration / retry constraints
	if wf.Timeout < 0 {
		errs = append(errs, ValidationError{
			Field:   "timeout",
			Message: "workflow timeout cannot be negative",
		})
	}
	if wf.RetryDelay < 0 {
		errs = append(errs, ValidationError{
			Field:   "retry_delay",
			Message: "workflow retry_delay cannot be negative",
		})
	}
	if wf.MaxRetries < 0 {
		errs = append(errs, ValidationError{
			Field:   "max_retries",
			Message: "workflow max_retries cannot be negative",
		})
	}

	// 4. Steps list presence
	if len(wf.Steps) == 0 {
		errs = append(errs, ValidationError{
			Field:   "steps",
			Message: "workflow must contain at least one step",
		})
		return errs
	}

	// 5. First pass: validate individual step fields and collect known step IDs
	seenIDs := make(map[string]bool, len(wf.Steps))
	for i, step := range wf.Steps {
		stepIndexLabel := fmt.Sprintf("step[%d]", i)
		stepID := strings.TrimSpace(step.ID)

		if stepID == "" {
			errs = append(errs, ValidationError{
				StepID:  stepIndexLabel,
				Field:   "id",
				Message: "step ID cannot be empty",
			})
		} else {
			if !validSlugRegex.MatchString(step.ID) {
				errs = append(errs, ValidationError{
					StepID:  step.ID,
					Field:   "id",
					Message: fmt.Sprintf("step ID %q must be a valid identifier/slug (alphanumeric, hyphens, underscores)", step.ID),
				})
			}

			if seenIDs[step.ID] {
				errs = append(errs, ValidationError{
					StepID:  step.ID,
					Field:   "id",
					Message: fmt.Sprintf("duplicate step ID %q found in workflow", step.ID),
				})
			} else {
				seenIDs[step.ID] = true
			}
		}

		if strings.TrimSpace(step.Command) == "" {
			errID := step.ID
			if errID == "" {
				errID = stepIndexLabel
			}
			errs = append(errs, ValidationError{
				StepID:  errID,
				Field:   "command",
				Message: "step command cannot be empty",
			})
		}

		if step.Timeout < 0 {
			errID := step.ID
			if errID == "" {
				errID = stepIndexLabel
			}
			errs = append(errs, ValidationError{
				StepID:  errID,
				Field:   "timeout",
				Message: "step timeout cannot be negative",
			})
		}
	}

	// 6. Second pass: validate depends_on relationships
	for _, step := range wf.Steps {
		if step.ID == "" {
			continue
		}

		seenStepDeps := make(map[string]bool, len(step.DependsOn))
		for _, dep := range step.DependsOn {
			trimmedDep := strings.TrimSpace(dep)

			// Self-dependency check
			if trimmedDep == step.ID {
				errs = append(errs, ValidationError{
					StepID:  step.ID,
					Field:   "depends_on",
					Message: fmt.Sprintf("step %q cannot depend on itself", step.ID),
				})
				continue
			}

			// Duplicate dependency check
			if seenStepDeps[trimmedDep] {
				errs = append(errs, ValidationError{
					StepID:  step.ID,
					Field:   "depends_on",
					Message: fmt.Sprintf("step %q has duplicate dependency %q", step.ID, trimmedDep),
				})
				continue
			}
			seenStepDeps[trimmedDep] = true

			// Existence check
			if !seenIDs[trimmedDep] {
				errs = append(errs, ValidationError{
					StepID:  step.ID,
					Field:   "depends_on",
					Message: fmt.Sprintf("step %q depends on unknown step %q", step.ID, trimmedDep),
				})
			}
		}
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}
