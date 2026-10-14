package engine

import (
	"sync"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// StepState tracks the execution status, exit code, and captured outputs of a workflow step.
type StepState struct {
	StepID        string
	StepRunID     string
	Status        store.StepRunStatus
	ExitCode      *int
	ErrorMessage  string
	StdoutSnippet string
	StderrSnippet string
	Attempts      int
	StartTime     time.Time
	EndTime       *time.Time
}

// StateMachine coordinates and thread-safely tracks step states during a workflow execution.
type StateMachine struct {
	mu     sync.RWMutex
	graph  *dag.Graph
	states map[string]*StepState
}

// NewStateMachine initializes a StateMachine with all steps in PENDING status.
func NewStateMachine(steps []workflow.StepConfig, graph *dag.Graph) *StateMachine {
	sm := &StateMachine{
		graph:  graph,
		states: make(map[string]*StepState, len(steps)),
	}

	for _, step := range steps {
		sm.states[step.ID] = &StepState{
			StepID: step.ID,
			Status: store.StepRunPending,
		}
	}

	return sm
}

// SetStepRunID associates a persisted SQLite step_run ID with the in-memory step state.
func (sm *StateMachine) SetStepRunID(stepID, stepRunID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if state, ok := sm.states[stepID]; ok {
		state.StepRunID = stepRunID
	}
}

// GetState returns a snapshot copy of the step's current state.
func (sm *StateMachine) GetState(stepID string) (StepState, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	state, ok := sm.states[stepID]
	if !ok {
		return StepState{}, false
	}
	return *state, true
}

// SetRunning marks a step as RUNNING and increments its execution attempts.
func (sm *StateMachine) SetRunning(stepID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if state, ok := sm.states[stepID]; ok {
		state.Status = store.StepRunRunning
		state.Attempts++
		if state.StartTime.IsZero() {
			state.StartTime = time.Now().UTC()
		}
	}
}

// SetCompleted marks a step as COMPLETED with exit code 0 and captured snippets.
func (sm *StateMachine) SetCompleted(stepID string, exitCode int, stdoutSnippet, stderrSnippet string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if state, ok := sm.states[stepID]; ok {
		state.Status = store.StepRunCompleted
		state.ExitCode = &exitCode
		state.StdoutSnippet = stdoutSnippet
		state.StderrSnippet = stderrSnippet
		now := time.Now().UTC()
		state.EndTime = &now
	}
}

// SetFailed marks a step as FAILED with its exit code, error message, and captured snippets.
func (sm *StateMachine) SetFailed(stepID string, exitCode int, errMsg, stdoutSnippet, stderrSnippet string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if state, ok := sm.states[stepID]; ok {
		state.Status = store.StepRunFailed
		state.ExitCode = &exitCode
		state.ErrorMessage = errMsg
		state.StdoutSnippet = stdoutSnippet
		state.StderrSnippet = stderrSnippet
		now := time.Now().UTC()
		state.EndTime = &now
	}
}

// SetSkipped marks a step as SKIPPED due to prerequisite failure or cancellation.
func (sm *StateMachine) SetSkipped(stepID string, reason string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if state, ok := sm.states[stepID]; ok {
		state.Status = store.StepRunSkipped
		state.ErrorMessage = reason
		now := time.Now().UTC()
		state.EndTime = &now
	}
}

// CanRun returns true if all direct dependencies of stepID have COMPLETED.
func (sm *StateMachine) CanRun(stepID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.graph == nil {
		return true
	}

	deps, err := sm.graph.Dependencies(stepID)
	if err != nil {
		return false
	}

	for _, dep := range deps {
		depState, ok := sm.states[dep.ID]
		if !ok || depState.Status != store.StepRunCompleted {
			return false
		}
	}

	return true
}

// ShouldSkip returns true if any direct dependency of stepID has FAILED or SKIPPED.
func (sm *StateMachine) ShouldSkip(stepID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.graph == nil {
		return false
	}

	deps, err := sm.graph.Dependencies(stepID)
	if err != nil {
		return false
	}

	for _, dep := range deps {
		depState, ok := sm.states[dep.ID]
		if ok && (depState.Status == store.StepRunFailed || depState.Status == store.StepRunSkipped) {
			return true
		}
	}

	return false
}

// IsTerminal returns true if the step is in a terminal state (COMPLETED, FAILED, or SKIPPED).
func (sm *StateMachine) IsTerminal(stepID string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if state, ok := sm.states[stepID]; ok {
		return state.Status == store.StepRunCompleted ||
			state.Status == store.StepRunFailed ||
			state.Status == store.StepRunSkipped
	}
	return false
}

// AllCompleted returns true if every step in the workflow finished with status COMPLETED.
func (sm *StateMachine) AllCompleted() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, state := range sm.states {
		if state.Status != store.StepRunCompleted {
			return false
		}
	}
	return true
}

// HasFailures returns true if at least one step in the workflow finished with status FAILED.
func (sm *StateMachine) HasFailures() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, state := range sm.states {
		if state.Status == store.StepRunFailed {
			return true
		}
	}
	return false
}

// GetFailedSteps returns a list of step IDs that failed.
func (sm *StateMachine) GetFailedSteps() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var failed []string
	for id, state := range sm.states {
		if state.Status == store.StepRunFailed {
			failed = append(failed, id)
		}
	}
	return failed
}

// GetSkippedSteps returns a list of step IDs that were skipped.
func (sm *StateMachine) GetSkippedSteps() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var skipped []string
	for id, state := range sm.states {
		if state.Status == store.StepRunSkipped {
			skipped = append(skipped, id)
		}
	}
	return skipped
}

// Snapshot returns a point-in-time copy of all step states.
func (sm *StateMachine) Snapshot() map[string]StepState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	cp := make(map[string]StepState, len(sm.states))
	for k, v := range sm.states {
		cp[k] = *v
	}
	return cp
}
