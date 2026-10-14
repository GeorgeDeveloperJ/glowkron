package dag

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// ErrCycleDetected is the sentinel error returned when circular dependencies exist.
var ErrCycleDetected = errors.New("cycle detected")

// CycleError conveys detailed cycle information, including the traversal path that closed the loop.
type CycleError struct {
	Path []string
}

// Error formats the exact cycle path as "cycle detected: a -> b -> c -> a".
func (e *CycleError) Error() string {
	return fmt.Sprintf("cycle detected: %s", strings.Join(e.Path, " -> "))
}

// Unwrap returns ErrCycleDetected for errors.Is inspection.
func (e *CycleError) Unwrap() error {
	return ErrCycleDetected
}

// Is returns true if target equals ErrCycleDetected.
func (e *CycleError) Is(target error) bool {
	return target == ErrCycleDetected
}

// Plan represents the topological execution stages of a workflow.
type Plan struct {
	// Stages is an ordered list of execution waves; steps within the same stage execute concurrently.
	Stages [][]string `json:"stages"`
	// Nodes provides the resolved *Node pointers grouped by execution stage.
	Nodes [][]*Node `json:"-"`
	// TotalSteps is the total number of steps across all stages.
	TotalSteps int `json:"total_steps"`
}

// StageCount returns the number of sequential stages in the plan.
func (p *Plan) StageCount() int {
	return len(p.Stages)
}

// Stage returns the slice of step IDs for the given stage index, or nil if index is out of bounds.
func (p *Plan) Stage(idx int) []string {
	if idx < 0 || idx >= len(p.Stages) {
		return nil
	}
	return p.Stages[idx]
}

// StageNodes returns the slice of *Node for the given stage index, or nil if index is out of bounds.
func (p *Plan) StageNodes(idx int) []*Node {
	if idx < 0 || idx >= len(p.Nodes) {
		return nil
	}
	return p.Nodes[idx]
}

// StepIDs returns all step IDs flattened in stage execution order.
func (p *Plan) StepIDs() []string {
	all := make([]string, 0, p.TotalSteps)
	for _, stage := range p.Stages {
		all = append(all, stage...)
	}
	return all
}

// IsEmpty returns true if the plan has no stages or steps.
func (p *Plan) IsEmpty() bool {
	return p.TotalSteps == 0
}

// findCycle runs a 3-color DFS to detect cycles and capture the full traversal chain.
// State markers:
//
//	0: unvisited
//	1: visiting (active on DFS call stack)
//	2: visited (fully explored)
func (g *Graph) findCycle() []string {
	state := make(map[string]int)
	var path []string
	var cyclePath []string

	nodeIDs := g.NodeIDs()

	var dfs func(id string) bool
	dfs = func(id string) bool {
		state[id] = 1 // visiting
		path = append(path, id)

		// Collect and sort dependent child IDs for deterministic traversal
		node := g.Node(id)
		var dependents []string
		if node != nil {
			dependents = node.DependentIDs()
		}

		for _, nextID := range dependents {
			if state[nextID] == 1 {
				// Cycle loop closed!
				cyclePath = make([]string, len(path)+1)
				copy(cyclePath, path)
				cyclePath[len(path)] = nextID
				return true
			}
			if state[nextID] == 0 {
				if dfs(nextID) {
					return true
				}
			}
		}

		// Backtrack
		path = path[:len(path)-1]
		state[id] = 2 // visited
		return false
	}

	for _, id := range nodeIDs {
		if state[id] == 0 {
			if dfs(id) {
				return cyclePath
			}
		}
	}
	return nil
}

// Resolve analyzes the Graph, verifies acyclicity, and constructs a staged parallel execution Plan using Kahn's algorithm.
func Resolve(g *Graph) (*Plan, error) {
	if g == nil {
		return nil, errors.New("cannot resolve nil graph")
	}

	// 1. Cycle detection: DFS with deterministic branch ordering
	if cycle := g.findCycle(); len(cycle) > 0 {
		return nil, &CycleError{Path: cycle}
	}

	totalNodes := g.NodeCount()
	if totalNodes == 0 {
		return &Plan{
			Stages:     make([][]string, 0),
			Nodes:      make([][]*Node, 0),
			TotalSteps: 0,
		}, nil
	}

	// 2. Kahn's algorithm: Initialize in-degrees for each node
	inDegree := make(map[string]int, totalNodes)
	for _, node := range g.Nodes() {
		inDegree[node.ID] = len(node.Dependencies)
	}

	// 3. Stage 0: Gather all root nodes with in-degree 0
	currentStage := make([]string, 0)
	for _, node := range g.Nodes() {
		if inDegree[node.ID] == 0 {
			currentStage = append(currentStage, node.ID)
		}
	}
	sort.Strings(currentStage)

	stages := make([][]string, 0)
	stageNodes := make([][]*Node, 0)
	processedCount := 0

	// 4. Wave-by-wave stage grouping
	for len(currentStage) > 0 {
		stages = append(stages, currentStage)

		// Populate Node pointers for current stage
		nodesInStage := make([]*Node, len(currentStage))
		for i, id := range currentStage {
			nodesInStage[i] = g.Node(id)
		}
		stageNodes = append(stageNodes, nodesInStage)
		processedCount += len(currentStage)

		// Determine next stage
		nextStageMap := make(map[string]bool)
		for _, id := range currentStage {
			node := g.Node(id)
			if node == nil {
				continue
			}
			for _, child := range node.Dependents {
				inDegree[child.ID]--
				if inDegree[child.ID] == 0 {
					nextStageMap[child.ID] = true
				}
			}
		}

		nextStage := make([]string, 0, len(nextStageMap))
		for id := range nextStageMap {
			nextStage = append(nextStage, id)
		}
		sort.Strings(nextStage)
		currentStage = nextStage
	}

	// 5. Sanity check: Ensure all nodes were accounted for
	if processedCount != totalNodes {
		return nil, fmt.Errorf("%w: unresolved dependencies remain (%d of %d steps scheduled)",
			ErrCycleDetected, processedCount, totalNodes)
	}

	return &Plan{
		Stages:     stages,
		Nodes:      stageNodes,
		TotalSteps: totalNodes,
	}, nil
}

// Resolve delegates directly to Resolve(g).
func (g *Graph) Resolve() (*Plan, error) {
	return Resolve(g)
}

// Validate verifies that the Graph has no cycles and is ready for execution.
func (g *Graph) Validate() error {
	_, err := g.Resolve()
	return err
}

// BuildAndResolve builds a graph from step configs and resolves its execution plan in a single operation.
func BuildAndResolve(steps []workflow.StepConfig) (*Plan, *Graph, error) {
	g, err := BuildGraph(steps)
	if err != nil {
		return nil, nil, err
	}
	plan, err := g.Resolve()
	if err != nil {
		return nil, g, err
	}
	return plan, g, nil
}
