package dag

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// Sentinel errors for graph construction and query operations.
var (
	// ErrNodeNotFound indicates that the requested step ID does not exist in the graph.
	ErrNodeNotFound = errors.New("node not found")
	// ErrDuplicateNode indicates that a node with the specified ID already exists.
	ErrDuplicateNode = errors.New("duplicate node")
	// ErrUnknownDependency indicates that a step references a dependency not present in the graph.
	ErrUnknownDependency = errors.New("unknown dependency")
)

// Node represents a discrete execution task within a workflow graph.
type Node struct {
	// ID is the unique identifier of the workflow step.
	ID string
	// Step is the parsed workflow configuration for this execution step.
	Step workflow.StepConfig
	// Dependencies holds direct predecessor nodes that must finish before this node can run.
	Dependencies []*Node
	// Dependents holds direct successor nodes that depend on this node's completion.
	Dependents []*Node

	depMap   map[string]*Node
	childMap map[string]*Node
}

// InDegree returns the number of incoming dependency edges (prerequisites).
func (n *Node) InDegree() int {
	return len(n.Dependencies)
}

// OutDegree returns the number of outgoing dependent edges (children).
func (n *Node) OutDegree() int {
	return len(n.Dependents)
}

// DependencyIDs returns a sorted list of step IDs that this node depends on.
func (n *Node) DependencyIDs() []string {
	ids := make([]string, len(n.Dependencies))
	for i, d := range n.Dependencies {
		ids[i] = d.ID
	}
	sort.Strings(ids)
	return ids
}

// DependentIDs returns a sorted list of step IDs that depend on this node.
func (n *Node) DependentIDs() []string {
	ids := make([]string, len(n.Dependents))
	for i, d := range n.Dependents {
		ids[i] = d.ID
	}
	sort.Strings(ids)
	return ids
}

// Graph represents a directed dependency graph of workflow steps.
type Graph struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

// NewGraph initializes an empty Graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*Node),
	}
}

// AddNode adds a new workflow step as a node in the graph.
// Returns ErrDuplicateNode if a node with the same ID already exists.
func (g *Graph) AddNode(step workflow.StepConfig) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	id := strings.TrimSpace(step.ID)
	if id == "" {
		return errors.New("step ID cannot be empty")
	}

	if _, exists := g.nodes[id]; exists {
		return fmt.Errorf("%w: step ID %q already exists in graph", ErrDuplicateNode, id)
	}

	g.nodes[id] = &Node{
		ID:           id,
		Step:         step,
		Dependencies: make([]*Node, 0),
		Dependents:   make([]*Node, 0),
		depMap:       make(map[string]*Node),
		childMap:     make(map[string]*Node),
	}
	return nil
}

// AddDependency adds a directed dependency edge: stepID depends on dependsOnID.
// This implies dependsOnID must complete before stepID can execute.
func (g *Graph) AddDependency(stepID, dependsOnID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	stepNode, exists := g.nodes[stepID]
	if !exists {
		return fmt.Errorf("%w: step %q not found", ErrNodeNotFound, stepID)
	}

	depNode, exists := g.nodes[dependsOnID]
	if !exists {
		return fmt.Errorf("%w: dependency target %q not found for step %q", ErrUnknownDependency, dependsOnID, stepID)
	}

	// Wire dependency into stepNode if not already linked
	if _, present := stepNode.depMap[dependsOnID]; !present {
		stepNode.depMap[dependsOnID] = depNode
		stepNode.Dependencies = append(stepNode.Dependencies, depNode)
	}

	// Wire dependent into depNode if not already linked
	if _, present := depNode.childMap[stepID]; !present {
		depNode.childMap[stepID] = stepNode
		depNode.Dependents = append(depNode.Dependents, stepNode)
	}

	return nil
}

// Node returns the Node corresponding to stepID, or nil if not found.
func (g *Graph) Node(stepID string) *Node {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.nodes[stepID]
}

// HasNode returns true if a step with stepID exists in the graph.
func (g *Graph) HasNode(stepID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.nodes[stepID]
	return exists
}

// NodeCount returns the total number of nodes in the graph.
func (g *Graph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}

// Nodes returns all nodes in the graph sorted deterministically by ID.
func (g *Graph) Nodes() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	result := make([]*Node, len(ids))
	for i, id := range ids {
		result[i] = g.nodes[id]
	}
	return result
}

// NodeIDs returns all step IDs in the graph sorted alphabetically.
func (g *Graph) NodeIDs() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Dependencies returns the direct dependency nodes of stepID (nodes that must run first).
// Returns ErrNodeNotFound if stepID does not exist. Results are sorted by step ID.
func (g *Graph) Dependencies(stepID string) ([]*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, exists := g.nodes[stepID]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, stepID)
	}

	result := make([]*Node, len(node.Dependencies))
	copy(result, node.Dependencies)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

// Dependents returns the direct dependent nodes of stepID (nodes waiting for stepID).
// Returns ErrNodeNotFound if stepID does not exist. Results are sorted by step ID.
func (g *Graph) Dependents(stepID string) ([]*Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, exists := g.nodes[stepID]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrNodeNotFound, stepID)
	}

	result := make([]*Node, len(node.Dependents))
	copy(result, node.Dependents)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

// RootNodes returns all nodes with zero dependencies (runnable immediately).
func (g *Graph) RootNodes() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	roots := make([]*Node, 0)
	for _, n := range g.Nodes() {
		if len(n.Dependencies) == 0 {
			roots = append(roots, n)
		}
	}
	return roots
}

// BuildGraph constructs and wires a Graph from a slice of workflow StepConfig declarations.
// Validates uniqueness of step IDs and ensures all depends_on references exist.
func BuildGraph(steps []workflow.StepConfig) (*Graph, error) {
	g := NewGraph()

	// 1. First pass: Add all nodes and verify uniqueness
	for _, step := range steps {
		if err := g.AddNode(step); err != nil {
			return nil, err
		}
	}

	// 2. Second pass: Wire dependencies
	for _, step := range steps {
		for _, depID := range step.DependsOn {
			trimmedDep := strings.TrimSpace(depID)
			if err := g.AddDependency(step.ID, trimmedDep); err != nil {
				return nil, err
			}
		}
	}

	return g, nil
}
