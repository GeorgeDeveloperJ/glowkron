package dag_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// helperStep creates a minimal workflow.StepConfig for testing.
func helperStep(id string, deps ...string) workflow.StepConfig {
	return workflow.StepConfig{
		ID:        id,
		Name:      "Step " + id,
		Command:   "echo " + id,
		DependsOn: deps,
		Timeout:   30 * time.Second,
	}
}

// 1. Single Step Test (1 stage)
func TestSingleStep(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A"),
	}

	g, err := dag.BuildGraph(steps)
	if err != nil {
		t.Fatalf("BuildGraph failed: %v", err)
	}

	if g.NodeCount() != 1 {
		t.Fatalf("expected NodeCount 1, got %d", g.NodeCount())
	}

	nodeA := g.Node("A")
	if nodeA == nil || nodeA.ID != "A" {
		t.Fatalf("expected node A, got %v", nodeA)
	}

	deps, err := g.Dependencies("A")
	if err != nil || len(deps) != 0 {
		t.Fatalf("expected 0 dependencies for A, got %v (err: %v)", deps, err)
	}

	children, err := g.Dependents("A")
	if err != nil || len(children) != 0 {
		t.Fatalf("expected 0 dependents for A, got %v (err: %v)", children, err)
	}

	plan, err := dag.Resolve(g)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	if plan.TotalSteps != 1 {
		t.Errorf("expected TotalSteps 1, got %d", plan.TotalSteps)
	}
	if plan.StageCount() != 1 {
		t.Errorf("expected 1 stage, got %d", plan.StageCount())
	}
	expectedStages := [][]string{{"A"}}
	if !reflect.DeepEqual(plan.Stages, expectedStages) {
		t.Errorf("expected stages %v, got %v", expectedStages, plan.Stages)
	}
}

// 2. Linear Sequential Pipeline: A -> B -> C (3 stages)
func TestLinearSequentialPipeline(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A"),
		helperStep("B", "A"),
		helperStep("C", "B"),
	}

	plan, g, err := dag.BuildAndResolve(steps)
	if err != nil {
		t.Fatalf("BuildAndResolve failed: %v", err)
	}

	if plan.TotalSteps != 3 {
		t.Errorf("expected 3 total steps, got %d", plan.TotalSteps)
	}
	if plan.StageCount() != 3 {
		t.Fatalf("expected 3 stages, got %d", plan.StageCount())
	}

	expectedStages := [][]string{
		{"A"},
		{"B"},
		{"C"},
	}
	if !reflect.DeepEqual(plan.Stages, expectedStages) {
		t.Errorf("expected stages %v, got %v", expectedStages, plan.Stages)
	}

	// Inspect graph relationships
	depsB, err := g.Dependencies("B")
	if err != nil || len(depsB) != 1 || depsB[0].ID != "A" {
		t.Errorf("expected B's dependency to be A, got %v", depsB)
	}
	childrenB, err := g.Dependents("B")
	if err != nil || len(childrenB) != 1 || childrenB[0].ID != "C" {
		t.Errorf("expected B's dependent to be C, got %v", childrenB)
	}
}

// 3. Diamond DAG: A -> B, A -> C, B -> D, C -> D (3 stages: [A], [B, C], [D])
func TestDiamondDAG(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A"),
		helperStep("B", "A"),
		helperStep("C", "A"),
		helperStep("D", "B", "C"),
	}

	plan, g, err := dag.BuildAndResolve(steps)
	if err != nil {
		t.Fatalf("BuildAndResolve failed: %v", err)
	}

	if plan.TotalSteps != 4 {
		t.Errorf("expected 4 total steps, got %d", plan.TotalSteps)
	}
	if plan.StageCount() != 3 {
		t.Fatalf("expected 3 stages, got %d", plan.StageCount())
	}

	expectedStages := [][]string{
		{"A"},
		{"B", "C"},
		{"D"},
	}
	if !reflect.DeepEqual(plan.Stages, expectedStages) {
		t.Errorf("expected stages %v, got %v", expectedStages, plan.Stages)
	}

	// Verify Node pointers in plan
	stage1Nodes := plan.StageNodes(1)
	if len(stage1Nodes) != 2 || stage1Nodes[0].ID != "B" || stage1Nodes[1].ID != "C" {
		t.Errorf("expected Stage 1 nodes [B, C], got %v", stage1Nodes)
	}

	// Check D's dependencies
	depsD, err := g.Dependencies("D")
	if err != nil || len(depsD) != 2 {
		t.Fatalf("expected 2 dependencies for D, got %v", depsD)
	}
	if depsD[0].ID != "B" || depsD[1].ID != "C" {
		t.Errorf("expected D dependencies [B, C], got [%s, %s]", depsD[0].ID, depsD[1].ID)
	}
}

// 4. Disconnected Components (independent parallel subgraphs)
func TestDisconnectedComponents(t *testing.T) {
	steps := []workflow.StepConfig{
		// Subgraph 1: A -> B
		helperStep("A"),
		helperStep("B", "A"),
		// Subgraph 2: X -> Y
		helperStep("X"),
		helperStep("Y", "X"),
		// Subgraph 3: Z (isolated)
		helperStep("Z"),
	}

	plan, _, err := dag.BuildAndResolve(steps)
	if err != nil {
		t.Fatalf("BuildAndResolve failed: %v", err)
	}

	if plan.TotalSteps != 5 {
		t.Errorf("expected 5 total steps, got %d", plan.TotalSteps)
	}
	if plan.StageCount() != 2 {
		t.Fatalf("expected 2 stages, got %d", plan.StageCount())
	}

	expectedStages := [][]string{
		{"A", "X", "Z"},
		{"B", "Y"},
	}
	if !reflect.DeepEqual(plan.Stages, expectedStages) {
		t.Errorf("expected stages %v, got %v", expectedStages, plan.Stages)
	}
}

// 5. 2-Node Cycle: A -> B -> A
func TestTwoNodeCycle(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A", "B"),
		helperStep("B", "A"),
	}

	g, err := dag.BuildGraph(steps)
	if err != nil {
		t.Fatalf("expected BuildGraph to succeed with cyclic definition, got: %v", err)
	}

	plan, err := dag.Resolve(g)
	if err == nil {
		t.Fatalf("expected error from Resolve for 2-node cycle, got plan: %v", plan)
	}

	if !errors.Is(err, dag.ErrCycleDetected) {
		t.Fatalf("expected errors.Is(err, ErrCycleDetected), got: %v", err)
	}

	expectedErrMsg := "cycle detected: A -> B -> A"
	if err.Error() != expectedErrMsg {
		t.Errorf("expected error message %q, got %q", expectedErrMsg, err.Error())
	}
}

// 6. Multi-Node Cycle: A -> B -> C -> D -> B
func TestMultiNodeCycle(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A"),
		helperStep("B", "A", "D"),
		helperStep("C", "B"),
		helperStep("D", "C"),
	}

	g, err := dag.BuildGraph(steps)
	if err != nil {
		t.Fatalf("expected BuildGraph to succeed with cyclic definition, got: %v", err)
	}

	plan, err := dag.Resolve(g)
	if err == nil {
		t.Fatalf("expected cycle error, got plan: %v", plan)
	}

	if !errors.Is(err, dag.ErrCycleDetected) {
		t.Fatalf("expected errors.Is(err, ErrCycleDetected), got: %v", err)
	}

	expectedErrMsg := "cycle detected: A -> B -> C -> D -> B"
	if err.Error() != expectedErrMsg {
		t.Errorf("expected error message %q, got %q", expectedErrMsg, err.Error())
	}
}

// 7. Self-Dependency Cycle: A -> A
func TestSelfDependencyCycle(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A", "A"),
	}

	g, err := dag.BuildGraph(steps)
	if err != nil {
		t.Fatalf("expected BuildGraph to succeed with self-loop, got: %v", err)
	}

	plan, err := dag.Resolve(g)
	if err == nil {
		t.Fatalf("expected cycle error for self-dependency, got plan: %v", plan)
	}

	if !errors.Is(err, dag.ErrCycleDetected) {
		t.Fatalf("expected errors.Is(err, ErrCycleDetected), got: %v", err)
	}

	expectedErrMsg := "cycle detected: A -> A"
	if err.Error() != expectedErrMsg {
		t.Errorf("expected error message %q, got %q", expectedErrMsg, err.Error())
	}
}

// 8. Unknown Dependency Reference Handling
func TestUnknownDependencyReference(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("A", "non-existent-step"),
	}

	_, err := dag.BuildGraph(steps)
	if err == nil {
		t.Fatal("expected BuildGraph to return error for unknown dependency, got nil")
	}

	if !errors.Is(err, dag.ErrUnknownDependency) {
		t.Errorf("expected error to wrap ErrUnknownDependency, got: %v", err)
	}
}

// 9. Empty Graph Test
func TestEmptyGraph(t *testing.T) {
	plan, g, err := dag.BuildAndResolve([]workflow.StepConfig{})
	if err != nil {
		t.Fatalf("expected success on empty steps, got: %v", err)
	}

	if g.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", g.NodeCount())
	}
	if plan.TotalSteps != 0 || plan.StageCount() != 0 {
		t.Errorf("expected 0 total steps and 0 stages, got steps=%d stages=%d", plan.TotalSteps, plan.StageCount())
	}
	if !plan.IsEmpty() {
		t.Error("expected plan.IsEmpty() to be true")
	}
}

// 10. Duplicate Step ID
func TestDuplicateStepID(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("dup-id"),
		helperStep("dup-id"),
	}

	_, err := dag.BuildGraph(steps)
	if err == nil {
		t.Fatal("expected error on duplicate step ID, got nil")
	}

	if !errors.Is(err, dag.ErrDuplicateNode) {
		t.Errorf("expected error to wrap ErrDuplicateNode, got: %v", err)
	}
}

// 11. Graph Inspection Methods & Edge Lookups
func TestGraphInspectionMethods(t *testing.T) {
	steps := []workflow.StepConfig{
		helperStep("root1"),
		helperStep("root2"),
		helperStep("child", "root1", "root2"),
	}

	g, err := dag.BuildGraph(steps)
	if err != nil {
		t.Fatalf("BuildGraph failed: %v", err)
	}

	if !g.HasNode("child") {
		t.Error("expected HasNode('child') to be true")
	}
	if g.HasNode("ghost") {
		t.Error("expected HasNode('ghost') to be false")
	}

	nodes := g.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	// Verify deterministic sorting
	expectedIDs := []string{"child", "root1", "root2"}
	for i, id := range expectedIDs {
		if nodes[i].ID != id {
			t.Errorf("expected node at index %d to be %q, got %q", i, id, nodes[i].ID)
		}
	}

	roots := g.RootNodes()
	if len(roots) != 2 || roots[0].ID != "root1" || roots[1].ID != "root2" {
		t.Errorf("expected root nodes [root1, root2], got %v", roots)
	}

	// Non-existent node inspection
	if _, err := g.Dependencies("ghost"); !errors.Is(err, dag.ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for ghost dependencies, got: %v", err)
	}
	if _, err := g.Dependents("ghost"); !errors.Is(err, dag.ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for ghost dependents, got: %v", err)
	}
}

// 12. Complex Multi-Branch DAG
func TestComplexMultiBranchDAG(t *testing.T) {
	// Graph Topology:
	// R1, R2 (roots)
	// M1 depends on R1
	// M2 depends on R1, R2
	// M3 depends on R2
	// S1 depends on M1, M2, M3
	steps := []workflow.StepConfig{
		helperStep("R1"),
		helperStep("R2"),
		helperStep("M1", "R1"),
		helperStep("M2", "R1", "R2"),
		helperStep("M3", "R2"),
		helperStep("S1", "M1", "M2", "M3"),
	}

	plan, _, err := dag.BuildAndResolve(steps)
	if err != nil {
		t.Fatalf("BuildAndResolve failed: %v", err)
	}

	if plan.TotalSteps != 6 {
		t.Errorf("expected 6 total steps, got %d", plan.TotalSteps)
	}
	if plan.StageCount() != 3 {
		t.Fatalf("expected 3 stages, got %d", plan.StageCount())
	}

	expectedStages := [][]string{
		{"R1", "R2"},
		{"M1", "M2", "M3"},
		{"S1"},
	}
	if !reflect.DeepEqual(plan.Stages, expectedStages) {
		t.Errorf("expected stages %v, got %v", expectedStages, plan.Stages)
	}

	stepIDs := plan.StepIDs()
	expectedFlattened := []string{"R1", "R2", "M1", "M2", "M3", "S1"}
	if !reflect.DeepEqual(stepIDs, expectedFlattened) {
		t.Errorf("expected flattened IDs %v, got %v", expectedFlattened, stepIDs)
	}
}
