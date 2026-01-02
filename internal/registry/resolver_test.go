package registry

import (
	"errors"
	"strings"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// TestResolveDependenciesEmptyRegistry tests empty registry handling
func TestResolveDependenciesEmptyRegistry(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed on empty registry: %v", err)
	}

	if len(ordered) != 0 {
		t.Errorf("expected 0 modules, got %d", len(ordered))
	}
}

// TestResolveDependenciesNoDependencies tests modules without dependencies
func TestResolveDependenciesNoDependencies(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// Register multiple modules with no dependencies
	_ = registry.Register(&mockModule{name: "A"})
	_ = registry.Register(&mockModule{name: "B"})
	_ = registry.Register(&mockModule{name: "C"})

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 3 {
		t.Fatalf("expected 3 modules, got %d", len(ordered))
	}

	// All modules should appear (order doesn't matter when no dependencies)
	names := make(map[string]bool)
	for _, m := range ordered {
		names[m.Name()] = true
	}

	for _, expected := range []string{"A", "B", "C"} {
		if !names[expected] {
			t.Errorf("module %s not found in result", expected)
		}
	}
}

// TestResolveDependenciesMultipleDependencies tests module with multiple dependencies
func TestResolveDependenciesMultipleDependencies(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// D depends on both B and C
	// B and C have no dependencies
	moduleB := &mockModule{name: "B"}
	moduleC := &mockModule{name: "C"}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"B", "C"},
	}

	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 3 {
		t.Fatalf("expected 3 modules, got %d", len(ordered))
	}

	// Verify D comes after both B and C
	positions := make(map[string]int)
	for i, m := range ordered {
		positions[m.Name()] = i
	}

	if positions["D"] <= positions["B"] {
		t.Error("D should come after B")
	}

	if positions["D"] <= positions["C"] {
		t.Error("D should come after C")
	}
}

// TestResolveDependenciesComplexGraph tests a complex dependency graph
func TestResolveDependenciesComplexGraph(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// Complex graph:
	//       E
	//      / \
	//     D   C
	//      \ / \
	//       B   A
	//
	// E depends on: D, C
	// D depends on: B
	// C depends on: B, A
	// B depends on: nothing
	// A depends on: nothing

	moduleA := &mockModule{name: "A"}
	moduleB := &mockModule{name: "B"}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"B", "A"},
	}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"B"},
	}
	moduleE := &mockDependentModule{
		mockModule: mockModule{name: "E"},
		deps:       []string{"D", "C"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)
	_ = registry.Register(moduleE)

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 5 {
		t.Fatalf("expected 5 modules, got %d", len(ordered))
	}

	// Build positions map
	positions := make(map[string]int)
	for i, m := range ordered {
		positions[m.Name()] = i
	}

	// Verify dependency ordering
	if positions["B"] >= positions["D"] {
		t.Error("B should come before D")
	}
	if positions["B"] >= positions["C"] {
		t.Error("B should come before C")
	}
	if positions["A"] >= positions["C"] {
		t.Error("A should come before C")
	}
	if positions["D"] >= positions["E"] {
		t.Error("D should come before E")
	}
	if positions["C"] >= positions["E"] {
		t.Error("C should come before E")
	}
}

// TestResolveDependenciesSelfReference tests self-referencing module
func TestResolveDependenciesSelfReference(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A depends on itself
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for self-referencing module")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesSimpleCircular tests simple circular dependency
func TestResolveDependenciesSimpleCircular(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> A (simple cycle)
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for circular dependency")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesMultipleMissing tests multiple missing dependencies
func TestResolveDependenciesMultipleMissing(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A depends on B, C, D (all missing)
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B", "C", "D"},
	}

	_ = registry.Register(moduleA)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for missing dependencies")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesPartialMissing tests partially missing dependencies
func TestResolveDependenciesPartialMissing(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A depends on B and C, but only B is registered
	moduleB := &mockModule{name: "B"}
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B", "C"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for missing dependency C")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesLongChain tests a long dependency chain
func TestResolveDependenciesLongChain(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> D -> E -> F (long chain)
	moduleF := &mockModule{name: "F"}
	moduleE := &mockDependentModule{
		mockModule: mockModule{name: "E"},
		deps:       []string{"F"},
	}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"E"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"D"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)
	_ = registry.Register(moduleE)
	_ = registry.Register(moduleF)

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 6 {
		t.Fatalf("expected 6 modules, got %d", len(ordered))
	}

	// Verify order: F, E, D, C, B, A
	expected := []string{"F", "E", "D", "C", "B", "A"}
	for i, m := range ordered {
		if m.Name() != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], m.Name())
		}
	}
}

// TestBuildDependencyGraph tests the dependency graph construction
func TestBuildDependencyGraph(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	moduleA := &mockModule{name: "A"}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)

	graph := buildDependencyGraph(registry)

	// Verify graph structure
	if len(graph.allModules) != 2 {
		t.Errorf("expected 2 modules in graph, got %d", len(graph.allModules))
	}

	if !graph.allModules["A"] {
		t.Error("module A not in graph")
	}

	if !graph.allModules["B"] {
		t.Error("module B not in graph")
	}

	// Verify adjacency list
	if len(graph.adjacencyList["A"]) != 0 {
		t.Errorf("A should have no dependencies, got %d", len(graph.adjacencyList["A"]))
	}

	if len(graph.adjacencyList["B"]) != 1 {
		t.Errorf("B should have 1 dependency, got %d", len(graph.adjacencyList["B"]))
	}

	// Verify in-degree
	if graph.inDegree["A"] != 0 {
		t.Errorf("A should have in-degree 0, got %d", graph.inDegree["A"])
	}

	if graph.inDegree["B"] != 1 {
		t.Errorf("B should have in-degree 1, got %d", graph.inDegree["B"])
	}
}

// TestDetectMissingDependencies tests missing dependency detection
func TestDetectMissingDependencies(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"}, // B doesn't exist
	}

	_ = registry.Register(moduleA)

	graph := buildDependencyGraph(registry)
	err := detectMissingDependencies(graph, registry)

	if err == nil {
		t.Fatal("expected error for missing dependency")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestFindCircularDependencyChain tests circular dependency detection
func TestFindCircularDependencyChain(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> A
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)

	graph := buildDependencyGraph(registry)
	cycle := findCircularDependencyChain(graph, registry)

	if len(cycle) == 0 {
		t.Fatal("expected to find circular dependency chain")
	}

	// Cycle should contain A, B, C
	foundModules := make(map[string]bool)
	for _, name := range cycle {
		foundModules[name] = true
	}

	// Should find at least the cycle participants
	if !foundModules["A"] && !foundModules["B"] && !foundModules["C"] {
		t.Errorf("expected cycle to contain A, B, or C, got %v", cycle)
	}
}

// TestResolveDependenciesLongerCircular tests longer circular dependency (4 nodes)
func TestResolveDependenciesLongerCircular(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> D -> A (4-node cycle)
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"D"},
	}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for 4-node circular dependency")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}

	// Verify the error message mentions circular dependency
	if !strings.Contains(err.Error(), "circular") {
		t.Errorf("expected error message to contain 'circular', got: %s", err.Error())
	}
}

// TestResolveDependenciesCycleWithNonCyclicModules tests cycle detection with
// additional non-cyclic modules in the registry
func TestResolveDependenciesCycleWithNonCyclicModules(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// Non-cyclic modules
	moduleX := &mockModule{name: "X"}
	moduleY := &mockDependentModule{
		mockModule: mockModule{name: "Y"},
		deps:       []string{"X"},
	}

	// Cyclic modules: A -> B -> C -> A
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"A"},
	}

	// Register all modules
	_ = registry.Register(moduleX)
	_ = registry.Register(moduleY)
	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for circular dependency even with non-cyclic modules present")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesCycleAtEndOfChain tests a cycle that exists at the end
// of a dependency chain: A -> B -> C -> D -> E -> C (E points back to C)
func TestResolveDependenciesCycleAtEndOfChain(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> D -> E -> C
	// The cycle is C -> D -> E -> C, with A and B as non-cyclic prefix
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"D"},
	}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"E"},
	}
	moduleE := &mockDependentModule{
		mockModule: mockModule{name: "E"},
		deps:       []string{"C"}, // Points back to C, creating a cycle
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)
	_ = registry.Register(moduleE)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for cycle at end of chain")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestResolveDependenciesMultipleSeparateCycles tests detection when there are
// multiple independent cycles in the dependency graph
func TestResolveDependenciesMultipleSeparateCycles(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// Cycle 1: A -> B -> A
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"A"},
	}

	// Cycle 2: X -> Y -> Z -> X
	moduleX := &mockDependentModule{
		mockModule: mockModule{name: "X"},
		deps:       []string{"Y"},
	}
	moduleY := &mockDependentModule{
		mockModule: mockModule{name: "Y"},
		deps:       []string{"Z"},
	}
	moduleZ := &mockDependentModule{
		mockModule: mockModule{name: "Z"},
		deps:       []string{"X"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleX)
	_ = registry.Register(moduleY)
	_ = registry.Register(moduleZ)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for multiple separate cycles")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

// TestCircularDependencyErrorChainContainsAllCycleMembers verifies that the
// error chain includes all modules that are part of the cycle
func TestCircularDependencyErrorChainContainsAllCycleMembers(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> A
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for circular dependency")
	}

	errMsg := err.Error()

	// The error message should mention all cycle participants
	// At minimum, it should contain references to the cycle
	if !strings.Contains(errMsg, "circular") {
		t.Errorf("expected error to mention 'circular', got: %s", errMsg)
	}

	// Verify the dependency chain is present in the error
	var depErr *monoerrors.DependencyError
	if errors.As(err, &depErr) {
		if len(depErr.Chain) == 0 {
			t.Error("expected dependency chain to be non-empty")
		}
		// Chain should contain at least some of the cycle members
		chainStr := strings.Join(depErr.Chain, ", ")
		hasA := strings.Contains(chainStr, "A")
		hasB := strings.Contains(chainStr, "B")
		hasC := strings.Contains(chainStr, "C")

		// At least one cycle member should be in the chain
		if !hasA && !hasB && !hasC {
			t.Errorf("expected chain to contain A, B, or C, got: %v", depErr.Chain)
		}
	}
}

// TestFindCircularDependencyChainLongerCycle tests the cycle finder with 4+ nodes
func TestFindCircularDependencyChainLongerCycle(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> D -> A (4-node cycle)
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"C"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"D"},
	}
	moduleD := &mockDependentModule{
		mockModule: mockModule{name: "D"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)

	graph := buildDependencyGraph(registry)
	cycle := findCircularDependencyChain(graph, registry)

	if len(cycle) == 0 {
		t.Fatal("expected to find circular dependency chain for 4-node cycle")
	}

	// Verify cycle contains the expected modules
	foundModules := make(map[string]bool)
	for _, name := range cycle {
		foundModules[name] = true
	}

	cycleCount := 0
	for _, name := range []string{"A", "B", "C", "D"} {
		if foundModules[name] {
			cycleCount++
		}
	}

	if cycleCount < 2 {
		t.Errorf("expected cycle to contain at least 2 of A, B, C, D, got: %v", cycle)
	}
}
