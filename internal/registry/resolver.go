package registry

import (
	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// ResolveDependencies resolves module dependencies and returns modules in topological order.
//
// The returned slice contains modules ordered such that dependencies appear before
// modules that depend on them. This ensures modules can be started in the correct order.
//
// Returns an error if:
// - Any module has a missing dependency
// - A circular dependency is detected
//
// Example:
//
//	registry := NewModuleRegistry()
//	// ... register modules ...
//	ordered, err := ResolveDependencies(registry)
//	if err != nil {
//	    log.Fatal("Dependency resolution failed:", err)
//	}
//	// Start modules in dependency order
//	for _, module := range ordered {
//	    module.Start(ctx)
//	}
func ResolveDependencies(registry ModuleRegistry) ([]types.Module, error) {
	// Build dependency graph
	graph := buildDependencyGraph(registry)

	// Detect missing dependencies
	if err := detectMissingDependencies(graph, registry); err != nil {
		return nil, err
	}

	// Perform topological sort
	return topologicalSort(graph, registry)
}

// dependencyGraph represents the module dependency graph.
type dependencyGraph struct {
	// adjacencyList maps each module to the modules it depends on
	adjacencyList map[string][]string

	// inDegree counts how many modules each module depends on
	inDegree map[string]int

	// allModules contains all module names
	allModules map[string]bool
}

// buildDependencyGraph constructs the dependency graph from the registry.
func buildDependencyGraph(registry ModuleRegistry) *dependencyGraph {
	graph := &dependencyGraph{
		adjacencyList: make(map[string][]string),
		inDegree:      make(map[string]int),
		allModules:    make(map[string]bool),
	}

	modules := registry.All()

	// Initialize all modules in the graph
	for name := range modules {
		graph.allModules[name] = true
		graph.inDegree[name] = 0
		graph.adjacencyList[name] = []string{}
	}

	// Build adjacency list and calculate in-degrees
	for name, module := range modules {
		// Check if module implements DependentModule interface
		if depMod, ok := module.(types.DependentModule); ok {
			deps := depMod.Dependencies()
			graph.adjacencyList[name] = deps

			// Each dependency increases the in-degree of the dependent module
			graph.inDegree[name] = len(deps)
		}
	}

	return graph
}

// detectMissingDependencies checks if all dependencies exist in the registry.
func detectMissingDependencies(graph *dependencyGraph, registry ModuleRegistry) error {
	for module, deps := range graph.adjacencyList {
		for _, dep := range deps {
			if !graph.allModules[dep] {
				return monoerrors.WrapMissingDependency(module, dep)
			}
		}
	}
	return nil
}

// topologicalSort performs Kahn's algorithm to compute topological order.
func topologicalSort(graph *dependencyGraph, registry ModuleRegistry) ([]types.Module, error) {
	// Create a copy of in-degrees to avoid modifying the original
	inDegree := make(map[string]int)
	for name, count := range graph.inDegree {
		inDegree[name] = count
	}

	// Queue for modules with no dependencies
	queue := []string{}
	for name := range graph.allModules {
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}

	// Result list
	result := []types.Module{}
	modules := registry.All()

	// Process modules in dependency order
	for len(queue) > 0 {
		// Dequeue
		current := queue[0]
		queue = queue[1:]

		// Add to result
		result = append(result, modules[current])

		// Process all modules that depend on current
		for module, deps := range graph.adjacencyList {
			// Check if this module depends on current
			for _, dep := range deps {
				if dep == current {
					inDegree[module]--
					if inDegree[module] == 0 {
						queue = append(queue, module)
					}
					break
				}
			}
		}
	}

	// Check if all modules were processed
	if len(result) != len(graph.allModules) {
		// Circular dependency detected
		cycle := findCircularDependencyChain(graph, registry)
		return nil, monoerrors.WrapCircularDependency(cycle)
	}

	return result, nil
}

// findCircularDependencyChain uses DFS to find a circular dependency chain.
func findCircularDependencyChain(graph *dependencyGraph, registry ModuleRegistry) []string {
	// Track visit states: 0=unvisited, 1=visiting, 2=visited
	state := make(map[string]int)
	for name := range graph.allModules {
		state[name] = 0 // unvisited
	}

	// DFS stack to track current path
	var stack []string
	var cycle []string

	// DFS function
	var dfs func(string) bool
	dfs = func(module string) bool {
		if state[module] == 2 {
			// Already visited, no cycle here
			return false
		}

		if state[module] == 1 {
			// Found a cycle - module is in our current path
			// Find where the cycle starts
			for i, m := range stack {
				if m == module {
					cycle = append([]string{}, stack[i:]...)
					cycle = append(cycle, module) // Close the cycle
					return true
				}
			}
		}

		// Mark as visiting
		state[module] = 1
		stack = append(stack, module)

		// Visit all dependencies
		for _, dep := range graph.adjacencyList[module] {
			if dfs(dep) {
				return true
			}
		}

		// Mark as visited
		state[module] = 2
		stack = stack[:len(stack)-1]
		return false
	}

	// Try DFS from each unvisited module
	for module := range graph.allModules {
		if state[module] == 0 {
			if dfs(module) {
				return cycle
			}
		}
	}

	// If no cycle found (shouldn't happen if topological sort failed)
	return []string{"<cycle detection failed>"}
}
