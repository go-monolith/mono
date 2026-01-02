package registry

import (
	"context"
	"testing"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// Mock logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...any)       {}
func (m *mockLogger) Info(msg string, args ...any)        {}
func (m *mockLogger) Warn(msg string, args ...any)        {}
func (m *mockLogger) Error(msg string, args ...any)       {}
func (m *mockLogger) With(args ...any) types.Logger       { return m }
func (m *mockLogger) WithModule(name string) types.Logger { return m }
func (m *mockLogger) WithError(err error) types.Logger    { return m }

// Mock module implementations for testing

type mockModule struct {
	name string
}

func (m *mockModule) Name() string {
	return m.name
}

func (m *mockModule) Start(ctx context.Context) error {
	return nil
}

func (m *mockModule) Stop(ctx context.Context) error {
	return nil
}

type mockDependentModule struct {
	mockModule
	deps       []string
	containers map[string]types.ServiceContainer
}

func (m *mockDependentModule) Dependencies() []string {
	return m.deps
}

func (m *mockDependentModule) SetDependencyServiceContainer(dependency string, container types.ServiceContainer) {
	if m.containers == nil {
		m.containers = make(map[string]types.ServiceContainer)
	}
	m.containers[dependency] = container
}

// Test Registry basic operations

func TestNewModuleRegistry(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})
	if registry == nil {
		t.Fatal("NewModuleRegistry returned nil")
	}

	names := registry.List()
	if len(names) != 0 {
		t.Errorf("expected empty registry, got %d modules", len(names))
	}
}

func TestNewModuleRegistry_NilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when logger is nil")
		}
	}()

	NewModuleRegistry(nil)
}

func TestRegisterModule(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	module := &mockModule{name: "test-module"}
	err := registry.Register(module)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	names := registry.List()
	if len(names) != 1 {
		t.Fatalf("expected 1 module, got %d", len(names))
	}

	if names[0] != "test-module" {
		t.Errorf("expected 'test-module', got '%s'", names[0])
	}
}

func TestRegisterNilModule(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	err := registry.Register(nil)
	if err == nil {
		t.Fatal("expected error for nil module")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestRegisterDuplicateModule(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	module := &mockModule{name: "duplicate"}
	err := registry.Register(module)
	if err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	// Try to register again
	err = registry.Register(module)
	if err == nil {
		t.Fatal("expected error for duplicate module")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

func TestGetModule(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	module := &mockModule{name: "test"}
	_ = registry.Register(module)

	retrieved, err := registry.Get("test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name() != "test" {
		t.Errorf("expected 'test', got '%s'", retrieved.Name())
	}
}

func TestGetNonExistentModule(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	_, err := registry.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent module")
	}

	if !monoerrors.IsModuleError(err) {
		t.Errorf("expected ModuleError, got %T", err)
	}
}

// Test Dependency Resolution

func TestResolveDependenciesSimple(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A depends on nothing
	// B depends on A
	// Expected order: [A, B]

	moduleA := &mockModule{name: "A"}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"A"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 2 {
		t.Fatalf("expected 2 modules, got %d", len(ordered))
	}

	if ordered[0].Name() != "A" {
		t.Errorf("expected A first, got %s", ordered[0].Name())
	}

	if ordered[1].Name() != "B" {
		t.Errorf("expected B second, got %s", ordered[1].Name())
	}
}

func TestResolveDependenciesChain(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C
	// Expected order: [C, B, A]

	moduleC := &mockModule{name: "C"}
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

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 3 {
		t.Fatalf("expected 3 modules, got %d", len(ordered))
	}

	// Verify C comes before B, and B comes before A
	positions := make(map[string]int)
	for i, m := range ordered {
		positions[m.Name()] = i
	}

	if positions["C"] >= positions["B"] {
		t.Error("C should come before B")
	}

	if positions["B"] >= positions["A"] {
		t.Error("B should come before A")
	}
}

func TestResolveDependenciesMissing(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A depends on B, but B is not registered
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B"},
	}

	_ = registry.Register(moduleA)

	_, err := ResolveDependencies(registry)
	if err == nil {
		t.Fatal("expected error for missing dependency")
	}

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

func TestResolveDependenciesCircular(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// A -> B -> C -> A (circular)
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

	if !monoerrors.IsDependencyError(err) {
		t.Errorf("expected DependencyError, got %T", err)
	}
}

func TestResolveDependenciesDiamond(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	// Diamond dependency:
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D

	moduleD := &mockModule{name: "D"}
	moduleB := &mockDependentModule{
		mockModule: mockModule{name: "B"},
		deps:       []string{"D"},
	}
	moduleC := &mockDependentModule{
		mockModule: mockModule{name: "C"},
		deps:       []string{"D"},
	}
	moduleA := &mockDependentModule{
		mockModule: mockModule{name: "A"},
		deps:       []string{"B", "C"},
	}

	_ = registry.Register(moduleA)
	_ = registry.Register(moduleB)
	_ = registry.Register(moduleC)
	_ = registry.Register(moduleD)

	ordered, err := ResolveDependencies(registry)
	if err != nil {
		t.Fatalf("ResolveDependencies failed: %v", err)
	}

	if len(ordered) != 4 {
		t.Fatalf("expected 4 modules, got %d", len(ordered))
	}

	// Verify D comes first
	if ordered[0].Name() != "D" {
		t.Errorf("expected D first, got %s", ordered[0].Name())
	}

	// Verify A comes last
	if ordered[3].Name() != "A" {
		t.Errorf("expected A last, got %s", ordered[3].Name())
	}
}

func TestListPreservesOrder(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	names := []string{"Module1", "Module2", "Module3"}
	for _, name := range names {
		_ = registry.Register(&mockModule{name: name})
	}

	listed := registry.List()
	if len(listed) != len(names) {
		t.Fatalf("expected %d modules, got %d", len(names), len(listed))
	}

	for i, name := range names {
		if listed[i] != name {
			t.Errorf("position %d: expected %s, got %s", i, name, listed[i])
		}
	}
}

func TestAllReturnsAllModules(t *testing.T) {
	registry := NewModuleRegistry(&mockLogger{})

	modules := []*mockModule{
		{name: "A"},
		{name: "B"},
		{name: "C"},
	}

	for _, m := range modules {
		_ = registry.Register(m)
	}

	all := registry.All()
	if len(all) != len(modules) {
		t.Fatalf("expected %d modules, got %d", len(modules), len(all))
	}

	for _, m := range modules {
		if _, exists := all[m.Name()]; !exists {
			t.Errorf("module %s not found in All()", m.Name())
		}
	}
}
