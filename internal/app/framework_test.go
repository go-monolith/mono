package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/go-monolith/mono/internal/nats"
	"github.com/go-monolith/mono/pkg/types"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// mockModule implements types.Module for testing
type mockModule struct {
	name         string
	startCalled  bool
	stopCalled   bool
	startErr     error
	stopErr      error
	dependencies []string
	mu           sync.Mutex
}

func (m *mockModule) Name() string {
	return m.name
}
func (m *mockModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	return m.startErr
}
func (m *mockModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	return m.stopErr
}
func (m *mockModule) Dependencies() []string {
	return m.dependencies
}

// SetDependencyServiceContainer implements types.DependentModule interface.
// This is needed to satisfy the full interface for dependency resolution to work.
func (m *mockModule) SetDependencyServiceContainer(_ string, _ types.ServiceContainer) {
	// No-op for tests - we only need the dependencies field for ordering
}
func (m *mockModule) wasStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalled
}
func (m *mockModule) wasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

// mockHealthCheckableModule implements types.HealthCheckableModule for testing
type mockHealthCheckableModule struct {
	mockModule
	healthStatus types.HealthStatus
}

func (m *mockHealthCheckableModule) Health(_ context.Context) types.HealthStatus {
	return m.healthStatus
}

// mockPanicHealthModule implements HealthCheckableModule and panics for testing
type mockPanicHealthModule struct {
	mockModule
}

func (m *mockPanicHealthModule) Health(_ context.Context) types.HealthStatus {
	panic("intentional panic for testing")
}

// mockLogger implements types.Logger
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...any)       {}
func (m *mockLogger) Info(msg string, args ...any)        {}
func (m *mockLogger) Warn(msg string, args ...any)        {}
func (m *mockLogger) Error(msg string, args ...any)       {}
func (m *mockLogger) With(args ...any) types.Logger       { return m }
func (m *mockLogger) WithModule(name string) types.Logger { return m }
func (m *mockLogger) WithError(err error) types.Logger    { return m }

// newTestFramework creates a framework instance configured for testing with in-process NATS.
// This avoids the NATS startup timeout issues in test environments by using DontListen and InProcessConn.
func newTestFramework(t *testing.T) types.MonoFramework {
	t.Helper()
	logger := &mockLogger{}
	// Use in-process NATS mode to avoid TCP binding and timeouts in test environment
	fw, err := NewFrameworkAppInstance(
		logger,
		0,                        // No queue group optimistic window
		nats.WithDontListen(),    // Don't bind TCP port
		nats.WithInProcessConn(), // Use in-process connections only
	)
	if err != nil {
		t.Fatalf("Failed to create test framework: %v", err)
	}
	return fw
}
func TestFramework_RegisterBeforeStart(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	modules := fw.Modules()
	if len(modules) != 1 {
		t.Errorf("Expected 1 module, got %d", len(modules))
	}
	if modules[0] != "test-module" {
		t.Errorf("Expected module 'test-module', got '%s'", modules[0])
	}
}
func TestFramework_RegisterAfterStart_ShouldFail(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	module := &mockModule{name: "test-module"}
	err := fw.Register(module)
	if err == nil {
		t.Fatal("Expected error when registering after start, got nil")
	}
}
func TestFramework_FullLifecycle(t *testing.T) {
	fw := newTestFramework(t)
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	if !module.wasStarted() {
		t.Error("Module was not started")
	}
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}
	if !module.wasStopped() {
		t.Error("Module was not stopped")
	}
}
func TestFramework_MultipleModulesWithDependencies(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &mockModule{name: "module-c", dependencies: []string{"module-b"}}
	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := fw.Register(moduleC); err != nil {
		t.Fatalf("Failed to register module C: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// All modules should be started
	if !moduleA.wasStarted() {
		t.Error("Module A was not started")
	}
	if !moduleB.wasStarted() {
		t.Error("Module B was not started")
	}
	if !moduleC.wasStarted() {
		t.Error("Module C was not started")
	}
}
func TestFramework_ModuleStartFailure(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	moduleA := &mockModule{name: "module-a"}
	// module-b depends on module-a to ensure deterministic start order.
	// Without this dependency, start order is non-deterministic (Go map iteration),
	// causing test flakiness as module-a might not be started before module-b fails.
	moduleB := &mockModule{name: "module-b", dependencies: []string{"module-a"}, startErr: errors.New("start failed")}
	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	ctx := context.Background()
	err := fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when module start fails")
	}
	// Module A should have been rolled back
	if !moduleA.wasStarted() {
		t.Error("Module A was not started")
	}
	if !moduleA.wasStopped() {
		t.Error("Module A was not stopped during rollback")
	}
}
func TestFramework_DuplicateRegistration(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module1 := &mockModule{name: "test-module"}
	module2 := &mockModule{name: "test-module"}
	if err := fw.Register(module1); err != nil {
		t.Fatalf("Failed to register first module: %v", err)
	}
	err := fw.Register(module2)
	if err == nil {
		t.Fatal("Expected error when registering duplicate module")
	}
}
func TestFramework_EmptyModuleName(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module := &mockModule{name: ""}
	err := fw.Register(module)
	if err == nil {
		t.Fatal("Expected error when registering module with empty name")
	}
}
func TestFramework_Health(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	// Health before start should be unhealthy
	health := fw.Health(ctx)
	if health.Healthy {
		t.Error("Application should be unhealthy before start")
	}
	// Start framework
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// Health after start should be healthy
	health = fw.Health(ctx)
	if !health.Healthy {
		t.Errorf("Application should be healthy after start: %s", health.Message)
	}
	if !health.NATSHealthy {
		t.Error("NATS should be healthy")
	}
	if health.State != types.StateRunning {
		t.Errorf("Expected state Running, got %d", health.State)
	}
}
func TestFramework_Health_WithHealthCheckableModule_Healthy(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module := &mockHealthCheckableModule{
		mockModule: mockModule{name: "health-module"},
		healthStatus: types.HealthStatus{
			Healthy: true,
			Message: "operational",
			Details: map[string]any{"connections": 5},
		},
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	health := fw.Health(ctx)
	// Framework should be healthy
	if !health.Healthy {
		t.Errorf("Application should be healthy: %s", health.Message)
	}
	// Module should be in the map
	moduleHealth, ok := health.Modules["health-module"]
	if !ok {
		t.Fatal("Module health-module not found in Modules map")
	}
	if !moduleHealth.SupportsHealth {
		t.Error("Module should support health checking")
	}
	if !moduleHealth.Healthy {
		t.Error("Module should be healthy")
	}
	if moduleHealth.Message != "operational" {
		t.Errorf("Expected message 'operational', got '%s'", moduleHealth.Message)
	}
	if moduleHealth.Details["connections"] != 5 {
		t.Errorf("Expected connections=5, got %v", moduleHealth.Details["connections"])
	}
}
func TestFramework_Health_WithHealthCheckableModule_Unhealthy(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	module := &mockHealthCheckableModule{
		mockModule: mockModule{name: "unhealthy-module"},
		healthStatus: types.HealthStatus{
			Healthy: false,
			Message: "database connection lost",
			Details: map[string]any{"error": "timeout"},
		},
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	health := fw.Health(ctx)
	// Framework should be unhealthy
	if health.Healthy {
		t.Error("Application should be unhealthy when a module is unhealthy")
	}
	// Message should indicate unhealthy module
	if health.Message == "" {
		t.Error("Application should have a message indicating unhealthy modules")
	}
	// Module should be in the map
	moduleHealth, ok := health.Modules["unhealthy-module"]
	if !ok {
		t.Fatal("Module unhealthy-module not found in Modules map")
	}
	if !moduleHealth.SupportsHealth {
		t.Error("Module should support health checking")
	}
	if moduleHealth.Healthy {
		t.Error("Module should be unhealthy")
	}
}
func TestFramework_Health_WithNonHealthCheckableModule(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Regular module without HealthCheckableModule interface
	module := &mockModule{name: "basic-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	health := fw.Health(ctx)
	// Framework should be healthy
	if !health.Healthy {
		t.Errorf("Application should be healthy: %s", health.Message)
	}
	// Module should be in the map with SupportsHealth=false
	moduleHealth, ok := health.Modules["basic-module"]
	if !ok {
		t.Fatal("Module basic-module not found in Modules map")
	}
	if moduleHealth.SupportsHealth {
		t.Error("Module should NOT support health checking")
	}
	// Healthy should be true for non-health modules
	if !moduleHealth.Healthy {
		t.Error("Non-health modules should default to healthy=true")
	}
}
func TestFramework_Health_MixedModules(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Module 1: Basic module without health checking
	basicModule := &mockModule{name: "basic-module"}
	// Module 2: Health-aware module that is healthy
	healthyModule := &mockHealthCheckableModule{
		mockModule: mockModule{name: "healthy-module"},
		healthStatus: types.HealthStatus{
			Healthy: true,
			Message: "ok",
		},
	}
	if err := fw.Register(basicModule); err != nil {
		t.Fatalf("Failed to register basic module: %v", err)
	}
	if err := fw.Register(healthyModule); err != nil {
		t.Fatalf("Failed to register healthy module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	health := fw.Health(ctx)
	// Framework should be healthy
	if !health.Healthy {
		t.Errorf("Application should be healthy: %s", health.Message)
	}
	// Should have 2 modules in the map
	if len(health.Modules) != 2 {
		t.Errorf("Expected 2 modules in map, got %d", len(health.Modules))
	}
	// Check basic module
	basicHealth := health.Modules["basic-module"]
	if basicHealth.SupportsHealth {
		t.Error("basic-module should not support health")
	}
	// Check healthy module
	healthyHealth := health.Modules["healthy-module"]
	if !healthyHealth.SupportsHealth {
		t.Error("healthy-module should support health")
	}
	if !healthyHealth.Healthy {
		t.Error("healthy-module should be healthy")
	}
}
func TestFramework_Health_OneUnhealthyAmongMultiple(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Module 1: Healthy
	healthyModule := &mockHealthCheckableModule{
		mockModule:   mockModule{name: "healthy-module"},
		healthStatus: types.HealthStatus{Healthy: true, Message: "ok"},
	}
	// Module 2: Unhealthy
	unhealthyModule := &mockHealthCheckableModule{
		mockModule:   mockModule{name: "unhealthy-module"},
		healthStatus: types.HealthStatus{Healthy: false, Message: "failed"},
	}
	// Module 3: Basic (no health)
	basicModule := &mockModule{name: "basic-module"}
	if err := fw.Register(healthyModule); err != nil {
		t.Fatalf("Failed to register healthy module: %v", err)
	}
	if err := fw.Register(unhealthyModule); err != nil {
		t.Fatalf("Failed to register unhealthy module: %v", err)
	}
	if err := fw.Register(basicModule); err != nil {
		t.Fatalf("Failed to register basic module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	health := fw.Health(ctx)
	// Framework should be unhealthy because one module is unhealthy
	if health.Healthy {
		t.Error("Application should be unhealthy when ANY module is unhealthy")
	}
	// Should have 3 modules
	if len(health.Modules) != 3 {
		t.Errorf("Expected 3 modules in map, got %d", len(health.Modules))
	}
	// Message should contain the unhealthy module name
	if health.Message == "" {
		t.Error("Application message should indicate which module is unhealthy")
	}
}
func TestFramework_Health_ModulePanics(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	panicModule := &mockPanicHealthModule{
		mockModule: mockModule{name: "panic-module"},
	}
	if err := fw.Register(panicModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// This should not panic - the panic should be recovered
	health := fw.Health(ctx)
	// Framework should be unhealthy
	if health.Healthy {
		t.Error("Application should be unhealthy when module panics")
	}
	// Module should be marked unhealthy
	moduleHealth, ok := health.Modules["panic-module"]
	if !ok {
		t.Fatal("Panic module not found in Modules map")
	}
	if moduleHealth.Healthy {
		t.Error("Panicking module should be marked unhealthy")
	}
	if moduleHealth.Message == "" {
		t.Error("Panicking module should have error message")
	}
	// Message should indicate panic
	if moduleHealth.Message == "" || len(moduleHealth.Message) == 0 {
		t.Error("Expected panic message in module health")
	}
}
func TestFramework_EventBus(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	eventBus := fw.EventBus("test-module")
	if eventBus == nil {
		t.Fatal("EventBus should not be nil")
	}
}
func TestFramework_DoubleStart(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	err := fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when starting already running application")
	}
}
func TestFramework_DoubleStop(t *testing.T) {
	fw := newTestFramework(t)
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}
	err := fw.Stop(ctx)
	if err == nil {
		t.Fatal("Expected error when stopping already stopped application")
	}
}

// mockPluginModule implements types.PluginModule for testing
type mockPluginModule struct {
	mockModule
	container types.ServiceContainer
}

func (m *mockPluginModule) SetContainer(container types.ServiceContainer) {
	m.container = container
}
func (m *mockPluginModule) Container() types.ServiceContainer {
	return m.container
}

// Tests for RegisterPlugin and Plugin methods
func TestFramework_RegisterPlugin(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	plugin := &mockPluginModule{
		mockModule: mockModule{name: "test-plugin"},
	}
	// Register plugin with alias
	if err := fw.RegisterPlugin(plugin, "my-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}
	// Retrieve plugin by alias
	retrievedPlugin := fw.Plugin("my-plugin")
	if retrievedPlugin == nil {
		t.Fatal("Plugin should not be nil")
	}
	if retrievedPlugin != plugin {
		t.Error("Retrieved plugin should be the same as registered plugin")
	}
}
func TestFramework_RegisterPlugin_DuplicateAlias(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	plugin1 := &mockPluginModule{mockModule: mockModule{name: "plugin-1"}}
	plugin2 := &mockPluginModule{mockModule: mockModule{name: "plugin-2"}}
	// Register first plugin
	if err := fw.RegisterPlugin(plugin1, "shared-alias"); err != nil {
		t.Fatalf("Failed to register first plugin: %v", err)
	}
	// Try to register second plugin with same alias
	err := fw.RegisterPlugin(plugin2, "shared-alias")
	if err == nil {
		t.Fatal("Expected error when registering plugin with duplicate alias")
	}
}
func TestFramework_Plugin_NonExistent(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Try to retrieve non-existent plugin
	plugin := fw.Plugin("non-existent")
	if plugin != nil {
		t.Error("Non-existent plugin should return nil")
	}
}

// Tests for Services method
func TestFramework_Services(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Register a module
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	// Start framework to initialize containers
	ctx := context.Background()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// Get services for the module
	services := fw.Services("test-module")
	if services == nil {
		t.Fatal("Services should not be nil for registered module")
	}
}
func TestFramework_Services_NonExistentModule(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Try to get services for non-existent module
	services := fw.Services("non-existent")
	if services != nil {
		t.Error("Services should return nil for non-existent module")
	}
}

// Tests for Logger method
func TestFramework_Logger(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Get logger
	logger := fw.Logger()
	if logger == nil {
		t.Fatal("Logger should not be nil")
	}
	// Verify logger is the same as the one we passed in newTestFramework
	// (mockLogger should be returned)
	if _, ok := logger.(*mockLogger); !ok {
		t.Error("Logger should be of type *mockLogger")
	}
}

// Additional tests for better coverage
func TestFramework_RegisterPlugin_AfterStart(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Start framework first
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// Try to register plugin after start
	plugin := &mockPluginModule{mockModule: mockModule{name: "late-plugin"}}
	err := fw.RegisterPlugin(plugin, "late-alias")
	if err == nil {
		t.Fatal("Expected error when registering plugin after start")
	}
}
func TestFramework_Stop_WithoutStart(t *testing.T) {
	fw := newTestFramework(t)
	// Stop without starting
	if err := fw.Stop(context.Background()); err != nil {
		t.Fatalf("Stop should succeed even without Start: %v", err)
	}
}
func TestFramework_Services_BeforeStart(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Register a module
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	// Try to get services before starting - should return nil (not running yet)
	services := fw.Services("test-module")
	if services != nil {
		t.Error("Services should return nil before Start (framework not running)")
	}
}
func TestFramework_Services_EmptyModuleName(t *testing.T) {
	fw := newTestFramework(t)
	defer func() {
		if err := fw.Stop(context.Background()); err != nil {
			t.Errorf("Failed to stop framework: %v", err)
		}
	}()
	// Start framework
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	// Try to get services with empty module name
	services := fw.Services("")
	if services != nil {
		t.Error("Services should return nil for empty module name")
	}
}

// mockFlushableLogger implements types.Logger with Flush() support
type mockFlushableLogger struct {
	mockLogger
	flushErr    error
	flushCalled bool
	mu          sync.Mutex
}

func (m *mockFlushableLogger) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalled = true
	return m.flushErr
}

func (m *mockFlushableLogger) wasFlushCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushCalled
}

// TestFlushLogger_NilLogger tests flushLogger() with nil logger
func TestFlushLogger_NilLogger(t *testing.T) {
	// Should not panic with nil logger
	flushLogger(nil)
}

// TestFlushLogger_NoFlushSupport tests flushLogger() with logger that doesn't implement Flush()
func TestFlushLogger_NoFlushSupport(t *testing.T) {
	logger := &mockLogger{}
	// Should not panic or error when logger doesn't implement Flush()
	flushLogger(logger)
}

// TestFlushLogger_FlushSuccess tests flushLogger() with successful flush
func TestFlushLogger_FlushSuccess(t *testing.T) {
	logger := &mockFlushableLogger{flushErr: nil}
	flushLogger(logger)

	if !logger.wasFlushCalled() {
		t.Error("Flush() should have been called")
	}
}

// TestFlushLogger_FlushFailure tests flushLogger() when Flush() returns error
func TestFlushLogger_FlushFailure(t *testing.T) {
	logger := &mockFlushableLogger{flushErr: errors.New("flush failed")}
	// Should not panic even when Flush() fails - error is written to stderr
	flushLogger(logger)

	if !logger.wasFlushCalled() {
		t.Error("Flush() should have been called even if it fails")
	}
}

// TestFinalizeShutdown_NilLogger tests finalizeShutdown() with nil logger
func TestFinalizeShutdown_NilLogger(t *testing.T) {
	// Should not panic with nil logger
	finalizeShutdown(nil)
}

// TestFinalizeShutdown_ValidLogger tests finalizeShutdown() with valid logger
func TestFinalizeShutdown_ValidLogger(t *testing.T) {
	logger := &mockFlushableLogger{flushErr: nil}
	finalizeShutdown(logger)

	if !logger.wasFlushCalled() {
		t.Error("Logger should have been flushed during finalize")
	}
}

// TestFramework_Stop_AlreadyStopped tests Stop() when already stopped
func TestFramework_Stop_AlreadyStopped(t *testing.T) {
	fw := newTestFramework(t)

	// First stop should succeed
	if err := fw.Stop(context.Background()); err != nil {
		t.Fatalf("First Stop should succeed: %v", err)
	}

	// Second stop should fail with "already stopped"
	err := fw.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when stopping already stopped framework")
	}
	if err.Error() != "application already stopped" {
		t.Errorf("Expected 'application already stopped' error, got: %v", err)
	}
}

// TestFramework_Stop_WithRunningModules tests Stop() after Start() to ensure lifecycle errors are handled
func TestFramework_Stop_WithRunningModules(t *testing.T) {
	fw := newTestFramework(t)

	// Register a module
	module := &mockModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start framework
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Stop should succeed and stop the module
	if err := fw.Stop(context.Background()); err != nil {
		t.Fatalf("Stop should succeed: %v", err)
	}

	if !module.wasStopped() {
		t.Error("Module should have been stopped")
	}
}

// TestFramework_Stop_WithModuleFailure tests Stop() when module stop fails
func TestFramework_Stop_WithModuleFailure(t *testing.T) {
	fw := newTestFramework(t)

	// Register a module that will fail during stop
	module := &mockModule{
		name:    "failing-module",
		stopErr: errors.New("module stop failed"),
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start framework
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Stop should return the module's stop error
	err := fw.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when module stop fails")
	}

	if !module.wasStopped() {
		t.Error("Module stop should have been called even if it fails")
	}
}

// TestNewFrameworkAppInstance_InvalidNATSOptions tests creation with invalid NATS options
func TestNewFrameworkAppInstance_InvalidNATSOptions(t *testing.T) {
	logger := &mockLogger{}

	// Try to create framework with invalid NATS options (port out of range)
	_, err := NewFrameworkAppInstance(
		logger,
		0,
		nats.WithPort(70000), // Invalid port > 65535
	)

	if err == nil {
		t.Fatal("Expected error when creating framework with invalid NATS options")
	}

	// Should contain "failed to create NATS manager" and port validation error
	errMsg := err.Error()
	if errMsg == "" {
		t.Error("Error message should not be empty")
	}
}

// TestNewFrameworkAppInstance_NATSManagerCreationFailure tests framework creation when NATS manager fails
func TestNewFrameworkAppInstance_NATSManagerCreationFailure(t *testing.T) {
	logger := &mockLogger{}

	// Try to create framework with conflicting NATS options
	// DontListen requires UseInProcessConn to be true
	_, err := NewFrameworkAppInstance(
		logger,
		0,
		nats.WithDontListen(), // DontListen without InProcessConn should fail validation
	)

	if err == nil {
		t.Fatal("Expected error when creating framework with invalid NATS config")
	}

	// Should get a "failed to create NATS manager" error
	if err.Error() == "" {
		t.Error("Error message should not be empty")
	}
}

// mockNATSManager implements nats.NATSManager for testing
type mockNATSManager struct {
	startErr    error
	stopErr     error
	startCalled bool
	stopCalled  bool
	mu          sync.Mutex
}

func (m *mockNATSManager) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	return m.startErr
}

func (m *mockNATSManager) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	return m.stopErr
}

func (m *mockNATSManager) Connection() (*natslib.Conn, error) {
	return nil, nil
}

func (m *mockNATSManager) JetStream() (jetstream.JetStream, error) {
	return nil, nil
}

func (m *mockNATSManager) ServerInfo() nats.ServerInfo {
	return nats.ServerInfo{}
}

// mockLifecycleManager implements lifecycle.LifecycleManager for testing
type mockLifecycleManager struct {
	startErr   error
	stopErr    error
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func newMockLifecycleManager() *mockLifecycleManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockLifecycleManager{
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func (m *mockLifecycleManager) Start(_ context.Context) error {
	return m.startErr
}

func (m *mockLifecycleManager) Stop(_ context.Context) error {
	return m.stopErr
}

func (m *mockLifecycleManager) WaitForShutdown(_ context.Context) error {
	<-m.ctx.Done()
	return nil
}

func (m *mockLifecycleManager) GetRuntimeContext() context.Context {
	return m.ctx
}

func (m *mockLifecycleManager) GetMiddlewareHook() func(context.Context, types.ModuleLifecycleEvent) {
	return func(_ context.Context, _ types.ModuleLifecycleEvent) {}
}

func (m *mockLifecycleManager) GetServiceContainer(_ string) types.ServiceContainer {
	return nil
}

func (m *mockLifecycleManager) GetPlugin(_ string) types.PluginModule {
	return nil
}

func (m *mockLifecycleManager) GetPluginContainer(_ string) types.ServiceContainer {
	return nil
}

// TestFramework_Stop_NATSManagerError tests Stop() when NATS manager fails
func TestFramework_Stop_NATSManagerError(t *testing.T) {
	mockNATS := &mockNATSManager{
		stopErr: errors.New("NATS stop failed"),
	}
	mockLM := newMockLifecycleManager()
	logger := &mockLogger{}

	// Create framework directly with mock components
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateRunning, // Must be running to trigger Stop logic
	}

	// Stop should return the NATS manager's error
	err := fw.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when NATS manager stop fails")
	}

	if !strings.Contains(err.Error(), "NATS stop failed") {
		t.Errorf("Expected error containing 'NATS stop failed', got: %v", err)
	}
}

// TestFramework_Stop_BothManagersError tests Stop() when both lifecycle and NATS fail
func TestFramework_Stop_BothManagersError(t *testing.T) {
	mockNATS := &mockNATSManager{
		stopErr: errors.New("NATS stop failed"),
	}
	mockLM := newMockLifecycleManager()
	mockLM.stopErr = errors.New("lifecycle stop failed")
	logger := &mockLogger{}

	// Create framework directly with mock components
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateRunning,
	}

	// Stop should return the lifecycle error (first error takes precedence)
	err := fw.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when both managers fail")
	}

	// The lifecycle error should be returned since it comes first
	if !strings.Contains(err.Error(), "lifecycle stop failed") {
		t.Errorf("Expected error containing 'lifecycle stop failed', got: %v", err)
	}
}

// TestFramework_Stop_NotRunning_WithNATSError tests Stop() when not running but NATS fails
func TestFramework_Stop_NotRunning_WithNATSError(t *testing.T) {
	mockNATS := &mockNATSManager{
		stopErr: errors.New("NATS stop failed"),
	}
	mockLM := newMockLifecycleManager()
	logger := &mockLogger{}

	// Create framework with StateCreated (not running)
	// In this state, wasRunning is false, so lifecycle.Stop() is not called
	// but NATS manager Stop() is still called
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateCreated,
	}

	// Stop should return the NATS manager's error
	err := fw.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when NATS manager stop fails")
	}

	if !strings.Contains(err.Error(), "NATS stop failed") {
		t.Errorf("Expected error containing 'NATS stop failed', got: %v", err)
	}
}

// TestFramework_Start_AfterStop tests Start() fails when already stopped
func TestFramework_Start_AfterStop(t *testing.T) {
	mockNATS := &mockNATSManager{}
	mockLM := newMockLifecycleManager()
	logger := &mockLogger{}

	// Create framework in StateStopped state
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateStopped,
	}

	// Start should fail because the framework was already stopped
	err := fw.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when starting already stopped framework")
	}

	expectedMsg := "application already stopped, cannot restart"
	if err.Error() != expectedMsg {
		t.Errorf("Expected '%s' error, got: %v", expectedMsg, err)
	}
}

// mockNATSManagerWithConnection implements nats.NATSManager with configurable connection behavior
type mockNATSManagerWithConnection struct {
	mockNATSManager
	connErr error
	connNil bool
}

func (m *mockNATSManagerWithConnection) Connection() (*natslib.Conn, error) {
	if m.connErr != nil {
		return nil, m.connErr
	}
	if m.connNil {
		// Simulate connection is nil (not initialized)
		return nil, nil
	}
	// NOTE: Returns nil conn to simulate disconnected state.
	// Cannot mock IsConnected() as it requires actual nats.Conn instance.
	// Tests relying on IsConnected() should use integration tests.
	return nil, nil
}

// TestFramework_Health_NATSConnectionError tests Health() when NATS connection fails
func TestFramework_Health_NATSConnectionError(t *testing.T) {
	mockNATS := &mockNATSManagerWithConnection{
		connErr: errors.New("NATS connection failed"),
	}
	mockLM := newMockLifecycleManager()
	logger := &mockLogger{}

	// Create framework in Running state
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateRunning,
	}

	health := fw.Health(context.Background())

	if health.NATSHealthy {
		t.Error("Expected NATSHealthy to be false when connection fails")
	}

	if health.Message != "NATS server unhealthy" {
		t.Errorf("Expected 'NATS server unhealthy' message, got: %s", health.Message)
	}
}

// TestFramework_Health_NATSConnectionNil tests Health() when NATS connection returns nil
func TestFramework_Health_NATSConnectionNil(t *testing.T) {
	mockNATS := &mockNATSManagerWithConnection{
		connNil: true,
	}
	mockLM := newMockLifecycleManager()
	logger := &mockLogger{}

	// Create framework in Running state
	fw := &frameworkApplication{
		logger:           logger,
		natsManager:      mockNATS,
		lifecycleManager: mockLM,
		state:            types.StateRunning,
	}

	health := fw.Health(context.Background())

	if health.NATSHealthy {
		t.Error("Expected NATSHealthy to be false when connection is nil")
	}

	if health.Message != "NATS server unhealthy" {
		t.Errorf("Expected 'NATS server unhealthy' message, got: %s", health.Message)
	}
}
