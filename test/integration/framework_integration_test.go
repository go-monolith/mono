//go:build integration
// +build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
)

// testModule implements a basic module for integration testing
type testModule struct {
	name                 string
	dependencies         []string
	startCalled          bool
	stopCalled           bool
	eventBus             mono.EventBus
	container            mono.ServiceContainer
	registerServicesFunc func(mono.ServiceContainer) error
	startFunc            func(context.Context) error
	mu                   sync.Mutex
}

func (m *testModule) Name() string {
	return m.name
}

func (m *testModule) Dependencies() []string {
	return m.dependencies
}

// SetDependencyServiceContainer implements mono.DependentModule interface.
func (m *testModule) SetDependencyServiceContainer(_ string, _ mono.ServiceContainer) {
	// No-op for tests
}

func (m *testModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true

	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *testModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	return nil
}

func (m *testModule) SetEventBus(eventBus mono.EventBus) {
	m.eventBus = eventBus
}

func (m *testModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	if m.registerServicesFunc != nil {
		return m.registerServicesFunc(container)
	}
	return nil
}

func (m *testModule) wasStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalled
}

func (m *testModule) wasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

// noOpsLogger suppresses all logging output for testing
type noOpsLogger struct{}

func (m *noOpsLogger) Debug(msg string, args ...any)      {}
func (m *noOpsLogger) Info(msg string, args ...any)       {}
func (m *noOpsLogger) Warn(msg string, args ...any)       {}
func (m *noOpsLogger) Error(msg string, args ...any)      {}
func (m *noOpsLogger) With(args ...any) mono.Logger       { return m }
func (m *noOpsLogger) WithModule(name string) mono.Logger { return m }
func (m *noOpsLogger) WithError(err error) mono.Logger    { return m }

// mockAuditLogger for testing

func TestIntegration_BasicFrameworkLifecycle(t *testing.T) {
	// Create framework with embedded NATS
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register a simple module
	module := &testModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify module was started
	if !module.wasStarted() {
		t.Error("Module was not started")
	}

	// Check framework health
	health := fw.Health(ctx)
	if !health.Healthy {
		t.Errorf("Application should be healthy: %s", health.Message)
	}
	if !health.NATSHealthy {
		t.Error("NATS should be healthy")
	}

	// Stop framework
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Verify module was stopped
	if !module.wasStopped() {
		t.Error("Module was not stopped")
	}
}

func TestIntegration_MultiModuleWithDependencies(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create modules with dependency chain
	moduleA := &testModule{name: "module-a"}
	moduleB := &testModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &testModule{name: "module-c", dependencies: []string{"module-b"}}

	// Register modules
	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := fw.Register(moduleC); err != nil {
		t.Fatalf("Failed to register module C: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify all modules started
	if !moduleA.wasStarted() {
		t.Error("Module A was not started")
	}
	if !moduleB.wasStarted() {
		t.Error("Module B was not started")
	}
	if !moduleC.wasStarted() {
		t.Error("Module C was not started")
	}

	// Verify modules have EventBus
	if moduleA.eventBus == nil {
		t.Error("Module A did not receive EventBus")
	}
	if moduleB.eventBus == nil {
		t.Error("Module B did not receive EventBus")
	}
	if moduleC.eventBus == nil {
		t.Error("Module C did not receive EventBus")
	}
}

func TestIntegration_EventPublishSubscribe(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	module := &testModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Test event publishing and subscription
	eventBus := fw.EventBus("test-module")
	if eventBus == nil {
		t.Fatal("EventBus is nil")
	}

	received := make(chan bool, 1)
	subject := "test.event"

	// Subscribe to event
	sub, err := eventBus.Subscribe(subject, func(ctx context.Context, msg *mono.Msg) {
		received <- true
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish event
	if err := eventBus.Publish(subject, []byte("test message")); err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait for message
	select {
	case <-received:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("Did not receive published message")
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	moduleA := &testModule{name: "module-a"}
	moduleB := &testModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := fw.Stop(shutdownCtx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Verify both modules stopped
	if !moduleA.wasStopped() {
		t.Error("Module A was not stopped")
	}
	if !moduleB.wasStopped() {
		t.Error("Module B was not stopped")
	}

	// Verify framework health after shutdown
	health := fw.Health(context.Background())
	if health.Healthy {
		t.Error("Application should not be healthy after shutdown")
	}
}

func TestIntegration_HealthCheck(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	module := &testModule{name: "test-module"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Health before start
	health := fw.Health(ctx)
	if health.Healthy {
		t.Error("Application should not be healthy before start")
	}

	// Start framework
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Health after start
	health = fw.Health(ctx)
	if !health.Healthy {
		t.Errorf("Application should be healthy after start: %s", health.Message)
	}
	if !health.NATSHealthy {
		t.Error("NATS should be healthy")
	}
	if health.State != mono.StateRunning {
		t.Errorf("Expected state Running, got %d", health.State)
	}
}
