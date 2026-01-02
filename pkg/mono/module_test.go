package mono_test

import (
	"context"
	"testing"

	"github.com/go-monolith/mono/v1"
)

// Mock module implementations for testing interface compliance

// BasicModule implements only Module interface
type BasicModule struct {
	name    string
	started bool
	stopped bool
}

func (m *BasicModule) Name() string {
	return m.name
}

func (m *BasicModule) Start(_ context.Context) error {
	m.started = true
	return nil
}

func (m *BasicModule) Stop(_ context.Context) error {
	m.stopped = true
	return nil
}

// TestModuleInterface verifies basic Module interface
func TestModuleInterface(t *testing.T) {
	var _ mono.Module = (*BasicModule)(nil) // Compile-time interface check

	t.Run("Name returns module name", func(t *testing.T) {
		m := &BasicModule{name: "test-module"}
		if m.Name() != "test-module" {
			t.Errorf("Name() = %q, want %q", m.Name(), "test-module")
		}
	})

	t.Run("Start sets started flag", func(t *testing.T) {
		m := &BasicModule{name: "test"}
		ctx := context.Background()

		if m.started {
			t.Error("module should not be started initially")
		}

		if err := m.Start(ctx); err != nil {
			t.Errorf("Start() error = %v", err)
		}

		if !m.started {
			t.Error("module should be started after Start()")
		}
	})

	t.Run("Stop sets stopped flag", func(t *testing.T) {
		m := &BasicModule{name: "test"}
		ctx := context.Background()

		if m.stopped {
			t.Error("module should not be stopped initially")
		}

		if err := m.Stop(ctx); err != nil {
			t.Errorf("Stop() error = %v", err)
		}

		if !m.stopped {
			t.Error("module should be stopped after Stop()")
		}
	})
}

// DependentTestModule implements DependentModule
type DependentTestModule struct {
	BasicModule
	deps       []string
	containers map[string]mono.ServiceContainer
}

func (m *DependentTestModule) Dependencies() []string {
	return m.deps
}

func (m *DependentTestModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	if m.containers == nil {
		m.containers = make(map[string]mono.ServiceContainer)
	}
	m.containers[dependency] = container
}

// TestDependentModuleInterface verifies DependentModule interface
func TestDependentModuleInterface(t *testing.T) {
	var _ mono.DependentModule = (*DependentTestModule)(nil) // Compile-time interface check

	t.Run("Dependencies returns dependency list", func(t *testing.T) {
		m := &DependentTestModule{
			BasicModule: BasicModule{name: "order"},
			deps:        []string{"inventory", "payment"},
		}

		deps := m.Dependencies()
		if len(deps) != 2 {
			t.Errorf("Dependencies() length = %d, want 2", len(deps))
		}

		if deps[0] != "inventory" {
			t.Errorf("Dependencies()[0] = %q, want %q", deps[0], "inventory")
		}

		if deps[1] != "payment" {
			t.Errorf("Dependencies()[1] = %q, want %q", deps[1], "payment")
		}
	})

	t.Run("Dependencies returns empty slice for no dependencies", func(t *testing.T) {
		m := &DependentTestModule{
			BasicModule: BasicModule{name: "standalone"},
			deps:        []string{},
		}

		deps := m.Dependencies()
		if deps == nil {
			t.Error("Dependencies() should return empty slice, not nil")
		}

		if len(deps) != 0 {
			t.Errorf("Dependencies() length = %d, want 0", len(deps))
		}
	})

	t.Run("SetDependencyServiceContainer stores containers", func(t *testing.T) {
		m := &DependentTestModule{
			BasicModule: BasicModule{name: "order"},
			deps:        []string{"inventory", "payment"},
		}

		var inventoryContainer mono.ServiceContainer
		var paymentContainer mono.ServiceContainer

		m.SetDependencyServiceContainer("inventory", inventoryContainer)
		m.SetDependencyServiceContainer("payment", paymentContainer)

		if m.containers["inventory"] != inventoryContainer {
			t.Error("inventory container not stored correctly")
		}

		if m.containers["payment"] != paymentContainer {
			t.Error("payment container not stored correctly")
		}

		if len(m.containers) != 2 {
			t.Errorf("containers map length = %d, want 2", len(m.containers))
		}
	})
}

// ServiceProviderTestModule implements ServiceProviderModule
type ServiceProviderTestModule struct {
	BasicModule
	registerCalled bool
	registerError  error
}

func (m *ServiceProviderTestModule) RegisterServices(_ mono.ServiceContainer) error {
	m.registerCalled = true
	return m.registerError
}

// TestServiceProviderModuleInterface verifies ServiceProviderModule interface
func TestServiceProviderModuleInterface(t *testing.T) {
	var _ mono.ServiceProviderModule = (*ServiceProviderTestModule)(nil) // Compile-time interface check

	t.Run("RegisterServices is called", func(t *testing.T) {
		m := &ServiceProviderTestModule{
			BasicModule: BasicModule{name: "inventory"},
		}

		if m.registerCalled {
			t.Error("RegisterServices should not be called initially")
		}

		// Mock container (nil for testing)
		var container mono.ServiceContainer
		if err := m.RegisterServices(container); err != nil {
			t.Errorf("RegisterServices() error = %v", err)
		}

		if !m.registerCalled {
			t.Error("RegisterServices should have been called")
		}
	})
}

// NATSAwareTestModule implements EventBusAwareModule
type NATSAwareTestModule struct {
	BasicModule
	eventBus mono.EventBus
}

func (m *NATSAwareTestModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// TestEventBusAwareModuleInterface verifies EventBusAwareModule interface
func TestEventBusAwareModuleInterface(t *testing.T) {
	var _ mono.EventBusAwareModule = (*NATSAwareTestModule)(nil) // Compile-time interface check

	t.Run("SetEventBus stores event bus", func(t *testing.T) {
		m := &NATSAwareTestModule{
			BasicModule: BasicModule{name: "notification"},
		}

		if m.eventBus != nil {
			t.Error("eventBus should be nil initially")
		}

		// Mock event bus (nil for testing)
		var bus mono.EventBus
		m.SetEventBus(bus)

		if m.eventBus != bus {
			t.Error("eventBus not stored correctly")
		}
	})
}

// HealthAwareTestModule implements HealthAwareModule
type HealthAwareTestModule struct {
	BasicModule
	healthStatus mono.HealthStatus
}

func (m *HealthAwareTestModule) Health(_ context.Context) mono.HealthStatus {
	return m.healthStatus
}

// TestHealthAwareModuleInterface verifies HealthAwareModule interface
func TestHealthAwareModuleInterface(t *testing.T) {
	var _ mono.HealthCheckableModule = (*HealthAwareTestModule)(nil) // Compile-time interface check

	t.Run("Health returns status", func(t *testing.T) {
		m := &HealthAwareTestModule{
			BasicModule: BasicModule{name: "database"},
			healthStatus: mono.HealthStatus{
				Healthy: true,
				Message: "operational",
				Details: map[string]any{"connections": 10},
			},
		}

		ctx := context.Background()
		status := m.Health(ctx)

		if !status.Healthy {
			t.Error("Health status should be healthy")
		}

		if status.Message != "operational" {
			t.Errorf("Health message = %q, want %q", status.Message, "operational")
		}

		if status.Details["connections"] != 10 {
			t.Errorf("Health details connections = %v, want 10", status.Details["connections"])
		}
	})

	t.Run("Health returns unhealthy status", func(t *testing.T) {
		m := &HealthAwareTestModule{
			BasicModule: BasicModule{name: "database"},
			healthStatus: mono.HealthStatus{
				Healthy: false,
				Message: "connection lost",
				Details: map[string]any{"error": "timeout"},
			},
		}

		ctx := context.Background()
		status := m.Health(ctx)

		if status.Healthy {
			t.Error("Health status should be unhealthy")
		}

		if status.Message != "connection lost" {
			t.Errorf("Health message = %q, want %q", status.Message, "connection lost")
		}
	})
}

// CompositeModule implements multiple interfaces
type CompositeModule struct {
	DependentTestModule
	eventBus       mono.EventBus
	containers     map[string]mono.ServiceContainer
	healthStatus   mono.HealthStatus
	registerCalled bool
}

func (m *CompositeModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

func (m *CompositeModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	if m.containers == nil {
		m.containers = make(map[string]mono.ServiceContainer)
	}
	m.containers[dependency] = container
}

func (m *CompositeModule) RegisterServices(_ mono.ServiceContainer) error {
	m.registerCalled = true
	return nil
}

func (m *CompositeModule) Health(_ context.Context) mono.HealthStatus {
	return m.healthStatus
}

// TestCompositeModuleInterfaces verifies a module can implement multiple interfaces
func TestCompositeModuleInterfaces(t *testing.T) {
	// Compile-time interface checks
	var _ mono.Module = (*CompositeModule)(nil)
	var _ mono.DependentModule = (*CompositeModule)(nil)
	var _ mono.ServiceProviderModule = (*CompositeModule)(nil)
	var _ mono.EventBusAwareModule = (*CompositeModule)(nil)
	var _ mono.HealthCheckableModule = (*CompositeModule)(nil)

	t.Run("Composite module implements all interfaces", func(t *testing.T) {
		m := &CompositeModule{
			DependentTestModule: DependentTestModule{
				BasicModule: BasicModule{name: "order"},
				deps:        []string{"inventory", "payment"},
			},
			healthStatus: mono.HealthStatus{Healthy: true, Message: "ok"},
		}

		// Test Module interface
		if m.Name() != "order" {
			t.Errorf("Name() = %q, want %q", m.Name(), "order")
		}

		// Test DependentModule interface
		deps := m.Dependencies()
		if len(deps) != 2 {
			t.Errorf("Dependencies() length = %d, want 2", len(deps))
		}

		// Test DependencyServiceContainerAwareModule interface
		var container mono.ServiceContainer
		m.SetDependencyServiceContainer("inventory", container)
		if m.containers["inventory"] != container {
			t.Error("SetDependencyServiceContainer not working")
		}

		// Test ServiceProviderModule interface
		if err := m.RegisterServices(nil); err != nil {
			t.Errorf("RegisterServices() error = %v", err)
		}
		if !m.registerCalled {
			t.Error("RegisterServices not called")
		}

		// Test EventBusAwareModule interface
		var bus mono.EventBus
		m.SetEventBus(bus)
		if m.eventBus != bus {
			t.Error("SetEventBus not working")
		}

		// Test HealthAwareModule interface
		ctx := context.Background()
		status := m.Health(ctx)
		if !status.Healthy {
			t.Error("Health not working")
		}
	})
}

// TestHealthStatus verifies mono.HealthStatus struct
func TestHealthStatus(t *testing.T) {
	t.Run("mono.HealthStatus with all fields", func(t *testing.T) {
		status := mono.HealthStatus{
			Healthy: true,
			Message: "all systems operational",
			Details: map[string]any{
				"connections": 42,
				"uptime":      "5h30m",
				"errors":      0,
			},
		}

		if !status.Healthy {
			t.Error("Healthy should be true")
		}

		if status.Message != "all systems operational" {
			t.Errorf("Message = %q, want %q", status.Message, "all systems operational")
		}

		if status.Details["connections"] != 42 {
			t.Errorf("Details connections = %v, want 42", status.Details["connections"])
		}
	})

	t.Run("mono.HealthStatus zero value", func(t *testing.T) {
		var status mono.HealthStatus

		if status.Healthy {
			t.Error("Zero value Healthy should be false")
		}

		if status.Message != "" {
			t.Error("Zero value Message should be empty string")
		}

		if status.Details != nil {
			t.Error("Zero value Details should be nil")
		}
	})
}

// TestInterfaceGodocExamples tests examples from interface godoc comments
func TestInterfaceGodocExamples(t *testing.T) {
	t.Run("DependentModule example", func(t *testing.T) {
		// Example from DependentModule godoc
		type OrderModule struct {
			BasicModule
		}

		orderModule := &OrderModule{
			BasicModule: BasicModule{name: "order"},
		}

		if orderModule.Name() != "order" {
			t.Error("OrderModule example failed")
		}
	})

	t.Run("HealthAwareModule example", func(t *testing.T) {
		// Example from HealthAwareModule godoc
		type DatabaseModule struct {
			BasicModule
			dbHealthy bool
		}

		dbModule := &DatabaseModule{
			BasicModule: BasicModule{name: "database"},
			dbHealthy:   true,
		}

		health := func(_ context.Context) mono.HealthStatus {
			if !dbModule.dbHealthy {
				return mono.HealthStatus{
					Healthy: false,
					Message: "database connection failed",
					Details: map[string]any{"error": "connection refused"},
				}
			}
			return mono.HealthStatus{Healthy: true, Message: "operational"}
		}

		ctx := context.Background()
		status := health(ctx)

		if !status.Healthy {
			t.Error("Database health check should pass")
		}
	})
}
