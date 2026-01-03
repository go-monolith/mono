package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

// mockPluginModule implements types.PluginModule for testing
type mockPluginModule struct {
	name       string
	container  types.ServiceContainer
	startOrder *[]string
	stopOrder  *[]string
	startErr   error
	stopErr    error
	mu         sync.Mutex
}

func (m *mockPluginModule) Name() string {
	return m.name
}

func (m *mockPluginModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return m.startErr
}

func (m *mockPluginModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return m.stopErr
}

func (m *mockPluginModule) SetContainer(container types.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.container = container
}

func (m *mockPluginModule) Container() types.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.container
}

// mockUsePluginModule implements types.UsePluginModule for testing
type mockUsePluginModule struct {
	mockModule
	setPlugins map[string]types.PluginModule
	startOrder *[]string
}

func (m *mockUsePluginModule) SetPlugin(alias string, plugin types.PluginModule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setPlugins == nil {
		m.setPlugins = make(map[string]types.PluginModule)
	}
	m.setPlugins[alias] = plugin
}

func (m *mockUsePluginModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return m.startErr
}

func TestPluginsStartBeforeMiddleware(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	startOrder := []string{}

	// Register middleware
	mw := &mockMiddlewareModule{
		mockModule: mockModule{name: "test-middleware"},
		startOrder: &startOrder,
	}
	_ = reg.Register(mw)

	// Register plugin
	plugin := &mockPluginModule{
		name:       "test-plugin",
		startOrder: &startOrder,
	}
	_ = pluginReg.Register(plugin, "test-alias")

	// Create lifecycle manager
	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	// Start
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Verify order: plugin should start before middleware
	if len(startOrder) != 2 {
		t.Fatalf("expected 2 items in start order, got %d: %v", len(startOrder), startOrder)
	}

	if startOrder[0] != "test-plugin" {
		t.Errorf("expected plugin to start first, got: %v", startOrder)
	}

	if startOrder[1] != "test-middleware" {
		t.Errorf("expected middleware to start second, got: %v", startOrder)
	}
}

func TestPluginsStopAfterModulesAndMiddleware(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	stopOrder := []string{}

	// Register middleware
	mw := &mockMiddlewareModule{
		mockModule: mockModule{name: "test-middleware"},
		stopOrder:  &stopOrder,
	}
	_ = reg.Register(mw)

	// Register regular module
	module := &mockModule{
		name:      "test-module",
		stopOrder: &stopOrder,
	}
	_ = reg.Register(module)

	// Register plugin
	plugin := &mockPluginModule{
		name:      "test-plugin",
		stopOrder: &stopOrder,
	}
	_ = pluginReg.Register(plugin, "test-alias")

	// Create lifecycle manager
	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	// Start
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop
	err = lm.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify order: regular module -> middleware -> plugin
	if len(stopOrder) != 3 {
		t.Fatalf("expected 3 items in stop order, got %d: %v", len(stopOrder), stopOrder)
	}

	if stopOrder[0] != "test-module" {
		t.Errorf("expected regular module to stop first, got: %v", stopOrder)
	}

	if stopOrder[1] != "test-middleware" {
		t.Errorf("expected middleware to stop second, got: %v", stopOrder)
	}

	if stopOrder[2] != "test-plugin" {
		t.Errorf("expected plugin to stop last, got: %v", stopOrder)
	}
}

func TestPluginContainerInjection(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	plugin := &mockPluginModule{name: "test-plugin"}
	_ = pluginReg.Register(plugin, "test-alias")

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Verify container was injected
	if plugin.Container() == nil {
		t.Error("expected container to be injected into plugin")
	}
}

func TestUsePluginModuleInjection(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	// Register plugin
	plugin := &mockPluginModule{name: "storage-plugin"}
	_ = pluginReg.Register(plugin, "primary-storage")

	// Register module that uses the plugin
	module := &mockUsePluginModule{
		mockModule: mockModule{name: "consumer-module"},
	}
	_ = reg.Register(module)

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Verify all registered plugins were injected
	if module.setPlugins == nil || module.setPlugins["primary-storage"] == nil {
		t.Error("expected plugin to be injected into module")
	}

	if module.setPlugins["primary-storage"].Name() != "storage-plugin" {
		t.Errorf("expected injected plugin to be 'storage-plugin', got '%s'",
			module.setPlugins["primary-storage"].Name())
	}
}

func TestUsePluginModuleReceivesAllPlugins(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	// Register multiple plugins
	plugin1 := &mockPluginModule{name: "storage-plugin"}
	plugin2 := &mockPluginModule{name: "cache-plugin"}
	_ = pluginReg.Register(plugin1, "primary-storage")
	_ = pluginReg.Register(plugin2, "cache")

	// Register module that implements UsePluginModule
	module := &mockUsePluginModule{
		mockModule: mockModule{name: "consumer-module"},
	}
	_ = reg.Register(module)

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Verify all registered plugins were injected
	if len(module.setPlugins) != 2 {
		t.Errorf("expected 2 plugins to be injected, got %d", len(module.setPlugins))
	}

	if module.setPlugins["primary-storage"] == nil {
		t.Error("expected primary-storage plugin to be injected")
	}

	if module.setPlugins["cache"] == nil {
		t.Error("expected cache plugin to be injected")
	}
}

func TestGetPlugin(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	plugin := &mockPluginModule{name: "test-plugin"}
	_ = pluginReg.Register(plugin, "my-alias")

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Get plugin by alias
	retrieved := lm.GetPlugin("my-alias")
	if retrieved == nil {
		t.Fatal("expected to retrieve plugin")
	}

	if retrieved.Name() != "test-plugin" {
		t.Errorf("expected 'test-plugin', got '%s'", retrieved.Name())
	}
}

func TestGetPluginContainer(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	plugin := &mockPluginModule{name: "test-plugin"}
	_ = pluginReg.Register(plugin, "my-alias")

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Get plugin container by alias
	container := lm.GetPluginContainer("my-alias")
	if container == nil {
		t.Error("expected to retrieve plugin container")
	}
}

func TestMultiplePluginsWithSameName(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)

	// Same plugin type, different aliases
	plugin1 := &mockPluginModule{name: "storage-plugin"}
	plugin2 := &mockPluginModule{name: "storage-plugin"}

	_ = pluginReg.Register(plugin1, "primary-storage")
	_ = pluginReg.Register(plugin2, "backup-storage")

	lm := NewLifecycleManager(reg, pluginReg, &mockEventBus{}, nil, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer lm.Stop(context.Background())

	// Both should have containers
	if plugin1.Container() == nil {
		t.Error("expected primary-storage to have container")
	}

	if plugin2.Container() == nil {
		t.Error("expected backup-storage to have container")
	}

	// Containers should be different instances
	if plugin1.Container() == plugin2.Container() {
		t.Error("expected different container instances for each plugin")
	}
}

// mockMiddlewareModule with start/stop order tracking
type mockMiddlewareModule struct {
	mockModule
	startOrder *[]string
	stopOrder  *[]string
}

func (m *mockMiddlewareModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return m.startErr
}

func (m *mockMiddlewareModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	if m.stopPanic != nil {
		panic(m.stopPanic)
	}
	return m.stopErr
}

func (m *mockMiddlewareModule) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	return event
}

func (m *mockMiddlewareModule) OnServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	return reg
}

func (m *mockMiddlewareModule) OnConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	return event
}

func (m *mockMiddlewareModule) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx
}

func (m *mockMiddlewareModule) OnEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	return entry
}

func (m *mockMiddlewareModule) OnEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	return entry
}

// Add stopOrder tracking to base mockModule
func init() {
	// Ensure mockModule has stopOrder field
}

// Update mockModule with stopOrder tracking
type mockModuleWithStopOrder struct {
	mockModule
	stopOrder *[]string
}

func (m *mockModuleWithStopOrder) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return m.stopErr
}

// TestPluginRollbackOnStartFailure tests plugin rollback when one plugin fails to start
func TestPluginRollbackOnStartFailure(t *testing.T) {
	t.Run("rolls back successfully started plugins when later plugin fails", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		// Track stop order
		stopOrder := []string{}

		// Create two plugins: first succeeds, second fails
		plugin1 := &mockPluginModule{name: "plugin1", stopOrder: &stopOrder}
		plugin2 := &mockPluginModule{name: "plugin2", startErr: fmt.Errorf("start failed")}

		if err := pluginReg.Register(plugin1, "alias1"); err != nil {
			t.Fatal(err)
		}
		if err := pluginReg.Register(plugin2, "alias2"); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

		// Start should fail due to plugin2 error
		err := lm.Start(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		// Verify plugin1 was stopped during rollback
		if len(stopOrder) == 0 || stopOrder[0] != "plugin1" {
			t.Errorf("expected plugin1 to be stopped during rollback, got stopOrder: %v", stopOrder)
		}
	})
}
