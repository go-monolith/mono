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

// testPluginModule implements mono.PluginModule for integration testing
type testPluginModule struct {
	name       string
	container  mono.ServiceContainer
	startOrder *[]string
	stopOrder  *[]string
	mu         sync.Mutex
}

func (m *testPluginModule) Name() string {
	return m.name
}

func (m *testPluginModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return nil
}

func (m *testPluginModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return nil
}

func (m *testPluginModule) SetContainer(container mono.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.container = container
}

func (m *testPluginModule) Container() mono.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.container
}

// testMiddlewareModuleWithOrder implements mono.MiddlewareModule for integration testing
type testMiddlewareModuleWithOrder struct {
	name       string
	startOrder *[]string
	stopOrder  *[]string
	mu         sync.Mutex
}

func (m *testMiddlewareModuleWithOrder) Name() string {
	return m.name
}

func (m *testMiddlewareModuleWithOrder) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return nil
}

func (m *testMiddlewareModuleWithOrder) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return nil
}

func (m *testMiddlewareModuleWithOrder) OnModuleLifecycle(ctx context.Context, event mono.ModuleLifecycleEvent) mono.ModuleLifecycleEvent {
	return event
}

func (m *testMiddlewareModuleWithOrder) OnServiceRegistration(ctx context.Context, reg mono.ServiceRegistration) mono.ServiceRegistration {
	return reg
}

func (m *testMiddlewareModuleWithOrder) OnConfigurationChange(ctx context.Context, event mono.ConfigurationEvent) mono.ConfigurationEvent {
	return event
}

func (m *testMiddlewareModuleWithOrder) OnOutgoingMessage(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
	return octx
}

func (m *testMiddlewareModuleWithOrder) OnEventConsumerRegistration(ctx context.Context, entry mono.EventConsumerEntry) mono.EventConsumerEntry {
	return entry
}

func (m *testMiddlewareModuleWithOrder) OnEventStreamConsumerRegistration(ctx context.Context, entry mono.EventStreamConsumerEntry) mono.EventStreamConsumerEntry {
	return entry
}

// testUsePluginModule implements mono.UsePluginModule for integration testing
type testUsePluginModule struct {
	name       string
	setPlugins map[string]mono.PluginModule
	startOrder *[]string
	stopOrder  *[]string
	mu         sync.Mutex
}

func (m *testUsePluginModule) Name() string {
	return m.name
}

func (m *testUsePluginModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return nil
}

func (m *testUsePluginModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return nil
}

func (m *testUsePluginModule) SetPlugin(alias string, plugin mono.PluginModule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setPlugins == nil {
		m.setPlugins = make(map[string]mono.PluginModule)
	}
	m.setPlugins[alias] = plugin
}

func (m *testUsePluginModule) getPlugin(alias string) mono.PluginModule {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setPlugins == nil {
		return nil
	}
	return m.setPlugins[alias]
}

// testRegularModuleWithOrder is a regular module that tracks start/stop order
type testRegularModuleWithOrder struct {
	name       string
	startOrder *[]string
	stopOrder  *[]string
	mu         sync.Mutex
}

func (m *testRegularModuleWithOrder) Name() string {
	return m.name
}

func (m *testRegularModuleWithOrder) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startOrder != nil {
		*m.startOrder = append(*m.startOrder, m.name)
	}
	return nil
}

func (m *testRegularModuleWithOrder) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	return nil
}

func TestIntegration_PluginsStartBeforeMiddleware(t *testing.T) {
	startOrder := []string{}

	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register plugin
	plugin := &testPluginModule{
		name:       "test-plugin",
		startOrder: &startOrder,
	}
	if err := fw.RegisterPlugin(plugin, "my-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Register middleware
	middleware := &testMiddlewareModuleWithOrder{
		name:       "test-middleware",
		startOrder: &startOrder,
	}
	if err := fw.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	// Register regular module
	regular := &testRegularModuleWithOrder{
		name:       "test-regular",
		startOrder: &startOrder,
	}
	if err := fw.Register(regular); err != nil {
		t.Fatalf("Failed to register regular module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify order: plugin -> middleware -> regular
	if len(startOrder) != 3 {
		t.Fatalf("expected 3 items in start order, got %d: %v", len(startOrder), startOrder)
	}

	if startOrder[0] != "test-plugin" {
		t.Errorf("expected plugin to start first, got: %v", startOrder)
	}

	if startOrder[1] != "test-middleware" {
		t.Errorf("expected middleware to start second, got: %v", startOrder)
	}

	if startOrder[2] != "test-regular" {
		t.Errorf("expected regular module to start third, got: %v", startOrder)
	}
}

func TestIntegration_PluginsStopAfterMiddlewareAndModules(t *testing.T) {
	stopOrder := []string{}

	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register plugin
	plugin := &testPluginModule{
		name:      "test-plugin",
		stopOrder: &stopOrder,
	}
	if err := fw.RegisterPlugin(plugin, "my-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Register middleware
	middleware := &testMiddlewareModuleWithOrder{
		name:      "test-middleware",
		stopOrder: &stopOrder,
	}
	if err := fw.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	// Register regular module
	regular := &testRegularModuleWithOrder{
		name:      "test-regular",
		stopOrder: &stopOrder,
	}
	if err := fw.Register(regular); err != nil {
		t.Fatalf("Failed to register regular module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Stop framework
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Verify order: regular -> middleware -> plugin
	if len(stopOrder) != 3 {
		t.Fatalf("expected 3 items in stop order, got %d: %v", len(stopOrder), stopOrder)
	}

	if stopOrder[0] != "test-regular" {
		t.Errorf("expected regular module to stop first, got: %v", stopOrder)
	}

	if stopOrder[1] != "test-middleware" {
		t.Errorf("expected middleware to stop second, got: %v", stopOrder)
	}

	if stopOrder[2] != "test-plugin" {
		t.Errorf("expected plugin to stop last, got: %v", stopOrder)
	}
}

func TestIntegration_PluginContainerInjection(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	plugin := &testPluginModule{name: "test-plugin"}
	if err := fw.RegisterPlugin(plugin, "my-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify container was injected
	if plugin.Container() == nil {
		t.Error("expected container to be injected into plugin")
	}

	// Verify plugin is accessible via framework
	retrieved := fw.Plugin("my-plugin")
	if retrieved == nil {
		t.Error("expected to retrieve plugin via framework")
	}

	if retrieved.Name() != "test-plugin" {
		t.Errorf("expected 'test-plugin', got '%s'", retrieved.Name())
	}
}

func TestIntegration_UsePluginModuleInjection(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register plugin
	plugin := &testPluginModule{name: "storage-plugin"}
	if err := fw.RegisterPlugin(plugin, "primary-storage"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Register module that uses the plugin
	consumer := &testUsePluginModule{
		name: "consumer-module",
	}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify all registered plugins were injected
	injectedPlugin := consumer.getPlugin("primary-storage")
	if injectedPlugin == nil {
		t.Error("expected plugin to be injected into consumer module")
	}

	if injectedPlugin.Name() != "storage-plugin" {
		t.Errorf("expected injected plugin to be 'storage-plugin', got '%s'",
			injectedPlugin.Name())
	}
}

func TestIntegration_UsePluginModuleReceivesAllPlugins(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register multiple plugins
	plugin1 := &testPluginModule{name: "storage-plugin"}
	plugin2 := &testPluginModule{name: "cache-plugin"}
	if err := fw.RegisterPlugin(plugin1, "storage"); err != nil {
		t.Fatalf("Failed to register storage plugin: %v", err)
	}
	if err := fw.RegisterPlugin(plugin2, "cache"); err != nil {
		t.Fatalf("Failed to register cache plugin: %v", err)
	}

	// Register module that implements UsePluginModule
	consumer := &testUsePluginModule{
		name: "consumer-module",
	}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Verify ALL registered plugins were injected
	storagePlugin := consumer.getPlugin("storage")
	cachePlugin := consumer.getPlugin("cache")

	if storagePlugin == nil {
		t.Error("expected storage plugin to be injected")
	}
	if cachePlugin == nil {
		t.Error("expected cache plugin to be injected")
	}

	// Verify plugin count
	if len(consumer.setPlugins) != 2 {
		t.Errorf("expected 2 plugins to be injected, got %d", len(consumer.setPlugins))
	}
}

func TestIntegration_MultiplePluginsWithSameModuleName(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Same plugin module type, different aliases
	plugin1 := &testPluginModule{name: "storage-plugin"}
	plugin2 := &testPluginModule{name: "storage-plugin"}

	if err := fw.RegisterPlugin(plugin1, "primary-storage"); err != nil {
		t.Fatalf("Failed to register plugin 1: %v", err)
	}
	if err := fw.RegisterPlugin(plugin2, "backup-storage"); err != nil {
		t.Fatalf("Failed to register plugin 2: %v", err)
	}

	// Consumer receives all plugins automatically
	consumer := &testUsePluginModule{
		name: "consumer-module",
	}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Both plugins should have containers
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

	// Both should be injected into consumer
	primary := consumer.getPlugin("primary-storage")
	backup := consumer.getPlugin("backup-storage")

	if primary == nil || backup == nil {
		t.Error("expected both plugins to be injected into consumer")
	}

	// They should be the same plugin instances
	if primary != plugin1 {
		t.Error("expected primary-storage to be plugin1")
	}
	if backup != plugin2 {
		t.Error("expected backup-storage to be plugin2")
	}
}

func TestIntegration_PluginAccessAfterStop(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	plugin := &testPluginModule{name: "test-plugin"}
	if err := fw.RegisterPlugin(plugin, "my-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Plugin is accessible before stop
	if fw.Plugin("my-plugin") == nil {
		t.Error("expected plugin to be accessible before stop")
	}

	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Plugin might still be accessible after stop (reference is kept)
	// This is consistent with how Modules() behaves
	retrieved := fw.Plugin("my-plugin")
	if retrieved == nil {
		t.Log("Plugin not accessible after stop (acceptable behavior)")
	}
}

func TestIntegration_RegisterPluginAfterStart(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Try to register plugin after start
	plugin := &testPluginModule{name: "test-plugin"}
	err = fw.RegisterPlugin(plugin, "my-plugin")
	if err == nil {
		t.Error("expected error when registering plugin after start")
	}
}

func TestIntegration_DuplicatePluginAlias(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	plugin1 := &testPluginModule{name: "plugin-1"}
	plugin2 := &testPluginModule{name: "plugin-2"}

	if err := fw.RegisterPlugin(plugin1, "my-alias"); err != nil {
		t.Fatalf("Failed to register first plugin: %v", err)
	}

	// Try to register second plugin with same alias
	err = fw.RegisterPlugin(plugin2, "my-alias")
	if err == nil {
		t.Error("expected error when registering plugin with duplicate alias")
	}
}
