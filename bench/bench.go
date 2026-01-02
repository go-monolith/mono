// Package bench provides shared types and helper functions for benchmarking
// the mono-framework. Inspired by nats.go/bench/bench.go patterns.
package bench

import (
	"context"
	"sync"

	mono "github.com/go-monolith/mono/v1"
)

// DefaultPayloadSize is the standard payload size for benchmarks (256 bytes).
const DefaultPayloadSize = 256

// PayloadSizes defines payload sizes for sub-benchmarks (256B, 1KB, 5KB).
var PayloadSizes = []int{256, 1024, 5120}

// BenchAppOptions configures benchmark application creation.
type BenchAppOptions struct {
	InProcess    bool   // Use in-process connections (no TCP socket)
	JetStreamDir string // Enable JetStream with this storage directory (empty = disabled)
}

// GeneratePayload creates a byte slice of the given size with non-zero data.
// This prevents compiler optimization and simulates real message payloads.
func GeneratePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = 'x'
	}
	return payload
}

// NewBenchAppWithOptions creates a MonoApplication with configurable options.
//   - InProcess=true: Uses in-process connections (no TCP listening)
//   - InProcess=false: Uses TCP socket connections (listens on auto-assigned port)
//   - JetStreamDir non-empty: Enables JetStream with the specified storage directory
func NewBenchAppWithOptions(opts BenchAppOptions) (mono.MonoApplication, error) {
	monoOpts := []mono.MonoFrameworkOption{mono.WithLogLevel(mono.LogLevelError)}

	if opts.InProcess {
		monoOpts = append(monoOpts, mono.WithNATSDontListen(), mono.WithNATSInProcessConn())
	}
	// When InProcess=false, no extra options needed - NATS listens on auto port

	if opts.JetStreamDir != "" {
		monoOpts = append(monoOpts, mono.WithJetStreamStorageDir(opts.JetStreamDir))
	}

	return mono.NewMonoApplication(monoOpts...)
}

// NewBenchApp creates a MonoApplication configured for minimal overhead benchmarks.
// It uses in-process connections with TCP listening disabled.
func NewBenchApp() (mono.MonoApplication, error) {
	return NewBenchAppWithOptions(BenchAppOptions{InProcess: true})
}

// NewBenchAppWithJetStream creates a MonoApplication with JetStream enabled.
// Uses in-process connections for minimal overhead.
func NewBenchAppWithJetStream(storageDir string) (mono.MonoApplication, error) {
	return NewBenchAppWithOptions(BenchAppOptions{InProcess: true, JetStreamDir: storageDir})
}

// BenchProviderModule is a minimal provider module for benchmarks.
// It implements Module and ServiceProviderModule interfaces.
type BenchProviderModule struct {
	name      string
	container mono.ServiceContainer
	// SetupFunc is called during RegisterServices - set by benchmark to configure services
	SetupFunc func(container mono.ServiceContainer) error
}

// NewBenchProviderModule creates a new provider module with the given name.
func NewBenchProviderModule(name string) *BenchProviderModule {
	return &BenchProviderModule{name: name}
}

// Name returns the module name.
func (m *BenchProviderModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchProviderModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchProviderModule) Stop(_ context.Context) error { return nil }

// RegisterServices registers services using the SetupFunc if provided.
func (m *BenchProviderModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	if m.SetupFunc != nil {
		return m.SetupFunc(container)
	}
	return nil
}

// Container returns the module's service container.
func (m *BenchProviderModule) Container() mono.ServiceContainer { return m.container }

// =============================================================================
// RealisticBenchModule - More realistic module for memory/startup benchmarks
// =============================================================================

// RealisticBenchModule is a more realistic module for memory/startup benchmarks.
// It includes internal data structures, proper lifecycle, and a registered service.
// Use this for benchmarks that need to simulate real-world module behavior with
// internal state and service registration overhead.
//
// For minimal overhead benchmarks focusing on framework-level operations,
// use BenchProviderModule instead.
type RealisticBenchModule struct {
	name      string
	container mono.ServiceContainer

	// Internal data structures (realistic state)
	config    map[string]string
	dataStore []byte
	mu        sync.RWMutex
}

// Compile-time interface check.
var _ mono.ServiceProviderModule = (*RealisticBenchModule)(nil)

// NewRealisticBenchModule creates a new realistic module with the given name.
func NewRealisticBenchModule(name string) *RealisticBenchModule {
	return &RealisticBenchModule{
		name:      name,
		config:    make(map[string]string),
		dataStore: make([]byte, 0, DefaultPayloadSize), // Match benchmark payload size
	}
}

// Name returns the module name.
func (m *RealisticBenchModule) Name() string { return m.name }

// Start initializes internal data structures (simulates loading config/state).
// No lock needed - Start() is called during single-threaded initialization.
func (m *RealisticBenchModule) Start(_ context.Context) error {
	m.config["module.name"] = m.name
	m.config["module.version"] = "1.0.0"
	m.config["module.started"] = "true"
	m.dataStore = append(m.dataStore, []byte("initialized")...)

	return nil
}

// Stop cleans up internal resources.
// Clear instead of nil to allow restart (common in benchmark scenarios).
func (m *RealisticBenchModule) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear map entries
	for k := range m.config {
		delete(m.config, k)
	}
	// Reset slice length but keep capacity
	m.dataStore = m.dataStore[:0]

	return nil
}

// RegisterServices registers a simple echo service.
func (m *RealisticBenchModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container

	// Register a simple echo RequestReply service
	handler := func(_ context.Context, req *mono.Msg) ([]byte, error) {
		return req.Data, nil // Echo back
	}
	return container.RegisterRequestReplyService("echo", handler)
}

// Container returns the module's service container.
func (m *RealisticBenchModule) Container() mono.ServiceContainer { return m.container }

// BenchConsumerModule is a minimal consumer module for benchmarks.
// It implements Module and DependentModule interfaces.
// Note: This module only stores a single dependency container for simplicity.
// If multiple dependencies are needed, use a map[string]mono.ServiceContainer instead.
type BenchConsumerModule struct {
	name         string
	deps         []string
	depContainer mono.ServiceContainer // Only stores the last dependency set
}

// NewBenchConsumerModule creates a new consumer module with the given name and dependencies.
func NewBenchConsumerModule(name string, deps []string) *BenchConsumerModule {
	return &BenchConsumerModule{
		name: name,
		deps: deps,
	}
}

// Name returns the module name.
func (m *BenchConsumerModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchConsumerModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchConsumerModule) Stop(_ context.Context) error { return nil }

// Dependencies returns the list of module dependencies.
func (m *BenchConsumerModule) Dependencies() []string { return m.deps }

// SetDependencyServiceContainer stores the dependency's service container.
func (m *BenchConsumerModule) SetDependencyServiceContainer(_ string, container mono.ServiceContainer) {
	m.depContainer = container
}

// DepContainer returns the stored dependency container.
func (m *BenchConsumerModule) DepContainer() mono.ServiceContainer { return m.depContainer }

// =============================================================================
// Event Benchmark Modules
// =============================================================================

// BenchEventEmitterModule is a minimal event emitter module for benchmarks.
// It implements Module, EventBusAwareModule, and EventEmitterModule interfaces.
type BenchEventEmitterModule struct {
	name     string
	eventBus mono.EventBus
	eventDef mono.BaseEventDefinition
}

// NewBenchEventEmitterModule creates a new event emitter module.
func NewBenchEventEmitterModule(name string, eventDef mono.BaseEventDefinition) *BenchEventEmitterModule {
	return &BenchEventEmitterModule{
		name:     name,
		eventDef: eventDef,
	}
}

// Name returns the module name.
func (m *BenchEventEmitterModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchEventEmitterModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchEventEmitterModule) Stop(_ context.Context) error { return nil }

// SetEventBus stores the event bus for publishing events.
func (m *BenchEventEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

// EmitEvents returns the list of event definitions this module emits.
func (m *BenchEventEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{m.eventDef}
}

// EventBus returns the stored event bus.
func (m *BenchEventEmitterModule) EventBus() mono.EventBus { return m.eventBus }

// BenchEventConsumerModule is a minimal event consumer module for benchmarks.
// It implements Module and EventConsumerModule interfaces.
type BenchEventConsumerModule struct {
	name     string
	eventDef mono.BaseEventDefinition
	handler  mono.EventConsumerHandler
}

// NewBenchEventConsumerModule creates a new event consumer module.
func NewBenchEventConsumerModule(name string, eventDef mono.BaseEventDefinition, handler mono.EventConsumerHandler) *BenchEventConsumerModule {
	return &BenchEventConsumerModule{
		name:     name,
		eventDef: eventDef,
		handler:  handler,
	}
}

// NewBenchEventStreamConsumerModule creates a new event stream consumer module with JetStream config.
func NewBenchEventStreamConsumerModule(
	name string,
	eventDef mono.BaseEventDefinition,
	config mono.StreamConsumerConfig,
	handler mono.EventStreamConsumerHandler,
) *BenchEventStreamConsumerModule {
	return &BenchEventStreamConsumerModule{
		name:      name,
		eventDef:  eventDef,
		streamCfg: config,
		handler:   handler,
	}
}

// Name returns the module name.
func (m *BenchEventConsumerModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchEventConsumerModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchEventConsumerModule) Stop(_ context.Context) error { return nil }

// RegisterEventConsumers registers the event consumer handler.
func (m *BenchEventConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return registry.RegisterEventConsumer(m.eventDef, m.handler, m)
}

// BenchEventStreamConsumerModule is a minimal event stream consumer module for benchmarks.
// It implements Module and EventConsumerModule interfaces with JetStream support.
type BenchEventStreamConsumerModule struct {
	name      string
	eventDef  mono.BaseEventDefinition
	streamCfg mono.StreamConsumerConfig
	handler   mono.EventStreamConsumerHandler
}

// Name returns the module name.
func (m *BenchEventStreamConsumerModule) Name() string { return m.name }

// Start is a no-op for benchmark modules.
func (m *BenchEventStreamConsumerModule) Start(_ context.Context) error { return nil }

// Stop is a no-op for benchmark modules.
func (m *BenchEventStreamConsumerModule) Stop(_ context.Context) error { return nil }

// RegisterEventConsumers registers the event stream consumer with JetStream config.
func (m *BenchEventStreamConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return registry.RegisterEventStreamConsumer(m.eventDef, m.streamCfg, m.handler, m)
}
