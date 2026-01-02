package fsjetstream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ModuleName is the name of the fs-jetstream plugin module.
const ModuleName = "fs-jetstream"

// PluginModule implements the fs-jetstream plugin module.
// It provides FileStorage capabilities using JetStream ObjectStore.
type PluginModule struct {
	config   Config
	eventBus types.EventBus
	js       jetstream.JetStream
	logger   *slog.Logger

	// Per-bucket adapters (wrapping backends)
	buckets map[string]*FileStorageAdapter
	mu      sync.RWMutex

	// Plugin container (injected by lifecycle manager)
	container types.ServiceContainer
}

// Compile-time interface checks.
var (
	_ types.PluginModule        = (*PluginModule)(nil)
	_ types.EventBusAwareModule = (*PluginModule)(nil)
)

// New creates a new fs-jetstream module with the given configuration.
func New(config Config, opts ...Option) (*PluginModule, error) {
	// Validate config
	if len(config.Buckets) == 0 {
		return nil, fmt.Errorf("at least one bucket must be configured")
	}

	// Validate bucket names and check for duplicates
	seen := make(map[string]bool)
	for _, bucket := range config.Buckets {
		if bucket.Name == "" {
			return nil, fmt.Errorf("bucket name cannot be empty")
		}
		if seen[bucket.Name] {
			return nil, fmt.Errorf("duplicate bucket name: %s", bucket.Name)
		}
		seen[bucket.Name] = true
	}

	m := &PluginModule{
		config:  config,
		buckets: make(map[string]*FileStorageAdapter),
		logger:  slog.Default().With("module", ModuleName),
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

// Name returns the module name.
func (m *PluginModule) Name() string {
	return ModuleName
}

// SetContainer implements types.PluginModule.
// Receives the dedicated ServiceContainer from the framework during plugin initialization.
func (m *PluginModule) SetContainer(container types.ServiceContainer) {
	m.container = container
}

// Container implements types.PluginModule.
// Returns the plugin's ServiceContainer for access by dependent modules.
func (m *PluginModule) Container() types.ServiceContainer {
	return m.container
}

// SetEventBus implements types.EventBusAwareModule.
// Receives the EventBus for NATS connection access.
func (m *PluginModule) SetEventBus(bus types.EventBus) {
	m.eventBus = bus
}

// Start initializes the module and creates all configured buckets.
func (m *PluginModule) Start(ctx context.Context) error {
	// Get NATS connection from EventBus using generic interface
	provider, ok := m.eventBus.(types.EventBusWithConn[*nats.Conn])
	if !ok {
		return fmt.Errorf("EventBus does not implement EventBusWithConn[*nats.Conn]")
	}
	conn := provider.Conn()
	if conn == nil {
		return fmt.Errorf("failed to get NATS connection from EventBus")
	}

	// Create JetStream context
	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}
	m.js = js

	// Create each bucket's ObjectStore, backend, and adapter
	for _, bucketCfg := range m.config.Buckets {
		os, err := m.createOrGetObjectStore(ctx, bucketCfg)
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucketCfg.Name, err)
		}

		// Create backend (implements storage.Storage and extended interfaces)
		backend := NewJetStreamBackend(bucketCfg.Name, os, bucketCfg.TTL, m.logger)

		// Wrap backend in adapter (implements FileStoragePort)
		adapter := NewAdapter(backend, m.logger)

		m.mu.Lock()
		m.buckets[bucketCfg.Name] = adapter
		m.mu.Unlock()

		m.logger.Info("created bucket", "bucket", bucketCfg.Name)
	}

	m.logger.Info("module started", "buckets", len(m.buckets))
	return nil
}

// Stop gracefully shuts down the module.
func (m *PluginModule) Stop(_ context.Context) error {
	m.logger.Info("module stopped")
	return nil
}

// createOrGetObjectStore creates or retrieves a JetStream ObjectStore.
func (m *PluginModule) createOrGetObjectStore(ctx context.Context, cfg BucketConfig) (jetstream.ObjectStore, error) {
	osCfg := jetstream.ObjectStoreConfig{
		Bucket:      cfg.Name,
		Description: cfg.Description,
		TTL:         cfg.TTL,
		MaxBytes:    cfg.MaxBytes,
		Replicas:    cfg.Replicas,
		Compression: cfg.Compression,
	}

	if cfg.Storage == MemoryStorage {
		osCfg.Storage = jetstream.MemoryStorage
	} else {
		osCfg.Storage = jetstream.FileStorage
	}

	// Set defaults
	if osCfg.Replicas == 0 {
		osCfg.Replicas = 1
	}

	return m.js.CreateOrUpdateObjectStore(ctx, osCfg)
}

// Bucket returns a FileStoragePort for the specified bucket.
// Returns nil if the bucket does not exist.
//
// Consumer modules type-cast the plugin to *Module and call this method:
//
//	func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
//	    if alias == "storage" {
//	        m.storagePlugin = plugin.(*fsjetstream.PluginModule)
//	    }
//	}
//
//	func (m *MyModule) Start(ctx context.Context) error {
//	    m.documents = m.storagePlugin.Bucket("documents")
//	    return nil
//	}
func (m *PluginModule) Bucket(name string) FileStoragePort {
	m.mu.RLock()
	defer m.mu.RUnlock()
	adapter := m.buckets[name]
	if adapter == nil {
		return nil // Explicitly return nil interface, not nil pointer
	}
	return adapter
}

// Buckets returns a list of all configured bucket names.
func (m *PluginModule) Buckets() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.buckets))
	for name := range m.buckets {
		names = append(names, name)
	}
	return names
}

// HasBucket checks if a bucket with the given name exists.
func (m *PluginModule) HasBucket(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.buckets[name]
	return ok
}
