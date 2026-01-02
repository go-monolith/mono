package kvjetstream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PluginModule implements the kv-jetstream plugin module.
// It provides Key-Value storage capabilities using JetStream KV Store.
type PluginModule struct {
	config   Config
	eventBus types.EventBus
	js       jetstream.JetStream
	logger   *slog.Logger

	// Per-bucket adapters (wrapping backends)
	buckets map[string]*KVStorageAdapter
	mu      sync.RWMutex

	// Plugin container (injected by lifecycle manager)
	container types.ServiceContainer
}

// Compile-time interface checks.
var (
	_ types.PluginModule        = (*PluginModule)(nil)
	_ types.EventBusAwareModule = (*PluginModule)(nil)
)

// New creates a new kv-jetstream module with the given configuration.
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
		buckets: make(map[string]*KVStorageAdapter),
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

	// Create each bucket's KV store, backend, and adapter
	for _, bucketCfg := range m.config.Buckets {
		kvStore, err := m.createOrGetKVStore(ctx, bucketCfg)
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucketCfg.Name, err)
		}

		// Create backend (implements storage.Storage and extended interfaces)
		backend := NewJetStreamKVBackend(bucketCfg.Name, kvStore, bucketCfg.TTL, m.logger)

		// Wrap backend in adapter (implements KVStoragePort)
		adapter := NewKVAdapter(backend, m.logger)

		m.mu.Lock()
		m.buckets[bucketCfg.Name] = adapter
		m.mu.Unlock()

		m.logger.Info("created KV bucket", "bucket", bucketCfg.Name)
	}

	m.logger.Info("module started", "buckets", len(m.buckets))
	return nil
}

// Stop gracefully shuts down the module.
// Resources are released naturally when the module is garbage collected.
// Note: Callers should ensure no KV operations are in-flight when Stop() is called.
func (m *PluginModule) Stop(_ context.Context) error {
	m.logger.Info("module stopped")
	return nil
}

// createOrGetKVStore creates or retrieves a JetStream KeyValue store.
func (m *PluginModule) createOrGetKVStore(ctx context.Context, cfg BucketConfig) (jetstream.KeyValue, error) {
	kvCfg := jetstream.KeyValueConfig{
		Bucket:       cfg.Name,
		Description:  cfg.Description,
		MaxValueSize: cfg.MaxValueSize,
		TTL:          cfg.TTL,
		MaxBytes:     cfg.MaxBytes,
		Replicas:     cfg.Replicas,
	}

	if cfg.Storage == MemoryStorage {
		kvCfg.Storage = jetstream.MemoryStorage
	} else {
		kvCfg.Storage = jetstream.FileStorage
	}

	if cfg.Compression {
		kvCfg.Compression = true
	}

	// Set defaults
	if kvCfg.Replicas == 0 {
		kvCfg.Replicas = 1
	}

	return m.js.CreateOrUpdateKeyValue(ctx, kvCfg)
}

// Bucket returns a KVStoragePort for the specified bucket.
// Returns nil if the bucket does not exist.
//
// Consumer modules type-cast the plugin to *Module and call this method:
//
//	func (m *MyModule) SetPlugin(alias string, plugin mono.PluginModule) {
//	    if alias == "kv" {
//	        m.kvPlugin = plugin.(*kvjetstream.PluginModule)
//	    }
//	}
//
//	func (m *MyModule) Start(ctx context.Context) error {
//	    m.cache = m.kvPlugin.Bucket("cache")
//	    return nil
//	}
func (m *PluginModule) Bucket(name string) KVStoragePort {
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
