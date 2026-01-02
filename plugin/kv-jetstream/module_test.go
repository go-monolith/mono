package kvjetstream

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/storage"
	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Mock Implementations
// =============================================================================

// mockServiceContainer implements types.ServiceContainer for testing.
type mockServiceContainer struct {
	bound bool
}

func (m *mockServiceContainer) BindModule(_ types.Module) error                  { m.bound = true; return nil }
func (m *mockServiceContainer) SetEventBus(_ types.EventBus)                     {}
func (m *mockServiceContainer) SetQueueGroupOptimisticWindow(_ time.Duration)    {}
func (m *mockServiceContainer) SetMiddlewareChain(_ types.MiddlewareChainRunner) {}
func (m *mockServiceContainer) RegisterChannelService(_ string, _ chan *types.Msg, _ chan *types.Msg) error {
	return nil
}
func (m *mockServiceContainer) RegisterRequestReplyService(_ string, _ types.RequestReplyHandler) error {
	return nil
}
func (m *mockServiceContainer) RegisterQueueGroupService(_ string, _ ...types.QGHP) error { return nil }
func (m *mockServiceContainer) RegisterStreamConsumerService(_ string, _ types.StreamConsumerConfig, _ types.StreamConsumerHandler) error {
	return nil
}
func (m *mockServiceContainer) GetChannelService(_, _ string) (chan *types.Msg, chan *types.Msg, error) {
	return nil, nil, nil
}
func (m *mockServiceContainer) MustGetChannelService(_, _ string) (chan *types.Msg, chan *types.Msg) {
	return nil, nil
}
func (m *mockServiceContainer) GetRequestReplyService(_ string) (types.RequestReplyServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) GetQueueGroupService(_ string) (types.QueueGroupServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) GetStreamConsumerService(_ string) (types.StreamConsumerServiceClient, error) {
	return nil, nil
}
func (m *mockServiceContainer) Has(_ string) bool                     { return false }
func (m *mockServiceContainer) Unregister(_ string) error             { return nil }
func (m *mockServiceContainer) Entries() []*types.ServiceEntry        { return nil }
func (m *mockServiceContainer) StartChannelRouters(_ context.Context) {}

var _ types.ServiceContainer = (*mockServiceContainer)(nil)

// mockBackend implements storage.Storage and extended interfaces for testing.
type mockBackend struct {
	bucketName string
	data       map[string]*mockEntry
	mu         sync.RWMutex
	revision   uint64

	// Error injection
	putErr    error
	getErr    error
	createErr error
	updateErr error
	deleteErr error
	purgeErr  error
	keysErr   error
	watchErr  error
	statusErr error
}

// mockEntry stores entry data for mock backend.
type mockEntry struct {
	value     []byte
	revision  uint64
	timestamp time.Time
}

func newMockBackend(bucketName string) *mockBackend {
	return &mockBackend{
		bucketName: bucketName,
		data:       make(map[string]*mockEntry),
		revision:   0,
	}
}

// =============================================================================
// storage.StorageWithBucket Implementation
// =============================================================================

func (m *mockBackend) BucketName() string {
	return m.bucketName
}

// =============================================================================
// storage.Storage Implementation
// =============================================================================

func (m *mockBackend) GetWithContext(_ context.Context, key string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok {
		return nil, nil // Key not found returns nil, nil per storage.Storage contract
	}
	return entry.value, nil
}

func (m *mockBackend) Get(key string) ([]byte, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockBackend) SetWithContext(_ context.Context, key string, val []byte, _ time.Duration) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revision++
	m.data[key] = &mockEntry{
		value:     val,
		revision:  m.revision,
		timestamp: time.Now(),
	}
	return nil
}

func (m *mockBackend) Set(key string, val []byte, exp time.Duration) error {
	return m.SetWithContext(context.Background(), key, val, exp)
}

func (m *mockBackend) DeleteWithContext(_ context.Context, key string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *mockBackend) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockBackend) ResetWithContext(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]*mockEntry)
	return nil
}

func (m *mockBackend) Reset() error {
	return m.ResetWithContext(context.Background())
}

func (m *mockBackend) Close() error {
	return nil
}

// =============================================================================
// storage.StorageWithRevision Implementation
// =============================================================================

func (m *mockBackend) CreateWithContext(_ context.Context, key string, val []byte, _ time.Duration) (uint64, error) {
	if m.createErr != nil {
		return 0, m.createErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return 0, ErrKeyExists
	}
	m.revision++
	m.data[key] = &mockEntry{
		value:     val,
		revision:  m.revision,
		timestamp: time.Now(),
	}
	return m.revision, nil
}

func (m *mockBackend) Create(key string, val []byte, exp time.Duration) (uint64, error) {
	return m.CreateWithContext(context.Background(), key, val, exp)
}

func (m *mockBackend) UpdateWithContext(_ context.Context, key string, val []byte, _ time.Duration, revision uint64) (uint64, error) {
	if m.updateErr != nil {
		return 0, m.updateErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.data[key]
	if !ok {
		return 0, ErrKeyNotFound
	}
	if entry.revision != revision {
		return 0, ErrRevisionMismatch
	}
	m.revision++
	m.data[key] = &mockEntry{
		value:     val,
		revision:  m.revision,
		timestamp: time.Now(),
	}
	return m.revision, nil
}

func (m *mockBackend) Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error) {
	return m.UpdateWithContext(context.Background(), key, val, exp, revision)
}

func (m *mockBackend) PurgeWithContext(_ context.Context, key string) error {
	if m.purgeErr != nil {
		return m.purgeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *mockBackend) Purge(key string) error {
	return m.PurgeWithContext(context.Background(), key)
}

func (m *mockBackend) GetEntryWithContext(_ context.Context, key string) (*storage.Entry, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return &storage.Entry{
		Bucket:    m.bucketName,
		Key:       key,
		Value:     entry.value,
		Revision:  entry.revision,
		Timestamp: entry.timestamp,
		Operation: storage.KeyOperationPut,
	}, nil
}

func (m *mockBackend) GetEntry(key string) (*storage.Entry, error) {
	return m.GetEntryWithContext(context.Background(), key)
}

func (m *mockBackend) PutWithRevisionWithContext(_ context.Context, key string, val []byte, _ time.Duration) (uint64, error) {
	if m.putErr != nil {
		return 0, m.putErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revision++
	m.data[key] = &mockEntry{
		value:     val,
		revision:  m.revision,
		timestamp: time.Now(),
	}
	return m.revision, nil
}

func (m *mockBackend) PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error) {
	return m.PutWithRevisionWithContext(context.Background(), key, val, exp)
}

// =============================================================================
// storage.StorageWithKeys Implementation
// =============================================================================

func (m *mockBackend) KeysWithContext(_ context.Context) ([]string, error) {
	if m.keysErr != nil {
		return nil, m.keysErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *mockBackend) Keys() ([]string, error) {
	return m.KeysWithContext(context.Background())
}

// =============================================================================
// storage.StorageWithWatch Implementation
// =============================================================================

func (m *mockBackend) WatchWithContext(_ context.Context, _ string, _ ...storage.WatchOption) (storage.KeyWatcher, error) {
	if m.watchErr != nil {
		return nil, m.watchErr
	}
	return &mockStorageKeyWatcher{updates: make(chan *storage.Entry)}, nil
}

func (m *mockBackend) Watch(pattern string, opts ...storage.WatchOption) (storage.KeyWatcher, error) {
	return m.WatchWithContext(context.Background(), pattern, opts...)
}

// =============================================================================
// storage.StorageWithStatus Implementation
// =============================================================================

func (m *mockBackend) StatusWithContext(_ context.Context) (*storage.BucketStatus, error) {
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &storage.BucketStatus{
		Bucket: m.bucketName,
		Values: uint64(len(m.data)),
	}, nil
}

func (m *mockBackend) Status() (*storage.BucketStatus, error) {
	return m.StatusWithContext(context.Background())
}

// Compile-time interface checks.
var (
	_ storage.Storage             = (*mockBackend)(nil)
	_ storage.StorageWithBucket   = (*mockBackend)(nil)
	_ storage.StorageWithWatch    = (*mockBackend)(nil)
	_ storage.StorageWithRevision = (*mockBackend)(nil)
	_ storage.StorageWithKeys     = (*mockBackend)(nil)
	_ storage.StorageWithStatus   = (*mockBackend)(nil)
)

// mockStorageKeyWatcher implements storage.KeyWatcher for testing.
type mockStorageKeyWatcher struct {
	updates chan *storage.Entry
	stopped bool
}

func (m *mockStorageKeyWatcher) Updates() <-chan *storage.Entry {
	return m.updates
}

func (m *mockStorageKeyWatcher) Stop() error {
	if !m.stopped {
		m.stopped = true
		close(m.updates)
	}
	return nil
}

var _ storage.KeyWatcher = (*mockStorageKeyWatcher)(nil)

// mockKeyWatcher implements KeyWatcher for testing (plugin-level interface).
type mockKeyWatcher struct {
	updates chan *KVEntry
	stopped bool
}

func (m *mockKeyWatcher) Updates() <-chan *KVEntry {
	return m.updates
}

func (m *mockKeyWatcher) Stop() error {
	if !m.stopped {
		m.stopped = true
		close(m.updates)
	}
	return nil
}

var _ KeyWatcher = (*mockKeyWatcher)(nil)

// =============================================================================
// Test Helpers
// =============================================================================

// setupTestNATSWithJetStream creates an embedded NATS server with JetStream for testing.
func setupTestNATSWithJetStream(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		Port:      -1, // Random port
		StoreDir:  t.TempDir(),
	}

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create NATS server: %v", err)
	}

	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})

	conn, err := nats.Connect(s.ClientURL(),
		nats.Name("test-client"),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}

	return conn, js
}

// createTestKeyValue creates a KeyValue store for testing.
func createTestKeyValue(t *testing.T, js jetstream.JetStream, cfg BucketConfig) jetstream.KeyValue {
	t.Helper()

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

	if kvCfg.Replicas == 0 {
		kvCfg.Replicas = 1
	}

	kv, err := js.CreateKeyValue(context.Background(), kvCfg)
	if err != nil {
		t.Fatalf("failed to create KeyValue: %v", err)
	}

	return kv
}

// =============================================================================
// Module Constructor Tests
// =============================================================================

func TestNew_ValidConfig(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "cache", Description: "Test bucket"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module == nil {
		t.Fatal("expected module, got nil")
	}

	if module.Name() != ModuleName {
		t.Errorf("expected name %q, got %q", ModuleName, module.Name())
	}
}

func TestNew_MultipleBuckets(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "cache"},
			{Name: "sessions"},
			{Name: "config"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module == nil {
		t.Fatal("expected module, got nil")
	}

	if len(module.config.Buckets) != 3 {
		t.Errorf("expected 3 buckets in config, got %d", len(module.config.Buckets))
	}
}

func TestNew_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "empty buckets",
			config:  Config{Buckets: []BucketConfig{}},
			wantErr: "at least one bucket must be configured",
		},
		{
			name:    "nil buckets",
			config:  Config{},
			wantErr: "at least one bucket must be configured",
		},
		{
			name:    "empty bucket name",
			config:  Config{Buckets: []BucketConfig{{Name: ""}}},
			wantErr: "bucket name cannot be empty",
		},
		{
			name: "duplicate bucket name",
			config: Config{Buckets: []BucketConfig{
				{Name: "cache"},
				{Name: "cache"},
			}},
			wantErr: "duplicate bucket name: cache",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.config)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestNew_WithLogger(t *testing.T) {
	logger := slog.Default().With("test", "value")
	config := Config{
		Buckets: []BucketConfig{{Name: "cache"}},
	}

	module, err := New(config, WithLogger(logger))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module.logger != logger {
		t.Error("expected custom logger to be set")
	}
}

// =============================================================================
// Module Interface Tests
// =============================================================================

func TestModule_Name(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	if module.Name() != "kv-jetstream" {
		t.Errorf("expected name 'kv-jetstream', got %q", module.Name())
	}
}

func TestModule_SetContainer(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})
	container := &mockServiceContainer{}

	module.SetContainer(container)

	if module.container != container {
		t.Error("expected container to be set")
	}
}

func TestModule_Container(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})
	container := &mockServiceContainer{}

	module.SetContainer(container)

	if module.Container() != container {
		t.Error("expected Container() to return set container")
	}
}

func TestModule_SetEventBus(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	// SetEventBus should not panic with nil
	module.SetEventBus(nil)

	if module.eventBus != nil {
		t.Error("expected eventBus to be nil")
	}
}

// =============================================================================
// Bucket Access Tests
// =============================================================================

func TestModule_Bucket_BeforeStart(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	bucket := module.Bucket("cache")
	if bucket != nil {
		t.Error("expected nil bucket before Start()")
	}
}

func TestModule_Bucket_NotFound(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	bucket := module.Bucket("nonexistent")
	if bucket != nil {
		t.Error("expected nil for nonexistent bucket")
	}
}

func TestModule_Buckets_BeforeStart(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{
		{Name: "cache"},
		{Name: "sessions"},
	}})

	buckets := module.Buckets()
	if len(buckets) != 0 {
		t.Errorf("expected 0 buckets before Start(), got %d", len(buckets))
	}
}

func TestModule_HasBucket_BeforeStart(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	if module.HasBucket("cache") {
		t.Error("expected HasBucket to return false before Start()")
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestModule_ConcurrentBucketAccess(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	// Simulate adding a bucket for concurrent access test
	module.mu.Lock()
	backend := newMockBackend("cache")
	module.buckets["cache"] = NewKVAdapter(backend, slog.Default())
	module.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_ = module.Bucket("cache")
		}()
		go func() {
			defer wg.Done()
			_ = module.Buckets()
		}()
		go func() {
			defer wg.Done()
			_ = module.HasBucket("cache")
		}()
	}
	wg.Wait()
}

// =============================================================================
// Type Tests
// =============================================================================

func TestKVEntry_Fields(t *testing.T) {
	now := time.Now()
	entry := KVEntry{
		Bucket:    "cache",
		Key:       "user:123",
		Value:     []byte("test data"),
		Revision:  42,
		Timestamp: now,
		Operation: KeyOperationPut,
	}

	if entry.Bucket != "cache" {
		t.Errorf("expected bucket 'cache', got %q", entry.Bucket)
	}
	if entry.Key != "user:123" {
		t.Errorf("expected key 'user:123', got %q", entry.Key)
	}
	if string(entry.Value) != "test data" {
		t.Errorf("expected value 'test data', got %q", string(entry.Value))
	}
	if entry.Revision != 42 {
		t.Errorf("expected revision 42, got %d", entry.Revision)
	}
	if !entry.Timestamp.Equal(now) {
		t.Errorf("expected timestamp %v, got %v", now, entry.Timestamp)
	}
	if entry.Operation != KeyOperationPut {
		t.Errorf("expected operation KeyOperationPut, got %v", entry.Operation)
	}
}

func TestBucketStatus_Fields(t *testing.T) {
	status := BucketStatus{
		Bucket:       "cache",
		Values:       100,
		TTL:          24 * time.Hour,
		BackingStore: "KV_cache",
		Bytes:        1024,
	}

	if status.Bucket != "cache" {
		t.Errorf("expected bucket 'cache', got %q", status.Bucket)
	}
	if status.Values != 100 {
		t.Errorf("expected values 100, got %d", status.Values)
	}
	if status.TTL != 24*time.Hour {
		t.Errorf("expected TTL 24h, got %v", status.TTL)
	}
	if status.BackingStore != "KV_cache" {
		t.Errorf("expected backing store 'KV_cache', got %q", status.BackingStore)
	}
	if status.Bytes != 1024 {
		t.Errorf("expected bytes 1024, got %d", status.Bytes)
	}
}

func TestBucketConfig_Defaults(t *testing.T) {
	cfg := BucketConfig{
		Name: "cache",
	}
	_ = cfg.Name // Use Name field to satisfy linter

	// Verify default values (zero values)
	if cfg.MaxValueSize != 0 {
		t.Error("expected default MaxValueSize to be 0")
	}
	if cfg.TTL != 0 {
		t.Error("expected default TTL to be 0")
	}
	if cfg.MaxBytes != 0 {
		t.Error("expected default MaxBytes to be 0")
	}
	if cfg.Replicas != 0 {
		t.Error("expected default Replicas to be 0")
	}
	if cfg.Storage != FileStorage {
		t.Error("expected default Storage to be FileStorage")
	}
	if cfg.Compression {
		t.Error("expected default Compression to be false")
	}
}

func TestStorageType_Constants(t *testing.T) {
	if FileStorage != 0 {
		t.Errorf("expected FileStorage to be 0, got %d", FileStorage)
	}
	if MemoryStorage != 1 {
		t.Errorf("expected MemoryStorage to be 1, got %d", MemoryStorage)
	}
}

func TestKeyOperation_Constants(t *testing.T) {
	if KeyOperationPut != 0 {
		t.Errorf("expected KeyOperationPut to be 0, got %d", KeyOperationPut)
	}
	if KeyOperationDelete != 1 {
		t.Errorf("expected KeyOperationDelete to be 1, got %d", KeyOperationDelete)
	}
	if KeyOperationPurge != 2 {
		t.Errorf("expected KeyOperationPurge to be 2, got %d", KeyOperationPurge)
	}
}

// =============================================================================
// Error Tests
// =============================================================================

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrKeyNotFound", ErrKeyNotFound, "key not found"},
		{"ErrKeyExists", ErrKeyExists, "key already exists"},
		{"ErrRevisionMismatch", ErrRevisionMismatch, "revision mismatch"},
		{"ErrBucketNotFound", ErrBucketNotFound, "bucket not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.msg {
				t.Errorf("expected message %q, got %q", tt.msg, tt.err.Error())
			}
		})
	}
}

// =============================================================================
// Options Tests
// =============================================================================

func TestDeleteOptions(t *testing.T) {
	opts := &DeleteOptions{}
	WithDeleteRevision(42)(opts)

	if opts.Revision != 42 {
		t.Errorf("expected revision 42, got %d", opts.Revision)
	}
}

func TestWatchOptions(t *testing.T) {
	tests := []struct {
		name   string
		opt    WatchOption
		check  func(*WatchOptions) bool
		expect bool
	}{
		{
			name:   "WithUpdatesOnly",
			opt:    WithUpdatesOnly(),
			check:  func(o *WatchOptions) bool { return o.UpdatesOnly },
			expect: true,
		},
		{
			name:   "WithIgnoreDeletes",
			opt:    WithIgnoreDeletes(),
			check:  func(o *WatchOptions) bool { return o.IgnoreDeletes },
			expect: true,
		},
		{
			name:   "WithMetaOnly",
			opt:    WithMetaOnly(),
			check:  func(o *WatchOptions) bool { return o.MetaOnly },
			expect: true,
		},
		{
			name:   "WithResumeFromRevision",
			opt:    WithResumeFromRevision(100),
			check:  func(o *WatchOptions) bool { return o.ResumeFromRevision == 100 },
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &WatchOptions{}
			tt.opt(opts)
			if tt.check(opts) != tt.expect {
				t.Errorf("option %s did not set expected value", tt.name)
			}
		})
	}
}

// =============================================================================
// Module Stop Test
// =============================================================================

func TestModule_Stop(t *testing.T) {
	module, _ := New(Config{Buckets: []BucketConfig{{Name: "cache"}}})

	err := module.Stop(context.Background())
	if err != nil {
		t.Errorf("expected no error from Stop(), got: %v", err)
	}
}

// =============================================================================
// Module Start() Integration Tests
// =============================================================================

// realEventBus wraps a NATS connection to provide a real EventBus for testing.
type realEventBus struct {
	conn *nats.Conn
}

func newRealEventBus(conn *nats.Conn) *realEventBus {
	return &realEventBus{conn: conn}
}

// Implement types.EventBus interface with proper types.
func (r *realEventBus) Publish(string, []byte) error                              { return nil }
func (r *realEventBus) PublishMsg(*types.Msg) error                               { return nil }
func (r *realEventBus) Request(string, []byte, time.Duration) (*types.Msg, error) { return nil, nil }
func (r *realEventBus) RequestWithContext(context.Context, string, []byte) (*types.Msg, error) {
	return nil, nil
}
func (r *realEventBus) RequestMsgWithContext(context.Context, *types.Msg) (*types.Msg, error) {
	return nil, nil
}
func (r *realEventBus) Subscribe(string, types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}
func (r *realEventBus) SubscribeSync(string) (types.Subscription, error) { return nil, nil }
func (r *realEventBus) QueueSubscribe(string, string, types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}
func (r *realEventBus) QueueSubscribeSync(string, string) (types.Subscription, error) {
	return nil, nil
}
func (r *realEventBus) ChanSubscribe(string, chan *types.Msg) (types.Subscription, error) {
	return nil, nil
}
func (r *realEventBus) EventStream() (types.EventStream, error) {
	return nil, nil
}
func (r *realEventBus) SetRuntimeContext(context.Context) {}

// Conn returns the underlying NATS connection.
func (r *realEventBus) Conn() *nats.Conn { return r.conn }

// fakeEventBusNoConn simulates an EventBus without Conn() method.
type fakeEventBusNoConn struct{}

func (f *fakeEventBusNoConn) Publish(string, []byte) error { return nil }
func (f *fakeEventBusNoConn) PublishMsg(*types.Msg) error  { return nil }
func (f *fakeEventBusNoConn) Request(string, []byte, time.Duration) (*types.Msg, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) RequestWithContext(context.Context, string, []byte) (*types.Msg, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) RequestMsgWithContext(context.Context, *types.Msg) (*types.Msg, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) Subscribe(string, types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) SubscribeSync(string) (types.Subscription, error) { return nil, nil }
func (f *fakeEventBusNoConn) QueueSubscribe(string, string, types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) QueueSubscribeSync(string, string) (types.Subscription, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) ChanSubscribe(string, chan *types.Msg) (types.Subscription, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) EventStream() (types.EventStream, error) {
	return nil, nil
}
func (f *fakeEventBusNoConn) SetRuntimeContext(context.Context) {}

var _ types.EventBus = (*fakeEventBusNoConn)(nil)

// fakeEventBusNilConn simulates an EventBus with Conn() returning nil.
type fakeEventBusNilConn struct {
	fakeEventBusNoConn
}

func (f *fakeEventBusNilConn) Conn() *nats.Conn { return nil }

var _ types.EventBusWithConn[*nats.Conn] = (*fakeEventBusNilConn)(nil)

// TestModule_Start_Success tests the Start() method with a real NATS server.
func TestModule_Start_Success(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:        "cache",
				Description: "Cache bucket",
				MaxBytes:    10 * 1024 * 1024,
				Storage:     MemoryStorage,
				Replicas:    1,
			},
			{
				Name:        "sessions",
				Description: "Session storage",
				Storage:     FileStorage,
				Replicas:    1,
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	// Set EventBus with real NATS connection
	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Verify buckets were created
	if !module.HasBucket("cache") {
		t.Error("expected 'cache' bucket to exist")
	}
	if !module.HasBucket("sessions") {
		t.Error("expected 'sessions' bucket to exist")
	}

	// Verify bucket count
	buckets := module.Buckets()
	if len(buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(buckets))
	}

	// Verify we can get bucket adapters
	cacheBucket := module.Bucket("cache")
	if cacheBucket == nil {
		t.Error("expected non-nil cache bucket adapter")
	}

	sessionsBucket := module.Bucket("sessions")
	if sessionsBucket == nil {
		t.Error("expected non-nil sessions bucket adapter")
	}

	// Test Stop()
	err = module.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
}

// TestModule_Start_NoEventBusWithConn tests Start() when EventBus doesn't implement EventBusWithConn.
func TestModule_Start_NoEventBusWithConn(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "cache", Storage: MemoryStorage, Replicas: 1},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	// Set EventBus that doesn't implement EventBusWithConn
	module.SetEventBus(&fakeEventBusNoConn{})

	ctx := context.Background()
	err = module.Start(ctx)
	if err == nil {
		t.Fatal("expected error from Start()")
	}

	expectedErr := "EventBus does not implement EventBusWithConn[*nats.Conn]"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

// TestModule_Start_NilNATSConnection tests Start() when Conn() returns nil.
func TestModule_Start_NilNATSConnection(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "cache", Storage: MemoryStorage, Replicas: 1},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	// Set EventBus with nil connection
	module.SetEventBus(&fakeEventBusNilConn{})

	ctx := context.Background()
	err = module.Start(ctx)
	if err == nil {
		t.Fatal("expected error from Start()")
	}

	expectedErr := "failed to get NATS connection from EventBus"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

// TestModule_Start_WithCompression tests Start() with compression enabled.
func TestModule_Start_WithCompression(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:        "compressed-cache",
				Description: "Compressed cache bucket",
				Storage:     MemoryStorage,
				Replicas:    1,
				Compression: true,
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Verify bucket was created
	if !module.HasBucket("compressed-cache") {
		t.Error("expected 'compressed-cache' bucket to exist")
	}

	bucket := module.Bucket("compressed-cache")
	if bucket == nil {
		t.Error("expected non-nil bucket adapter")
	}
}

// TestModule_Start_WithTTL tests Start() with TTL configuration.
func TestModule_Start_WithTTL(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:     "ttl-cache",
				Storage:  MemoryStorage,
				Replicas: 1,
				TTL:      1 * time.Hour,
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	if !module.HasBucket("ttl-cache") {
		t.Error("expected 'ttl-cache' bucket to exist")
	}
}

// TestModule_Start_WithMaxValueSize tests Start() with MaxValueSize configuration.
func TestModule_Start_WithMaxValueSize(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:         "limited-values",
				Storage:      MemoryStorage,
				Replicas:     1,
				MaxValueSize: 1024, // 1KB max value size
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	if !module.HasBucket("limited-values") {
		t.Error("expected 'limited-values' bucket to exist")
	}
}

// TestModule_Start_DefaultReplicas tests that Replicas defaults to 1 when not set.
func TestModule_Start_DefaultReplicas(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:    "default-replicas",
				Storage: MemoryStorage,
				// Replicas not set, should default to 1
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	if !module.HasBucket("default-replicas") {
		t.Error("expected 'default-replicas' bucket to exist")
	}
}

// TestModule_Start_MultipleBuckets_AllStorageTypes tests Start() with different storage types.
func TestModule_Start_MultipleBuckets_AllStorageTypes(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{Name: "memory-bucket", Storage: MemoryStorage, Replicas: 1},
			{Name: "file-bucket", Storage: FileStorage, Replicas: 1},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Verify both buckets created
	if !module.HasBucket("memory-bucket") {
		t.Error("expected 'memory-bucket' to exist")
	}
	if !module.HasBucket("file-bucket") {
		t.Error("expected 'file-bucket' to exist")
	}

	// Verify bucket operations work
	memBucket := module.Bucket("memory-bucket")
	if memBucket == nil {
		t.Fatal("expected non-nil memory bucket")
	}

	fileBucket := module.Bucket("file-bucket")
	if fileBucket == nil {
		t.Fatal("expected non-nil file bucket")
	}

	// Test basic operations on memory bucket
	ctx = context.Background()
	_, err = memBucket.PutWithRevisionWithContext(ctx, "test-key", []byte("test-value"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	entry, err := memBucket.GetEntryWithContext(ctx, "test-key")
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if string(entry.Value) != "test-value" {
		t.Errorf("expected 'test-value', got %q", string(entry.Value))
	}
}

// TestModule_BucketOperations_AfterStart tests bucket operations after Start().
func TestModule_BucketOperations_AfterStart(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{Name: "ops-bucket", Storage: MemoryStorage, Replicas: 1},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	module.SetEventBus(newRealEventBus(conn))

	ctx := context.Background()
	err = module.Start(ctx)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	bucket := module.Bucket("ops-bucket")
	if bucket == nil {
		t.Fatal("expected non-nil bucket")
	}

	// Test PutWithRevision and GetEntry
	_, err = bucket.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	entry, err := bucket.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected 'value1', got %q", string(entry.Value))
	}

	// Test Create (returns revision)
	rev, err := bucket.CreateWithContext(ctx, "key2", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision from Create")
	}

	// Test Delete
	err = bucket.DeleteWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key1 is deleted (GetEntry returns error for non-existent keys)
	_, err = bucket.GetEntryWithContext(ctx, "key1")
	if err == nil {
		t.Error("expected error when getting deleted key")
	}

	// Test Keys
	keys, err := bucket.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	// Should have at least key2
	if len(keys) == 0 {
		t.Error("expected at least one key")
	}

	// Test Status
	status, err := bucket.StatusWithContext(ctx)
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status == nil {
		t.Error("expected non-nil status")
	}
}
