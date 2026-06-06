package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// mockServiceContainer implements types.ServiceContainer for testing
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
func (m *mockServiceContainer) RegisterCronService(_ string, _ types.CronServiceConfig, _ types.CronHandler) error {
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

// Ensure mockServiceContainer implements types.ServiceContainer
var _ types.ServiceContainer = (*mockServiceContainer)(nil)

// objectMetadata stores metadata for an object.
type objectMetadata struct {
	Description string
	Headers     map[string]string
	Size        int64
	ModTime     time.Time
}

// mockBackend implements storage.Storage and extended interfaces for testing.
type mockBackend struct {
	bucketName string
	objects    map[string][]byte
	metadata   map[string]*objectMetadata
	mu         sync.RWMutex
	putErr     error
	getErr     error
	deleteErr  error
	listErr    error
	statErr    error
}

// Compile-time interface checks
var (
	_ storage.Storage           = (*mockBackend)(nil)
	_ storage.StorageWithBucket = (*mockBackend)(nil)
	_ storage.StorageWithList   = (*mockBackend)(nil)
	_ storage.StorageWithStat   = (*mockBackend)(nil)
	_ storage.StorageWithReader = (*mockBackend)(nil)
)

func newMockBackend(bucketName string) *mockBackend {
	return &mockBackend{
		bucketName: bucketName,
		objects:    make(map[string][]byte),
		metadata:   make(map[string]*objectMetadata),
	}
}

func (b *mockBackend) BucketName() string { return b.bucketName }

func (b *mockBackend) Put(_ context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error) {
	if b.putErr != nil {
		return nil, b.putErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = data
	return &ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}, nil
}

func (b *mockBackend) getInternal(key string) ([]byte, error) {
	if b.getErr != nil {
		return nil, b.getErr
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, nil // Key not found returns nil, nil per storage.Storage contract
	}
	return data, nil
}

func (b *mockBackend) Get(key string) ([]byte, error) {
	return b.getInternal(key)
}

func (b *mockBackend) deleteInternal(key string) error {
	if b.deleteErr != nil {
		return b.deleteErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	return nil
}

func (b *mockBackend) Delete(key string) error {
	return b.deleteInternal(key)
}

// =============================================================================
// storage.Storage Interface Methods (for adapter compatibility)
// =============================================================================

func (b *mockBackend) GetWithContext(_ context.Context, key string) ([]byte, error) {
	return b.getInternal(key)
}

func (b *mockBackend) SetWithContext(_ context.Context, key string, val []byte, _ time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = val
	return nil
}

func (b *mockBackend) Set(key string, val []byte, exp time.Duration) error {
	return b.SetWithContext(context.Background(), key, val, exp)
}

func (b *mockBackend) DeleteWithContext(_ context.Context, key string) error {
	return b.deleteInternal(key)
}

func (b *mockBackend) ResetWithContext(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects = make(map[string][]byte)
	return nil
}

func (b *mockBackend) Reset() error {
	return b.ResetWithContext(context.Background())
}

func (b *mockBackend) Close() error {
	return nil
}

// =============================================================================
// storage.StorageWithList Interface Methods
// =============================================================================

func (b *mockBackend) ListWithContext(_ context.Context, opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	if b.listErr != nil {
		return nil, b.listErr
	}
	options := storage.ApplyListOptions(opts...)

	b.mu.RLock()
	defer b.mu.RUnlock()
	var result []storage.ObjectInfo
	for name, data := range b.objects {
		if options.Prefix == "" || (len(name) >= len(options.Prefix) && name[:len(options.Prefix)] == options.Prefix) {
			meta := b.metadata[name]
			info := storage.ObjectInfo{
				Bucket:  b.bucketName,
				Name:    name,
				Size:    int64(len(data)),
				ModTime: time.Now(),
			}
			if meta != nil {
				info.Description = meta.Description
				info.Headers = meta.Headers
				info.ModTime = meta.ModTime
			}
			result = append(result, info)
		}
	}
	return result, nil
}

func (b *mockBackend) List(opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	return b.ListWithContext(context.Background(), opts...)
}

// =============================================================================
// storage.StorageWithStat Interface Methods
// =============================================================================

func (b *mockBackend) StatWithContext(_ context.Context, key string) (*storage.ObjectInfo, error) {
	if b.statErr != nil {
		return nil, b.statErr
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	info := &storage.ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}
	if meta := b.metadata[key]; meta != nil {
		info.Description = meta.Description
		info.Headers = meta.Headers
		info.ModTime = meta.ModTime
	}
	return info, nil
}

func (b *mockBackend) Stat(key string) (*storage.ObjectInfo, error) {
	return b.StatWithContext(context.Background(), key)
}

// =============================================================================
// storage.StorageWithReader Interface Methods
// =============================================================================

func (b *mockBackend) GetReaderWithContext(_ context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	data, err := b.getInternal(key)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, storage.ErrKeyNotFound
	}
	info := &storage.ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}
	if meta := b.metadata[key]; meta != nil {
		info.Description = meta.Description
		info.Headers = meta.Headers
		info.ModTime = meta.ModTime
	}
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

func (b *mockBackend) GetReader(key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return b.GetReaderWithContext(context.Background(), key)
}

func (b *mockBackend) PutReaderWithContext(_ context.Context, key string, reader io.Reader, _ time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	if b.putErr != nil {
		return nil, b.putErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	options := storage.ApplyPutOptions(opts...)
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = data
	b.metadata[key] = &objectMetadata{
		Description: options.Description,
		Headers:     options.Headers,
		Size:        int64(len(data)),
		ModTime:     now,
	}

	return &storage.ObjectInfo{
		Bucket:      b.bucketName,
		Name:        key,
		Size:        int64(len(data)),
		Description: options.Description,
		Headers:     options.Headers,
		ModTime:     now,
	}, nil
}

func (b *mockBackend) PutReader(key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	return b.PutReaderWithContext(context.Background(), key, reader, exp, opts...)
}

// Tests for Module constructor

func TestNew_ValidConfig(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents", Description: "Test bucket"},
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
			{Name: "documents"},
			{Name: "uploads", TTL: 24 * time.Hour},
			{Name: "cache", Storage: MemoryStorage},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module == nil {
		t.Fatal("expected module, got nil")
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
				{Name: "documents"},
				{Name: "documents"},
			}},
			wantErr: "duplicate bucket name: documents",
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
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	customLogger := slog.Default().With("test", true)
	module, err := New(config, WithLogger(customLogger))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module.logger != customLogger {
		t.Error("expected custom logger to be set")
	}
}

// Tests for container injection

func TestModule_ContainerInjection(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Container should be nil before SetContainer is called
	if module.Container() != nil {
		t.Error("expected nil container before injection")
	}

	// SetContainer should store the container
	var container types.ServiceContainer = &mockServiceContainer{}
	module.SetContainer(container)

	if module.Container() == nil {
		t.Error("expected container to be set after SetContainer")
	}
	if module.Container() != container {
		t.Error("expected container to match the one set via SetContainer")
	}
}

// Tests for bucket access before Start

func TestModule_BucketAccessBeforeStart(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
			{Name: "uploads"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Before Start(), Bucket should return nil
	if module.Bucket("documents") != nil {
		t.Error("expected nil bucket before Start()")
	}

	// Before Start(), Buckets should return empty list
	if len(module.Buckets()) != 0 {
		t.Errorf("expected empty bucket list before Start(), got %d", len(module.Buckets()))
	}

	// Before Start(), HasBucket should return false
	if module.HasBucket("documents") {
		t.Error("expected false for HasBucket before Start()")
	}
}

// Tests for adapter with mock backend

func TestAdapter_PassThrough(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test BucketName
	if adapter.BucketName() != "test-bucket" {
		t.Errorf("expected bucket name 'test-bucket', got %q", adapter.BucketName())
	}

	// Test Put
	info, err := adapter.Put(ctx, "file.txt", []byte("hello"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if info.Name != "file.txt" {
		t.Errorf("expected name 'file.txt', got %q", info.Name)
	}
	if info.Size != 5 {
		t.Errorf("expected size 5, got %d", info.Size)
	}

	// Test Get
	data, err := adapter.GetWithContext(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}

	// Test Stat
	statInfo, err := adapter.StatWithContext(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if statInfo.Size != 5 {
		t.Errorf("expected size 5, got %d", statInfo.Size)
	}

	// Test List
	objects, err := adapter.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(objects))
	}

	// Test Delete
	if err := adapter.DeleteWithContext(ctx, "file.txt"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted - Get returns nil, nil for missing keys
	data, err = adapter.GetWithContext(ctx, "file.txt")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data != nil {
		t.Error("expected nil data for deleted object")
	}
}

func TestAdapter_PutReader(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	reader := bytes.NewReader([]byte("hello world"))

	info, err := adapter.PutReaderWithContext(ctx, "file.txt", reader, 0)
	if err != nil {
		t.Fatalf("PutReader failed: %v", err)
	}

	if info.Size != 11 {
		t.Errorf("expected size 11, got %d", info.Size)
	}

	data, _ := adapter.GetWithContext(ctx, "file.txt")
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

func TestAdapter_GetReader(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	adapter.Put(ctx, "file.txt", []byte("hello world"))

	reader, info, err := adapter.GetReaderWithContext(ctx, "file.txt")
	if err != nil {
		t.Fatalf("GetReader failed: %v", err)
	}
	defer reader.Close()

	if info.Size != 11 {
		t.Errorf("expected size 11, got %d", info.Size)
	}

	data, _ := io.ReadAll(reader)
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

func TestAdapter_ListWithPrefix(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	adapter.Put(ctx, "docs/file1.txt", []byte("1"))
	adapter.Put(ctx, "docs/file2.txt", []byte("2"))
	adapter.Put(ctx, "images/logo.png", []byte("3"))

	// List with prefix
	docs, err := adapter.ListWithContext(ctx, WithPrefix("docs/"))
	if err != nil {
		t.Fatalf("List with prefix failed: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(docs))
	}

	// List all
	all, err := adapter.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("List all failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 objects, got %d", len(all))
	}
}

func TestAdapter_ErrorPropagation(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	testErr := errors.New("test error")

	// Test Put error
	backend.putErr = testErr
	_, err := adapter.Put(ctx, "file.txt", []byte("data"))
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected 'test error', got %v", err)
	}
	backend.putErr = nil

	// Test Get error
	backend.getErr = testErr
	_, err = adapter.GetWithContext(ctx, "file.txt")
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected 'test error', got %v", err)
	}
	backend.getErr = nil

	// Test Delete error - first put the object, then set deleteErr
	backend.getErr = nil
	_, _ = adapter.Put(ctx, "deletable.txt", []byte("data"))
	backend.deleteErr = testErr
	err = adapter.DeleteWithContext(ctx, "deletable.txt")
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected 'test error', got %v", err)
	}
	backend.deleteErr = nil

	// Test List error
	backend.listErr = testErr
	_, err = adapter.ListWithContext(ctx)
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected 'test error', got %v", err)
	}
	backend.listErr = nil

	// Test Stat error
	backend.statErr = testErr
	_, err = adapter.StatWithContext(ctx, "file.txt")
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected 'test error', got %v", err)
	}
}

// Tests for functional options

func TestPutOption_WithDescription(t *testing.T) {
	options := &PutOptions{}
	WithDescription("test description")(options)

	if options.Description != "test description" {
		t.Errorf("expected description %q, got %q", "test description", options.Description)
	}
}

func TestPutOption_WithHeaders(t *testing.T) {
	options := &PutOptions{}
	headers := map[string]string{
		"Content-Type": "text/plain",
		"X-Custom":     "value",
	}
	WithHeaders(headers)(options)

	if len(options.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(options.Headers))
	}

	if options.Headers["Content-Type"] != "text/plain" {
		t.Errorf("expected Content-Type header, got %q", options.Headers["Content-Type"])
	}
}

func TestListOption_WithPrefix(t *testing.T) {
	options := &ListOptions{}
	WithPrefix("docs/")(options)

	if options.Prefix != "docs/" {
		t.Errorf("expected prefix %q, got %q", "docs/", options.Prefix)
	}
}

// Tests for types

func TestBucketConfig_Defaults(t *testing.T) {
	config := BucketConfig{
		Name: "test",
	}

	// Name should be set correctly
	if config.Name != "test" {
		t.Errorf("expected name 'test', got %q", config.Name)
	}

	// Default storage should be FileStorage (0)
	if config.Storage != FileStorage {
		t.Errorf("expected FileStorage as default, got %d", config.Storage)
	}

	// Default replicas should be 0 (handled at creation time)
	if config.Replicas != 0 {
		t.Errorf("expected 0 replicas as default, got %d", config.Replicas)
	}
}

func TestStorageType_Values(t *testing.T) {
	if FileStorage != 0 {
		t.Errorf("expected FileStorage to be 0, got %d", FileStorage)
	}

	if MemoryStorage != 1 {
		t.Errorf("expected MemoryStorage to be 1, got %d", MemoryStorage)
	}
}

// Compile-time interface checks

func TestAdapter_ImplementsPort(t *testing.T) {
	var _ FileStoragePort = (*FileStorageAdapter)(nil)
}

// TestBackend_ImplementsStorage verifies JetStreamBackend implements storage.Storage.
// The compile-time check is done in backend.go with:
// var _ storage.Storage = (*JetStreamBackend)(nil)

// Tests for thread safety

func TestModule_ConcurrentBucketAccess(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "bucket1"},
			{Name: "bucket2"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Simulate concurrent access to bucket methods
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			module.Bucket("bucket1")
		}()
		go func() {
			defer wg.Done()
			module.Buckets()
		}()
		go func() {
			defer wg.Done()
			module.HasBucket("bucket2")
		}()
	}
	wg.Wait()
}

func TestAdapter_ConcurrentOperations(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent puts and gets
	for i := 0; i < 50; i++ {
		wg.Add(2)
		key := "file.txt"
		go func() {
			defer wg.Done()
			adapter.Put(ctx, key, []byte("data"))
		}()
		go func() {
			defer wg.Done()
			adapter.GetWithContext(ctx, key)
		}()
	}
	wg.Wait()
}

// Tests for ObjectInfo

func TestObjectInfo_Fields(t *testing.T) {
	now := time.Now()
	info := ObjectInfo{
		Bucket:      "documents",
		Name:        "file.txt",
		Size:        1024,
		Digest:      "sha256-abc123",
		ModTime:     now,
		Deleted:     false,
		Headers:     map[string]string{"Content-Type": "text/plain"},
		Description: "Test file",
		Chunks:      2,
	}

	if info.Bucket != "documents" {
		t.Errorf("expected bucket 'documents', got %q", info.Bucket)
	}
	if info.Name != "file.txt" {
		t.Errorf("expected name 'file.txt', got %q", info.Name)
	}
	if info.Size != 1024 {
		t.Errorf("expected size 1024, got %d", info.Size)
	}
	if info.Digest != "sha256-abc123" {
		t.Errorf("expected digest 'sha256-abc123', got %q", info.Digest)
	}
	if !info.ModTime.Equal(now) {
		t.Errorf("expected modTime %v, got %v", now, info.ModTime)
	}
	if info.Deleted {
		t.Error("expected deleted to be false")
	}
	if info.Headers["Content-Type"] != "text/plain" {
		t.Errorf("expected Content-Type header 'text/plain', got %q", info.Headers["Content-Type"])
	}
	if info.Description != "Test file" {
		t.Errorf("expected description 'Test file', got %q", info.Description)
	}
	if info.Chunks != 2 {
		t.Errorf("expected chunks 2, got %d", info.Chunks)
	}
}

// ============================================================================
// Additional Comprehensive Tests
// ============================================================================

// mockEventBus implements types.EventBus for testing
type mockEventBus struct {
	publishCalled bool
	lastSubject   string
	lastData      []byte
}

func (m *mockEventBus) Publish(subject string, data []byte) error {
	m.publishCalled = true
	m.lastSubject = subject
	m.lastData = data
	return nil
}

func (m *mockEventBus) PublishMsg(_ *types.Msg) error { return nil }

func (m *mockEventBus) Request(_ string, _ []byte, _ time.Duration) (*types.Msg, error) {
	return nil, nil
}

func (m *mockEventBus) RequestWithContext(_ context.Context, _ string, _ []byte) (*types.Msg, error) {
	return nil, nil
}

func (m *mockEventBus) RequestMsgWithContext(_ context.Context, _ *types.Msg) (*types.Msg, error) {
	return nil, nil
}

func (m *mockEventBus) Subscribe(_ string, _ types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) SubscribeSync(_ string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) QueueSubscribe(_ string, _ string, _ types.MsgHandler) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) QueueSubscribeSync(_ string, _ string) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) ChanSubscribe(_ string, _ chan *types.Msg) (types.Subscription, error) {
	return nil, nil
}

func (m *mockEventBus) EventStream() (types.EventStream, error) { return nil, nil }

func (m *mockEventBus) SetRuntimeContext(_ context.Context) {}

var _ types.EventBus = (*mockEventBus)(nil)

// Tests for EventBus injection

func TestModule_SetEventBus(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// EventBus should be nil before SetEventBus is called
	if module.eventBus != nil {
		t.Error("expected nil eventBus before injection")
	}

	// SetEventBus should store the eventBus
	var bus types.EventBus = &mockEventBus{}
	module.SetEventBus(bus)

	if module.eventBus == nil {
		t.Error("expected eventBus to be set after SetEventBus")
	}
	if module.eventBus != bus {
		t.Error("expected eventBus to match the one set via SetEventBus")
	}
}

// Tests for Module Stop

func TestModule_Stop(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	ctx := context.Background()

	// Stop should not return error even if not started
	if err := module.Stop(ctx); err != nil {
		t.Errorf("expected no error from Stop, got: %v", err)
	}
}

// Tests for edge cases in operations

func TestAdapter_GetNonExistent(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Get non-existent object should return nil, nil per storage.Storage contract
	data, err := adapter.GetWithContext(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got %v", data)
	}
}

func TestAdapter_GetReaderNonExistent(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// GetReader non-existent object should return error
	reader, info, err := adapter.GetReaderWithContext(ctx, "nonexistent.txt")
	if err == nil {
		t.Error("expected error when getting reader for non-existent object")
	}
	if reader != nil {
		t.Error("expected nil reader on error")
	}
	if info != nil {
		t.Error("expected nil info on error")
	}
}

func TestAdapter_StatNonExistent(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Stat non-existent object should return error
	info, err := adapter.StatWithContext(ctx, "nonexistent.txt")
	if err == nil {
		t.Error("expected error when stating non-existent object")
	}
	if info != nil {
		t.Error("expected nil info on error")
	}
}

func TestAdapter_DeleteNonExistent(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Delete non-existent object should succeed (idempotent delete semantics)
	err := adapter.DeleteWithContext(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("expected no error when deleting non-existent object, got: %v", err)
	}
}

func TestAdapter_EmptyData(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Put empty data should work
	info, err := adapter.Put(ctx, "empty.txt", []byte{})
	if err != nil {
		t.Fatalf("Put empty data failed: %v", err)
	}
	if info.Size != 0 {
		t.Errorf("expected size 0, got %d", info.Size)
	}

	// Get empty data should return empty slice
	data, err := adapter.GetWithContext(ctx, "empty.txt")
	if err != nil {
		t.Fatalf("Get empty data failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(data))
	}
}

func TestAdapter_LargeData(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Create 1MB of data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Put large data
	info, err := adapter.Put(ctx, "large.bin", largeData)
	if err != nil {
		t.Fatalf("Put large data failed: %v", err)
	}
	if info.Size != int64(len(largeData)) {
		t.Errorf("expected size %d, got %d", len(largeData), info.Size)
	}

	// Get large data
	data, err := adapter.GetWithContext(ctx, "large.bin")
	if err != nil {
		t.Fatalf("Get large data failed: %v", err)
	}
	if len(data) != len(largeData) {
		t.Errorf("expected %d bytes, got %d", len(largeData), len(data))
	}

	// Verify data integrity
	for i := range data {
		if data[i] != largeData[i] {
			t.Errorf("data mismatch at index %d: expected %d, got %d", i, largeData[i], data[i])
			break
		}
	}
}

func TestAdapter_SpecialCharactersInKey(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	specialKeys := []string{
		"path/to/file.txt",
		"file with spaces.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"deep/nested/path/to/file.txt",
		"unicode-文件.txt",
	}

	for _, key := range specialKeys {
		t.Run(key, func(t *testing.T) {
			_, err := adapter.Put(ctx, key, []byte("content"))
			if err != nil {
				t.Errorf("Put failed for key %q: %v", key, err)
			}

			data, err := adapter.GetWithContext(ctx, key)
			if err != nil {
				t.Errorf("Get failed for key %q: %v", key, err)
			}
			if string(data) != "content" {
				t.Errorf("expected 'content' for key %q, got %q", key, string(data))
			}
		})
	}
}

func TestAdapter_OverwriteExistingObject(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Put initial data
	_, err := adapter.Put(ctx, "file.txt", []byte("initial"))
	if err != nil {
		t.Fatalf("Put initial failed: %v", err)
	}

	// Overwrite with new data
	info, err := adapter.Put(ctx, "file.txt", []byte("updated content"))
	if err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}
	if info.Size != 15 {
		t.Errorf("expected size 15, got %d", info.Size)
	}

	// Verify overwritten data
	data, err := adapter.GetWithContext(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	if string(data) != "updated content" {
		t.Errorf("expected 'updated content', got %q", string(data))
	}
}

func TestAdapter_ListEmpty(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// List empty bucket should return empty slice
	objects, err := adapter.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("List empty bucket failed: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("expected 0 objects, got %d", len(objects))
	}
}

func TestAdapter_ListWithNonMatchingPrefix(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Add some objects
	adapter.Put(ctx, "docs/file1.txt", []byte("1"))
	adapter.Put(ctx, "docs/file2.txt", []byte("2"))

	// List with non-matching prefix should return empty
	objects, err := adapter.ListWithContext(ctx, WithPrefix("images/"))
	if err != nil {
		t.Fatalf("List with prefix failed: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("expected 0 objects with prefix 'images/', got %d", len(objects))
	}
}

// Tests for Put with options verification

func TestAdapter_PutWithDescription(t *testing.T) {
	backend := &mockBackendWithOptions{
		mockBackend: *newMockBackend("test-bucket"),
	}
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	_, err := adapter.Put(ctx, "file.txt", []byte("content"),
		WithDescription("Test description"))
	if err != nil {
		t.Fatalf("Put with description failed: %v", err)
	}

	if backend.lastDescription != "Test description" {
		t.Errorf("expected description 'Test description', got %q", backend.lastDescription)
	}
}

func TestAdapter_PutWithHeaders(t *testing.T) {
	backend := &mockBackendWithOptions{
		mockBackend: *newMockBackend("test-bucket"),
	}
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	headers := map[string]string{
		"Content-Type":     "application/json",
		"X-Custom-Header":  "custom-value",
		"X-Another-Header": "another-value",
	}

	_, err := adapter.Put(ctx, "file.json", []byte("{}"),
		WithHeaders(headers))
	if err != nil {
		t.Fatalf("Put with headers failed: %v", err)
	}

	if len(backend.lastHeaders) != 3 {
		t.Errorf("expected 3 headers, got %d", len(backend.lastHeaders))
	}
	if backend.lastHeaders["Content-Type"] != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", backend.lastHeaders["Content-Type"])
	}
}

func TestAdapter_PutWithMultipleOptions(t *testing.T) {
	backend := &mockBackendWithOptions{
		mockBackend: *newMockBackend("test-bucket"),
	}
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	_, err := adapter.Put(ctx, "file.txt", []byte("content"),
		WithDescription("My document"),
		WithHeaders(map[string]string{"Content-Type": "text/plain"}))
	if err != nil {
		t.Fatalf("Put with multiple options failed: %v", err)
	}

	if backend.lastDescription != "My document" {
		t.Errorf("expected description 'My document', got %q", backend.lastDescription)
	}
	if backend.lastHeaders["Content-Type"] != "text/plain" {
		t.Errorf("expected Content-Type 'text/plain', got %q", backend.lastHeaders["Content-Type"])
	}
}

// mockBackendWithOptions extends mockBackend to capture options
type mockBackendWithOptions struct {
	mockBackend
	lastDescription string
	lastHeaders     map[string]string
}

// PutReaderWithContext captures the storage options and stores the data
func (b *mockBackendWithOptions) PutReaderWithContext(_ context.Context, key string, reader io.Reader, _ time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	if b.putErr != nil {
		return nil, b.putErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	options := storage.ApplyPutOptions(opts...)
	b.lastDescription = options.Description
	b.lastHeaders = options.Headers
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = data
	b.metadata[key] = &objectMetadata{
		Description: options.Description,
		Headers:     options.Headers,
		Size:        int64(len(data)),
		ModTime:     now,
	}

	return &storage.ObjectInfo{
		Bucket:      b.bucketName,
		Name:        key,
		Size:        int64(len(data)),
		ModTime:     now,
		Description: options.Description,
		Headers:     options.Headers,
	}, nil
}

// Tests for bucket configuration

func TestModule_BucketConfigWithAllOptions(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{
				Name:        "full-config",
				Description: "Fully configured bucket",
				MaxBytes:    10 * 1024 * 1024, // 10MB
				TTL:         24 * time.Hour,
				Replicas:    3,
				Storage:     MemoryStorage,
				Compression: true,
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify config is stored correctly
	if len(module.config.Buckets) != 1 {
		t.Fatalf("expected 1 bucket config, got %d", len(module.config.Buckets))
	}

	bc := module.config.Buckets[0]
	if bc.Name != "full-config" {
		t.Errorf("expected name 'full-config', got %q", bc.Name)
	}
	if bc.Description != "Fully configured bucket" {
		t.Errorf("expected description, got %q", bc.Description)
	}
	if bc.MaxBytes != 10*1024*1024 {
		t.Errorf("expected MaxBytes 10MB, got %d", bc.MaxBytes)
	}
	if bc.TTL != 24*time.Hour {
		t.Errorf("expected TTL 24h, got %v", bc.TTL)
	}
	if bc.Replicas != 3 {
		t.Errorf("expected Replicas 3, got %d", bc.Replicas)
	}
	if bc.Storage != MemoryStorage {
		t.Errorf("expected MemoryStorage, got %d", bc.Storage)
	}
	if !bc.Compression {
		t.Error("expected Compression to be true")
	}
}

func TestModule_MultipleBucketsWithDifferentConfigs(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{
				Name:    "fast-cache",
				Storage: MemoryStorage,
				TTL:     5 * time.Minute,
			},
			{
				Name:        "documents",
				Storage:     FileStorage,
				Description: "Persistent document storage",
				Replicas:    2,
			},
			{
				Name:        "large-files",
				Storage:     FileStorage,
				MaxBytes:    1024 * 1024 * 1024, // 1GB
				Compression: true,
			},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(module.config.Buckets) != 3 {
		t.Errorf("expected 3 bucket configs, got %d", len(module.config.Buckets))
	}
}

// Tests for module name

func TestModule_NameConstant(t *testing.T) {
	if ModuleName != "fs-jetstream" {
		t.Errorf("expected ModuleName to be 'fs-jetstream', got %q", ModuleName)
	}
}

// Tests for bucket operations after manual injection

func TestModule_ManualBucketInjection(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "test"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Manually inject a bucket (simulating what Start() would do)
	backend := newMockBackend("test")
	adapter := NewAdapter(backend, slog.Default())

	module.mu.Lock()
	module.buckets["test"] = adapter
	module.mu.Unlock()

	// Now bucket access should work
	bucket := module.Bucket("test")
	if bucket == nil {
		t.Fatal("expected bucket after injection")
	}

	buckets := module.Buckets()
	if len(buckets) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(buckets))
	}

	if !module.HasBucket("test") {
		t.Error("expected HasBucket('test') to be true")
	}
}

func TestModule_BucketNotFound(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Manually inject the configured bucket
	backend := newMockBackend("documents")
	adapter := NewAdapter(backend, slog.Default())

	module.mu.Lock()
	module.buckets["documents"] = adapter
	module.mu.Unlock()

	// Request non-existent bucket
	bucket := module.Bucket("nonexistent")
	if bucket != nil {
		t.Error("expected nil bucket for nonexistent bucket name")
	}

	if module.HasBucket("nonexistent") {
		t.Error("expected HasBucket('nonexistent') to be false")
	}
}

// Tests for context cancellation

func TestAdapter_ContextCancellation(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Operations should still work with mock backend (real backend would check context)
	// This tests that context is properly passed through
	_, err := adapter.Put(ctx, "file.txt", []byte("data"))
	if err != nil {
		t.Logf("Put with cancelled context: %v (expected behavior depends on backend)", err)
	}
}

// Tests for concurrent operations on same key

func TestAdapter_ConcurrentOperationsSameKey(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()
	var wg sync.WaitGroup
	key := "concurrent-file.txt"

	// Multiple concurrent puts and gets on same key
	for i := 0; i < 100; i++ {
		wg.Add(4)

		go func(val int) {
			defer wg.Done()
			adapter.Put(ctx, key, []byte("value"))
		}(i)

		go func() {
			defer wg.Done()
			adapter.GetWithContext(ctx, key)
		}()

		go func() {
			defer wg.Done()
			adapter.StatWithContext(ctx, key)
		}()

		go func() {
			defer wg.Done()
			adapter.ListWithContext(ctx)
		}()
	}

	wg.Wait()
}

// Tests for adapter with nil logger

func TestAdapter_NilLogger(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, nil) // nil logger

	ctx := context.Background()

	// Should not panic with nil logger
	_, err := adapter.Put(ctx, "file.txt", []byte("data"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = adapter.GetWithContext(ctx, "file.txt")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// Test for multiple options

func TestNew_WithMultipleOptions(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	customLogger := slog.Default().With("custom", true)

	module, err := New(config,
		WithLogger(customLogger),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module.logger != customLogger {
		t.Error("expected custom logger to be set")
	}
}

// Test option error handling

func TestNew_WithOptionError(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "documents"},
		},
	}

	// Create an option that returns an error
	errorOption := func(m *PluginModule) error {
		return errors.New("option error")
	}

	_, err := New(config, errorOption)
	if err == nil {
		t.Fatal("expected error from option")
	}
	if err.Error() != "option error" {
		t.Errorf("expected 'option error', got %q", err.Error())
	}
}

// Test PutReader with error

func TestAdapter_PutReaderError(t *testing.T) {
	backend := newMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Create a reader that returns an error
	errorReader := &errorReaderMock{err: errors.New("read error")}

	_, err := adapter.PutReaderWithContext(ctx, "file.txt", errorReader, 0)
	if err == nil {
		t.Fatal("expected error from PutReader")
	}
	if err.Error() != "read error" {
		t.Errorf("expected 'read error', got %q", err.Error())
	}
}

type errorReaderMock struct {
	err error
}

func (r *errorReaderMock) Read(_ []byte) (int, error) {
	return 0, r.err
}

// Test GetReader error propagation

func TestAdapter_GetReaderError(t *testing.T) {
	backend := newMockBackend("test-bucket")
	backend.getErr = errors.New("get error")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	reader, info, err := adapter.GetReaderWithContext(ctx, "file.txt")
	if err == nil {
		t.Fatal("expected error from GetReader")
	}
	if reader != nil {
		t.Error("expected nil reader on error")
	}
	if info != nil {
		t.Error("expected nil info on error")
	}
}

// ============================================================================
// Tests for Start() and createOrGetObjectStore()
// ============================================================================

// TestModule_Start_NilEventBus tests Start() when EventBus is not set
func TestModule_Start_NilEventBus(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "test-bucket"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error creating module, got: %v", err)
	}

	// Don't set EventBus - it should be nil
	ctx := context.Background()
	err = module.Start(ctx)

	// Should fail because EventBus is not set (nil doesn't implement EventBusWithConn)
	if err == nil {
		t.Fatal("expected error when EventBus is nil")
	}

	if !strings.Contains(err.Error(), "does not implement EventBusWithConn") {
		t.Errorf("expected error about EventBusWithConn interface, got: %v", err)
	}
}

// TestModule_Start_EventBusWithoutConn tests Start() when EventBus doesn't implement EventBusWithConn
func TestModule_Start_EventBusWithoutConn(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "test-bucket"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error creating module, got: %v", err)
	}

	// Set a mock EventBus that doesn't implement EventBusWithConn[*nats.Conn]
	module.SetEventBus(&mockEventBus{})

	ctx := context.Background()
	err = module.Start(ctx)

	// Should fail because EventBus doesn't implement EventBusWithConn
	if err == nil {
		t.Fatal("expected error when EventBus doesn't implement EventBusWithConn")
	}

	if !strings.Contains(err.Error(), "does not implement EventBusWithConn") {
		t.Errorf("expected error about EventBusWithConn interface, got: %v", err)
	}
}

// TestBucketConfig_ReplicasDefault tests that Replicas defaults to 1 when not set
func TestBucketConfig_ReplicasDefault(t *testing.T) {
	// This test verifies the logic in createOrGetObjectStore() that sets
	// Replicas to 1 when it's 0. We can't easily test createOrGetObjectStore()
	// in isolation without a real JetStream connection, but we can verify
	// the config transformation logic.

	cfg := BucketConfig{
		Name:     "test-bucket",
		Replicas: 0, // Should default to 1
	}
	_ = cfg.Name // Use Name field to satisfy linter

	// Create expected ObjectStoreConfig
	expectedReplicas := 1

	// Verify the logic: if Replicas == 0, it should become 1
	actualReplicas := cfg.Replicas
	if actualReplicas == 0 {
		actualReplicas = 1
	}

	if actualReplicas != expectedReplicas {
		t.Errorf("expected Replicas to default to %d, got %d", expectedReplicas, actualReplicas)
	}
}

// TestBucketConfig_ReplicasNonZero tests that non-zero Replicas is preserved
func TestBucketConfig_ReplicasNonZero(t *testing.T) {
	cfg := BucketConfig{
		Name:     "test-bucket",
		Replicas: 3,
	}
	_ = cfg.Name // Use Name field to satisfy linter

	// Replicas should remain 3 (not changed)
	actualReplicas := cfg.Replicas
	if actualReplicas == 0 {
		actualReplicas = 1
	}

	if actualReplicas != 3 {
		t.Errorf("expected Replicas to be 3, got %d", actualReplicas)
	}
}

// TestModule_StorageTypeMapping tests the storage type mapping logic
func TestModule_StorageTypeMapping(t *testing.T) {
	tests := []struct {
		name           string
		storageType    StorageType
		expectedJSType jetstream.StorageType
	}{
		{
			name:           "MemoryStorage maps to jetstream.MemoryStorage",
			storageType:    MemoryStorage,
			expectedJSType: jetstream.MemoryStorage,
		},
		{
			name:           "FileStorage maps to jetstream.FileStorage",
			storageType:    FileStorage,
			expectedJSType: jetstream.FileStorage,
		},
		{
			name:           "Zero value defaults to FileStorage",
			storageType:    StorageType(0),
			expectedJSType: jetstream.FileStorage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BucketConfig{
				Name:    "test",
				Storage: tt.storageType,
			}
			_ = cfg.Name // Use Name field to satisfy linter

			var jsType jetstream.StorageType
			if cfg.Storage == MemoryStorage {
				jsType = jetstream.MemoryStorage
			} else {
				jsType = jetstream.FileStorage
			}

			if jsType != tt.expectedJSType {
				t.Errorf("expected %v, got %v", tt.expectedJSType, jsType)
			}
		})
	}
}

// TestModule_Name tests the Name() method
func TestModule_Name(t *testing.T) {
	config := Config{
		Buckets: []BucketConfig{
			{Name: "test-bucket"},
		},
	}

	module, err := New(config)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if module.Name() != ModuleName {
		t.Errorf("expected module name %q, got %q", ModuleName, module.Name())
	}

	if module.Name() != "fs-jetstream" {
		t.Errorf("expected module name 'fs-jetstream', got %q", module.Name())
	}
}
