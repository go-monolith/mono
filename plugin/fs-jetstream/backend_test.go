package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
	"github.com/go-monolith/mono/pkg/types"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// setupTestNATSWithJetStream creates an embedded NATS server with JetStream for testing.
// The server is automatically shut down when the test ends.
func setupTestNATSWithJetStream(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	// Configure NATS server with JetStream
	opts := natsserver.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1 // Random port
	opts.StoreDir = t.TempDir()

	// Start embedded NATS server
	s := natsserver.RunServer(&opts)
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})

	// Connect to the server
	conn, err := nats.Connect(s.ClientURL(),
		nats.Name("test-client"),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to connect to NATS server: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	// Create JetStream context
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}

	return conn, js
}

// createTestObjectStore creates a test ObjectStore with the given configuration
func createTestObjectStore(t *testing.T, js jetstream.JetStream, cfg BucketConfig) jetstream.ObjectStore {
	t.Helper()

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

	os, err := js.CreateObjectStore(context.Background(), osCfg)
	if err != nil {
		t.Fatalf("failed to create ObjectStore: %v", err)
	}

	return os
}

// testLogger returns a logger for tests
func testLogger() *slog.Logger {
	return slog.Default()
}

// TestNewJetStreamBackend tests the creation of JetStream backend
func TestNewJetStreamBackend(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:        "test-bucket",
		Description: "Test bucket",
		MaxBytes:    1024 * 1024, // 1MB
		TTL:         1 * time.Hour,
		Storage:     FileStorage,
		Replicas:    1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, config.TTL, testLogger())

	if backend == nil {
		t.Fatal("expected backend, got nil")
	}

	if backend.BucketName() != "test-bucket" {
		t.Errorf("expected bucket name 'test-bucket', got %q", backend.BucketName())
	}
}

// TestJetStreamBackend_StorageInterface tests that backend implements storage.Storage
func TestJetStreamBackend_StorageInterface(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Verify interfaces are implemented
	var _ storage.Storage = backend
	var _ storage.StorageWithBucket = backend
	var _ storage.StorageWithList = backend
	var _ storage.StorageWithStat = backend
	var _ storage.StorageWithReader = backend
}

// TestJetStreamBackend_SetAndGet tests basic set and get operations
func TestJetStreamBackend_SetAndGet(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Test Set
	testData := []byte("hello world")
	err := backend.SetWithContext(ctx, "file.txt", testData, 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Test Get
	data, err := backend.GetWithContext(ctx, "file.txt")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}

	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

// TestJetStreamBackend_GetNotFound tests Get for non-existent key
func TestJetStreamBackend_GetNotFound(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Get non-existent key should return nil, nil per storage.Storage contract
	data, err := backend.GetWithContext(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("expected no error for non-existent key, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for non-existent key, got: %v", data)
	}
}

// TestJetStreamBackend_PutReaderAndGetReader tests reader-based operations
func TestJetStreamBackend_PutReaderAndGetReader(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Test PutReader
	testData := []byte("hello from reader")
	reader := bytes.NewReader(testData)
	info, err := backend.PutReaderWithContext(ctx, "file-reader.txt", reader, 0)
	if err != nil {
		t.Fatalf("PutReaderWithContext failed: %v", err)
	}

	if info.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size)
	}

	// Test GetReader
	dataReader, getInfo, err := backend.GetReaderWithContext(ctx, "file-reader.txt")
	if err != nil {
		t.Fatalf("GetReaderWithContext failed: %v", err)
	}
	defer dataReader.Close()

	if getInfo.Name != "file-reader.txt" {
		t.Errorf("expected name 'file-reader.txt', got %q", getInfo.Name)
	}

	readData, err := io.ReadAll(dataReader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(readData) != "hello from reader" {
		t.Errorf("expected 'hello from reader', got %q", string(readData))
	}
}

// TestJetStreamBackend_Delete tests delete operation
func TestJetStreamBackend_Delete(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Put a file
	err := backend.SetWithContext(ctx, "delete-me.txt", []byte("data"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Verify it exists
	data, err := backend.GetWithContext(ctx, "delete-me.txt")
	if err != nil || data == nil {
		t.Fatalf("GetWithContext failed before delete: %v", err)
	}

	// Delete the file
	err = backend.DeleteWithContext(ctx, "delete-me.txt")
	if err != nil {
		t.Fatalf("DeleteWithContext failed: %v", err)
	}

	// Verify it's gone (returns nil, nil per storage.Storage contract)
	data, err = backend.GetWithContext(ctx, "delete-me.txt")
	if err != nil {
		t.Errorf("expected no error after delete, got: %v", err)
	}
	if data != nil {
		t.Error("expected nil data after delete")
	}
}

// TestJetStreamBackend_DeleteNotFound tests delete of non-existent key
func TestJetStreamBackend_DeleteNotFound(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Delete non-existent should not error per storage.Storage contract
	err := backend.DeleteWithContext(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("expected no error deleting non-existent key, got: %v", err)
	}
}

// TestJetStreamBackend_Reset tests reset operation
func TestJetStreamBackend_Reset(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Put some files
	for i := 0; i < 3; i++ {
		err := backend.SetWithContext(ctx, "file"+string(rune('0'+i))+".txt", []byte("data"), 0)
		if err != nil {
			t.Fatalf("SetWithContext failed: %v", err)
		}
	}

	// Reset
	err := backend.ResetWithContext(ctx)
	if err != nil {
		t.Fatalf("ResetWithContext failed: %v", err)
	}

	// Verify all deleted
	infos, err := backend.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("ListWithContext failed: %v", err)
	}
	if len(infos) != 0 {
		t.Errorf("expected 0 objects after reset, got %d", len(infos))
	}
}

// TestJetStreamBackend_Close tests close operation
func TestJetStreamBackend_Close(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Close should be a no-op
	err := backend.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestJetStreamBackend_List tests list operation
func TestJetStreamBackend_List(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Put multiple files
	files := map[string]string{
		"docs/file1.txt":  "content1",
		"docs/file2.txt":  "content2",
		"images/logo.png": "image data",
		"images/icon.png": "icon data",
		"readme.txt":      "readme content",
	}

	for key, content := range files {
		err := backend.SetWithContext(ctx, key, []byte(content), 0)
		if err != nil {
			t.Fatalf("SetWithContext(%q) failed: %v", key, err)
		}
	}

	// List all objects
	allObjects, err := backend.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("ListWithContext failed: %v", err)
	}

	if len(allObjects) < len(files) {
		t.Errorf("expected at least %d objects, got %d", len(files), len(allObjects))
	}

	// List with prefix "docs/"
	docsObjects, err := backend.ListWithContext(ctx, storage.WithListPrefix("docs/"))
	if err != nil {
		t.Fatalf("ListWithContext with prefix failed: %v", err)
	}

	if len(docsObjects) < 2 {
		t.Errorf("expected at least 2 docs objects, got %d", len(docsObjects))
	}

	// Verify objects have bucket name set
	for _, obj := range allObjects {
		if obj.Bucket != "test-bucket" {
			t.Errorf("expected bucket 'test-bucket', got %q", obj.Bucket)
		}
	}
}

// TestJetStreamBackend_Stat tests stat operation
func TestJetStreamBackend_Stat(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Put a file
	testData := []byte("test content for stat")
	err := backend.SetWithContext(ctx, "stat-me.txt", testData, 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Stat the file
	info, err := backend.StatWithContext(ctx, "stat-me.txt")
	if err != nil {
		t.Fatalf("StatWithContext failed: %v", err)
	}

	if info.Name != "stat-me.txt" {
		t.Errorf("expected name 'stat-me.txt', got %q", info.Name)
	}
	if info.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size)
	}
	if info.Bucket != "test-bucket" {
		t.Errorf("expected bucket 'test-bucket', got %q", info.Bucket)
	}
}

// TestJetStreamBackend_StatNotFound tests stat for non-existent key
func TestJetStreamBackend_StatNotFound(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Stat non-existent should return ErrKeyNotFound
	_, err := backend.StatWithContext(ctx, "nonexistent.txt")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestJetStreamBackend_PutReaderWithOptions tests put reader with functional options
func TestJetStreamBackend_PutReaderWithOptions(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Put with description and headers
	testData := []byte("content with metadata")
	reader := bytes.NewReader(testData)
	info, err := backend.PutReaderWithContext(ctx, "metadata-file.txt", reader, 0,
		storage.WithPutDescription("File with custom metadata"),
		storage.WithPutHeaders(map[string]string{
			"Content-Type": "text/plain",
			"X-Custom":     "custom-value",
		}),
	)
	if err != nil {
		t.Fatalf("PutReaderWithContext with options failed: %v", err)
	}

	if info.Description != "File with custom metadata" {
		t.Errorf("expected description, got %q", info.Description)
	}
}

// ============================================================================
// Integration Tests for Module Start() and createOrGetObjectStore()
// ============================================================================

// realEventBus wraps a NATS connection to provide a real EventBus for testing
type realEventBus struct {
	conn *nats.Conn
}

func newRealEventBus(conn *nats.Conn) *realEventBus {
	return &realEventBus{conn: conn}
}

// Implement types.EventBus interface with proper types
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

// Conn returns the underlying NATS connection (implements types.EventBusWithConn[*nats.Conn])
func (r *realEventBus) Conn() *nats.Conn { return r.conn }

// TestModule_Start_Integration tests the Start() method with a real NATS server
func TestModule_Start_Integration(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:        "documents",
				Description: "Document storage",
				MaxBytes:    10 * 1024 * 1024, // 10MB
				Storage:     FileStorage,
				Replicas:    1,
			},
			{
				Name:     "images",
				Storage:  MemoryStorage,
				Replicas: 1,
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
	if !module.HasBucket("documents") {
		t.Error("expected 'documents' bucket to exist")
	}
	if !module.HasBucket("images") {
		t.Error("expected 'images' bucket to exist")
	}

	// Verify bucket count
	buckets := module.Buckets()
	if len(buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(buckets))
	}

	// Verify we can get bucket adapters
	docsBucket := module.Bucket("documents")
	if docsBucket == nil {
		t.Error("expected non-nil documents bucket adapter")
	}

	imagesBucket := module.Bucket("images")
	if imagesBucket == nil {
		t.Error("expected non-nil images bucket adapter")
	}

	// Test Stop()
	err = module.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
}

// TestModule_Bucket_NotFound tests Bucket() for non-existent bucket
func TestModule_Bucket_NotFound(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)

	config := Config{
		Buckets: []BucketConfig{
			{
				Name:     "existing-bucket",
				Storage:  MemoryStorage,
				Replicas: 1,
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

	// Get non-existent bucket
	bucket := module.Bucket("non-existent")
	if bucket != nil {
		t.Error("expected nil for non-existent bucket")
	}

	// Verify existing bucket is found
	existingBucket := module.Bucket("existing-bucket")
	if existingBucket == nil {
		t.Error("expected non-nil for existing bucket")
	}
}

// ============================================================================
// Mock-based Error Path Tests
// ============================================================================

// mockObjectStore implements jetstream.ObjectStore for testing error paths
type mockObjectStore struct {
	putErr     error
	getErr     error
	deleteErr  error
	listErr    error
	getInfoErr error
	getResult  *mockObjectResult
	listResult []*jetstream.ObjectInfo
	getInfoRes *jetstream.ObjectInfo
}

func (m *mockObjectStore) Put(_ context.Context, _ jetstream.ObjectMeta, _ io.Reader) (*jetstream.ObjectInfo, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	return &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "test"}, Size: 100}, nil
}

func (m *mockObjectStore) PutBytes(_ context.Context, _ string, _ []byte) (*jetstream.ObjectInfo, error) {
	return nil, nil
}

func (m *mockObjectStore) PutString(_ context.Context, _ string, _ string) (*jetstream.ObjectInfo, error) {
	return nil, nil
}

func (m *mockObjectStore) PutFile(_ context.Context, _ string) (*jetstream.ObjectInfo, error) {
	return nil, nil
}

func (m *mockObjectStore) Get(_ context.Context, _ string, _ ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	if m.getResult != nil {
		return m.getResult, nil
	}
	return &mockObjectResult{data: []byte("test")}, nil
}

func (m *mockObjectStore) GetBytes(_ context.Context, _ string, _ ...jetstream.GetObjectOpt) ([]byte, error) {
	return nil, nil
}

func (m *mockObjectStore) GetString(_ context.Context, _ string, _ ...jetstream.GetObjectOpt) (string, error) {
	return "", nil
}

func (m *mockObjectStore) GetFile(_ context.Context, _, _ string, _ ...jetstream.GetObjectOpt) error {
	return nil
}

func (m *mockObjectStore) GetInfo(_ context.Context, _ string, _ ...jetstream.GetObjectInfoOpt) (*jetstream.ObjectInfo, error) {
	if m.getInfoErr != nil {
		return nil, m.getInfoErr
	}
	if m.getInfoRes != nil {
		return m.getInfoRes, nil
	}
	return &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "test"}, Size: 100}, nil
}

func (m *mockObjectStore) UpdateMeta(_ context.Context, _ string, _ jetstream.ObjectMeta) error {
	return nil
}

func (m *mockObjectStore) Delete(_ context.Context, _ string) error {
	return m.deleteErr
}

func (m *mockObjectStore) AddLink(_ context.Context, _ string, _ *jetstream.ObjectInfo) (*jetstream.ObjectInfo, error) {
	return nil, nil
}

func (m *mockObjectStore) AddBucketLink(_ context.Context, _ string, _ jetstream.ObjectStore) (*jetstream.ObjectInfo, error) {
	return nil, nil
}

func (m *mockObjectStore) Seal(_ context.Context) error {
	return nil
}

func (m *mockObjectStore) Watch(_ context.Context, _ ...jetstream.WatchOpt) (jetstream.ObjectWatcher, error) {
	return nil, nil
}

func (m *mockObjectStore) List(_ context.Context, _ ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResult, nil
}

func (m *mockObjectStore) Status(_ context.Context) (jetstream.ObjectStoreStatus, error) {
	return nil, nil
}

func (m *mockObjectStore) Bucket() string {
	return "test-bucket"
}

// mockObjectResult implements jetstream.ObjectResult for testing
type mockObjectResult struct {
	data    []byte
	readErr error
	infoErr error
	info    *jetstream.ObjectInfo
	readPos int
	closed  bool
}

func (r *mockObjectResult) Read(p []byte) (n int, err error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	if r.readPos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.readPos:])
	r.readPos += n
	return n, nil
}

func (r *mockObjectResult) Close() error {
	r.closed = true
	return nil
}

func (r *mockObjectResult) Info() (*jetstream.ObjectInfo, error) {
	if r.infoErr != nil {
		return nil, r.infoErr
	}
	if r.info != nil {
		return r.info, nil
	}
	return &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "test"}, Size: uint64(len(r.data))}, nil
}

func (r *mockObjectResult) Error() error {
	return nil
}

// TestJetStreamBackend_SetWithContext_Error tests error path
func TestJetStreamBackend_SetWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		putErr: errors.New("storage full"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	err := backend.SetWithContext(ctx, "file.txt", []byte("data"), 0)
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to put object") {
		t.Errorf("expected 'failed to put object' in error, got: %v", err)
	}
}

// TestJetStreamBackend_GetWithContext_Error tests error path
func TestJetStreamBackend_GetWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		getErr: errors.New("connection reset"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, err := backend.GetWithContext(ctx, "file.txt")
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to get object") {
		t.Errorf("expected 'failed to get object' in error, got: %v", err)
	}
}

// TestJetStreamBackend_GetWithContext_ReadError tests ReadAll error path
func TestJetStreamBackend_GetWithContext_ReadError(t *testing.T) {
	mockOS := &mockObjectStore{
		getResult: &mockObjectResult{
			data:    []byte("test"),
			readErr: errors.New("read timeout"),
		},
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, err := backend.GetWithContext(ctx, "file.txt")
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to read object") {
		t.Errorf("expected 'failed to read object' in error, got: %v", err)
	}
}

// TestJetStreamBackend_DeleteWithContext_Error tests error path
func TestJetStreamBackend_DeleteWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		deleteErr: errors.New("permission denied"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	err := backend.DeleteWithContext(ctx, "file.txt")
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to delete object") {
		t.Errorf("expected 'failed to delete object' in error, got: %v", err)
	}
}

// TestJetStreamBackend_ListWithContext_Error tests error path
func TestJetStreamBackend_ListWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		listErr: errors.New("bucket unavailable"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, err := backend.ListWithContext(ctx)
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to list objects") {
		t.Errorf("expected 'failed to list objects' in error, got: %v", err)
	}
}

// TestJetStreamBackend_StatWithContext_Error tests error path
func TestJetStreamBackend_StatWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		getInfoErr: errors.New("metadata unavailable"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, err := backend.StatWithContext(ctx, "file.txt")
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to stat object") {
		t.Errorf("expected 'failed to stat object' in error, got: %v", err)
	}
}

// ============================================================================
// Wrapper Function Tests (non-context versions)
// These test the wrapper functions that call *WithContext methods
// ============================================================================

// TestJetStreamBackend_Get tests the Get wrapper function
func TestJetStreamBackend_Get(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set a value first
	err := backend.Set("test-key", []byte("test-value"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Get wrapper
	data, err := backend.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(data) != "test-value" {
		t.Errorf("expected 'test-value', got %q", string(data))
	}

	// Test Get for non-existent key
	data, err = backend.Get("nonexistent")
	if err != nil {
		t.Errorf("expected no error for non-existent key, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for non-existent key, got: %v", data)
	}
}

// TestJetStreamBackend_Set tests the Set wrapper function
func TestJetStreamBackend_Set(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Test Set wrapper
	err := backend.Set("set-key", []byte("set-value"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify it was set
	data, err := backend.Get("set-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(data) != "set-value" {
		t.Errorf("expected 'set-value', got %q", string(data))
	}

	// Test Set with empty key (should be ignored)
	err = backend.Set("", []byte("value"), 0)
	if err != nil {
		t.Errorf("expected no error for empty key, got: %v", err)
	}

	// Test Set with nil value (should be ignored)
	err = backend.Set("some-key", nil, 0)
	if err != nil {
		t.Errorf("expected no error for nil value, got: %v", err)
	}
}

// TestJetStreamBackend_DeleteWrapper tests the Delete wrapper function
func TestJetStreamBackend_DeleteWrapper(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set a value first
	err := backend.Set("delete-key", []byte("to-be-deleted"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Delete wrapper
	err = backend.Delete("delete-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it was deleted
	data, err := backend.Get("delete-key")
	if err != nil {
		t.Errorf("expected no error after delete, got: %v", err)
	}
	if data != nil {
		t.Error("expected nil data after delete")
	}

	// Test Delete for non-existent key (should not error)
	err = backend.Delete("nonexistent")
	if err != nil {
		t.Errorf("expected no error deleting non-existent key, got: %v", err)
	}
}

// TestJetStreamBackend_ResetWrapper tests the Reset wrapper function
func TestJetStreamBackend_ResetWrapper(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set multiple values
	for i := 0; i < 3; i++ {
		err := backend.Set("key"+string(rune('0'+i)), []byte("value"), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Verify they exist
	infos, err := backend.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(infos) == 0 {
		t.Fatal("expected objects to exist before reset")
	}

	// Test Reset wrapper
	err = backend.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify all deleted
	infos, err = backend.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(infos) != 0 {
		t.Errorf("expected 0 objects after reset, got %d", len(infos))
	}
}

// TestJetStreamBackend_ListWrapper tests the List wrapper function
func TestJetStreamBackend_ListWrapper(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set some files with prefixes
	files := []struct {
		key   string
		value string
	}{
		{"docs/file1.txt", "content1"},
		{"docs/file2.txt", "content2"},
		{"images/logo.png", "image"},
	}

	for _, f := range files {
		err := backend.Set(f.key, []byte(f.value), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Test List wrapper (all objects)
	allInfos, err := backend.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(allInfos) < 3 {
		t.Errorf("expected at least 3 objects, got %d", len(allInfos))
	}

	// Test List with prefix option
	docsInfos, err := backend.List(storage.WithListPrefix("docs/"))
	if err != nil {
		t.Fatalf("List with prefix failed: %v", err)
	}

	if len(docsInfos) < 2 {
		t.Errorf("expected at least 2 docs objects, got %d", len(docsInfos))
	}
}

// TestJetStreamBackend_StatWrapper tests the Stat wrapper function
func TestJetStreamBackend_StatWrapper(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set a value
	testData := []byte("content for stat")
	err := backend.Set("stat-file.txt", testData, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Stat wrapper
	info, err := backend.Stat("stat-file.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Name != "stat-file.txt" {
		t.Errorf("expected name 'stat-file.txt', got %q", info.Name)
	}
	if info.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size)
	}

	// Test Stat for non-existent key
	_, err = backend.Stat("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestJetStreamBackend_GetReader tests the GetReader wrapper function
func TestJetStreamBackend_GetReader(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Set a value
	testData := []byte("reader content")
	err := backend.Set("reader-file.txt", testData, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test GetReader wrapper
	reader, info, err := backend.GetReader("reader-file.txt")
	if err != nil {
		t.Fatalf("GetReader failed: %v", err)
	}
	defer reader.Close()

	if info.Name != "reader-file.txt" {
		t.Errorf("expected name 'reader-file.txt', got %q", info.Name)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != "reader content" {
		t.Errorf("expected 'reader content', got %q", string(data))
	}

	// Test GetReader for non-existent key
	_, _, err = backend.GetReader("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestJetStreamBackend_PutReader tests the PutReader wrapper function
func TestJetStreamBackend_PutReader(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "test-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("test-bucket", os, 0, testLogger())

	// Test PutReader wrapper
	testData := []byte("put reader content")
	reader := bytes.NewReader(testData)
	info, err := backend.PutReader("put-reader.txt", reader, 0)
	if err != nil {
		t.Fatalf("PutReader failed: %v", err)
	}

	if info.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size)
	}

	// Verify it was stored
	data, err := backend.Get("put-reader.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(data) != "put reader content" {
		t.Errorf("expected 'put reader content', got %q", string(data))
	}

	// Test PutReader with options
	reader2 := bytes.NewReader([]byte("content with description"))
	info2, err := backend.PutReader("with-desc.txt", reader2, 0,
		storage.WithPutDescription("A file with description"),
	)
	if err != nil {
		t.Fatalf("PutReader with options failed: %v", err)
	}

	if info2.Description != "A file with description" {
		t.Errorf("expected description, got %q", info2.Description)
	}
}

// TestJetStreamBackend_ResetWithContext_EmptyBucket tests Reset on empty bucket
func TestJetStreamBackend_ResetWithContext_EmptyBucket(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)

	config := BucketConfig{
		Name:     "empty-bucket",
		Storage:  MemoryStorage,
		Replicas: 1,
	}

	os := createTestObjectStore(t, js, config)
	backend := NewJetStreamBackend("empty-bucket", os, 0, testLogger())

	ctx := context.Background()

	// Reset on empty bucket should not error
	err := backend.ResetWithContext(ctx)
	if err != nil {
		t.Errorf("expected no error resetting empty bucket, got: %v", err)
	}
}

// TestJetStreamBackend_GetReaderWithContext_InfoError tests GetReader info retrieval error path
func TestJetStreamBackend_GetReaderWithContext_InfoError(t *testing.T) {
	mockOS := &mockObjectStore{
		getResult: &mockObjectResult{
			data:    []byte("test data"),
			infoErr: errors.New("failed to get info"),
		},
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, _, err := backend.GetReaderWithContext(ctx, "file.txt")
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to get object info") {
		t.Errorf("expected 'failed to get object info' in error, got: %v", err)
	}
}

// TestJetStreamBackend_ResetWithContext_ListError tests Reset when list fails
func TestJetStreamBackend_ResetWithContext_ListError(t *testing.T) {
	mockOS := &mockObjectStore{
		listErr: errors.New("bucket unavailable"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	err := backend.ResetWithContext(ctx)
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to list objects for reset") {
		t.Errorf("expected 'failed to list objects for reset' in error, got: %v", err)
	}
}

// TestJetStreamBackend_PutReaderWithContext_Error tests PutReader error path
func TestJetStreamBackend_PutReaderWithContext_Error(t *testing.T) {
	mockOS := &mockObjectStore{
		putErr: errors.New("storage full"),
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	reader := bytes.NewReader([]byte("test"))
	_, err := backend.PutReaderWithContext(ctx, "file.txt", reader, 0)
	if err == nil {
		t.Error("expected error")
	}

	if !strings.Contains(err.Error(), "failed to put object") {
		t.Errorf("expected 'failed to put object' in error, got: %v", err)
	}
}

// TestJetStreamBackend_GetReaderWithContext_NotFound tests GetReader for non-existent key
func TestJetStreamBackend_GetReaderWithContext_NotFound(t *testing.T) {
	mockOS := &mockObjectStore{
		getErr: jetstream.ErrObjectNotFound,
	}

	backend := NewJetStreamBackend("test-bucket", mockOS, 0, testLogger())
	ctx := context.Background()

	_, _, err := backend.GetReaderWithContext(ctx, "nonexistent.txt")
	if err == nil {
		t.Error("expected error for non-existent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}
