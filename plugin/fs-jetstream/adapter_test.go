package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/storage"
)

// =============================================================================
// Mock Backend for Adapter Tests
// =============================================================================

// adapterMockBackend implements storage.Storage and extended interfaces for adapter testing.
// This is similar to mockBackend in module_test.go but specifically for adapter tests.
type adapterMockBackend struct {
	bucketName string
	objects    map[string][]byte
	metadata   map[string]*adapterObjectMetadata
	mu         sync.RWMutex
	putErr     error
	getErr     error
	deleteErr  error
	resetErr   error
	closeErr   error
	listErr    error
	statErr    error
}

type adapterObjectMetadata struct {
	Description string
	Headers     map[string]string
	Size        int64
	ModTime     time.Time
}

// Compile-time interface checks
var (
	_ storage.Storage           = (*adapterMockBackend)(nil)
	_ storage.StorageWithBucket = (*adapterMockBackend)(nil)
	_ storage.StorageWithList   = (*adapterMockBackend)(nil)
	_ storage.StorageWithStat   = (*adapterMockBackend)(nil)
	_ storage.StorageWithReader = (*adapterMockBackend)(nil)
)

func newAdapterMockBackend(bucketName string) *adapterMockBackend {
	return &adapterMockBackend{
		bucketName: bucketName,
		objects:    make(map[string][]byte),
		metadata:   make(map[string]*adapterObjectMetadata),
	}
}

func (b *adapterMockBackend) BucketName() string { return b.bucketName }

func (b *adapterMockBackend) Get(key string) ([]byte, error) {
	return b.GetWithContext(context.Background(), key)
}

func (b *adapterMockBackend) GetWithContext(_ context.Context, key string) ([]byte, error) {
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

func (b *adapterMockBackend) Set(key string, val []byte, exp time.Duration) error {
	return b.SetWithContext(context.Background(), key, val, exp)
}

func (b *adapterMockBackend) SetWithContext(_ context.Context, key string, val []byte, _ time.Duration) error {
	if b.putErr != nil {
		return b.putErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = val
	return nil
}

func (b *adapterMockBackend) Delete(key string) error {
	return b.DeleteWithContext(context.Background(), key)
}

func (b *adapterMockBackend) DeleteWithContext(_ context.Context, key string) error {
	if b.deleteErr != nil {
		return b.deleteErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	return nil
}

func (b *adapterMockBackend) Reset() error {
	return b.ResetWithContext(context.Background())
}

func (b *adapterMockBackend) ResetWithContext(_ context.Context) error {
	if b.resetErr != nil {
		return b.resetErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects = make(map[string][]byte)
	return nil
}

func (b *adapterMockBackend) Close() error {
	return b.closeErr
}

// storage.StorageWithList implementation
func (b *adapterMockBackend) List(opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	return b.ListWithContext(context.Background(), opts...)
}

func (b *adapterMockBackend) ListWithContext(_ context.Context, opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	if b.listErr != nil {
		return nil, b.listErr
	}
	options := storage.ApplyListOptions(opts...)

	b.mu.RLock()
	defer b.mu.RUnlock()
	var result []storage.ObjectInfo
	for name, data := range b.objects {
		if options.Prefix == "" || (len(name) >= len(options.Prefix) && name[:len(options.Prefix)] == options.Prefix) {
			info := storage.ObjectInfo{
				Bucket:  b.bucketName,
				Name:    name,
				Size:    int64(len(data)),
				ModTime: time.Now(),
			}
			result = append(result, info)
		}
	}
	return result, nil
}

// storage.StorageWithStat implementation
func (b *adapterMockBackend) Stat(key string) (*storage.ObjectInfo, error) {
	return b.StatWithContext(context.Background(), key)
}

func (b *adapterMockBackend) StatWithContext(_ context.Context, key string) (*storage.ObjectInfo, error) {
	if b.statErr != nil {
		return nil, b.statErr
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return &storage.ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}, nil
}

// storage.StorageWithReader implementation
func (b *adapterMockBackend) GetReader(key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return b.GetReaderWithContext(context.Background(), key)
}

func (b *adapterMockBackend) GetReaderWithContext(_ context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	data, err := b.GetWithContext(context.Background(), key)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, storage.ErrKeyNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}, nil
}

func (b *adapterMockBackend) PutReader(key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	return b.PutReaderWithContext(context.Background(), key, reader, exp, opts...)
}

func (b *adapterMockBackend) PutReaderWithContext(_ context.Context, key string, reader io.Reader, _ time.Duration, _ ...storage.PutOption) (*storage.ObjectInfo, error) {
	if b.putErr != nil {
		return nil, b.putErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = data

	return &storage.ObjectInfo{
		Bucket:  b.bucketName,
		Name:    key,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}, nil
}

// =============================================================================
// Tests for Adapter Wrapper Functions (Task 11)
// =============================================================================

// TestAdapter_Get_Wrapper tests the Get() wrapper function (line 52)
func TestAdapter_Get_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store a value first
	backend.objects["test-key"] = []byte("test-value")

	// Test Get() wrapper - should delegate to GetWithContext()
	data, err := adapter.Get("test-key")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if string(data) != "test-value" {
		t.Errorf("expected 'test-value', got %q", string(data))
	}
}

// TestAdapter_Get_Wrapper_NonExistent tests Get() for non-existent key
func TestAdapter_Get_Wrapper_NonExistent(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Get() for non-existent key
	data, err := adapter.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for non-existent key, got %v", data)
	}
}

// TestAdapter_Get_Wrapper_Error tests Get() when backend returns error
func TestAdapter_Get_Wrapper_Error(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	backend.getErr = errors.New("get error")
	adapter := NewAdapter(backend, slog.Default())

	// Test Get() with error
	_, err := adapter.Get("test-key")
	if err == nil {
		t.Fatal("expected error from Get()")
	}
	if err.Error() != "get error" {
		t.Errorf("expected 'get error', got %q", err.Error())
	}
}

// TestAdapter_Set_Wrapper tests the Set() wrapper function (line 64)
func TestAdapter_Set_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Set() wrapper - should delegate to SetWithContext()
	err := adapter.Set("test-key", []byte("test-value"), 0)
	if err != nil {
		t.Fatalf("Set() returned error: %v", err)
	}

	// Verify the value was stored
	data, _ := adapter.Get("test-key")
	if string(data) != "test-value" {
		t.Errorf("expected 'test-value', got %q", string(data))
	}
}

// TestAdapter_Set_Wrapper_WithExpiration tests Set() with expiration
func TestAdapter_Set_Wrapper_WithExpiration(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Set() with expiration (expiration is passed through but our mock ignores it)
	err := adapter.Set("expiring-key", []byte("expires"), 1*time.Hour)
	if err != nil {
		t.Fatalf("Set() with expiration returned error: %v", err)
	}

	// Verify the value was stored
	data, _ := adapter.Get("expiring-key")
	if string(data) != "expires" {
		t.Errorf("expected 'expires', got %q", string(data))
	}
}

// TestAdapter_SetWithContext_EmptyKey tests SetWithContext() with empty key (line 69-75)
func TestAdapter_SetWithContext_EmptyKey(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test SetWithContext() with empty key - should return nil without storing
	err := adapter.SetWithContext(ctx, "", []byte("value"), 0)
	if err != nil {
		t.Fatalf("SetWithContext() with empty key returned error: %v", err)
	}

	// Verify nothing was stored
	if len(backend.objects) != 0 {
		t.Errorf("expected no objects stored for empty key, got %d", len(backend.objects))
	}
}

// TestAdapter_SetWithContext_NilValue tests SetWithContext() with nil value
func TestAdapter_SetWithContext_NilValue(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test SetWithContext() with nil value - should return nil without storing
	err := adapter.SetWithContext(ctx, "test-key", nil, 0)
	if err != nil {
		t.Fatalf("SetWithContext() with nil value returned error: %v", err)
	}

	// Verify nothing was stored
	if len(backend.objects) != 0 {
		t.Errorf("expected no objects stored for nil value, got %d", len(backend.objects))
	}
}

// TestAdapter_SetWithContext_Success tests SetWithContext() successful path
func TestAdapter_SetWithContext_Success(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test SetWithContext() successful path
	err := adapter.SetWithContext(ctx, "valid-key", []byte("valid-value"), 0)
	if err != nil {
		t.Fatalf("SetWithContext() returned error: %v", err)
	}

	// Verify the value was stored
	data, _ := adapter.Get("valid-key")
	if string(data) != "valid-value" {
		t.Errorf("expected 'valid-value', got %q", string(data))
	}
}

// TestAdapter_Delete_Wrapper tests the Delete() wrapper function (line 79)
func TestAdapter_Delete_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store a value first
	backend.objects["delete-key"] = []byte("to-delete")

	// Test Delete() wrapper - should delegate to DeleteWithContext()
	err := adapter.Delete("delete-key")
	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	// Verify the value was deleted
	if _, exists := backend.objects["delete-key"]; exists {
		t.Error("expected key to be deleted")
	}
}

// TestAdapter_Delete_Wrapper_NonExistent tests Delete() for non-existent key
func TestAdapter_Delete_Wrapper_NonExistent(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Delete() for non-existent key - should succeed (idempotent)
	err := adapter.Delete("nonexistent")
	if err != nil {
		t.Fatalf("Delete() for non-existent key returned error: %v", err)
	}
}

// TestAdapter_Delete_Wrapper_Error tests Delete() when backend returns error
func TestAdapter_Delete_Wrapper_Error(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	backend.deleteErr = errors.New("delete error")
	adapter := NewAdapter(backend, slog.Default())

	// Test Delete() with error
	err := adapter.Delete("test-key")
	if err == nil {
		t.Fatal("expected error from Delete()")
	}
	if err.Error() != "delete error" {
		t.Errorf("expected 'delete error', got %q", err.Error())
	}
}

// TestAdapter_Reset_Wrapper tests the Reset() wrapper function (line 90)
func TestAdapter_Reset_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store some values
	backend.objects["key1"] = []byte("value1")
	backend.objects["key2"] = []byte("value2")

	// Test Reset() wrapper - should delegate to ResetWithContext()
	err := adapter.Reset()
	if err != nil {
		t.Fatalf("Reset() returned error: %v", err)
	}

	// Verify all values were deleted
	if len(backend.objects) != 0 {
		t.Errorf("expected empty objects after Reset(), got %d", len(backend.objects))
	}
}

// TestAdapter_Reset_Wrapper_Error tests Reset() when backend returns error
func TestAdapter_Reset_Wrapper_Error(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	backend.resetErr = errors.New("reset error")
	adapter := NewAdapter(backend, slog.Default())

	// Test Reset() with error
	err := adapter.Reset()
	if err == nil {
		t.Fatal("expected error from Reset()")
	}
	if err.Error() != "reset error" {
		t.Errorf("expected 'reset error', got %q", err.Error())
	}
}

// TestAdapter_ResetWithContext tests ResetWithContext() directly (line 95)
func TestAdapter_ResetWithContext(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Store some values
	backend.objects["key1"] = []byte("value1")
	backend.objects["key2"] = []byte("value2")

	// Test ResetWithContext()
	err := adapter.ResetWithContext(ctx)
	if err != nil {
		t.Fatalf("ResetWithContext() returned error: %v", err)
	}

	// Verify all values were deleted
	if len(backend.objects) != 0 {
		t.Errorf("expected empty objects after ResetWithContext(), got %d", len(backend.objects))
	}
}

// TestAdapter_Close tests the Close() function (line 100)
func TestAdapter_Close(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Close() - should delegate to backend.Close()
	err := adapter.Close()
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

// TestAdapter_Close_Error tests Close() when backend returns error
func TestAdapter_Close_Error(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	backend.closeErr = errors.New("close error")
	adapter := NewAdapter(backend, slog.Default())

	// Test Close() with error
	err := adapter.Close()
	if err == nil {
		t.Fatal("expected error from Close()")
	}
	if err.Error() != "close error" {
		t.Errorf("expected 'close error', got %q", err.Error())
	}
}

// =============================================================================
// Additional Edge Case Tests
// =============================================================================

// TestAdapter_Set_EmptyKeyAndValue tests Set() with both empty key and value
func TestAdapter_Set_EmptyKeyAndValue(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Set() with empty key
	err := adapter.Set("", []byte("value"), 0)
	if err != nil {
		t.Fatalf("Set() with empty key returned error: %v", err)
	}

	// Verify nothing was stored
	if len(backend.objects) != 0 {
		t.Errorf("expected no objects stored for empty key, got %d", len(backend.objects))
	}
}

// TestAdapter_WrapperFunctions_ConcurrentAccess tests concurrent access to wrapper functions
func TestAdapter_WrapperFunctions_ConcurrentAccess(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	var wg sync.WaitGroup
	iterations := 50

	// Concurrent Get, Set, Delete, Reset operations
	for i := 0; i < iterations; i++ {
		wg.Add(4)

		go func(n int) {
			defer wg.Done()
			key := "concurrent-key"
			_ = adapter.Set(key, []byte("value"), 0)
		}(i)

		go func() {
			defer wg.Done()
			_, _ = adapter.Get("concurrent-key")
		}()

		go func() {
			defer wg.Done()
			_ = adapter.Delete("concurrent-key")
		}()

		go func() {
			defer wg.Done()
			// Only reset occasionally to avoid clearing all data
			if i%10 == 0 {
				_ = adapter.Reset()
			}
		}()
	}

	wg.Wait()
}

// =============================================================================
// Tests for List/Stat/Reader Methods (Task 12)
// =============================================================================

// minimalStorageBackend implements only storage.Storage (no extended interfaces).
// This is used to test the fallback paths in adapter.go.
type minimalStorageBackend struct {
	objects map[string][]byte
	mu      sync.RWMutex
	getErr  error
	setErr  error
}

// Compile-time check - only implements storage.Storage
var _ storage.Storage = (*minimalStorageBackend)(nil)

func newMinimalStorageBackend() *minimalStorageBackend {
	return &minimalStorageBackend{
		objects: make(map[string][]byte),
	}
}

func (b *minimalStorageBackend) Get(key string) ([]byte, error) {
	return b.GetWithContext(context.Background(), key)
}

func (b *minimalStorageBackend) GetWithContext(_ context.Context, key string) ([]byte, error) {
	if b.getErr != nil {
		return nil, b.getErr
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (b *minimalStorageBackend) Set(key string, val []byte, exp time.Duration) error {
	return b.SetWithContext(context.Background(), key, val, exp)
}

func (b *minimalStorageBackend) SetWithContext(_ context.Context, key string, val []byte, _ time.Duration) error {
	if b.setErr != nil {
		return b.setErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = val
	return nil
}

func (b *minimalStorageBackend) Delete(key string) error {
	return b.DeleteWithContext(context.Background(), key)
}

func (b *minimalStorageBackend) DeleteWithContext(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	return nil
}

func (b *minimalStorageBackend) Reset() error {
	return b.ResetWithContext(context.Background())
}

func (b *minimalStorageBackend) ResetWithContext(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects = make(map[string][]byte)
	return nil
}

func (b *minimalStorageBackend) Close() error {
	return nil
}

// TestAdapter_List_Wrapper tests the List() wrapper function (line 121)
func TestAdapter_List_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store some values
	backend.objects["file1.txt"] = []byte("content1")
	backend.objects["file2.txt"] = []byte("content2")

	// Test List() wrapper - should delegate to ListWithContext()
	objects, err := adapter.List()
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(objects))
	}
}

// TestAdapter_List_Wrapper_WithPrefix tests List() with prefix option
func TestAdapter_List_Wrapper_WithPrefix(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store some values
	backend.objects["docs/file1.txt"] = []byte("content1")
	backend.objects["docs/file2.txt"] = []byte("content2")
	backend.objects["images/logo.png"] = []byte("content3")

	// Test List() with prefix
	objects, err := adapter.List(storage.WithListPrefix("docs/"))
	if err != nil {
		t.Fatalf("List() with prefix returned error: %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("expected 2 objects with 'docs/' prefix, got %d", len(objects))
	}
}

// TestAdapter_ListWithContext_FallbackPath tests ListWithContext() fallback when list is nil (line 131-132)
func TestAdapter_ListWithContext_FallbackPath(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithList
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test ListWithContext() fallback - should return empty slice
	objects, err := adapter.ListWithContext(ctx)
	if err != nil {
		t.Fatalf("ListWithContext() fallback returned error: %v", err)
	}
	if objects == nil {
		t.Error("expected non-nil empty slice, got nil")
	}
	if len(objects) != 0 {
		t.Errorf("expected 0 objects in fallback, got %d", len(objects))
	}
}

// TestAdapter_Stat_Wrapper tests the Stat() wrapper function (line 140)
func TestAdapter_Stat_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store a value
	backend.objects["test-file.txt"] = []byte("test content")

	// Test Stat() wrapper - should delegate to StatWithContext()
	info, err := adapter.Stat("test-file.txt")
	if err != nil {
		t.Fatalf("Stat() returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Name != "test-file.txt" {
		t.Errorf("expected name 'test-file.txt', got %q", info.Name)
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}
}

// TestAdapter_Stat_Wrapper_NonExistent tests Stat() for non-existent key
func TestAdapter_Stat_Wrapper_NonExistent(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test Stat() for non-existent key
	info, err := adapter.Stat("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if info != nil {
		t.Error("expected nil ObjectInfo for non-existent key")
	}
}

// TestAdapter_StatWithContext_FallbackPath tests StatWithContext() fallback when stat is nil (lines 150-162)
func TestAdapter_StatWithContext_FallbackPath(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithStat
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Store a value
	backend.objects["test-file.txt"] = []byte("test content")

	// Test StatWithContext() fallback - should use Get and return minimal info
	info, err := adapter.StatWithContext(ctx, "test-file.txt")
	if err != nil {
		t.Fatalf("StatWithContext() fallback returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Name != "test-file.txt" {
		t.Errorf("expected name 'test-file.txt', got %q", info.Name)
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}
}

// TestAdapter_StatWithContext_FallbackPath_Error tests StatWithContext() fallback with Get error (line 152-153)
func TestAdapter_StatWithContext_FallbackPath_Error(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithStat
	backend := newMinimalStorageBackend()
	backend.getErr = errors.New("get error")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test StatWithContext() fallback with Get error
	info, err := adapter.StatWithContext(ctx, "test-file.txt")
	if err == nil {
		t.Fatal("expected error from StatWithContext() fallback")
	}
	if err.Error() != "get error" {
		t.Errorf("expected 'get error', got %q", err.Error())
	}
	if info != nil {
		t.Error("expected nil ObjectInfo on error")
	}
}

// TestAdapter_StatWithContext_FallbackPath_NilData tests StatWithContext() fallback with nil data (line 155-156)
func TestAdapter_StatWithContext_FallbackPath_NilData(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithStat
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test StatWithContext() fallback with nil data (key not found)
	info, err := adapter.StatWithContext(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if err != storage.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
	if info != nil {
		t.Error("expected nil ObjectInfo for non-existent key")
	}
}

// TestAdapter_GetReader_Wrapper tests the GetReader() wrapper function (line 170)
func TestAdapter_GetReader_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Store a value
	backend.objects["test-file.txt"] = []byte("test content")

	// Test GetReader() wrapper - should delegate to GetReaderWithContext()
	reader, info, err := adapter.GetReader("test-file.txt")
	if err != nil {
		t.Fatalf("GetReader() returned error: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
	defer reader.Close()

	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}

	// Read and verify content
	data, _ := io.ReadAll(reader)
	if string(data) != "test content" {
		t.Errorf("expected 'test content', got %q", string(data))
	}
}

// TestAdapter_GetReader_Wrapper_NonExistent tests GetReader() for non-existent key
func TestAdapter_GetReader_Wrapper_NonExistent(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test GetReader() for non-existent key
	reader, info, err := adapter.GetReader("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if reader != nil {
		t.Error("expected nil reader for non-existent key")
	}
	if info != nil {
		t.Error("expected nil ObjectInfo for non-existent key")
	}
}

// TestAdapter_GetReaderWithContext_FallbackPath tests GetReaderWithContext() fallback (lines 180-192)
func TestAdapter_GetReaderWithContext_FallbackPath(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Store a value
	backend.objects["test-file.txt"] = []byte("test content")

	// Test GetReaderWithContext() fallback
	reader, info, err := adapter.GetReaderWithContext(ctx, "test-file.txt")
	if err != nil {
		t.Fatalf("GetReaderWithContext() fallback returned error: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
	defer reader.Close()

	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}

	// Read and verify content
	data, _ := io.ReadAll(reader)
	if string(data) != "test content" {
		t.Errorf("expected 'test content', got %q", string(data))
	}
}

// TestAdapter_GetReaderWithContext_FallbackPath_Error tests GetReaderWithContext() fallback with error (line 182-183)
func TestAdapter_GetReaderWithContext_FallbackPath_Error(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	backend.getErr = errors.New("get error")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test GetReaderWithContext() fallback with error
	reader, info, err := adapter.GetReaderWithContext(ctx, "test-file.txt")
	if err == nil {
		t.Fatal("expected error from GetReaderWithContext() fallback")
	}
	if err.Error() != "get error" {
		t.Errorf("expected 'get error', got %q", err.Error())
	}
	if reader != nil {
		t.Error("expected nil reader on error")
	}
	if info != nil {
		t.Error("expected nil ObjectInfo on error")
	}
}

// TestAdapter_GetReaderWithContext_FallbackPath_NilData tests GetReaderWithContext() fallback with nil data (line 185-186)
func TestAdapter_GetReaderWithContext_FallbackPath_NilData(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test GetReaderWithContext() fallback with nil data (key not found)
	reader, info, err := adapter.GetReaderWithContext(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if err != storage.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
	if reader != nil {
		t.Error("expected nil reader for non-existent key")
	}
	if info != nil {
		t.Error("expected nil ObjectInfo for non-existent key")
	}
}

// TestAdapter_PutReader_Wrapper tests the PutReader() wrapper function (line 196)
func TestAdapter_PutReader_Wrapper(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test PutReader() wrapper - should delegate to PutReaderWithContext()
	reader := bytes.NewReader([]byte("test content"))
	info, err := adapter.PutReader("test-file.txt", reader, 0)
	if err != nil {
		t.Fatalf("PutReader() returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}

	// Verify the value was stored
	data, _ := adapter.Get("test-file.txt")
	if string(data) != "test content" {
		t.Errorf("expected 'test content', got %q", string(data))
	}
}

// TestAdapter_PutReader_Wrapper_WithExpiration tests PutReader() with expiration
func TestAdapter_PutReader_Wrapper_WithExpiration(t *testing.T) {
	backend := newAdapterMockBackend("test-bucket")
	adapter := NewAdapter(backend, slog.Default())

	// Test PutReader() with expiration
	reader := bytes.NewReader([]byte("expiring content"))
	info, err := adapter.PutReader("expiring-file.txt", reader, 1*time.Hour)
	if err != nil {
		t.Fatalf("PutReader() with expiration returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
}

// TestAdapter_PutReaderWithContext_FallbackPath tests PutReaderWithContext() fallback (lines 206-219)
func TestAdapter_PutReaderWithContext_FallbackPath(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test PutReaderWithContext() fallback
	reader := bytes.NewReader([]byte("test content"))
	info, err := adapter.PutReaderWithContext(ctx, "test-file.txt", reader, 0)
	if err != nil {
		t.Fatalf("PutReaderWithContext() fallback returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}

	// Verify the value was stored
	data := backend.objects["test-file.txt"]
	if string(data) != "test content" {
		t.Errorf("expected 'test content', got %q", string(data))
	}
}

// TestAdapter_PutReaderWithContext_FallbackPath_ReadError tests PutReaderWithContext() fallback with read error (line 207-209)
func TestAdapter_PutReaderWithContext_FallbackPath_ReadError(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test PutReaderWithContext() fallback with read error
	errorReader := &errorReaderForAdapter{err: errors.New("read error")}
	info, err := adapter.PutReaderWithContext(ctx, "test-file.txt", errorReader, 0)
	if err == nil {
		t.Fatal("expected error from PutReaderWithContext() fallback")
	}
	if err.Error() != "read error" {
		t.Errorf("expected 'read error', got %q", err.Error())
	}
	if info != nil {
		t.Error("expected nil ObjectInfo on error")
	}
}

// TestAdapter_PutReaderWithContext_FallbackPath_SetError tests PutReaderWithContext() fallback with Set error (line 211-213)
func TestAdapter_PutReaderWithContext_FallbackPath_SetError(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	backend.setErr = errors.New("set error")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test PutReaderWithContext() fallback with Set error
	reader := bytes.NewReader([]byte("test content"))
	info, err := adapter.PutReaderWithContext(ctx, "test-file.txt", reader, 0)
	if err == nil {
		t.Fatal("expected error from PutReaderWithContext() fallback")
	}
	if err.Error() != "set error" {
		t.Errorf("expected 'set error', got %q", err.Error())
	}
	if info != nil {
		t.Error("expected nil ObjectInfo on error")
	}
}

// TestAdapter_Put_FallbackPath tests Put() fallback when reader is nil (lines 238-248)
func TestAdapter_Put_FallbackPath(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test Put() fallback
	info, err := adapter.Put(ctx, "test-file.txt", []byte("test content"))
	if err != nil {
		t.Fatalf("Put() fallback returned error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil ObjectInfo")
	}
	if info.Size != 12 {
		t.Errorf("expected size 12, got %d", info.Size)
	}
	if info.Name != "test-file.txt" {
		t.Errorf("expected name 'test-file.txt', got %q", info.Name)
	}

	// Verify the value was stored
	data := backend.objects["test-file.txt"]
	if string(data) != "test content" {
		t.Errorf("expected 'test content', got %q", string(data))
	}
}

// TestAdapter_Put_FallbackPath_Error tests Put() fallback with Set error (line 239-241)
func TestAdapter_Put_FallbackPath_Error(t *testing.T) {
	// Use minimal backend that doesn't implement StorageWithReader
	backend := newMinimalStorageBackend()
	backend.setErr = errors.New("set error")
	adapter := NewAdapter(backend, slog.Default())

	ctx := context.Background()

	// Test Put() fallback with Set error
	info, err := adapter.Put(ctx, "test-file.txt", []byte("test content"))
	if err == nil {
		t.Fatal("expected error from Put() fallback")
	}
	if err.Error() != "set error" {
		t.Errorf("expected 'set error', got %q", err.Error())
	}
	if info != nil {
		t.Error("expected nil ObjectInfo on error")
	}
}

// errorReaderForAdapter is a reader that always returns an error
type errorReaderForAdapter struct {
	err error
}

func (r *errorReaderForAdapter) Read(_ []byte) (int, error) {
	return 0, r.err
}
