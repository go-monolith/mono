package kvjetstream

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Non-Context Wrapper Method Tests (Get, Set, Delete)
// These test the convenience wrapper methods that delegate to *WithContext()
// =============================================================================

func TestKVAdapter_Get_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Put first using the context-based method
	_, err := adapter.PutWithRevisionWithContext(context.Background(), "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Get() wrapper (non-context version)
	data, err := adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(data))
	}
}

func TestKVAdapter_Get_Wrapper_NotFound(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Get() for non-existent key should return nil, nil per storage.Storage contract
	data, err := adapter.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get should not return error for missing key, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for missing key, got: %v", data)
	}
}

func TestKVAdapter_Get_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.getErr = errors.New("get error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Get("key1")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "get error" {
		t.Errorf("expected 'get error', got %q", err.Error())
	}
}

func TestKVAdapter_Set_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Test Set() wrapper (non-context version)
	err := adapter.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify value was stored
	data, err := adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(data))
	}
}

func TestKVAdapter_Set_Wrapper_EmptyKey(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Set() with empty key should be ignored without error
	err := adapter.Set("", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set with empty key should not fail, got: %v", err)
	}
}

func TestKVAdapter_Set_Wrapper_NilValue(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Set() with nil value should be ignored without error
	err := adapter.Set("key1", nil, 0)
	if err != nil {
		t.Fatalf("Set with nil value should not fail, got: %v", err)
	}
}

func TestKVAdapter_Set_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.putErr = errors.New("put error")
	adapter := NewKVAdapter(backend, slog.Default())

	err := adapter.Set("key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "put error" {
		t.Errorf("expected 'put error', got %q", err.Error())
	}
}

func TestKVAdapter_Delete_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Put first
	_, err := adapter.PutWithRevisionWithContext(context.Background(), "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Delete() wrapper (non-context version)
	err = adapter.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify key was deleted
	data, err := adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get should not return error, got: %v", err)
	}
	if data != nil {
		t.Error("expected nil data after delete")
	}
}

func TestKVAdapter_Delete_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.deleteErr = errors.New("delete error")
	adapter := NewKVAdapter(backend, slog.Default())

	err := adapter.Delete("key1")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "delete error" {
		t.Errorf("expected 'delete error', got %q", err.Error())
	}
}

// =============================================================================
// Reset and Close Operation Tests
// These test Reset(), ResetWithContext(), and Close() methods
// =============================================================================

func TestKVAdapter_Reset_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Add some data first
	_, err := adapter.PutWithRevisionWithContext(context.Background(), "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Verify data exists
	data, err := adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if data == nil {
		t.Fatal("expected data before reset")
	}

	// Test Reset() wrapper (non-context version)
	err = adapter.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify data is cleared after reset
	data, err = adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get after reset failed: %v", err)
	}
	if data != nil {
		t.Error("expected nil data after reset")
	}
}

func TestKVAdapter_Reset_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.resetErr = errors.New("reset error")
	adapter := NewKVAdapter(backend, slog.Default())

	err := adapter.Reset()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "reset error" {
		t.Errorf("expected 'reset error', got %q", err.Error())
	}
}

func TestKVAdapter_ResetWithContext(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Add some data first
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	_, err = adapter.PutWithRevisionWithContext(ctx, "key2", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test ResetWithContext
	err = adapter.ResetWithContext(ctx)
	if err != nil {
		t.Fatalf("ResetWithContext failed: %v", err)
	}

	// Verify all data is cleared
	keys, err := adapter.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 keys after reset, got %d", len(keys))
	}
}

func TestKVAdapter_ResetWithContext_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.resetErr = errors.New("reset context error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	err := adapter.ResetWithContext(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "reset context error" {
		t.Errorf("expected 'reset context error', got %q", err.Error())
	}
}

func TestKVAdapter_Close(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Test Close()
	err := adapter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestKVAdapter_Close_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.closeErr = errors.New("close error")
	adapter := NewKVAdapter(backend, slog.Default())

	err := adapter.Close()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "close error" {
		t.Errorf("expected 'close error', got %q", err.Error())
	}
}

// =============================================================================
// Advanced Operation Wrapper Tests (Watch, Create, Update, Purge, Keys, Status, GetEntry, PutWithRevision)
// These test the non-context convenience wrapper methods
// =============================================================================

func TestKVAdapter_Watch_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Test Watch() wrapper (non-context version)
	watcher, err := adapter.Watch(">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if watcher == nil {
		t.Fatal("expected watcher, got nil")
	}
	defer watcher.Stop()
}

func TestKVAdapter_Watch_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.watchErr = errors.New("watch error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Watch(">")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "watch error" {
		t.Errorf("expected 'watch error', got %q", err.Error())
	}
}

func TestKVAdapter_Create_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Test Create() wrapper (non-context version)
	rev, err := adapter.Create("new-key", []byte("new-value"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Verify value was stored
	data, err := adapter.Get("new-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "new-value" {
		t.Errorf("expected 'new-value', got %q", string(data))
	}
}

func TestKVAdapter_Create_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.createErr = errors.New("create error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Create("key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "create error" {
		t.Errorf("expected 'create error', got %q", err.Error())
	}
}

func TestKVAdapter_Create_Wrapper_AlreadyExists(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Create first
	_, err := adapter.Create("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Second create should fail
	_, err = adapter.Create("key1", []byte("value2"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}
}

func TestKVAdapter_Update_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Put first to get a revision
	rev, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Update() wrapper (non-context version)
	newRev, err := adapter.Update("key1", []byte("value2"), 0, rev)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if newRev <= rev {
		t.Error("expected revision to increment")
	}
}

func TestKVAdapter_Update_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.updateErr = errors.New("update error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Update("key1", []byte("value1"), 0, 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "update error" {
		t.Errorf("expected 'update error', got %q", err.Error())
	}
}

func TestKVAdapter_Purge_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Put first
	_, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Purge() wrapper (non-context version)
	err = adapter.Purge("key1")
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}

	// Verify key was actually purged
	_, err = adapter.GetEntry("key1")
	if err == nil || !errors.Is(err, ErrKeyNotFound) {
		t.Error("expected key to be purged")
	}
}

func TestKVAdapter_Purge_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.purgeErr = errors.New("purge error")
	adapter := NewKVAdapter(backend, slog.Default())

	err := adapter.Purge("key1")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "purge error" {
		t.Errorf("expected 'purge error', got %q", err.Error())
	}
}

func TestKVAdapter_Keys_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Add some keys
	_, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	_, err = adapter.PutWithRevision("key2", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Keys() wrapper (non-context version)
	keys, err := adapter.Keys()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestKVAdapter_Keys_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.keysErr = errors.New("keys error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Keys()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "keys error" {
		t.Errorf("expected 'keys error', got %q", err.Error())
	}
}

func TestKVAdapter_Status_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Add some data
	_, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Status() wrapper (non-context version)
	status, err := adapter.Status()
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.Bucket != "test" {
		t.Errorf("expected bucket 'test', got %q", status.Bucket)
	}
	if status.Values != 1 {
		t.Errorf("expected 1 value, got %d", status.Values)
	}
}

func TestKVAdapter_Status_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.statusErr = errors.New("status error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.Status()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "status error" {
		t.Errorf("expected 'status error', got %q", err.Error())
	}
}

func TestKVAdapter_GetEntry_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Put first
	_, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test GetEntry() wrapper (non-context version)
	entry, err := adapter.GetEntry("key1")
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if entry.Key != "key1" {
		t.Errorf("expected key 'key1', got %q", entry.Key)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(entry.Value))
	}
}

func TestKVAdapter_GetEntry_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.getErr = errors.New("get error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.GetEntry("key1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVAdapter_GetEntry_Wrapper_NotFound(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.GetEntry("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestKVAdapter_PutWithRevision_Wrapper(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())

	// Test PutWithRevision() wrapper (non-context version)
	rev, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Verify value was stored
	data, err := adapter.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got %q", string(data))
	}
}

func TestKVAdapter_PutWithRevision_Wrapper_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.putErr = errors.New("put error")
	adapter := NewKVAdapter(backend, slog.Default())

	_, err := adapter.PutWithRevision("key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "put error" {
		t.Errorf("expected 'put error', got %q", err.Error())
	}
}

// =============================================================================
// Adapter Constructor Tests
// =============================================================================

func TestNewKVAdapter(t *testing.T) {
	backend := newMockBackend("test-bucket")
	logger := slog.Default()

	adapter := NewKVAdapter(backend, logger)

	if adapter == nil {
		t.Fatal("expected adapter, got nil")
	}
	if adapter.storage != backend {
		t.Error("expected storage to be set")
	}
	// Logger is enhanced with bucket context, so we just check it's not nil
	if adapter.logger == nil {
		t.Error("expected logger to be set")
	}
}

func TestKVAdapter_BucketName(t *testing.T) {
	backend := newMockBackend("my-bucket")
	adapter := NewKVAdapter(backend, slog.Default())

	if adapter.BucketName() != "my-bucket" {
		t.Errorf("expected 'my-bucket', got %q", adapter.BucketName())
	}
}

// =============================================================================
// PutWithRevision Operation Tests
// =============================================================================

func TestKVAdapter_PutWithRevision(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	rev, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}
}

func TestKVAdapter_PutWithRevision_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.putErr = errors.New("put error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "put error" {
		t.Errorf("expected 'put error', got %q", err.Error())
	}
}

// =============================================================================
// GetEntry Operation Tests (for revision metadata)
// =============================================================================

func TestKVAdapter_GetEntry(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	entry, err := adapter.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if entry.Key != "key1" {
		t.Errorf("expected key 'key1', got %q", entry.Key)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(entry.Value))
	}
}

func TestKVAdapter_GetEntry_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.getErr = errors.New("get error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.GetEntryWithContext(ctx, "key1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVAdapter_GetEntry_NotFound(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.GetEntryWithContext(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// =============================================================================
// Get Operation Tests (returns []byte, nil for missing keys)
// =============================================================================

func TestKVAdapter_Get(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	data, err := adapter.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(data))
	}
}

func TestKVAdapter_Get_NotFound(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Get non-existent key returns nil, nil per storage.Storage contract
	data, err := adapter.GetWithContext(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get should not return error for missing key, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for missing key, got: %v", data)
	}
}

// =============================================================================
// Create Operation Tests
// =============================================================================

func TestKVAdapter_Create(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	rev, err := adapter.CreateWithContext(ctx, "new-key", []byte("new-value"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}
}

func TestKVAdapter_Create_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.createErr = errors.New("create error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVAdapter_Create_AlreadyExists(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Create first
	_, err := adapter.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Second create should fail
	_, err = adapter.CreateWithContext(ctx, "key1", []byte("value2"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}
}

// =============================================================================
// Update Operation Tests
// =============================================================================

func TestKVAdapter_Update(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	rev, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Update
	newRev, err := adapter.UpdateWithContext(ctx, "key1", []byte("value2"), 0, rev)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if newRev <= rev {
		t.Error("expected revision to increment")
	}
}

func TestKVAdapter_Update_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.updateErr = errors.New("update error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.UpdateWithContext(ctx, "key1", []byte("value1"), 0, 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVAdapter_Update_RevisionMismatch(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	rev, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Update with wrong revision
	_, err = adapter.UpdateWithContext(ctx, "key1", []byte("value2"), 0, rev+100)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

// =============================================================================
// Delete Operation Tests
// =============================================================================

func TestKVAdapter_Delete(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Delete
	err = adapter.DeleteWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestKVAdapter_Delete_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.deleteErr = errors.New("delete error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	err := adapter.DeleteWithContext(ctx, "key1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// =============================================================================
// Purge Operation Tests
// =============================================================================

func TestKVAdapter_Purge(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Put first
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Purge
	err = adapter.PurgeWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}
}

func TestKVAdapter_Purge_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.purgeErr = errors.New("purge error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	err := adapter.PurgeWithContext(ctx, "key1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// =============================================================================
// Keys Operation Tests
// =============================================================================

func TestKVAdapter_Keys(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Add some keys
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	_, err = adapter.PutWithRevisionWithContext(ctx, "key2", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	keys, err := adapter.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestKVAdapter_Keys_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.keysErr = errors.New("keys error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.KeysWithContext(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

// =============================================================================
// Watch Operation Tests
// =============================================================================

func TestKVAdapter_Watch(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	watcher, err := adapter.WatchWithContext(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watcher.Stop()

	if watcher == nil {
		t.Fatal("expected watcher, got nil")
	}
}

func TestKVAdapter_Watch_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.watchErr = errors.New("watch error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.WatchWithContext(ctx, ">")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestKVAdapter_Watch_WithOptions(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	watcher, err := adapter.WatchWithContext(ctx, "user.*",
		WithUpdatesOnly(),
		WithIgnoreDeletes(),
	)
	if err != nil {
		t.Fatalf("Watch with options failed: %v", err)
	}
	defer watcher.Stop()
}

func TestKVAdapter_WatchAll(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	watcher, err := adapter.WatchAll(ctx)
	if err != nil {
		t.Fatalf("WatchAll failed: %v", err)
	}
	defer watcher.Stop()

	if watcher == nil {
		t.Fatal("expected watcher, got nil")
	}
}

// =============================================================================
// Status Operation Tests
// =============================================================================

func TestKVAdapter_Status(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	// Add some data
	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	status, err := adapter.StatusWithContext(ctx)
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status.Bucket != "test" {
		t.Errorf("expected bucket 'test', got %q", status.Bucket)
	}
	if status.Values != 1 {
		t.Errorf("expected 1 value, got %d", status.Values)
	}
}

func TestKVAdapter_Status_Error(t *testing.T) {
	backend := newMockBackend("test")
	backend.statusErr = errors.New("status error")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	_, err := adapter.StatusWithContext(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

func TestKVAdapter_ConcurrentOperations(t *testing.T) {
	backend := newMockBackend("test")
	adapter := NewKVAdapter(backend, slog.Default())
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(4)

		// Concurrent puts
		go func() {
			defer wg.Done()
			_, _ = adapter.PutWithRevisionWithContext(ctx, "key", []byte("value"), 0)
		}()

		// Concurrent gets
		go func() {
			defer wg.Done()
			_, _ = adapter.GetWithContext(ctx, "key")
		}()

		// Concurrent keys
		go func() {
			defer wg.Done()
			_, _ = adapter.KeysWithContext(ctx)
		}()

		// Concurrent status
		go func() {
			defer wg.Done()
			_, _ = adapter.StatusWithContext(ctx)
		}()
	}
	wg.Wait()
}

// =============================================================================
// Interface Compliance Tests
// =============================================================================

func TestKVAdapter_ImplementsKVStoragePort(t *testing.T) {
	var _ KVStoragePort = (*KVStorageAdapter)(nil)
}

// =============================================================================
// Minimal Storage Mock (only implements storage.Storage, no extended interfaces)
// =============================================================================

// minimalStorageMock implements only storage.Storage interface (no extended interfaces)
// This is used to test fallback paths in the adapter when extended interfaces are nil.
type minimalStorageMock struct {
	data   map[string][]byte
	getErr error
	setErr error
	delErr error
}

func newMinimalStorageMock() *minimalStorageMock {
	return &minimalStorageMock{
		data: make(map[string][]byte),
	}
}

func (m *minimalStorageMock) GetWithContext(_ context.Context, key string) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	val, ok := m.data[key]
	if !ok {
		return nil, nil // Key not found returns nil, nil per storage.Storage contract
	}
	return val, nil
}

func (m *minimalStorageMock) Get(key string) ([]byte, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *minimalStorageMock) SetWithContext(_ context.Context, key string, val []byte, _ time.Duration) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.data[key] = val
	return nil
}

func (m *minimalStorageMock) Set(key string, val []byte, exp time.Duration) error {
	return m.SetWithContext(context.Background(), key, val, exp)
}

func (m *minimalStorageMock) DeleteWithContext(_ context.Context, key string) error {
	if m.delErr != nil {
		return m.delErr
	}
	delete(m.data, key)
	return nil
}

func (m *minimalStorageMock) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *minimalStorageMock) ResetWithContext(_ context.Context) error {
	m.data = make(map[string][]byte)
	return nil
}

func (m *minimalStorageMock) Reset() error {
	return m.ResetWithContext(context.Background())
}

func (m *minimalStorageMock) Close() error {
	return nil
}

// =============================================================================
// Adapter Fallback Path Tests (when extended interfaces are nil)
// =============================================================================

func TestAdapter_BucketName_Fallback(t *testing.T) {
	storage := newMinimalStorageMock()
	adapter := NewKVAdapter(storage, slog.Default())

	// When StorageWithBucket is nil, BucketName should return empty string
	if adapter.BucketName() != "" {
		t.Errorf("expected empty bucket name, got %q", adapter.BucketName())
	}
}

func TestAdapter_PutWithRevision_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// PutWithRevision should use base storage.SetWithContext fallback (returns revision 0)
	rev, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision fallback failed: %v", err)
	}
	if rev != 0 {
		t.Errorf("expected revision 0 for fallback path, got %d", rev)
	}

	// Verify value was stored
	val, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestAdapter_PutWithRevision_Fallback_Error(t *testing.T) {
	s := newMinimalStorageMock()
	s.setErr = errors.New("set error")
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	_, err := adapter.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "set error" {
		t.Errorf("expected 'set error', got %q", err.Error())
	}
}

func TestAdapter_GetEntry_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Store data directly in mock
	s.data["key1"] = []byte("value1")

	// GetEntry should use base storage.GetWithContext fallback
	entry, err := adapter.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntry fallback failed: %v", err)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value1, got %s", string(entry.Value))
	}
	if entry.Key != "key1" {
		t.Errorf("expected key1, got %s", entry.Key)
	}
	// Fallback path returns revision 0
	if entry.Revision != 0 {
		t.Errorf("expected revision 0 for fallback path, got %d", entry.Revision)
	}
}

func TestAdapter_GetEntry_Fallback_NilData(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// GetEntry for non-existent key should return ErrKeyNotFound
	_, err := adapter.GetEntryWithContext(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestAdapter_GetEntry_Fallback_Error(t *testing.T) {
	s := newMinimalStorageMock()
	s.getErr = errors.New("get error")
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	_, err := adapter.GetEntryWithContext(ctx, "key1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdapter_Get_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Store data directly in mock
	s.data["key1"] = []byte("value1")

	// Get should return []byte
	data, err := adapter.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Get fallback failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected value1, got %s", string(data))
	}
}

func TestAdapter_Get_Fallback_NilData(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Get non-existent key should return nil, nil per storage.Storage contract
	data, err := adapter.GetWithContext(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get should not return error, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got: %v", data)
	}
}

func TestAdapter_Create_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Create should use fallback (check if exists, then set)
	rev, err := adapter.CreateWithContext(ctx, "new-key", []byte("new-value"), 0)
	if err != nil {
		t.Fatalf("Create fallback failed: %v", err)
	}
	if rev != 0 {
		t.Errorf("expected revision 0 for fallback path, got %d", rev)
	}

	// Verify value was stored
	val, err := s.Get("new-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "new-value" {
		t.Errorf("expected new-value, got %s", string(val))
	}
}

func TestAdapter_Create_Fallback_KeyExists(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Pre-populate key
	s.data["existing-key"] = []byte("existing-value")

	// Create should fail for existing key
	_, err := adapter.CreateWithContext(ctx, "existing-key", []byte("new-value"), 0)
	if err == nil {
		t.Fatal("expected error for existing key")
	}
	if !errors.Is(err, ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}
}

func TestAdapter_Create_Fallback_GetError(t *testing.T) {
	s := newMinimalStorageMock()
	s.getErr = errors.New("get error")
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	_, err := adapter.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdapter_Create_Fallback_SetError(t *testing.T) {
	s := newMinimalStorageMock()
	s.setErr = errors.New("set error")
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	_, err := adapter.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdapter_Update_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Update fallback just sets the value (no revision check)
	rev, err := adapter.UpdateWithContext(ctx, "key1", []byte("value1"), 0, 1)
	if err != nil {
		t.Fatalf("Update fallback failed: %v", err)
	}
	if rev != 0 {
		t.Errorf("expected revision 0 for fallback path, got %d", rev)
	}

	// Verify value was stored
	val, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestAdapter_Update_Fallback_Error(t *testing.T) {
	s := newMinimalStorageMock()
	s.setErr = errors.New("set error")
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	_, err := adapter.UpdateWithContext(ctx, "key1", []byte("value1"), 0, 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdapter_Purge_Fallback(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Pre-populate key
	s.data["key1"] = []byte("value1")

	// Purge fallback uses Delete
	err := adapter.PurgeWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Purge fallback failed: %v", err)
	}

	// Verify key was deleted
	val, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Error("expected key to be deleted")
	}
}

func TestAdapter_Keys_NoSupport(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Keys should return empty slice when StorageWithKeys is nil
	keys, err := adapter.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if keys == nil {
		t.Error("expected non-nil slice")
	}
	if len(keys) != 0 {
		t.Errorf("expected empty slice, got %d keys", len(keys))
	}
}

func TestAdapter_Watch_NoSupport(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Watch should return nil, nil when StorageWithWatch is nil
	watcher, err := adapter.WatchWithContext(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if watcher != nil {
		t.Error("expected nil watcher when watch not supported")
	}
}

func TestAdapter_WatchAll_NoSupport(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// WatchAll should return nil, nil when StorageWithWatch is nil
	watcher, err := adapter.WatchAll(ctx)
	if err != nil {
		t.Fatalf("WatchAll failed: %v", err)
	}
	if watcher != nil {
		t.Error("expected nil watcher when watch not supported")
	}
}

func TestAdapter_Status_NoSupport(t *testing.T) {
	s := newMinimalStorageMock()
	adapter := NewKVAdapter(s, slog.Default())
	ctx := context.Background()

	// Status should return nil, nil when StorageWithStatus is nil
	status, err := adapter.StatusWithContext(ctx)
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}
	if status != nil {
		t.Error("expected nil status when status not supported")
	}
}
