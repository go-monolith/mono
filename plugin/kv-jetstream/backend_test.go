package kvjetstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1/pkg/storage"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Test Helpers
// =============================================================================

// testLogger returns a logger for tests.
func testLogger() *slog.Logger {
	return slog.Default()
}

// setupBackendTestNATS creates an embedded NATS server with JetStream for backend tests.
func setupBackendTestNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	opts := &server.Options{
		JetStream: true,
		Port:      -1,
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
		nats.Name("backend-test-client"),
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

// createBackendTestKV creates a KeyValue store for backend tests.
func createBackendTestKV(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValue {
	t.Helper()

	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:   name,
		Replicas: 1,
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("failed to create KeyValue: %v", err)
	}

	return kv
}

// =============================================================================
// Constructor Tests
// =============================================================================

func TestNewJetStreamKVBackend(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "test-bucket")

	backend := NewJetStreamKVBackend("test-bucket", kv, 0, testLogger())

	if backend == nil {
		t.Fatal("expected backend, got nil")
	}
	if backend.bucketName != "test-bucket" {
		t.Errorf("expected bucket name 'test-bucket', got %q", backend.bucketName)
	}
}

func TestJetStreamKVBackend_BucketName(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "my-bucket")

	backend := NewJetStreamKVBackend("my-bucket", kv, 0, testLogger())

	if backend.BucketName() != "my-bucket" {
		t.Errorf("expected 'my-bucket', got %q", backend.BucketName())
	}
}

// =============================================================================
// SetWithContext Operation Tests (replaces Put)
// =============================================================================

func TestJetStreamKVBackend_SetWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "set-test")
	backend := NewJetStreamKVBackend("set-test", kv, 0, testLogger())
	ctx := context.Background()

	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Verify the value was stored
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got %q", string(data))
	}
}

func TestJetStreamKVBackend_SetWithContext_Overwrite(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "set-overwrite-test")
	backend := NewJetStreamKVBackend("set-overwrite-test", kv, 0, testLogger())
	ctx := context.Background()

	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("First SetWithContext failed: %v", err)
	}

	err = backend.SetWithContext(ctx, "key1", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Second SetWithContext failed: %v", err)
	}

	// Verify value was updated
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}
	if string(data) != "value2" {
		t.Errorf("expected 'value2', got %q", string(data))
	}
}

func TestJetStreamKVBackend_SetWithContext_LargeValue(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "set-large-test")
	backend := NewJetStreamKVBackend("set-large-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create a 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := backend.SetWithContext(ctx, "large-key", largeValue, 0)
	if err != nil {
		t.Fatalf("SetWithContext large value failed: %v", err)
	}

	// Verify retrieval
	data, err := backend.GetWithContext(ctx, "large-key")
	if err != nil {
		t.Fatalf("GetWithContext large value failed: %v", err)
	}
	if len(data) != len(largeValue) {
		t.Errorf("expected value length %d, got %d", len(largeValue), len(data))
	}
}

// =============================================================================
// PutWithRevisionWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_PutWithRevisionWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "put-rev-test")
	backend := NewJetStreamKVBackend("put-rev-test", kv, 0, testLogger())
	ctx := context.Background()

	rev, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevisionWithContext failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}
}

func TestJetStreamKVBackend_PutWithRevisionWithContext_Overwrite(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "put-rev-overwrite-test")
	backend := NewJetStreamKVBackend("put-rev-overwrite-test", kv, 0, testLogger())
	ctx := context.Background()

	rev1, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("First PutWithRevisionWithContext failed: %v", err)
	}

	rev2, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Second PutWithRevisionWithContext failed: %v", err)
	}

	if rev2 <= rev1 {
		t.Errorf("expected rev2 (%d) > rev1 (%d)", rev2, rev1)
	}
}

// =============================================================================
// GetWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_GetWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "get-test")
	backend := NewJetStreamKVBackend("get-test", kv, 0, testLogger())
	ctx := context.Background()

	// Put a value first
	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Get the value
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}

	if string(data) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(data))
	}
}

func TestJetStreamKVBackend_GetWithContext_NotFound(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "get-notfound-test")
	backend := NewJetStreamKVBackend("get-notfound-test", kv, 0, testLogger())
	ctx := context.Background()

	// GetWithContext returns nil, nil for non-existent key per storage.Storage contract
	data, err := backend.GetWithContext(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetWithContext should return nil error for non-existent key, got: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for non-existent key, got: %v", data)
	}
}

func TestJetStreamKVBackend_GetWithContext_AfterDelete(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "get-after-delete-test")
	backend := NewJetStreamKVBackend("get-after-delete-test", kv, 0, testLogger())
	ctx := context.Background()

	// Put then delete
	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	err = backend.DeleteWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("DeleteWithContext failed: %v", err)
	}

	// GetWithContext should return nil, nil
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext after delete should not error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data after delete, got: %v", data)
	}
}

// =============================================================================
// GetEntryWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_GetEntryWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "getentry-test")
	backend := NewJetStreamKVBackend("getentry-test", kv, 0, testLogger())
	ctx := context.Background()

	// Put a value first
	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Get the entry with metadata
	entry, err := backend.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntryWithContext failed: %v", err)
	}

	if entry.Key != "key1" {
		t.Errorf("expected key 'key1', got %q", entry.Key)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(entry.Value))
	}
	if entry.Revision == 0 {
		t.Error("expected non-zero revision")
	}
	if entry.Bucket != "getentry-test" {
		t.Errorf("expected bucket 'getentry-test', got %q", entry.Bucket)
	}
	if entry.Operation != storage.KeyOperationPut {
		t.Errorf("expected KeyOperationPut, got %v", entry.Operation)
	}
}

func TestJetStreamKVBackend_GetEntryWithContext_NotFound(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "getentry-notfound-test")
	backend := NewJetStreamKVBackend("getentry-notfound-test", kv, 0, testLogger())
	ctx := context.Background()

	_, err := backend.GetEntryWithContext(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// =============================================================================
// CreateWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_CreateWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "create-test")
	backend := NewJetStreamKVBackend("create-test", kv, 0, testLogger())
	ctx := context.Background()

	rev, err := backend.CreateWithContext(ctx, "new-key", []byte("new-value"), 0)
	if err != nil {
		t.Fatalf("CreateWithContext failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Verify value
	entry, err := backend.GetEntryWithContext(ctx, "new-key")
	if err != nil {
		t.Fatalf("GetEntryWithContext failed: %v", err)
	}
	if string(entry.Value) != "new-value" {
		t.Errorf("expected 'new-value', got %q", string(entry.Value))
	}
}

func TestJetStreamKVBackend_CreateWithContext_AlreadyExists(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "create-exists-test")
	backend := NewJetStreamKVBackend("create-exists-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create first
	_, err := backend.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("First CreateWithContext failed: %v", err)
	}

	// Second create should fail
	_, err = backend.CreateWithContext(ctx, "key1", []byte("value2"), 0)
	if err == nil {
		t.Fatal("expected error for existing key")
	}
	if !errors.Is(err, storage.ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}
}

// =============================================================================
// UpdateWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_UpdateWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "update-test")
	backend := NewJetStreamKVBackend("update-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create first
	rev1, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevisionWithContext failed: %v", err)
	}

	// Update with correct revision
	rev2, err := backend.UpdateWithContext(ctx, "key1", []byte("value2"), 0, rev1)
	if err != nil {
		t.Fatalf("UpdateWithContext failed: %v", err)
	}
	if rev2 <= rev1 {
		t.Errorf("expected rev2 (%d) > rev1 (%d)", rev2, rev1)
	}

	// Verify value
	entry, err := backend.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntryWithContext failed: %v", err)
	}
	if string(entry.Value) != "value2" {
		t.Errorf("expected 'value2', got %q", string(entry.Value))
	}
}

func TestJetStreamKVBackend_UpdateWithContext_RevisionMismatch(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "update-mismatch-test")
	backend := NewJetStreamKVBackend("update-mismatch-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create and get revision
	rev1, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevisionWithContext failed: %v", err)
	}

	// Update with wrong revision
	_, err = backend.UpdateWithContext(ctx, "key1", []byte("value2"), 0, rev1+100)
	if err == nil {
		t.Fatal("expected error for revision mismatch")
	}
	if !errors.Is(err, storage.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

func TestJetStreamKVBackend_UpdateWithContext_ConcurrentModification(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "update-concurrent-test")
	backend := NewJetStreamKVBackend("update-concurrent-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create initial value
	rev1, err := backend.PutWithRevisionWithContext(ctx, "counter", []byte("0"), 0)
	if err != nil {
		t.Fatalf("PutWithRevisionWithContext failed: %v", err)
	}

	// Simulate concurrent modification
	_, err = backend.PutWithRevisionWithContext(ctx, "counter", []byte("1"), 0)
	if err != nil {
		t.Fatalf("Concurrent PutWithRevisionWithContext failed: %v", err)
	}

	// Original update should fail
	_, err = backend.UpdateWithContext(ctx, "counter", []byte("2"), 0, rev1)
	if err == nil {
		t.Fatal("expected error for concurrent modification")
	}
	if !errors.Is(err, storage.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

// =============================================================================
// DeleteWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_DeleteWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "delete-test")
	backend := NewJetStreamKVBackend("delete-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create first
	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Delete
	err = backend.DeleteWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("DeleteWithContext failed: %v", err)
	}

	// Verify deleted - GetWithContext returns nil, nil for deleted keys
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext after delete should not error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data after delete, got: %v", data)
	}
}

func TestJetStreamKVBackend_DeleteWithContext_NotFound(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "delete-notfound-test")
	backend := NewJetStreamKVBackend("delete-notfound-test", kv, 0, testLogger())
	ctx := context.Background()

	// DeleteWithContext is idempotent - does not error for nonexistent keys
	err := backend.DeleteWithContext(ctx, "nonexistent")
	if err != nil {
		t.Errorf("DeleteWithContext should be idempotent, got error: %v", err)
	}
}

// =============================================================================
// PurgeWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_PurgeWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "purge-test")
	backend := NewJetStreamKVBackend("purge-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create
	err := backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Purge
	err = backend.PurgeWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("PurgeWithContext failed: %v", err)
	}

	// Verify purged
	data, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext after purge should not error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data after purge, got: %v", data)
	}
}

func TestJetStreamKVBackend_PurgeWithContext_NotFound(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "purge-notfound-test")
	backend := NewJetStreamKVBackend("purge-notfound-test", kv, 0, testLogger())
	ctx := context.Background()

	// JetStream Purge creates a purge marker even for non-existent keys
	// This is by design - purge ensures the key is fully removed
	err := backend.PurgeWithContext(ctx, "nonexistent")
	// Purge does not return error for non-existent keys in JetStream
	if err != nil {
		t.Logf("Purge returned error (may vary by JetStream version): %v", err)
	}
}

// =============================================================================
// KeysWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_KeysWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "keys-test")
	backend := NewJetStreamKVBackend("keys-test", kv, 0, testLogger())
	ctx := context.Background()

	// Add some keys
	backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	backend.SetWithContext(ctx, "key2", []byte("value2"), 0)
	backend.SetWithContext(ctx, "key3", []byte("value3"), 0)

	keys, err := backend.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("KeysWithContext failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	// Check all keys present
	keyMap := make(map[string]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	for _, expected := range []string{"key1", "key2", "key3"} {
		if !keyMap[expected] {
			t.Errorf("expected key %q to be present", expected)
		}
	}
}

func TestJetStreamKVBackend_KeysWithContext_Empty(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "keys-empty-test")
	backend := NewJetStreamKVBackend("keys-empty-test", kv, 0, testLogger())
	ctx := context.Background()

	keys, err := backend.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("KeysWithContext failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestJetStreamKVBackend_KeysWithContext_ExcludesDeleted(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "keys-excludes-deleted-test")
	backend := NewJetStreamKVBackend("keys-excludes-deleted-test", kv, 0, testLogger())
	ctx := context.Background()

	// Add keys
	backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	backend.SetWithContext(ctx, "key2", []byte("value2"), 0)
	backend.SetWithContext(ctx, "key3", []byte("value3"), 0)

	// Delete one
	backend.DeleteWithContext(ctx, "key2")

	keys, err := backend.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("KeysWithContext failed: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("expected 2 keys after delete, got %d", len(keys))
	}

	// key2 should not be in the list
	for _, k := range keys {
		if k == "key2" {
			t.Error("deleted key2 should not be in list")
		}
	}
}

// =============================================================================
// WatchWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_WatchWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "watch-test")
	backend := NewJetStreamKVBackend("watch-test", kv, 0, testLogger())
	ctx := context.Background()

	// Start watching
	watcher, err := backend.WatchWithContext(ctx, ">")
	if err != nil {
		t.Fatalf("WatchWithContext failed: %v", err)
	}
	defer watcher.Stop()

	// Put a value
	go func() {
		time.Sleep(100 * time.Millisecond)
		backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	}()

	// Wait for update (with timeout)
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			// Initial sync complete, wait for actual update
			entry = <-watcher.Updates()
		}
		if entry.Key != "key1" {
			t.Errorf("expected key 'key1', got %q", entry.Key)
		}
		if string(entry.Value) != "value1" {
			t.Errorf("expected value 'value1', got %q", string(entry.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch update")
	}
}

func TestJetStreamKVBackend_WatchWithContext_Pattern(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "watch-pattern-test")
	backend := NewJetStreamKVBackend("watch-pattern-test", kv, 0, testLogger())
	ctx := context.Background()

	// Start watching only user.* pattern
	watcher, err := backend.WatchWithContext(ctx, "user.*")
	if err != nil {
		t.Fatalf("WatchWithContext failed: %v", err)
	}
	defer watcher.Stop()

	// Put values
	go func() {
		time.Sleep(100 * time.Millisecond)
		backend.SetWithContext(ctx, "user.123", []byte("alice"), 0)
		backend.SetWithContext(ctx, "config.app", []byte("settings"), 0) // Should not match
		backend.SetWithContext(ctx, "user.456", []byte("bob"), 0)
	}()

	// Collect updates
	updates := make([]*storage.Entry, 0)
	timeout := time.After(2 * time.Second)

loop:
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue // Initial sync
			}
			updates = append(updates, entry)
			if len(updates) >= 2 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if len(updates) < 2 {
		t.Errorf("expected at least 2 updates, got %d", len(updates))
	}

	// All updates should be user.* keys
	for _, entry := range updates {
		if entry.Key != "user.123" && entry.Key != "user.456" {
			t.Errorf("unexpected key in watch: %q", entry.Key)
		}
	}
}

func TestJetStreamKVBackend_WatchWithContext_UpdatesOnly(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "watch-updates-only-test")
	backend := NewJetStreamKVBackend("watch-updates-only-test", kv, 0, testLogger())
	ctx := context.Background()

	// Put initial value before watch
	backend.SetWithContext(ctx, "existing", []byte("value"), 0)

	// Start watching with UpdatesOnly
	watcher, err := backend.WatchWithContext(ctx, ">", storage.WithWatchUpdatesOnly())
	if err != nil {
		t.Fatalf("WatchWithContext failed: %v", err)
	}
	defer watcher.Stop()

	// Put a new value
	go func() {
		time.Sleep(100 * time.Millisecond)
		backend.SetWithContext(ctx, "new-key", []byte("new-value"), 0)
	}()

	// Should receive only the new key, not the existing one
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("should not receive nil sentinel with UpdatesOnly")
		}
		if entry.Key != "new-key" {
			t.Errorf("expected 'new-key', got %q", entry.Key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update")
	}
}

func TestJetStreamKVBackend_WatchWithContext_Stop(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "watch-stop-test")
	backend := NewJetStreamKVBackend("watch-stop-test", kv, 0, testLogger())
	ctx := context.Background()

	watcher, err := backend.WatchWithContext(ctx, ">")
	if err != nil {
		t.Fatalf("WatchWithContext failed: %v", err)
	}

	// Stop should not error
	err = watcher.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}

	// Channel should be closed
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			// May receive remaining buffered updates, keep draining
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Error("channel not closed after Stop")
	}
}

// =============================================================================
// StatusWithContext Operation Tests
// =============================================================================

func TestJetStreamKVBackend_StatusWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "status-test")
	backend := NewJetStreamKVBackend("status-test", kv, 0, testLogger())
	ctx := context.Background()

	// Add some data
	backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	backend.SetWithContext(ctx, "key2", []byte("value2"), 0)

	status, err := backend.StatusWithContext(ctx)
	if err != nil {
		t.Fatalf("StatusWithContext failed: %v", err)
	}

	if status.Bucket != "status-test" {
		t.Errorf("expected bucket 'status-test', got %q", status.Bucket)
	}
	if status.Values != 2 {
		t.Errorf("expected 2 values, got %d", status.Values)
	}
	if status.Bytes == 0 {
		t.Error("expected non-zero bytes")
	}
}

// =============================================================================
// isRevisionMismatchError Tests
// =============================================================================

func TestIsRevisionMismatchError_NonAPIError(t *testing.T) {
	// Test with a regular error (not APIError)
	regularErr := errors.New("some random error")
	if isRevisionMismatchError(regularErr) {
		t.Error("expected false for non-APIError")
	}
}

func TestIsRevisionMismatchError_WrappedNonAPIError(t *testing.T) {
	// Test with a properly wrapped regular error using %w
	baseErr := errors.New("some random error")
	wrappedErr := fmt.Errorf("wrapped: %w", baseErr)
	if isRevisionMismatchError(wrappedErr) {
		t.Error("expected false for wrapped non-APIError")
	}
}

func TestIsRevisionMismatchError_WrappedAPIError(t *testing.T) {
	// Test with a wrapped APIError - verifies errors.As correctly unwraps
	baseErr := &jetstream.APIError{ErrorCode: jetStreamErrCodeWrongLastSequence}
	wrappedErr := fmt.Errorf("update failed: %w", baseErr)
	if !isRevisionMismatchError(wrappedErr) {
		t.Error("expected true for wrapped APIError with correct code")
	}
}

func TestIsRevisionMismatchError_NilError(t *testing.T) {
	// Test with nil error
	if isRevisionMismatchError(nil) {
		t.Error("expected false for nil error")
	}
}

func TestIsRevisionMismatchError_APIErrorDifferentCode(t *testing.T) {
	// Test with APIError but different error code
	apiErr := &jetstream.APIError{
		ErrorCode: 10001, // Different error code
	}
	if isRevisionMismatchError(apiErr) {
		t.Error("expected false for APIError with different error code")
	}
}

func TestIsRevisionMismatchError_APIErrorCorrectCode(t *testing.T) {
	// Test with APIError with correct error code (10071)
	apiErr := &jetstream.APIError{
		ErrorCode: jetStreamErrCodeWrongLastSequence,
	}
	if !isRevisionMismatchError(apiErr) {
		t.Error("expected true for APIError with code 10071")
	}
}

// =============================================================================
// Reset and Close Tests
// =============================================================================

func TestJetStreamKVBackend_ResetWithContext(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "reset-test")
	backend := NewJetStreamKVBackend("reset-test", kv, 0, testLogger())
	ctx := context.Background()

	// Add some data
	backend.SetWithContext(ctx, "key1", []byte("value1"), 0)
	backend.SetWithContext(ctx, "key2", []byte("value2"), 0)

	// Reset
	err := backend.ResetWithContext(ctx)
	if err != nil {
		t.Fatalf("ResetWithContext failed: %v", err)
	}

	// Verify all keys are deleted
	keys, err := backend.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("KeysWithContext failed: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 keys after reset, got %d", len(keys))
	}
}

func TestJetStreamKVBackend_Close(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "close-test")
	backend := NewJetStreamKVBackend("close-test", kv, 0, testLogger())

	// Close should not error
	err := backend.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

// =============================================================================
// Interface Compliance Tests
// =============================================================================

func TestJetStreamKVBackend_ImplementsStorage(t *testing.T) {
	var _ storage.Storage = (*JetStreamKVBackend)(nil)
}

func TestJetStreamKVBackend_ImplementsStorageWithBucket(t *testing.T) {
	var _ storage.StorageWithBucket = (*JetStreamKVBackend)(nil)
}

func TestJetStreamKVBackend_ImplementsStorageWithWatch(t *testing.T) {
	var _ storage.StorageWithWatch = (*JetStreamKVBackend)(nil)
}

func TestJetStreamKVBackend_ImplementsStorageWithRevision(t *testing.T) {
	var _ storage.StorageWithRevision = (*JetStreamKVBackend)(nil)
}

func TestJetStreamKVBackend_ImplementsStorageWithKeys(t *testing.T) {
	var _ storage.StorageWithKeys = (*JetStreamKVBackend)(nil)
}

func TestJetStreamKVBackend_ImplementsStorageWithStatus(t *testing.T) {
	var _ storage.StorageWithStatus = (*JetStreamKVBackend)(nil)
}

// =============================================================================
// Wrapper Function Tests (non-context versions)
// These test the wrapper functions that call *WithContext methods
// =============================================================================

// TestJetStreamKVBackend_Get tests the Get wrapper function
func TestJetStreamKVBackend_Get(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "get-wrapper-test")
	backend := NewJetStreamKVBackend("get-wrapper-test", kv, 0, testLogger())

	// Set a value first
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Get wrapper
	data, err := backend.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got %q", string(data))
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

// TestJetStreamKVBackend_Set tests the Set wrapper function
func TestJetStreamKVBackend_Set(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "set-wrapper-test")
	backend := NewJetStreamKVBackend("set-wrapper-test", kv, 0, testLogger())

	// Test Set wrapper
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify it was set
	data, err := backend.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got %q", string(data))
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

// TestJetStreamKVBackend_Delete tests the Delete wrapper function
func TestJetStreamKVBackend_Delete(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "delete-wrapper-test")
	backend := NewJetStreamKVBackend("delete-wrapper-test", kv, 0, testLogger())

	// Set a value first
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Delete wrapper
	err = backend.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it was deleted
	data, err := backend.Get("key1")
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

// TestJetStreamKVBackend_Reset tests the Reset wrapper function
func TestJetStreamKVBackend_Reset(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "reset-wrapper-test")
	backend := NewJetStreamKVBackend("reset-wrapper-test", kv, 0, testLogger())

	// Set multiple values
	for i := 0; i < 3; i++ {
		err := backend.Set("key"+string(rune('0'+i)), []byte("value"), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Verify they exist
	keys, err := backend.Keys()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("expected keys to exist before reset")
	}

	// Test Reset wrapper
	err = backend.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify all deleted
	keys, err = backend.Keys()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected 0 keys after reset, got %d", len(keys))
	}
}

// TestJetStreamKVBackend_Create tests the Create wrapper function
func TestJetStreamKVBackend_Create(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "create-wrapper-test")
	backend := NewJetStreamKVBackend("create-wrapper-test", kv, 0, testLogger())

	// Test Create wrapper
	rev, err := backend.Create("new-key", []byte("new-value"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Verify it was created
	data, err := backend.Get("new-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "new-value" {
		t.Errorf("expected 'new-value', got %q", string(data))
	}

	// Test Create on existing key (should fail)
	_, err = backend.Create("new-key", []byte("another-value"), 0)
	if err == nil {
		t.Fatal("expected error for existing key")
	}
	if !errors.Is(err, storage.ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}
}

// TestJetStreamKVBackend_Update tests the Update wrapper function
func TestJetStreamKVBackend_Update(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "update-wrapper-test")
	backend := NewJetStreamKVBackend("update-wrapper-test", kv, 0, testLogger())

	// Create first
	rev1, err := backend.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}

	// Test Update wrapper
	rev2, err := backend.Update("key1", []byte("value2"), 0, rev1)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if rev2 <= rev1 {
		t.Errorf("expected rev2 (%d) > rev1 (%d)", rev2, rev1)
	}

	// Verify it was updated
	data, err := backend.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value2" {
		t.Errorf("expected 'value2', got %q", string(data))
	}

	// Test Update with wrong revision
	_, err = backend.Update("key1", []byte("value3"), 0, rev1)
	if err == nil {
		t.Fatal("expected error for revision mismatch")
	}
	if !errors.Is(err, storage.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

// TestJetStreamKVBackend_Purge tests the Purge wrapper function
func TestJetStreamKVBackend_Purge(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "purge-wrapper-test")
	backend := NewJetStreamKVBackend("purge-wrapper-test", kv, 0, testLogger())

	// Create a value
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Purge wrapper
	err = backend.Purge("key1")
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}

	// Verify it was purged
	data, err := backend.Get("key1")
	if err != nil {
		t.Errorf("expected no error after purge, got: %v", err)
	}
	if data != nil {
		t.Error("expected nil data after purge")
	}
}

// TestJetStreamKVBackend_GetEntry tests the GetEntry wrapper function
func TestJetStreamKVBackend_GetEntry(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "getentry-wrapper-test")
	backend := NewJetStreamKVBackend("getentry-wrapper-test", kv, 0, testLogger())

	// Create a value
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test GetEntry wrapper
	entry, err := backend.GetEntry("key1")
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}

	if entry.Key != "key1" {
		t.Errorf("expected key 'key1', got %q", entry.Key)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected value 'value1', got %q", string(entry.Value))
	}
	if entry.Revision == 0 {
		t.Error("expected non-zero revision")
	}

	// Test GetEntry for non-existent key
	_, err = backend.GetEntry("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestJetStreamKVBackend_PutWithRevision tests the PutWithRevision wrapper function
func TestJetStreamKVBackend_PutWithRevision(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "putwithrev-wrapper-test")
	backend := NewJetStreamKVBackend("putwithrev-wrapper-test", kv, 0, testLogger())

	// Test PutWithRevision wrapper
	rev, err := backend.PutWithRevision("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Verify it was stored
	data, err := backend.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "value1" {
		t.Errorf("expected 'value1', got %q", string(data))
	}

	// Test overwrite
	rev2, err := backend.PutWithRevision("key1", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("PutWithRevision overwrite failed: %v", err)
	}
	if rev2 <= rev {
		t.Errorf("expected rev2 (%d) > rev (%d)", rev2, rev)
	}
}

// TestJetStreamKVBackend_Keys tests the Keys wrapper function
func TestJetStreamKVBackend_Keys(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "keys-wrapper-test")
	backend := NewJetStreamKVBackend("keys-wrapper-test", kv, 0, testLogger())

	// Add some keys
	for i := 0; i < 3; i++ {
		err := backend.Set("key"+string(rune('0'+i)), []byte("value"), 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Test Keys wrapper
	keys, err := backend.Keys()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

// TestJetStreamKVBackend_Watch tests the Watch wrapper function
func TestJetStreamKVBackend_Watch(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "watch-wrapper-test")
	backend := NewJetStreamKVBackend("watch-wrapper-test", kv, 0, testLogger())

	// Test Watch wrapper
	watcher, err := backend.Watch(">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watcher.Stop()

	// Put a value
	go func() {
		time.Sleep(100 * time.Millisecond)
		backend.Set("key1", []byte("value1"), 0)
	}()

	// Wait for update (with timeout)
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			// Initial sync complete, wait for actual update
			entry = <-watcher.Updates()
		}
		if entry.Key != "key1" {
			t.Errorf("expected key 'key1', got %q", entry.Key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch update")
	}
}

// TestJetStreamKVBackend_Status tests the Status wrapper function
func TestJetStreamKVBackend_Status(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "status-wrapper-test")
	backend := NewJetStreamKVBackend("status-wrapper-test", kv, 0, testLogger())

	// Add some data
	err := backend.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Status wrapper
	status, err := backend.Status()
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}

	if status.Bucket != "status-wrapper-test" {
		t.Errorf("expected bucket 'status-wrapper-test', got %q", status.Bucket)
	}
	if status.Values != 1 {
		t.Errorf("expected 1 value, got %d", status.Values)
	}
}

// TestJetStreamKVBackend_ResetWithContext_Empty tests Reset on empty bucket
func TestJetStreamKVBackend_ResetWithContext_Empty(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "reset-empty-test")
	backend := NewJetStreamKVBackend("reset-empty-test", kv, 0, testLogger())
	ctx := context.Background()

	// Reset on empty bucket should not error
	err := backend.ResetWithContext(ctx)
	if err != nil {
		t.Errorf("expected no error resetting empty bucket, got: %v", err)
	}
}

// TestJetStreamKVBackend_UpdateWithContext_KeyNotFound tests Update for non-existent key
// Note: NATS JetStream returns revision mismatch error when updating a non-existent key
// with a specific revision, not ErrKeyNotFound.
func TestJetStreamKVBackend_UpdateWithContext_KeyNotFound(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "update-notfound-test")
	backend := NewJetStreamKVBackend("update-notfound-test", kv, 0, testLogger())
	ctx := context.Background()

	// Update non-existent key - NATS returns revision mismatch, not key not found
	_, err := backend.UpdateWithContext(ctx, "nonexistent", []byte("value"), 0, 1)
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	// NATS JetStream Update returns revision mismatch when key doesn't exist
	if !errors.Is(err, storage.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

// TestJetStreamKVBackend_SetWithContext_TTLWarning tests TTL mismatch warning
func TestJetStreamKVBackend_SetWithContext_TTLWarning(t *testing.T) {
	_, js := setupBackendTestNATS(t)

	// Create bucket with specific TTL
	bucketTTL := 1 * time.Hour
	cfg := jetstream.KeyValueConfig{
		Bucket: "ttl-warning-test",
		TTL:    bucketTTL,
	}
	kv, err := js.CreateKeyValue(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create KV: %v", err)
	}

	// Create backend with bucket TTL
	backend := NewJetStreamKVBackend("ttl-warning-test", kv, bucketTTL, testLogger())
	ctx := context.Background()

	// Set with different TTL - should trigger warning log but still succeed
	differentTTL := 30 * time.Minute
	err = backend.SetWithContext(ctx, "key1", []byte("value1"), differentTTL)
	if err != nil {
		t.Fatalf("SetWithContext failed: %v", err)
	}

	// Verify value was stored
	entry, err := backend.GetWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}
	if !bytes.Equal(entry, []byte("value1")) {
		t.Errorf("expected value1, got %s", string(entry))
	}
}

// TestJetStreamKVBackend_PutWithRevisionWithContext_TTLWarning tests TTL mismatch warning
func TestJetStreamKVBackend_PutWithRevisionWithContext_TTLWarning(t *testing.T) {
	_, js := setupBackendTestNATS(t)

	// Create bucket with specific TTL
	bucketTTL := 1 * time.Hour
	cfg := jetstream.KeyValueConfig{
		Bucket: "ttl-warning-rev-test",
		TTL:    bucketTTL,
	}
	kv, err := js.CreateKeyValue(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create KV: %v", err)
	}

	// Create backend with bucket TTL
	backend := NewJetStreamKVBackend("ttl-warning-rev-test", kv, bucketTTL, testLogger())
	ctx := context.Background()

	// Put with different TTL - should trigger warning log but still succeed
	differentTTL := 30 * time.Minute
	revision, err := backend.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), differentTTL)
	if err != nil {
		t.Fatalf("PutWithRevisionWithContext failed: %v", err)
	}
	if revision == 0 {
		t.Error("expected revision > 0")
	}

	// Verify value was stored
	entry, err := backend.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("GetEntryWithContext failed: %v", err)
	}
	if !bytes.Equal(entry.Value, []byte("value1")) {
		t.Errorf("expected value1, got %s", string(entry.Value))
	}
}

// TestJetStreamKVBackend_UpdateWithContext_TTLWarning tests TTL mismatch warning on update
func TestJetStreamKVBackend_UpdateWithContext_TTLWarning(t *testing.T) {
	_, js := setupBackendTestNATS(t)

	// Create bucket with specific TTL
	bucketTTL := 1 * time.Hour
	cfg := jetstream.KeyValueConfig{
		Bucket: "ttl-warning-update-test",
		TTL:    bucketTTL,
	}
	kv, err := js.CreateKeyValue(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create KV: %v", err)
	}

	// Create backend with bucket TTL
	backend := NewJetStreamKVBackend("ttl-warning-update-test", kv, bucketTTL, testLogger())
	ctx := context.Background()

	// First create the key
	rev, err := backend.CreateWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("CreateWithContext failed: %v", err)
	}

	// Update with different TTL - should trigger warning log but still succeed
	differentTTL := 30 * time.Minute
	newRev, err := backend.UpdateWithContext(ctx, "key1", []byte("value2"), differentTTL, rev)
	if err != nil {
		t.Fatalf("UpdateWithContext failed: %v", err)
	}
	if newRev <= rev {
		t.Errorf("expected newRev > rev, got newRev=%d, rev=%d", newRev, rev)
	}
}

// TestJetStreamKVBackend_Set_EmptyKeyOrValue tests Set ignores empty key/value
func TestJetStreamKVBackend_Set_EmptyKeyOrValue(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "empty-key-value-test")
	backend := NewJetStreamKVBackend("empty-key-value-test", kv, 0, testLogger())

	tests := []struct {
		name  string
		key   string
		value []byte
	}{
		{"empty_key", "", []byte("value")},
		{"nil_value", "key", nil},
		{"empty_key_nil_value", "", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := backend.Set(tt.key, tt.value, 0)
			if err != nil {
				t.Errorf("Set should not return error for empty key or nil value, got: %v", err)
			}
		})
	}
}

// TestJetStreamKVBackend_PurgeWithContext_Success tests successful purge
func TestJetStreamKVBackend_PurgeWithContext_Success(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "purge-success-test")
	backend := NewJetStreamKVBackend("purge-success-test", kv, 0, testLogger())
	ctx := context.Background()

	// Create a key with multiple revisions
	err := backend.SetWithContext(ctx, "key1", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("first SetWithContext failed: %v", err)
	}
	err = backend.SetWithContext(ctx, "key1", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("second SetWithContext failed: %v", err)
	}

	// Purge the key
	err = backend.PurgeWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("PurgeWithContext failed: %v", err)
	}

	// Verify key is gone - GetWithContext returns nil, nil for non-existent keys
	// Use GetEntryWithContext which returns ErrKeyNotFound
	_, err = backend.GetEntryWithContext(ctx, "key1")
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestJetStreamKVBackend_KeysWithContext_WithData tests Keys with data
func TestJetStreamKVBackend_KeysWithContext_WithData(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "keys-data-test")
	backend := NewJetStreamKVBackend("keys-data-test", kv, 0, testLogger())
	ctx := context.Background()

	// Add some keys
	if err := backend.SetWithContext(ctx, "key1", []byte("v1"), 0); err != nil {
		t.Fatalf("SetWithContext key1 failed: %v", err)
	}
	if err := backend.SetWithContext(ctx, "key2", []byte("v2"), 0); err != nil {
		t.Fatalf("SetWithContext key2 failed: %v", err)
	}

	// List keys should work
	keys, err := backend.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("KeysWithContext failed: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

// =============================================================================
// Mock jetstream.KeyWatcher for testing storageKeyWatcher
// =============================================================================

// mockJetStreamKeyWatcher implements jetstream.KeyWatcher for testing.
type mockJetStreamKeyWatcher struct {
	updates chan jetstream.KeyValueEntry
	stopped bool
	stopErr error
	mu      sync.Mutex
}

func newMockJetStreamKeyWatcher() *mockJetStreamKeyWatcher {
	return &mockJetStreamKeyWatcher{
		updates: make(chan jetstream.KeyValueEntry, 10),
	}
}

func (m *mockJetStreamKeyWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return m.updates
}

func (m *mockJetStreamKeyWatcher) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	return m.stopErr
}

func (m *mockJetStreamKeyWatcher) isStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

// mockKeyValueEntry implements jetstream.KeyValueEntry for testing.
type mockKeyValueEntry struct {
	bucket    string
	key       string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation jetstream.KeyValueOp
}

func (e *mockKeyValueEntry) Bucket() string                  { return e.bucket }
func (e *mockKeyValueEntry) Key() string                     { return e.key }
func (e *mockKeyValueEntry) Value() []byte                   { return e.value }
func (e *mockKeyValueEntry) Revision() uint64                { return e.revision }
func (e *mockKeyValueEntry) Created() time.Time              { return e.created }
func (e *mockKeyValueEntry) Delta() uint64                   { return e.delta }
func (e *mockKeyValueEntry) Operation() jetstream.KeyValueOp { return e.operation }

var _ jetstream.KeyValueEntry = (*mockKeyValueEntry)(nil)

// =============================================================================
// storageKeyWatcher Tests
// =============================================================================

func TestStorageKeyWatcher_Updates(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)
	defer watcher.Stop()

	// Send an entry through the mock
	mock.updates <- &mockKeyValueEntry{
		bucket:   "test-bucket",
		key:      "test-key",
		value:    []byte("test-value"),
		revision: 1,
		created:  time.Now(),
	}

	// Receive from watcher's updates channel
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if entry.Key != "test-key" {
			t.Errorf("expected key 'test-key', got %q", entry.Key)
		}
		if string(entry.Value) != "test-value" {
			t.Errorf("expected value 'test-value', got %q", string(entry.Value))
		}
		if entry.Revision != 1 {
			t.Errorf("expected revision 1, got %d", entry.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for entry")
	}
}

func TestStorageKeyWatcher_NilEntry(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)
	defer watcher.Stop()

	// Send nil entry (signals end of initial values)
	mock.updates <- nil

	// Should receive nil through watcher
	select {
	case entry := <-watcher.Updates():
		if entry != nil {
			t.Errorf("expected nil entry, got %v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for nil entry")
	}
}

func TestStorageKeyWatcher_ContextDone(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)

	// Stop the watcher (cancels context)
	err := watcher.Stop()
	if err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}

	// After stop, the updates channel should be closed
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			// Drain any remaining entries
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestStorageKeyWatcher_ChannelClose(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)

	// Close the underlying mock's channel directly (simulating watcher closed by NATS)
	close(mock.updates)

	// The watcher's updates channel should eventually close
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			// Drain any remaining entries
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close after underlying close")
	}
}

func TestStorageKeyWatcher_ConvertEntry(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)
	defer watcher.Stop()

	now := time.Now()
	// Send entry with all fields populated
	mock.updates <- &mockKeyValueEntry{
		bucket:    "my-bucket",
		key:       "my-key",
		value:     []byte("my-value"),
		revision:  42,
		created:   now,
		delta:     5,
		operation: jetstream.KeyValuePut,
	}

	// Receive and verify conversion
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if entry.Key != "my-key" {
			t.Errorf("expected key 'my-key', got %q", entry.Key)
		}
		if string(entry.Value) != "my-value" {
			t.Errorf("expected value 'my-value', got %q", string(entry.Value))
		}
		if entry.Revision != 42 {
			t.Errorf("expected revision 42, got %d", entry.Revision)
		}
		if entry.Timestamp != now {
			t.Errorf("expected timestamp %v, got %v", now, entry.Timestamp)
		}
		if entry.Operation != storage.KeyOperationPut {
			t.Errorf("expected KeyOperationPut, got %v", entry.Operation)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for entry")
	}
}

func TestStorageKeyWatcher_Stop(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)

	// Stop should return nil on success
	err := watcher.Stop()
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// Underlying watcher should be stopped
	if !mock.isStopped() {
		t.Error("expected underlying watcher to be stopped")
	}
}

func TestStorageKeyWatcher_Stop_WithError(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	mock.stopErr = errors.New("stop error")
	watcher := newStorageKeyWatcher(mock)

	// Stop should return the underlying error
	err := watcher.Stop()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "stop error" {
		t.Errorf("expected 'stop error', got %q", err.Error())
	}
}

func TestStorageKeyWatcher_MultipleEntries(t *testing.T) {
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)
	defer watcher.Stop()

	// Send multiple entries
	for i := 0; i < 5; i++ {
		mock.updates <- &mockKeyValueEntry{
			key:      fmt.Sprintf("key-%d", i),
			value:    []byte(fmt.Sprintf("value-%d", i)),
			revision: uint64(i + 1),
		}
	}

	// Receive all entries
	received := 0
	timeout := time.After(time.Second)
	for received < 5 {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				t.Fatal("unexpected nil entry")
			}
			received++
		case <-timeout:
			t.Fatalf("timeout after receiving %d entries", received)
		}
	}

	if received != 5 {
		t.Errorf("expected 5 entries, received %d", received)
	}
}

func TestStorageKeyWatcher_StopDuringNilSend(t *testing.T) {
	// Test the ctx.Done path during nil entry send (line 459)
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)

	// Stop the watcher in background
	go func() {
		time.Sleep(10 * time.Millisecond)
		watcher.Stop()
	}()

	// Send nil entry - this should exit via ctx.Done
	mock.updates <- nil

	// Wait for watcher to stop
	time.Sleep(50 * time.Millisecond)

	// Verify stopped
	if !mock.isStopped() {
		t.Error("expected watcher to be stopped")
	}
}

func TestStorageKeyWatcher_StopDuringEntrySend(t *testing.T) {
	// Test the ctx.Done path during entry send (line 466)
	mock := newMockJetStreamKeyWatcher()
	watcher := newStorageKeyWatcher(mock)

	// Stop the watcher in background
	go func() {
		time.Sleep(10 * time.Millisecond)
		watcher.Stop()
	}()

	// Send entry - this should exit via ctx.Done
	mock.updates <- &mockKeyValueEntry{key: "key", value: []byte("value")}

	// Wait for watcher to stop
	time.Sleep(50 * time.Millisecond)

	// Verify stopped
	if !mock.isStopped() {
		t.Error("expected watcher to be stopped")
	}
}

func TestStorageKeyWatcher_OperationConversion(t *testing.T) {
	tests := []struct {
		name       string
		jsOp       jetstream.KeyValueOp
		expectedOp storage.KeyOperation
	}{
		{"Put", jetstream.KeyValuePut, storage.KeyOperationPut},
		{"Delete", jetstream.KeyValueDelete, storage.KeyOperationDelete},
		{"Purge", jetstream.KeyValuePurge, storage.KeyOperationPurge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := newMockJetStreamKeyWatcher()
			watcher := newStorageKeyWatcher(mock)
			defer watcher.Stop()

			mock.updates <- &mockKeyValueEntry{
				key:       "key",
				value:     []byte("value"),
				operation: tt.jsOp,
			}

			select {
			case entry := <-watcher.Updates():
				if entry.Operation != tt.expectedOp {
					t.Errorf("expected operation %v, got %v", tt.expectedOp, entry.Operation)
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for entry")
			}
		})
	}
}
