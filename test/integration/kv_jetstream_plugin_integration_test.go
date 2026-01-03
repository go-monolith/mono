//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mono "github.com/go-monolith/mono"
	kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
)

// =============================================================================
// Test Consumer Module
// =============================================================================

// testKVConsumerModule is a test module that consumes the kv-jetstream plugin.
type testKVConsumerModule struct {
	name     string
	kvPlugin *kvjetstream.PluginModule
	cache    kvjetstream.KVStoragePort
	sessions kvjetstream.KVStoragePort
}

func (m *testKVConsumerModule) Name() string {
	return m.name
}

func (m *testKVConsumerModule) SetPlugin(alias string, plugin mono.PluginModule) {
	if alias == "kv" {
		m.kvPlugin = plugin.(*kvjetstream.PluginModule)
	}
}

func (m *testKVConsumerModule) Start(_ context.Context) error {
	m.cache = m.kvPlugin.Bucket("cache")
	m.sessions = m.kvPlugin.Bucket("sessions")
	return nil
}

func (m *testKVConsumerModule) Stop(_ context.Context) error {
	return nil
}

// =============================================================================
// Test Helper
// =============================================================================

func setupKVTestFramework(t *testing.T, buckets []kvjetstream.BucketConfig) (mono.MonoApplication, *testKVConsumerModule) {
	t.Helper()

	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	kvStore, err := kvjetstream.New(kvjetstream.Config{
		Buckets: buckets,
	})
	if err != nil {
		t.Fatalf("Failed to create KV plugin: %v", err)
	}

	if err := fw.RegisterPlugin(kvStore, "kv"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	consumer := &testKVConsumerModule{name: "kv-consumer"}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	t.Cleanup(func() {
		fw.Stop(context.Background())
	})

	return fw, consumer
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestIntegration_KVJetstreamPlugin_BasicOperations(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Verify bucket is available
	if consumer.cache == nil {
		t.Fatal("expected cache bucket to be available")
	}

	// Put
	rev, err := consumer.cache.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	// Get
	entry, err := consumer.cache.GetEntryWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value) != "value1" {
		t.Errorf("expected 'value1', got %q", string(entry.Value))
	}

	// Delete
	err = consumer.cache.DeleteWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, err = consumer.cache.GetEntryWithContext(ctx, "key1")
	if !errors.Is(err, kvjetstream.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound after delete, got: %v", err)
	}
}

func TestIntegration_KVJetstreamPlugin_CreateAndUpdate(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Create (atomic)
	rev1, err := consumer.cache.CreateWithContext(ctx, "new-key", []byte("initial"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Second create should fail
	_, err = consumer.cache.CreateWithContext(ctx, "new-key", []byte("duplicate"), 0)
	if !errors.Is(err, kvjetstream.ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}

	// Update with correct revision
	rev2, err := consumer.cache.UpdateWithContext(ctx, "new-key", []byte("updated"), 0, rev1)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if rev2 <= rev1 {
		t.Error("expected revision to increment")
	}

	// Verify update
	entry, err := consumer.cache.GetEntryWithContext(ctx, "new-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value) != "updated" {
		t.Errorf("expected 'updated', got %q", string(entry.Value))
	}
}

func TestIntegration_KVJetstreamPlugin_OptimisticLocking(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Create initial value
	rev1, err := consumer.cache.CreateWithContext(ctx, "counter", []byte("0"), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update with correct revision succeeds
	rev2, err := consumer.cache.UpdateWithContext(ctx, "counter", []byte("1"), 0, rev1)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if rev2 <= rev1 {
		t.Error("expected revision to increment")
	}

	// Update with stale revision fails
	_, err = consumer.cache.UpdateWithContext(ctx, "counter", []byte("2"), 0, rev1)
	if err == nil {
		t.Fatal("expected error for stale revision")
	}
	if !errors.Is(err, kvjetstream.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}

	// Verify value unchanged
	entry, err := consumer.cache.GetEntryWithContext(ctx, "counter")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value) != "1" {
		t.Errorf("expected '1', got %q", string(entry.Value))
	}
}

func TestIntegration_KVJetstreamPlugin_Watch(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Start watching (updates only)
	watcher, err := consumer.cache.WatchWithContext(ctx, ">", kvjetstream.WithUpdatesOnly())
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watcher.Stop()

	// Put a value in a goroutine
	go func() {
		time.Sleep(100 * time.Millisecond)
		consumer.cache.PutWithRevisionWithContext(ctx, "watched-key", []byte("watched-value"), 0)
	}()

	// Wait for update
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("expected non-nil entry with UpdatesOnly")
		}
		if entry.Key != "watched-key" {
			t.Errorf("expected 'watched-key', got %q", entry.Key)
		}
		if string(entry.Value) != "watched-value" {
			t.Errorf("expected 'watched-value', got %q", string(entry.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch update")
	}
}

func TestIntegration_KVJetstreamPlugin_WatchPattern(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Watch only user.* keys
	watcher, err := consumer.cache.WatchWithContext(ctx, "user.*", kvjetstream.WithUpdatesOnly())
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watcher.Stop()

	// Put values
	go func() {
		time.Sleep(100 * time.Millisecond)
		consumer.cache.PutWithRevisionWithContext(ctx, "user.123", []byte("alice"), 0)
		consumer.cache.PutWithRevisionWithContext(ctx, "config.app", []byte("settings"), 0) // Should not match
		consumer.cache.PutWithRevisionWithContext(ctx, "user.456", []byte("bob"), 0)
	}()

	// Collect user updates
	updates := make([]*kvjetstream.KVEntry, 0)
	timeout := time.After(3 * time.Second)

loop:
	for {
		select {
		case entry := <-watcher.Updates():
			if entry != nil {
				updates = append(updates, entry)
				if len(updates) >= 2 {
					break loop
				}
			}
		case <-timeout:
			break loop
		}
	}

	if len(updates) < 2 {
		t.Errorf("expected at least 2 user updates, got %d", len(updates))
	}

	// All should be user.* keys
	for _, entry := range updates {
		if entry.Key != "user.123" && entry.Key != "user.456" {
			t.Errorf("unexpected key: %q", entry.Key)
		}
	}
}

func TestIntegration_KVJetstreamPlugin_MultipleBuckets(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
		{Name: "sessions", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Verify both buckets available
	if consumer.cache == nil {
		t.Fatal("expected cache bucket")
	}
	if consumer.sessions == nil {
		t.Fatal("expected sessions bucket")
	}

	// Operations on cache
	consumer.cache.PutWithRevisionWithContext(ctx, "cache-key", []byte("cache-value"), 0)

	// Operations on sessions
	consumer.sessions.PutWithRevisionWithContext(ctx, "session-key", []byte("session-value"), 0)

	// Verify isolation
	cacheEntry, _ := consumer.cache.GetEntryWithContext(ctx, "cache-key")
	if string(cacheEntry.Value) != "cache-value" {
		t.Error("cache bucket value mismatch")
	}

	sessionEntry, _ := consumer.sessions.GetEntryWithContext(ctx, "session-key")
	if string(sessionEntry.Value) != "session-value" {
		t.Error("sessions bucket value mismatch")
	}

	// Keys in one bucket should not appear in another
	_, err := consumer.cache.GetEntryWithContext(ctx, "session-key")
	if !errors.Is(err, kvjetstream.ErrKeyNotFound) {
		t.Error("session-key should not exist in cache bucket")
	}
}

func TestIntegration_KVJetstreamPlugin_SentinelErrors(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// ErrKeyNotFound
	_, err := consumer.cache.GetEntryWithContext(ctx, "nonexistent")
	if !errors.Is(err, kvjetstream.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got: %v", err)
	}

	// ErrKeyExists
	consumer.cache.CreateWithContext(ctx, "existing", []byte("value"), 0)
	_, err = consumer.cache.CreateWithContext(ctx, "existing", []byte("value2"), 0)
	if !errors.Is(err, kvjetstream.ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got: %v", err)
	}

	// ErrRevisionMismatch
	rev, _ := consumer.cache.PutWithRevisionWithContext(ctx, "versioned", []byte("v1"), 0)
	_, err = consumer.cache.UpdateWithContext(ctx, "versioned", []byte("v2"), 0, rev+100)
	if !errors.Is(err, kvjetstream.ErrRevisionMismatch) {
		t.Errorf("expected ErrRevisionMismatch, got: %v", err)
	}
}

func TestIntegration_KVJetstreamPlugin_KeysEnumeration(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Add keys
	consumer.cache.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	consumer.cache.PutWithRevisionWithContext(ctx, "key2", []byte("value2"), 0)
	consumer.cache.PutWithRevisionWithContext(ctx, "key3", []byte("value3"), 0)

	// List keys
	keys, err := consumer.cache.KeysWithContext(ctx)
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	// Verify all keys present
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

func TestIntegration_KVJetstreamPlugin_BucketStatus(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Add some data
	consumer.cache.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)
	consumer.cache.PutWithRevisionWithContext(ctx, "key2", []byte("value2"), 0)

	// Get status
	status, err := consumer.cache.StatusWithContext(ctx)
	if err != nil {
		t.Fatalf("Status failed: %v", err)
	}

	if status.Bucket != "cache" {
		t.Errorf("expected bucket 'cache', got %q", status.Bucket)
	}
	if status.Values != 2 {
		t.Errorf("expected 2 values, got %d", status.Values)
	}
	if status.Bytes == 0 {
		t.Error("expected non-zero bytes")
	}
}

func TestIntegration_KVJetstreamPlugin_Purge(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Put a value
	consumer.cache.PutWithRevisionWithContext(ctx, "key1", []byte("value1"), 0)

	// Purge (hard delete)
	err := consumer.cache.PurgeWithContext(ctx, "key1")
	if err != nil {
		t.Fatalf("Purge failed: %v", err)
	}

	// Verify purged
	_, err = consumer.cache.GetEntryWithContext(ctx, "key1")
	if !errors.Is(err, kvjetstream.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound after purge, got: %v", err)
	}
}

func TestIntegration_KVJetstreamPlugin_WatchAll(t *testing.T) {
	_, consumer := setupKVTestFramework(t, []kvjetstream.BucketConfig{
		{Name: "cache", Storage: kvjetstream.MemoryStorage},
	})

	ctx := context.Background()

	// Use WatchAll helper
	watcher, err := consumer.cache.WatchAll(ctx, kvjetstream.WithUpdatesOnly())
	if err != nil {
		t.Fatalf("WatchAll failed: %v", err)
	}
	defer watcher.Stop()

	// Put a value
	go func() {
		time.Sleep(100 * time.Millisecond)
		consumer.cache.PutWithRevisionWithContext(ctx, "any-key", []byte("any-value"), 0)
	}()

	// Should receive update
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if entry.Key != "any-key" {
			t.Errorf("expected 'any-key', got %q", entry.Key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WatchAll update")
	}
}

func TestIntegration_KVJetstreamPlugin_ConsumerModuleIntegration(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	kvStore, err := kvjetstream.New(kvjetstream.Config{
		Buckets: []kvjetstream.BucketConfig{
			{Name: "cache", Storage: kvjetstream.MemoryStorage},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create KV plugin: %v", err)
	}

	if err := fw.RegisterPlugin(kvStore, "kv"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Create a module that uses the plugin in Start()
	consumer := &testKVConsumerModule{name: "kv-user"}
	if err := fw.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Consumer should have received the plugin
	if consumer.kvPlugin == nil {
		t.Fatal("expected kvPlugin to be set via SetPlugin")
	}

	// Consumer should have initialized bucket references
	if consumer.cache == nil {
		t.Fatal("expected cache bucket to be initialized in Start()")
	}

	// Bucket should be usable
	_, err = consumer.cache.PutWithRevisionWithContext(ctx, "test", []byte("data"), 0)
	if err != nil {
		t.Fatalf("Failed to use bucket: %v", err)
	}
}
