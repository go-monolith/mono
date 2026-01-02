package kvjetstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Test Helpers
// =============================================================================

func setupWatcherTestNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
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
		nats.Name("watcher-test-client"),
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

func createWatcherTestKV(t *testing.T, js jetstream.JetStream, name string) jetstream.KeyValue {
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
// KeyWatcher Interface Tests
// =============================================================================

func TestJetStreamKeyWatcher_Updates(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-updates-test")
	ctx := context.Background()

	// Create JetStream watcher
	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Wrap it
	watcher := newJetStreamKeyWatcher(jsWatcher)
	defer watcher.Stop()

	// Updates channel should be available
	updates := watcher.Updates()
	if updates == nil {
		t.Fatal("expected updates channel, got nil")
	}
}

func TestJetStreamKeyWatcher_Stop(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-stop-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// Stop should not error
	err = watcher.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

func TestJetStreamKeyWatcher_ChannelClose(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-close-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// Stop the watcher
	watcher.Stop()

	// Channel should eventually close
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			// Drain any remaining updates
			for range watcher.Updates() {
			}
		}
		// Channel closed - success
	case <-time.After(2 * time.Second):
		t.Error("channel did not close after Stop")
	}
}

func TestJetStreamKeyWatcher_ConvertEntry(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-convert-test")
	ctx := context.Background()

	// Start watching before putting data
	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)
	defer watcher.Stop()

	// Put a value
	go func() {
		time.Sleep(100 * time.Millisecond)
		kv.Put(ctx, "test-key", []byte("test-value"))
	}()

	// Wait for the converted entry
	timeout := time.After(5 * time.Second)
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				// Initial sync sentinel, continue waiting
				continue
			}

			// Verify converted entry fields
			if entry.Bucket != "watcher-convert-test" {
				t.Errorf("expected bucket 'watcher-convert-test', got %q", entry.Bucket)
			}
			if entry.Key != "test-key" {
				t.Errorf("expected key 'test-key', got %q", entry.Key)
			}
			if string(entry.Value) != "test-value" {
				t.Errorf("expected value 'test-value', got %q", string(entry.Value))
			}
			if entry.Revision == 0 {
				t.Error("expected non-zero revision")
			}
			if entry.Timestamp.IsZero() {
				t.Error("expected non-zero timestamp")
			}
			if entry.Operation != KeyOperationPut {
				t.Errorf("expected KeyOperationPut, got %v", entry.Operation)
			}
			return

		case <-timeout:
			t.Fatal("timeout waiting for entry")
		}
	}
}

func TestJetStreamKeyWatcher_NilSentinel(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-sentinel-test")
	ctx := context.Background()

	// Put some data first
	kv.Put(ctx, "existing-key", []byte("existing-value"))

	// Start watching (not UpdatesOnly)
	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)
	defer watcher.Stop()

	// Should receive existing value first, then nil sentinel
	gotNil := false
	gotExisting := false
	timeout := time.After(5 * time.Second)

loop:
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				gotNil = true
				break loop
			}
			if entry.Key == "existing-key" {
				gotExisting = true
			}
		case <-timeout:
			break loop
		}
	}

	if !gotExisting {
		t.Error("expected to receive existing key")
	}
	if !gotNil {
		t.Error("expected to receive nil sentinel")
	}
}

func TestJetStreamKeyWatcher_ReceivesMultipleUpdates(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-multi-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">", jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)
	defer watcher.Stop()

	// Put multiple values
	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 5; i++ {
			kv.Put(ctx, "key", []byte("value"))
		}
	}()

	// Collect updates
	updates := make([]*KVEntry, 0)
	timeout := time.After(3 * time.Second)

loop:
	for {
		select {
		case entry := <-watcher.Updates():
			if entry != nil {
				updates = append(updates, entry)
				if len(updates) >= 5 {
					break loop
				}
			}
		case <-timeout:
			break loop
		}
	}

	if len(updates) < 5 {
		t.Errorf("expected at least 5 updates, got %d", len(updates))
	}
}

// =============================================================================
// Interface Compliance Tests
// =============================================================================

func TestJetStreamKeyWatcher_ImplementsKeyWatcher(t *testing.T) {
	var _ KeyWatcher = (*jetStreamKeyWatcher)(nil)
}

// =============================================================================
// processUpdates Context Cancellation Tests
// =============================================================================

func TestJetStreamKeyWatcher_StopDuringNilSentinel(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-stop-nil-test")
	ctx := context.Background()

	// Put some data so we get the nil sentinel
	kv.Put(ctx, "key1", []byte("value1"))

	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// Drain the initial value and nil sentinel to ensure watcher is ready
	gotNil := false
	timeout := time.After(5 * time.Second)

loop:
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				gotNil = true
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	// nil sentinel should be received when initial sync is complete
	if !gotNil {
		t.Log("nil sentinel not received within timeout (JetStream behavior may vary)")
	}

	// Main test: Stop should cleanly terminate without error
	err = watcher.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

func TestJetStreamKeyWatcher_ConcurrentStopAndUpdate(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-concurrent-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">", jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// Start a goroutine that continuously puts values
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			select {
			case <-done:
				return
			default:
				kv.Put(ctx, "key", []byte("value"))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Let some updates through
	time.Sleep(50 * time.Millisecond)

	// Stop the watcher while updates are happening
	err = watcher.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}

	close(done)

	// Channel should be closed
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Error("channel not closed after Stop")
	}
}

func TestJetStreamKeyWatcher_StopBeforeAnyUpdates(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-stop-early-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">", jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// Stop immediately before any updates
	err = watcher.Stop()
	if err != nil {
		t.Errorf("Stop returned error: %v", err)
	}

	// Channel should be closed
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Error("channel not closed after Stop")
	}
}

func TestJetStreamKeyWatcher_MultipleStop(t *testing.T) {
	_, js := setupWatcherTestNATS(t)
	kv := createWatcherTestKV(t, js, "watcher-multi-stop-test")
	ctx := context.Background()

	jsWatcher, err := kv.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	watcher := newJetStreamKeyWatcher(jsWatcher)

	// First stop
	err = watcher.Stop()
	if err != nil {
		t.Errorf("First Stop returned error: %v", err)
	}

	// Second stop should not panic (JetStream watcher may return error)
	// but the context is already cancelled so processUpdates is already done
	// The behavior here depends on the underlying JetStream implementation
	_ = watcher.Stop() // Just verify it doesn't panic
}

// =============================================================================
// convertKVEntry Operation Tests
// =============================================================================

// mockWatcherKeyValueEntry implements jetstream.KeyValueEntry for unit testing.
type mockWatcherKeyValueEntry struct {
	bucket    string
	key       string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation jetstream.KeyValueOp
}

func (e *mockWatcherKeyValueEntry) Bucket() string                  { return e.bucket }
func (e *mockWatcherKeyValueEntry) Key() string                     { return e.key }
func (e *mockWatcherKeyValueEntry) Value() []byte                   { return e.value }
func (e *mockWatcherKeyValueEntry) Revision() uint64                { return e.revision }
func (e *mockWatcherKeyValueEntry) Created() time.Time              { return e.created }
func (e *mockWatcherKeyValueEntry) Delta() uint64                   { return e.delta }
func (e *mockWatcherKeyValueEntry) Operation() jetstream.KeyValueOp { return e.operation }

var _ jetstream.KeyValueEntry = (*mockWatcherKeyValueEntry)(nil)

func TestConvertKVEntry_PutOperation(t *testing.T) {
	now := time.Now()
	entry := &mockWatcherKeyValueEntry{
		bucket:    "test-bucket",
		key:       "test-key",
		value:     []byte("test-value"),
		revision:  42,
		created:   now,
		delta:     0,
		operation: jetstream.KeyValuePut,
	}

	result := convertKVEntry(entry)

	if result.Bucket != "test-bucket" {
		t.Errorf("expected bucket 'test-bucket', got %q", result.Bucket)
	}
	if result.Key != "test-key" {
		t.Errorf("expected key 'test-key', got %q", result.Key)
	}
	if string(result.Value) != "test-value" {
		t.Errorf("expected value 'test-value', got %q", string(result.Value))
	}
	if result.Revision != 42 {
		t.Errorf("expected revision 42, got %d", result.Revision)
	}
	if result.Timestamp != now {
		t.Errorf("expected timestamp %v, got %v", now, result.Timestamp)
	}
	if result.Operation != KeyOperationPut {
		t.Errorf("expected KeyOperationPut, got %v", result.Operation)
	}
}

func TestConvertKVEntry_DeleteOperation(t *testing.T) {
	entry := &mockWatcherKeyValueEntry{
		bucket:    "delete-bucket",
		key:       "deleted-key",
		value:     nil, // Deleted keys have nil value
		revision:  99,
		created:   time.Now(),
		operation: jetstream.KeyValueDelete,
	}

	result := convertKVEntry(entry)

	if result.Key != "deleted-key" {
		t.Errorf("expected key 'deleted-key', got %q", result.Key)
	}
	if result.Value != nil {
		t.Errorf("expected nil value, got %v", result.Value)
	}
	if result.Operation != KeyOperationDelete {
		t.Errorf("expected KeyOperationDelete, got %v", result.Operation)
	}
}

func TestConvertKVEntry_PurgeOperation(t *testing.T) {
	entry := &mockWatcherKeyValueEntry{
		bucket:    "purge-bucket",
		key:       "purged-key",
		value:     nil,
		revision:  150,
		created:   time.Now(),
		operation: jetstream.KeyValuePurge,
	}

	result := convertKVEntry(entry)

	if result.Key != "purged-key" {
		t.Errorf("expected key 'purged-key', got %q", result.Key)
	}
	if result.Operation != KeyOperationPurge {
		t.Errorf("expected KeyOperationPurge, got %v", result.Operation)
	}
}

func TestConvertKVEntry_AllOperations(t *testing.T) {
	tests := []struct {
		name       string
		jsOp       jetstream.KeyValueOp
		expectedOp KeyOperation
	}{
		{"Put", jetstream.KeyValuePut, KeyOperationPut},
		{"Delete", jetstream.KeyValueDelete, KeyOperationDelete},
		{"Purge", jetstream.KeyValuePurge, KeyOperationPurge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &mockWatcherKeyValueEntry{
				bucket:    "test",
				key:       "key",
				value:     []byte("value"),
				revision:  1,
				created:   time.Now(),
				operation: tt.jsOp,
			}

			result := convertKVEntry(entry)

			if result.Operation != tt.expectedOp {
				t.Errorf("expected operation %v, got %v", tt.expectedOp, result.Operation)
			}
		})
	}
}

// =============================================================================
// Mock-based processUpdates Edge Case Tests
// =============================================================================

// mockWatcherJetStreamKeyWatcher implements jetstream.KeyWatcher for unit testing.
type mockWatcherJetStreamKeyWatcher struct {
	updates chan jetstream.KeyValueEntry
	stopped bool
	stopErr error
	mu      sync.Mutex
}

func newMockWatcherJetStreamKeyWatcher() *mockWatcherJetStreamKeyWatcher {
	return &mockWatcherJetStreamKeyWatcher{
		updates: make(chan jetstream.KeyValueEntry, 10),
	}
}

func (m *mockWatcherJetStreamKeyWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return m.updates
}

func (m *mockWatcherJetStreamKeyWatcher) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	return m.stopErr
}

func (m *mockWatcherJetStreamKeyWatcher) isStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

var _ jetstream.KeyWatcher = (*mockWatcherJetStreamKeyWatcher)(nil)

func TestJetStreamKeyWatcher_ProcessUpdates_NilEntry_Unit(t *testing.T) {
	mock := newMockWatcherJetStreamKeyWatcher()
	watcher := newJetStreamKeyWatcher(mock)
	defer watcher.Stop()

	// Send nil entry (end of initial values marker)
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

func TestJetStreamKeyWatcher_ProcessUpdates_ContextDone_Unit(t *testing.T) {
	mock := newMockWatcherJetStreamKeyWatcher()
	watcher := newJetStreamKeyWatcher(mock)

	// Stop the watcher (cancels context)
	err := watcher.Stop()
	if err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	// Updates channel should be closed
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestJetStreamKeyWatcher_ProcessUpdates_ChannelClose_Unit(t *testing.T) {
	mock := newMockWatcherJetStreamKeyWatcher()
	watcher := newJetStreamKeyWatcher(mock)

	// Close the underlying channel directly (simulating NATS watcher close)
	close(mock.updates)

	// Watcher's updates channel should close
	select {
	case _, ok := <-watcher.Updates():
		if ok {
			for range watcher.Updates() {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestJetStreamKeyWatcher_ProcessUpdates_StopDuringNilSend_Unit(t *testing.T) {
	// Test the ctx.Done path during nil entry send (line 71)
	mock := newMockWatcherJetStreamKeyWatcher()
	watcher := newJetStreamKeyWatcher(mock)

	// Stop in background while sending nil
	go func() {
		time.Sleep(10 * time.Millisecond)
		watcher.Stop()
	}()

	mock.updates <- nil

	// Wait for clean shutdown
	time.Sleep(50 * time.Millisecond)

	if !mock.isStopped() {
		t.Error("expected watcher to be stopped")
	}
}

func TestJetStreamKeyWatcher_ProcessUpdates_StopDuringEntrySend_Unit(t *testing.T) {
	// Test the ctx.Done path during entry send (line 79)
	mock := newMockWatcherJetStreamKeyWatcher()
	watcher := newJetStreamKeyWatcher(mock)

	// Stop in background while sending entry
	go func() {
		time.Sleep(10 * time.Millisecond)
		watcher.Stop()
	}()

	mock.updates <- &mockWatcherKeyValueEntry{
		key:       "key",
		value:     []byte("value"),
		operation: jetstream.KeyValuePut,
	}

	// Wait for clean shutdown
	time.Sleep(50 * time.Millisecond)

	if !mock.isStopped() {
		t.Error("expected watcher to be stopped")
	}
}

func TestJetStreamKeyWatcher_GoroutineStart_Unit(t *testing.T) {
	mock := newMockWatcherJetStreamKeyWatcher()

	// newJetStreamKeyWatcher should block until goroutine starts
	// This tests lines 41-46 (goroutine synchronization)
	watcher := newJetStreamKeyWatcher(mock)

	// Immediately after creation, the watcher should be ready to receive
	// Send an entry right away to verify goroutine is running
	mock.updates <- &mockWatcherKeyValueEntry{
		bucket:    "test",
		key:       "immediate-key",
		value:     []byte("value"),
		revision:  1,
		operation: jetstream.KeyValuePut,
	}

	// Should receive the entry
	select {
	case entry := <-watcher.Updates():
		if entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if entry.Key != "immediate-key" {
			t.Errorf("expected key 'immediate-key', got %q", entry.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout - goroutine may not have started")
	}

	watcher.Stop()
}
