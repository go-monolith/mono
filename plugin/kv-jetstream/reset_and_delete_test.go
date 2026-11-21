package kvjetstream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// PurgeFailingKV is a KeyValue whose Purge fails.
type PurgeFailingKV struct {
	jetstream.KeyValue
}

// Purge fails.
func (PurgeFailingKV) Purge(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return errors.New("permission denied")
}

// TestReset_FailureLeavesNoGoroutines: a Reset that fails on a key returned
// with the key lister still running: its goroutines stayed for good, blocked
// handing over the next key.
func TestReset_FailureLeavesNoGoroutines(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "reset-leak")
	for i := 0; i < 600; i++ { // more than the lister buffers
		if _, err := kv.Put(context.Background(), fmt.Sprintf("key%d", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	backend := NewJetStreamKVBackend("reset-leak", PurgeFailingKV{KeyValue: kv}, 0, testLogger())

	settle := func() int {
		n := runtime.NumGoroutine()
		for i := 0; i < 100; i++ {
			time.Sleep(10 * time.Millisecond)
			now := runtime.NumGoroutine()
			if now == n && i > 5 {
				break
			}
			n = now
		}
		return n
	}
	before := settle()
	const failures = 20
	for i := 0; i < failures; i++ {
		if err := backend.Reset(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("Reset = %v, want the purge error", err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		after := settle()
		if after <= before+3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before, %d after %d failed Resets", before, after, failures)
		}
	}
}

// TestDelete_MissingKeyWritesNothing: deleting a key that does not exist
// used to write a delete marker for it, which Reset cannot reclaim: a caller
// deleting keys it only guesses at grew the bucket by one message each.
func TestDelete_MissingKeyWritesNothing(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "delete-missing")
	backend := NewJetStreamKVBackend("delete-missing", kv, 0, testLogger())
	ctx := context.Background()
	if err := backend.SetWithContext(ctx, "kept", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	messages := func() uint64 {
		t.Helper()
		s, err := js.Stream(ctx, kvStreamPrefix+"delete-missing")
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs
	}
	before := messages()
	for i := 0; i < 100; i++ {
		if err := backend.DeleteWithContext(ctx, fmt.Sprintf("never-existed-%d", i)); err != nil {
			t.Fatalf("Delete of a missing key = %v, want nil", err)
		}
	}
	if after := messages(); after != before {
		t.Fatalf("the bucket's stream grew from %d to %d messages by deleting keys that never existed", before, after)
	}
	// A key that exists is still deleted, and deleting it twice is no error.
	for i := 0; i < 2; i++ {
		if err := backend.DeleteWithContext(ctx, "kept"); err != nil {
			t.Fatalf("Delete #%d of an existing key: %v", i+1, err)
		}
	}
	if _, err := backend.GetWithContext(ctx, "kept"); err == nil {
		// GetWithContext returns nil, nil for a missing key in this backend.
		if v, _ := backend.GetWithContext(ctx, "kept"); v != nil {
			t.Fatalf("the deleted key still has a value: %q", v)
		}
	}
	if after := messages(); after > before+1 {
		t.Fatalf("deleting one key twice wrote %d messages", after-before)
	}
}

// ReplicatedStatusKV is a KeyValue whose Get never finds a key and whose
// status reports three replicas, as a replica that has not caught up would
// answer for a replicated bucket.
type ReplicatedStatusKV struct {
	jetstream.KeyValue
	Replicas int
	Gets     int
}

// Get never finds the key.
func (k *ReplicatedStatusKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	k.Gets++
	return nil, jetstream.ErrKeyNotFound
}

// Status reports the bucket with Replicas replicas.
func (k *ReplicatedStatusKV) Status(ctx context.Context) (jetstream.KeyValueStatus, error) {
	status, err := k.KeyValue.Status(ctx)
	if err != nil {
		return nil, err
	}
	bucket, ok := status.(*jetstream.KeyValueBucketStatus)
	if !ok {
		return status, nil
	}
	bucket.StreamInfo().Config.Replicas = k.Replicas
	return bucket, nil
}

// TestDelete_ReplicatedBucketIsNotLookedUp: a read of a replicated bucket
// may be answered by a replica that is behind, so a key written a moment ago
// could be reported missing. There the delete is made without the lookup.
func TestDelete_ReplicatedBucketIsNotLookedUp(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "delete-replicated")
	ctx := context.Background()
	if _, err := kv.Put(ctx, "lock", []byte("held")); err != nil {
		t.Fatal(err)
	}

	stale := &ReplicatedStatusKV{KeyValue: kv, Replicas: 3}
	backend := NewJetStreamKVBackend("delete-replicated", stale, 0, testLogger())
	if backend.SingleReplica(ctx) {
		t.Fatal("a bucket with three replicas is taken for one with a single replica")
	}
	if err := backend.DeleteWithContext(ctx, "lock"); err != nil {
		t.Fatal(err)
	}
	if stale.Gets != 0 {
		t.Fatalf("the key was looked up %d time(s) on a replicated bucket", stale.Gets)
	}
	if _, err := kv.Get(ctx, "lock"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("the key was not deleted: %v", err)
	}

	// With one replica the lookup is made, and trusted.
	single := &ReplicatedStatusKV{KeyValue: kv, Replicas: 1}
	backend = NewJetStreamKVBackend("delete-replicated", single, 0, testLogger())
	if !backend.SingleReplica(ctx) {
		t.Fatal("a bucket with one replica is not recognised")
	}
	if err := backend.DeleteWithContext(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
	if single.Gets != 1 {
		t.Fatalf("lookups = %d, want 1", single.Gets)
	}
}
