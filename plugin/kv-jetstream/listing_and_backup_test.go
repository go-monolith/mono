package kvjetstream

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestKeysWithContext_ContextEnds: a listing cut short by its context is an
// error, never a shorter list.
func TestKeysWithContext_ContextEnds(t *testing.T) {
	_, js := setupBackendTestNATS(t)
	kv := createBackendTestKV(t, js, "many-keys")
	const n = 20000
	futures := make([]jetstream.PubAckFuture, 0, n)
	for i := 0; i < n; i++ {
		f, err := js.PublishAsync(fmt.Sprintf("$KV.many-keys.key%d", i), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, f)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(30 * time.Second):
		t.Fatal("publishing the keys timed out")
	}
	for _, f := range futures {
		select {
		case err := <-f.Err():
			t.Fatal(err)
		default:
		}
	}
	backend := NewJetStreamKVBackend("many-keys", kv, 0, testLogger())

	all, err := backend.KeysWithContext(context.Background())
	if err != nil || len(all) != n {
		t.Fatalf("full listing: %d keys, err=%v", len(all), err)
	}
	truncated := 0
	for _, timeout := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		keys, err := backend.KeysWithContext(ctx)
		cancel()
		if err == nil && len(keys) != n {
			t.Errorf("timeout %v: %d of %d keys returned with a nil error", timeout, len(keys), n)
		}
		if err != nil {
			truncated++
			if keys != nil {
				t.Errorf("timeout %v: %d keys returned beside the error", timeout, len(keys))
			}
		}
	}
	if truncated == 0 {
		t.Skip("no listing was cut short on this machine")
	}
}

// TestRecreate_BucketNamedLikeAStream: a bucket whose name starts with the KV
// stream prefix is backed up from its own stream, not from the bucket nats.go
// takes the name for (KV_x read as the stream of bucket x).
func TestRecreate_BucketNamedLikeAStream(t *testing.T) {
	conn, js := setupTestNATSWithJetStream(t)
	ctx := context.Background()
	// The sibling bucket "x" is at a higher sequence than the bucket "KV_x".
	sibling, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "x", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sibling.Put(ctx, fmt.Sprintf("sibling%d", i), []byte("s")); err != nil {
			t.Fatal(err)
		}
	}
	own, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "KV_x", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := own.Put(ctx, "own", []byte("o")); err != nil {
		t.Fatal(err)
	}

	if _, err := startPlugin(t, conn, BucketConfig{Name: "KV_x", Storage: MemoryStorage, RecreateOnStorageMismatch: true},
		WithLogger(slog.New(slog.DiscardHandler))); err != nil {
		t.Fatalf("Start: %v", err)
	}
	backups := backupBuckets(t, js, "KV_x")
	if len(backups) != 1 {
		t.Fatalf("backup buckets = %v, want one", backups)
	}
	backup, err := js.KeyValue(ctx, backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if entry, err := backup.Get(ctx, "own"); err != nil || string(entry.Value()) != "o" {
		t.Fatalf("the backup does not hold the bucket's own key: %v, %v", entry, err)
	}
	if _, err := backup.Get(ctx, "sibling0"); err == nil {
		t.Fatal("the backup holds a key of the sibling bucket")
	}
	// The sibling is untouched.
	if entry, err := sibling.Get(ctx, "sibling4"); err != nil || string(entry.Value()) != "s" {
		t.Fatalf("sibling key = %v, %v", entry, err)
	}
}

// TestRecreate_DetachesLeftoverBackups: a backup an interrupted recreate left
// attached is detached, so it does not copy what the recreated bucket
// receives. Buckets that are not backups keep their source.
func TestRecreate_DetachesLeftoverBackups(t *testing.T) {
	conn, js := setupTestNATSWithJetStream(t)
	ctx := context.Background()
	createFileBucket(t, js, "locks")
	source := []*jetstream.StreamSource{{
		Name:              kvStreamPrefix + "locks",
		SubjectTransforms: nil,
	}}
	const leftover = "locks-backup-20260101T000000Z-0123456789abcdef"
	for _, name := range []string{leftover, "locks-backup-mine", "locks-copy"} {
		if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, Storage: jetstream.FileStorage, Sources: source}); err != nil {
			t.Fatal(err)
		}
	}
	var logs strings.Builder
	m, err := startPlugin(t, conn, BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true},
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	sources := func(bucket string) int {
		t.Helper()
		s, err := js.Stream(ctx, kvStreamPrefix+bucket)
		if err != nil {
			t.Fatal(err)
		}
		return len(s.CachedInfo().Config.Sources)
	}
	if n := sources(leftover); n != 0 {
		t.Errorf("leftover backup still has %d source(s)", n)
	}
	for _, name := range []string{"locks-backup-mine", "locks-copy"} {
		if n := sources(name); n != 1 {
			t.Errorf("bucket %s has %d sources, want its own kept", name, n)
		}
	}
	if !strings.Contains(logs.String(), "backup_bucket="+leftover) {
		t.Errorf("log does not name the leftover backup: %s", logs.String())
	}

	// What the recreated bucket receives is not copied into the leftover.
	if err := m.Bucket("locks").Set("after", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	old, err := js.KeyValue(ctx, leftover)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Get(ctx, "after"); err == nil {
		t.Fatal("the leftover backup copied a key written after the recreate")
	}
}

// A bucket's stream may keep more versions of a key than a KV bucket can (64),
// or all of them, when it was created or edited as a stream. Its backup keeps
// as many: with fewer, the older versions would be gone with the bucket.
func TestRecreate_BackupKeepsHistoryBeyondTheBucketLimit(t *testing.T) {
	for name, perKey := range map[string]int64{"100 versions per key": 100, "every version": -1} {
		t.Run(name, func(t *testing.T) {
			conn, js := setupTestNATSWithJetStream(t)
			ctx := context.Background()
			kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "locks", Storage: jetstream.FileStorage})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := js.Stream(ctx, kvStreamPrefix+"locks")
			if err != nil {
				t.Fatal(err)
			}
			config := stream.CachedInfo().Config
			config.MaxMsgsPerSubject = perKey
			if _, err := js.UpdateStream(ctx, config); err != nil {
				t.Fatal(err)
			}
			const versions = 100
			for i := 0; i < versions; i++ {
				if _, err := kv.Put(ctx, "k", []byte(fmt.Sprintf("v%d", i))); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := startPlugin(t, conn, BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if got, msgs := streamStorage(t, js, "locks"); got != jetstream.MemoryStorage || msgs != 0 {
				t.Fatalf("stream storage=%v msgs=%d, want memory and empty", got, msgs)
			}
			backups := backupBuckets(t, js, "locks")
			if len(backups) != 1 {
				t.Fatalf("backup buckets = %v, want one", backups)
			}
			backup, err := js.Stream(ctx, kvStreamPrefix+backups[0])
			if err != nil {
				t.Fatal(err)
			}
			info := backup.CachedInfo()
			if info.State.Msgs != versions || info.Config.MaxMsgsPerSubject != perKey || len(info.Config.Sources) != 0 {
				t.Fatalf("backup holds %d messages, keeps %d per key, sources %v; want %d, %d and detached",
					info.State.Msgs, info.Config.MaxMsgsPerSubject, info.Config.Sources, versions, perKey)
			}
		})
	}
}
