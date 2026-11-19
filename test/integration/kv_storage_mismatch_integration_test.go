//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	mono "github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
	kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
)

// TestIntegration_KVJetstreamPlugin_StorageMismatch: a bucket whose stream
// another client created with file storage, while the application declares
// memory storage, fails Start with ErrStorageMismatch, or is recreated with
// RecreateOnStorageMismatch.
func TestIntegration_KVJetstreamPlugin_StorageMismatch(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "fails without the option"
		if recreate {
			name = "recreated with the option"
		}
		t.Run(name, func(t *testing.T) {
			app, err := mono.NewMonoApplication(
				mono.WithCustomLogger(&noOpsLogger{}),
				mono.WithNATSPort(freeTCPPort(t)),
				mono.WithJetStreamStorageDir(t.TempDir()),
			)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			t.Cleanup(func() { _ = app.Stop(ctx) })

			// The embedded server runs from NewMonoApplication: create the
			// bucket the way a client with default settings would.
			conn := app.EventBus("").(types.EventBusWithConn[*nats.Conn]).Conn()
			js, err := jetstream.New(conn)
			if err != nil {
				t.Fatal(err)
			}
			old, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "locks"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := old.Put(ctx, "old", []byte("v")); err != nil {
				t.Fatal(err)
			}

			kv, err := kvjetstream.New(kvjetstream.Config{Buckets: []kvjetstream.BucketConfig{
				{Name: "locks", Storage: kvjetstream.MemoryStorage, RecreateOnStorageMismatch: recreate},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := app.RegisterPlugin(kv, "kv"); err != nil {
				t.Fatal(err)
			}

			err = app.Start(ctx)
			stream, serr := js.Stream(ctx, "KV_locks")
			if serr != nil {
				t.Fatal(serr)
			}
			storage := stream.CachedInfo().Config.Storage
			if !recreate {
				if !errors.Is(err, kvjetstream.ErrStorageMismatch) {
					t.Fatalf("Start = %v, want ErrStorageMismatch", err)
				}
				if storage != jetstream.FileStorage {
					t.Fatalf("storage = %v, want the original file stream", storage)
				}
				return
			}
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if storage != jetstream.MemoryStorage {
				t.Fatalf("storage = %v, want memory", storage)
			}
			if _, err := kv.Bucket("locks").GetEntryWithContext(ctx, "old"); !errors.Is(err, kvjetstream.ErrKeyNotFound) {
				t.Fatalf("key of the old bucket: err = %v, want ErrKeyNotFound (recreating deletes the keys)", err)
			}
			// The old keys are in the backup bucket.
			var backups []string
			names := js.KeyValueStoreNames(ctx)
			for n := range names.Name() {
				if strings.HasPrefix(n, "locks-backup-") {
					backups = append(backups, n)
				}
			}
			if len(backups) != 1 {
				t.Fatalf("backup buckets = %v, want one", backups)
			}
			backup, err := js.KeyValue(ctx, backups[0])
			if err != nil {
				t.Fatal(err)
			}
			if e, err := backup.Get(ctx, "old"); err != nil || string(e.Value()) != "v" {
				t.Fatalf("backup key = %v, %v", e, err)
			}
			if err := kv.Bucket("locks").Set("k", []byte("v"), 0); err != nil {
				t.Fatalf("Set on the recreated bucket: %v", err)
			}
		})
	}
}
