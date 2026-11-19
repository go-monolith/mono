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
	fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"
)

// TestIntegration_FSJetstreamPlugin_StorageMismatch: an object store bucket
// whose stream another client created with file storage, while the
// application declares memory storage, fails Start with ErrStorageMismatch,
// or is recreated with RecreateOnStorageMismatch after its objects are
// copied to a readable backup bucket.
func TestIntegration_FSJetstreamPlugin_StorageMismatch(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "fails without the option"
		if recreate {
			name = "recreated with a backup"
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

			conn := app.EventBus("").(types.EventBusWithConn[*nats.Conn]).Conn()
			js, err := jetstream.New(conn)
			if err != nil {
				t.Fatal(err)
			}
			old, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "files"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := old.PutString(ctx, "report.txt", "q3"); err != nil {
				t.Fatal(err)
			}

			fs, err := fsjetstream.New(fsjetstream.Config{Buckets: []fsjetstream.BucketConfig{
				{Name: "files", Storage: fsjetstream.MemoryStorage, RecreateOnStorageMismatch: recreate},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := app.RegisterPlugin(fs, "fs"); err != nil {
				t.Fatal(err)
			}

			err = app.Start(ctx)
			stream, serr := js.Stream(ctx, "OBJ_files")
			if serr != nil {
				t.Fatal(serr)
			}
			storage := stream.CachedInfo().Config.Storage
			if !recreate {
				if !errors.Is(err, fsjetstream.ErrStorageMismatch) {
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
			if v, err := fs.Bucket("files").GetWithContext(ctx, "report.txt"); err != nil || v != nil {
				t.Fatalf("old object in the recreated bucket = %q, %v; want none (nil, nil)", v, err)
			}
			var backups []string
			names := js.ObjectStoreNames(ctx)
			for n := range names.Name() {
				if strings.HasPrefix(n, "files-backup-") {
					backups = append(backups, n)
				}
			}
			if len(backups) != 1 {
				t.Fatalf("backup buckets = %v, want one", backups)
			}
			backup, err := js.ObjectStore(ctx, backups[0])
			if err != nil {
				t.Fatal(err)
			}
			if s, err := backup.GetString(ctx, "report.txt"); err != nil || s != "q3" {
				t.Fatalf("backup object = %q, %v", s, err)
			}
		})
	}
}
