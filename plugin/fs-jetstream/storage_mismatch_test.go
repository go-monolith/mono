package fsjetstream

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// createFileObjectStore creates bucket with file storage holding a small
// object and a multi-chunk one, the state another client's defaults leave
// behind. It returns the multi-chunk content.
func createFileObjectStore(t *testing.T, js jetstream.JetStream, bucket string) []byte {
	t.Helper()
	ctx := context.Background()
	os, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 300*1024) // three chunks
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if _, err := os.PutBytes(ctx, "dir/big.bin", big); err != nil {
		t.Fatal(err)
	}
	if _, err := os.PutString(ctx, "small.txt", "hi"); err != nil {
		t.Fatal(err)
	}
	return big
}

func objStorage(t *testing.T, js jetstream.JetStream, bucket string) (jetstream.StorageType, uint64) {
	t.Helper()
	s, err := js.Stream(context.Background(), objStreamPrefix+bucket)
	if err != nil {
		t.Fatal(err)
	}
	return s.CachedInfo().Config.Storage, s.CachedInfo().State.Msgs
}

func objBackups(t *testing.T, js jetstream.JetStream, bucket string) []string {
	t.Helper()
	var names []string
	l := js.ObjectStoreNames(context.Background())
	for n := range l.Name() {
		if strings.HasPrefix(n, bucket+"-backup-") {
			names = append(names, n)
		}
	}
	if err := l.Error(); err != nil {
		t.Fatal(err)
	}
	return names
}

func startFSPlugin(t *testing.T, conn *nats.Conn, cfg BucketConfig, opts ...Option) (*PluginModule, error) {
	t.Helper()
	m, err := New(Config{Buckets: []BucketConfig{cfg}}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	m.SetEventBus(newRealEventBus(conn))
	return m, m.Start(context.Background())
}

func TestStart_StorageMismatch(t *testing.T) {
	t.Run("Start fails without the option and the bucket is kept", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileObjectStore(t, js, "files")
		_, err := startFSPlugin(t, conn, BucketConfig{Name: "files", Storage: MemoryStorage})
		if !errors.Is(err, ErrStorageMismatch) {
			t.Fatalf("err = %v, want ErrStorageMismatch", err)
		}
		for _, want := range []string{`bucket "files"`, "File storage", "Memory storage", "OBJ_files", "the bucket's objects", "RecreateOnStorageMismatch"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		if got, msgs := objStorage(t, js, "files"); got != jetstream.FileStorage || msgs == 0 {
			t.Fatalf("storage=%v msgs=%d, want the original", got, msgs)
		}
		if b := objBackups(t, js, "files"); len(b) != 0 {
			t.Fatalf("backups = %v", b)
		}
	})

	t.Run("recreated with a readable backup", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		big := createFileObjectStore(t, js, "files")
		var logs bytes.Buffer
		m, err := startFSPlugin(t, conn, BucketConfig{Name: "files", Storage: MemoryStorage, RecreateOnStorageMismatch: true},
			WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if got, msgs := objStorage(t, js, "files"); got != jetstream.MemoryStorage || msgs != 0 {
			t.Fatalf("storage=%v msgs=%d, want an empty memory bucket", got, msgs)
		}
		if err := m.Bucket("files").Set("new.txt", []byte("x"), 0); err != nil {
			t.Fatalf("Set on the recreated bucket: %v", err)
		}

		backups := objBackups(t, js, "files")
		if len(backups) != 1 {
			t.Fatalf("backups = %v, want one", backups)
		}
		ctx := context.Background()
		backup, err := js.ObjectStore(ctx, backups[0])
		if err != nil {
			t.Fatal(err)
		}
		got, err := backup.GetBytes(ctx, "dir/big.bin")
		if err != nil || !bytes.Equal(got, big) {
			t.Fatalf("backup dir/big.bin: equal=%v err=%v", bytes.Equal(got, big), err)
		}
		if s, err := backup.GetString(ctx, "small.txt"); err != nil || s != "hi" {
			t.Fatalf("backup small.txt = %q, %v", s, err)
		}
		if _, err := backup.GetInfo(ctx, "new.txt"); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("backup holds an object of the recreated bucket: %v", err)
		}
		bs, err := js.Stream(ctx, objStreamPrefix+backups[0])
		if err != nil {
			t.Fatal(err)
		}
		if cfg := bs.CachedInfo().Config; len(cfg.Sources) != 0 || cfg.Storage != jetstream.FileStorage || cfg.MaxAge != DefaultRecreateBackupTTL {
			t.Fatalf("backup sources=%v storage=%v max_age=%v", cfg.Sources, cfg.Storage, cfg.MaxAge)
		}
		for _, want := range []string{"Recreating object store bucket", "its objects are deleted", "bucket=files", "backup_bucket=" + backups[0]} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("log %q lacks %q", logs.String(), want)
			}
		}
	})

	t.Run("SkipBackupOnRecreate makes no backup", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileObjectStore(t, js, "files")
		if _, err := startFSPlugin(t, conn, BucketConfig{Name: "files", Storage: MemoryStorage, RecreateOnStorageMismatch: true, SkipBackupOnRecreate: true}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if b := objBackups(t, js, "files"); len(b) != 0 {
			t.Fatalf("backups = %v", b)
		}
	})

	t.Run("a matching bucket is left alone", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileObjectStore(t, js, "files")
		m, err := startFSPlugin(t, conn, BucketConfig{Name: "files", Storage: FileStorage, RecreateOnStorageMismatch: true})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if v, err := m.Bucket("files").GetWithContext(context.Background(), "small.txt"); err != nil || string(v) != "hi" {
			t.Fatalf("existing object = %q, %v", v, err)
		}
	})
}

// failingStreams makes the re-reads of the stream during the copy hang until
// the deadline, so the copy cannot be confirmed, and counts deletes.
type failingStreams struct {
	streamAPI
	calls, deletes *int
}

func (f failingStreams) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	if *f.calls++; *f.calls == 1 {
		return f.streamAPI.StreamInfo(ctx, name)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f failingStreams) DeleteStream(ctx context.Context, name string) error {
	*f.deletes++
	return f.streamAPI.DeleteStream(ctx, name)
}

func TestOpenBucket_BackupTimeoutDeletesNothing(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)
	createFileObjectStore(t, js, "files")
	cfg := BucketConfig{Name: "files", Storage: MemoryStorage, RecreateOnStorageMismatch: true, RecreateBackupTimeout: 300 * time.Millisecond}
	m, err := New(Config{Buckets: []BucketConfig{cfg}}, WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	m.js = js
	calls, deletes := 0, 0
	_, err = m.openBucket(context.Background(), failingStreams{streamAPI: streamAPI{js: js}, calls: &calls, deletes: &deletes}, cfg)
	if !errors.Is(err, ErrStorageMismatch) || !strings.Contains(err.Error(), "backup failed") {
		t.Fatalf("err = %v", err)
	}
	if deletes != 0 {
		t.Fatalf("DeleteStream called %d times", deletes)
	}
	if got, _ := objStorage(t, js, "files"); got != jetstream.FileStorage {
		t.Fatalf("storage = %v, want the original", got)
	}
	if b := objBackups(t, js, "files"); len(b) != 0 {
		t.Fatalf("incomplete backup left: %v", b)
	}
}

func TestNew_RecreateValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    BucketConfig
		want string
	}{
		{"timeout without the option", BucketConfig{Name: "b", RecreateBackupTimeout: time.Minute}, "requires RecreateOnStorageMismatch"},
		{"timeout without a backup", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, SkipBackupOnRecreate: true, RecreateBackupTimeout: time.Minute}, "requires RecreateOnStorageMismatch with a backup"},
		{"negative timeout", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, RecreateBackupTimeout: -1}, "cannot be negative"},
		{"TTL below the minimum", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, RecreateBackupTTL: 30 * time.Minute}, "at least"},
		{"TTL not above the timeout", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, RecreateBackupTTL: 2 * time.Hour, RecreateBackupTimeout: 3 * time.Hour}, "longer than the backup timeout"},
		{"skip without the option", BucketConfig{Name: "b", SkipBackupOnRecreate: true}, "require RecreateOnStorageMismatch"},
		{"valid", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, RecreateBackupTTL: 2 * time.Hour, RecreateBackupTimeout: time.Hour}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{Buckets: []BucketConfig{tt.b}})
			if tt.want == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestWithLogger_NilRejected(t *testing.T) {
	if _, err := New(Config{Buckets: []BucketConfig{{Name: "b"}}}, WithLogger(nil)); err == nil {
		t.Fatal("New with WithLogger(nil) succeeded")
	}
}

// TestStart_DefaultLoggerResolvedAtStart: without WithLogger, the plugin logs
// to the slog.Default in effect at Start. It replaces the global default, so
// it must not run in parallel with other tests.
func TestStart_DefaultLoggerResolvedAtStart(t *testing.T) {
	conn, _ := setupTestNATSWithJetStream(t)
	m, err := New(Config{Buckets: []BucketConfig{{Name: "files"}}})
	if err != nil {
		t.Fatal(err)
	}
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	m.SetEventBus(newRealEventBus(conn))
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.Contains(logs.String(), "module=fs-jetstream") {
		t.Fatalf("the default logger set after New did not receive the plugin's lines: %q", logs.String())
	}
}

// TestRecreate_BackupIsIndependent: the backup is an object store of its own.
// Its objects name the backup bucket, overwritten and deleted objects come
// out as in the original, and deleting an object in the backup leaves the
// recreated bucket alone.
func TestRecreate_BackupIsIndependent(t *testing.T) {
	conn, js := setupTestNATSWithJetStream(t)
	ctx := context.Background()
	big := createFileObjectStore(t, js, "files")
	old, err := js.ObjectStore(ctx, "files")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.PutString(ctx, "small.txt", "v2"); err != nil { // overwrite
		t.Fatal(err)
	}
	if _, err := old.PutString(ctx, "gone.txt", "x"); err != nil {
		t.Fatal(err)
	}
	if err := old.Delete(ctx, "gone.txt"); err != nil {
		t.Fatal(err)
	}

	m, err := startFSPlugin(t, conn, BucketConfig{Name: "files", Storage: MemoryStorage, RecreateOnStorageMismatch: true})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	backups := objBackups(t, js, "files")
	if len(backups) != 1 {
		t.Fatalf("backups = %v", backups)
	}
	backup, err := js.ObjectStore(ctx, backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if s, err := backup.GetString(ctx, "small.txt"); err != nil || s != "v2" {
		t.Fatalf("overwritten object = %q, %v; want v2", s, err)
	}
	if _, err := backup.GetInfo(ctx, "gone.txt"); !errors.Is(err, jetstream.ErrObjectNotFound) {
		t.Fatalf("deleted object: err = %v, want ErrObjectNotFound", err)
	}
	list, err := backup.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("backup lists %d objects, want 2", len(list))
	}
	for _, info := range list {
		if info.Bucket != backups[0] {
			t.Fatalf("object %s names bucket %q, want %q", info.Name, info.Bucket, backups[0])
		}
	}
	if got, err := backup.GetBytes(ctx, "dir/big.bin"); err != nil || !bytes.Equal(got, big) {
		t.Fatalf("big object after the rewrite: equal=%v err=%v", bytes.Equal(got, big), err)
	}

	// Restore one object into the recreated bucket, then delete it from the
	// backup: the restored copy must survive.
	if err := m.Bucket("files").Set("small.txt", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	if err := backup.Delete(ctx, "small.txt"); err != nil {
		t.Fatal(err)
	}
	if v, err := m.Bucket("files").GetWithContext(ctx, "small.txt"); err != nil || string(v) != "v2" {
		t.Fatalf("restored object after a delete in the backup = %q, %v", v, err)
	}
}

func TestNew_RecreateValidationDefaults(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    BucketConfig
		want string
	}{
		{"default TTL not above the timeout", BucketConfig{Name: "b", RecreateOnStorageMismatch: true, RecreateBackupTimeout: 8 * 24 * time.Hour}, "must be longer than the backup timeout"},
		{"name too long for a backup", BucketConfig{Name: strings.Repeat("a", 211), RecreateOnStorageMismatch: true}, "name too long"},
		{"longest name with a backup", BucketConfig{Name: strings.Repeat("a", 210), RecreateOnStorageMismatch: true}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{Buckets: []BucketConfig{tt.b}})
			if tt.want == "" {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
