package jsrecreate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var errMismatch = errors.New("test storage mismatch")

func startJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	s, err := server.NewServer(&server.Options{JetStream: true, Port: -1, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// fileBucket creates KV bucket name with file storage and n keys.
func fileBucket(t *testing.T, js jetstream.JetStream, name string, n int) {
	t.Helper()
	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: name, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := kv.Put(context.Background(), "k"+string(rune('a'+i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
}

// kvSpec is a Spec for KV bucket name configured with memory storage.
func kvSpec(js jetstream.JetStream, streams StreamStore, name string, o Options, logs *bytes.Buffer) Spec[jetstream.KeyValue] {
	if logs == nil {
		logs = &bytes.Buffer{}
	}
	return Spec[jetstream.KeyValue]{
		JS:           js,
		Streams:      streams,
		Logger:       slog.New(slog.NewTextHandler(logs, nil)),
		Bucket:       name,
		StreamPrefix: "KV_",
		Subjects:     []string{"$KV." + name + ".>"},
		Want:         jetstream.MemoryStorage,
		Options:      o,
		Kind:         "KV bucket",
		ContentNoun:  "keys",
		ErrMismatch:  errMismatch,
		Create: func(ctx context.Context) (jetstream.KeyValue, error) {
			return js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, Storage: jetstream.MemoryStorage})
		},
		CreateBackup: func(ctx context.Context, backup string, _ *jetstream.StreamInfo, ttl time.Duration) error {
			_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: backup, TTL: ttl, Storage: jetstream.FileStorage,
				Sources: []*jetstream.StreamSource{{Name: name}}})
			return err
		},
		DeleteBackup: func(ctx context.Context, backup string) error { return js.DeleteKeyValue(ctx, backup) },
	}
}

func storageOf(t *testing.T, js jetstream.JetStream, stream string) (jetstream.StorageType, uint64) {
	t.Helper()
	s, err := js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	return s.CachedInfo().Config.Storage, s.CachedInfo().State.Msgs
}

func backups(t *testing.T, js jetstream.JetStream, bucket string) []string {
	t.Helper()
	var names []string
	l := js.KeyValueStoreNames(context.Background())
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

// fakeStreams wraps the real API to inject lookup and delete outcomes and to
// count deletes.
type fakeStreams struct {
	StreamAPI
	info      func(ctx context.Context, name string) (*jetstream.StreamInfo, error)
	deleteErr error
	deletes   *int
}

func (f fakeStreams) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	if f.info != nil {
		return f.info(ctx, name)
	}
	return f.StreamAPI.StreamInfo(ctx, name)
}

func (f fakeStreams) DeleteStream(ctx context.Context, name string) error {
	if f.deletes != nil {
		*f.deletes++
	}
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.StreamAPI.DeleteStream(ctx, name)
}

func TestOpen_Recreate(t *testing.T) {
	js := startJetStream(t)
	fileBucket(t, js, "locks", 3)
	var logs bytes.Buffer
	kv, err := Open(context.Background(), kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, &logs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, msgs := storageOf(t, js, "KV_locks"); got != jetstream.MemoryStorage || msgs != 0 {
		t.Fatalf("storage=%v msgs=%d, want an empty memory bucket", got, msgs)
	}
	if _, err := kv.Put(context.Background(), "new", []byte("x")); err != nil {
		t.Fatal(err)
	}
	b := backups(t, js, "locks")
	if len(b) != 1 {
		t.Fatalf("backups = %v", b)
	}
	if got, msgs := storageOf(t, js, "KV_"+b[0]); got != jetstream.FileStorage || msgs != 3 {
		t.Fatalf("backup storage=%v msgs=%d, want file with 3 keys", got, msgs)
	}
	bs, _ := js.Stream(context.Background(), "KV_"+b[0])
	if cfg := bs.CachedInfo().Config; len(cfg.Sources) != 0 || cfg.MaxAge != DefaultBackupTTL {
		t.Fatalf("backup sources=%v max_age=%v, want detached and %v", cfg.Sources, cfg.MaxAge, DefaultBackupTTL)
	}
	for _, want := range []string{"Recreating KV bucket", "its keys are deleted", "messages=3", "backup_bucket=" + b[0]} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log %q lacks %q", logs.String(), want)
		}
	}
}

func TestOpen_NoBackup(t *testing.T) {
	for _, tt := range []struct {
		name string
		keys int
		o    Options
		ttl  time.Duration
	}{
		{"skipped", 2, Options{Recreate: true, SkipBackup: true}, 0},
		{"empty bucket", 0, Options{Recreate: true}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			fileBucket(t, js, "locks", tt.keys)
			if _, err := Open(context.Background(), kvSpec(js, StreamAPI{JS: js}, "locks", tt.o, nil)); err != nil {
				t.Fatalf("Open: %v", err)
			}
			if b := backups(t, js, "locks"); len(b) != 0 {
				t.Fatalf("backups = %v, want none", b)
			}
		})
	}
	t.Run("a negative TTL keeps the backup", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		if _, err := Open(context.Background(), kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true, BackupTTL: -1}, nil)); err != nil {
			t.Fatalf("Open: %v", err)
		}
		b := backups(t, js, "locks")
		bs, err := js.Stream(context.Background(), "KV_"+b[0])
		if err != nil || bs.CachedInfo().Config.MaxAge != 0 {
			t.Fatalf("backup max age = %v, %v; want 0", bs.CachedInfo().Config.MaxAge, err)
		}
	})
}

// TestOpen_NeverDeletes covers every path after a failed create that must
// not delete anything.
func TestOpen_NeverDeletes(t *testing.T) {
	type infoFn = func(context.Context, string) (*jetstream.StreamInfo, error)
	altered := func(real StreamAPI, edit func(*jetstream.StreamInfo)) infoFn {
		return func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			info, err := real.StreamInfo(ctx, name)
			if err == nil {
				edit(info)
			}
			return info, err
		}
	}
	for _, tt := range []struct {
		name     string
		o        Options
		info     func(real StreamAPI) infoFn
		ctx      func() (context.Context, context.CancelFunc)
		mismatch bool // the error wraps errMismatch (else: the create error)
		contains string
	}{
		{name: "no opt-in", o: Options{}, mismatch: true, contains: "nats stream rm KV_locks, which deletes the bucket's keys"},
		{name: "lookup fails", o: Options{Recreate: true}, info: func(StreamAPI) infoFn {
			return func(context.Context, string) (*jetstream.StreamInfo, error) { return nil, errors.New("timeout") }
		}},
		{name: "no info", o: Options{Recreate: true}, info: func(StreamAPI) infoFn {
			return func(context.Context, string) (*jetstream.StreamInfo, error) { return nil, nil }
		}},
		{name: "storage matches", o: Options{Recreate: true}, info: func(StreamAPI) infoFn {
			return func(context.Context, string) (*jetstream.StreamInfo, error) {
				return &jetstream.StreamInfo{Config: jetstream.StreamConfig{Storage: jetstream.MemoryStorage}}, nil
			}
		}},
		{name: "a mirror", o: Options{Recreate: true}, mismatch: true, contains: "not a plain KV bucket", info: func(r StreamAPI) infoFn {
			return altered(r, func(i *jetstream.StreamInfo) { i.Config.Mirror = &jetstream.StreamSource{Name: "KV_up"} })
		}},
		{name: "other subjects", o: Options{Recreate: true}, mismatch: true, contains: "not a plain KV bucket", info: func(r StreamAPI) infoFn {
			return altered(r, func(i *jetstream.StreamInfo) { i.Config.Subjects = []string{"orders.>"} })
		}},
		{name: "create failed for another reason", o: Options{Recreate: true}, mismatch: true, contains: "failed for another reason",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			info: func(r StreamAPI) infoFn {
				return func(_ context.Context, name string) (*jetstream.StreamInfo, error) {
					return r.StreamInfo(context.Background(), name)
				}
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			js := startJetStream(t)
			fileBucket(t, js, "locks", 1)
			real := StreamAPI{JS: js}
			deletes := 0
			fs := fakeStreams{StreamAPI: real, deletes: &deletes}
			if tt.info != nil {
				fs.info = tt.info(real)
			}
			ctx, cancel := context.Background(), func() {}
			if tt.ctx != nil {
				ctx, cancel = tt.ctx()
			}
			defer cancel()
			_, err := Open(ctx, kvSpec(js, fs, "locks", tt.o, nil))
			if err == nil {
				t.Fatal("Open succeeded")
			}
			if errors.Is(err, errMismatch) != tt.mismatch {
				t.Fatalf("err = %v, mismatch=%v", err, tt.mismatch)
			}
			if tt.contains != "" && !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("err = %v, want %q", err, tt.contains)
			}
			if deletes != 0 {
				t.Fatalf("DeleteStream called %d times", deletes)
			}
			if got, msgs := storageOf(t, js, "KV_locks"); got != jetstream.FileStorage || msgs != 1 {
				t.Fatalf("storage=%v msgs=%d, want the original", got, msgs)
			}
			if b := backups(t, js, "locks"); len(b) != 0 {
				t.Fatalf("backups = %v", b)
			}
		})
	}
}

func TestOpen_Concurrency(t *testing.T) {
	t.Run("repaired before the lookup", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		real := StreamAPI{JS: js}
		deletes := 0
		info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			if err := real.DeleteStream(ctx, name); err != nil {
				return nil, err
			}
			return nil, jetstream.ErrStreamNotFound
		}
		if _, err := Open(context.Background(), kvSpec(js, fakeStreams{StreamAPI: real, info: info, deletes: &deletes}, "locks", Options{Recreate: true}, nil)); err != nil {
			t.Fatalf("Open: %v", err)
		}
		if deletes != 0 {
			t.Fatalf("deletes = %d", deletes)
		}
	})
	t.Run("repaired during the copy", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		real := StreamAPI{JS: js}
		deletes, calls := 0, 0
		info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			if calls++; calls == 2 {
				if err := real.DeleteStream(ctx, name); err != nil {
					return nil, err
				}
				if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "locks", Storage: jetstream.MemoryStorage}); err != nil {
					return nil, err
				}
			}
			return real.StreamInfo(ctx, name)
		}
		if _, err := Open(context.Background(), kvSpec(js, fakeStreams{StreamAPI: real, info: info, deletes: &deletes}, "locks", Options{Recreate: true}, nil)); err != nil {
			t.Fatalf("Open: %v", err)
		}
		if deletes != 0 {
			t.Fatalf("deletes = %d", deletes)
		}
		if b := backups(t, js, "locks"); len(b) != 0 {
			t.Fatalf("redundant backup left: %v", b)
		}
	})
	t.Run("a delete lost to another instance", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		real := StreamAPI{JS: js}
		fs := fakeStreams{StreamAPI: real, deleteErr: jetstream.ErrStreamNotFound}
		// The real stream stays: the recreate then fails; the delete error
		// itself is not reported.
		_, err := Open(context.Background(), kvSpec(js, fs, "locks", Options{Recreate: true, SkipBackup: true}, nil))
		if err == nil || !strings.Contains(err.Error(), "was deleted, but recreating") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a failed delete", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		fs := fakeStreams{StreamAPI: StreamAPI{JS: js}, deleteErr: errors.New("denied")}
		_, err := Open(context.Background(), kvSpec(js, fs, "locks", Options{Recreate: true, SkipBackup: true}, nil))
		if err == nil || !strings.Contains(err.Error(), "failed to delete stream KV_locks") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestOpen_BackupFailures(t *testing.T) {
	t.Run("expired messages do not block the copy", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 2)
		real := StreamAPI{JS: js}
		info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			i, err := real.StreamInfo(ctx, name)
			if err == nil {
				i.State.LastSeq += 10
				i.State.Msgs += 10
			}
			return i, err
		}
		if _, err := Open(context.Background(), kvSpec(js, fakeStreams{StreamAPI: real, info: info}, "locks", Options{Recreate: true}, nil)); err != nil {
			t.Fatalf("Open: %v", err)
		}
	})
	t.Run("a copy that cannot be confirmed deletes nothing", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		real := StreamAPI{JS: js}
		deletes, calls := 0, 0
		info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			if calls++; calls == 1 {
				return real.StreamInfo(ctx, name)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		spec := kvSpec(js, fakeStreams{StreamAPI: real, info: info, deletes: &deletes}, "locks", Options{Recreate: true}, nil)
		spec.CopyTimeout = 300 * time.Millisecond
		_, err := Open(context.Background(), spec)
		if !errors.Is(err, errMismatch) || !strings.Contains(err.Error(), "backup failed") {
			t.Fatalf("err = %v", err)
		}
		if deletes != 0 {
			t.Fatalf("deletes = %d", deletes)
		}
		if b := backups(t, js, "locks"); len(b) != 0 {
			t.Fatalf("incomplete backup left: %v", b)
		}
	})
	t.Run("a failing backup create deletes nothing", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 1)
		deletes, backupDeletes := 0, 0
		spec := kvSpec(js, fakeStreams{StreamAPI: StreamAPI{JS: js}, deletes: &deletes}, "locks", Options{Recreate: true}, nil)
		spec.CreateBackup = func(context.Context, string, *jetstream.StreamInfo, time.Duration) error {
			return errors.New("no space")
		}
		spec.DeleteBackup = func(context.Context, string) error { backupDeletes++; return nil }
		_, err := Open(context.Background(), spec)
		if !errors.Is(err, errMismatch) || !strings.Contains(err.Error(), "no space") {
			t.Fatalf("err = %v", err)
		}
		if deletes != 0 || backupDeletes != 1 {
			t.Fatalf("deletes = %d, backup deletes = %d; want 0 and 1", deletes, backupDeletes)
		}
	})
}

func TestSourceSequence(t *testing.T) {
	for _, tt := range []struct {
		header string
		want   uint64
		ok     bool
	}{
		{"KV_locks 12 $KV.locks.> $KV.b.>", 12, true},
		{"OBJ_files:ab 7", 7, true},
		{"", 0, false},
		{"KV_locks", 0, false},
		{"KV_locks x", 0, false},
	} {
		if got, ok := SourceSequence(tt.header); got != tt.want || ok != tt.ok {
			t.Errorf("SourceSequence(%q) = %d, %v", tt.header, got, ok)
		}
	}
}

func TestIsPlainStream(t *testing.T) {
	subjects := []string{"$O.f.C.>", "$O.f.M.>"}
	cfg := func(subj []string) *jetstream.StreamInfo {
		return &jetstream.StreamInfo{Config: jetstream.StreamConfig{Subjects: subj}}
	}
	if !isPlainStream(cfg([]string{"$O.f.M.>", "$O.f.C.>"}), subjects) {
		t.Error("reordered subjects rejected")
	}
	for _, bad := range [][]string{{"$O.f.C.>"}, {"$O.f.C.>", "$O.g.M.>"}, {"$O.f.C.>", "$O.f.C.>"}} {
		if isPlainStream(cfg(bad), subjects) {
			t.Errorf("%v accepted", bad)
		}
	}
	sourced := cfg(subjects)
	sourced.Config.Sources = []*jetstream.StreamSource{{Name: "x"}}
	if isPlainStream(sourced, subjects) {
		t.Error("sourced stream accepted")
	}
}

func TestValidateOptions(t *testing.T) {
	min := 10 * time.Minute
	long := strings.Repeat("a", MaxStreamNameLen-len("OBJ_")-BackupNameSuffixLen+1)
	for _, tt := range []struct {
		name string
		o    Options
		want string
	}{
		{"a", Options{SkipBackup: true}, "require RecreateOnStorageMismatch"},
		{"a", Options{BackupTTL: time.Hour}, "require RecreateOnStorageMismatch"},
		{"a", Options{Recreate: true, SkipBackup: true, BackupTTL: time.Hour}, "no effect"},
		{"a", Options{Recreate: true, BackupTTL: time.Minute}, "at least"},
		{long, Options{Recreate: true}, "name too long"},
		{long, Options{Recreate: true, SkipBackup: true}, ""},
		{"a", Options{Recreate: true, BackupTTL: -1}, ""},
		{"a", Options{}, ""},
	} {
		err := ValidateOptions(tt.name, "OBJ_", tt.o, min)
		if (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
			t.Errorf("ValidateOptions(%d chars, %+v) = %v, want %q", len(tt.name), tt.o, err, tt.want)
		}
	}
}

func TestBackupName(t *testing.T) {
	a, err := BackupName("locks")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := BackupName("locks")
	if a == b || len(a) != len("locks")+BackupNameSuffixLen || !strings.HasPrefix(a, "locks-backup-") {
		t.Fatalf("names %q, %q", a, b)
	}
}

func TestOpen_SpecCheck(t *testing.T) {
	if _, err := Open(context.Background(), Spec[jetstream.KeyValue]{}); err == nil || !strings.Contains(err.Error(), "Spec needs") {
		t.Fatalf("err = %v", err)
	}
	js := startJetStream(t)
	spec := kvSpec(js, StreamAPI{JS: js}, "locks", Options{}, nil)
	spec.ErrMismatch = nil
	if _, err := Open(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "ErrMismatch") {
		t.Fatalf("err = %v", err)
	}
}

func TestOpen_AfterCopy(t *testing.T) {
	t.Run("runs once the backup is detached", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 2)
		spec := kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, nil)
		var sources int
		spec.AfterCopy = func(ctx context.Context, backup string) error {
			s, err := js.Stream(ctx, "KV_"+backup)
			if err != nil {
				return err
			}
			sources = len(s.CachedInfo().Config.Sources)
			return nil
		}
		if _, err := Open(context.Background(), spec); err != nil {
			t.Fatalf("Open: %v", err)
		}
		if sources != 0 {
			t.Fatalf("AfterCopy saw %d sources, want a detached backup", sources)
		}
	})
	t.Run("a failure deletes nothing", func(t *testing.T) {
		js := startJetStream(t)
		fileBucket(t, js, "locks", 2)
		deletes := 0
		spec := kvSpec(js, fakeStreams{StreamAPI: StreamAPI{JS: js}, deletes: &deletes}, "locks", Options{Recreate: true}, nil)
		spec.AfterCopy = func(context.Context, string) error { return errors.New("rewrite failed") }
		_, err := Open(context.Background(), spec)
		if !errors.Is(err, errMismatch) || !strings.Contains(err.Error(), "rewrite failed") {
			t.Fatalf("err = %v", err)
		}
		if deletes != 0 {
			t.Fatalf("deletes = %d", deletes)
		}
		if b := backups(t, js, "locks"); len(b) != 0 {
			t.Fatalf("backup left: %v", b)
		}
	})
}

func TestIsBackupName(t *testing.T) {
	made, err := BackupName("locks")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		bucket, name string
		want         bool
	}{
		{"locks", made, true},
		{"locks", "locks-backup-20260101T000000Z-0123456789abcdef", true},
		{"locks", "locks-backup-20260101T000000Z-0123456789ABCDEF", false},
		{"locks", "locks-backup-20260101T000000Z-0123456789abcde", false},
		{"locks", "locks-backup-20260101T000000Z_0123456789abcdef", false},
		{"locks", "locks-backup-20261301T000000Z-0123456789abcdef", false},
		{"locks", "locks-backup-mine", false},
		{"locks", "locks-backup-", false},
		{"locks", "locks", false},
		{"locks", "other-backup-20260101T000000Z-0123456789abcdef", false},
		{"lock", "locks-backup-20260101T000000Z-0123456789abcdef", false},
	}
	for _, tt := range tests {
		if got := IsBackupName(tt.bucket, tt.name); got != tt.want {
			t.Errorf("IsBackupName(%q, %q) = %v, want %v", tt.bucket, tt.name, got, tt.want)
		}
	}
}

func TestSourceStream(t *testing.T) {
	for header, want := range map[string]string{
		"KV_x 2 $KV.x.> $KV.b.> $KV.x.k": "KV_x",
		"KV_x":                           "KV_x",
		"":                               "",
		"   ":                            "",
	} {
		if got := SourceStream(header); got != want {
			t.Errorf("SourceStream(%q) = %q, want %q", header, got, want)
		}
	}
}

// TestOpen_BackupOfAnotherStream: a backup that copies another stream never
// counts as a copy of the bucket, so the bucket is not deleted.
func TestOpen_BackupOfAnotherStream(t *testing.T) {
	js := startJetStream(t)
	fileBucket(t, js, "locks", 2)
	fileBucket(t, js, "other", 5)
	deletes := 0
	spec := kvSpec(js, fakeStreams{StreamAPI: StreamAPI{JS: js}, deletes: &deletes}, "locks", Options{Recreate: true}, nil)
	spec.CreateBackup = func(ctx context.Context, backup string, _ *jetstream.StreamInfo, ttl time.Duration) error {
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: backup, TTL: ttl, Storage: jetstream.FileStorage,
			Sources: []*jetstream.StreamSource{{Name: "other"}}})
		return err
	}
	_, err := Open(context.Background(), spec)
	if !errors.Is(err, errMismatch) || !strings.Contains(err.Error(), "copies stream KV_other, not only stream KV_locks") {
		t.Fatalf("err = %v", err)
	}
	if deletes != 0 {
		t.Fatalf("the bucket's stream was deleted %d time(s)", deletes)
	}
	if got, msgs := storageOf(t, js, "KV_locks"); got != jetstream.FileStorage || msgs != 2 {
		t.Fatalf("bucket storage=%v msgs=%d, want it untouched", got, msgs)
	}
	if left := backups(t, js, "locks"); len(left) != 0 {
		t.Fatalf("backups left behind: %v", left)
	}
}

// TestOpen_DetachesLeftoverBackups: after a recreate, backups an interrupted
// recreate left copying the bucket are detached and kept; buckets that are
// not backups, backups of other buckets and the backup just made are left as
// they are.
func TestOpen_DetachesLeftoverBackups(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	fileBucket(t, js, "locks", 2)
	fileBucket(t, js, "other", 1)
	sourcing := func(bucket, source string) {
		t.Helper()
		if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, Storage: jetstream.FileStorage,
			Sources: []*jetstream.StreamSource{{Name: source}}}); err != nil {
			t.Fatal(err)
		}
	}
	const (
		leftover      = "locks-backup-20260101T000000Z-0123456789abcdef"
		otherLeftover = "locks-backup-20260101T000000Z-fedcba9876543210" // a backup name, but of bucket "other"
		detached      = "locks-backup-20250101T000000Z-0000000000000000" // an old, complete backup
	)
	sourcing(leftover, "locks")
	sourcing(otherLeftover, "other")
	sourcing("locks-copy", "locks")
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: detached, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	if _, err := Open(ctx, kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, &logs)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sources := func(bucket string) []*jetstream.StreamSource {
		t.Helper()
		s, err := js.Stream(ctx, "KV_"+bucket)
		if err != nil {
			t.Fatalf("stream of %s: %v", bucket, err)
		}
		return s.CachedInfo().Config.Sources
	}
	if got := sources(leftover); len(got) != 0 {
		t.Errorf("the leftover backup still copies %d stream(s)", len(got))
	}
	for _, kept := range []string{otherLeftover, "locks-copy"} {
		if got := sources(kept); len(got) != 1 {
			t.Errorf("bucket %s has %d sources, want its own kept", kept, len(got))
		}
	}
	if n := strings.Count(logs.String(), "Detached a leftover KV bucket backup"); n != 1 {
		t.Errorf("%d leftovers reported as detached, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "backup_bucket="+leftover) {
		t.Errorf("the log does not name the leftover:\n%s", logs.String())
	}
	// Nothing was deleted: the four other buckets and this recreate's backup.
	if got := backups(t, js, "locks"); len(got) != 4 {
		t.Errorf("backup buckets = %v, want the three that existed and the new one", got)
	}
}

// TestDetachLeftoverBackups_Failures: the search is best effort and never
// fails the bucket that was just recreated.
func TestDetachLeftoverBackups_Failures(t *testing.T) {
	t.Run("the listing fails", func(t *testing.T) {
		js := startJetStream(t)
		var logs bytes.Buffer
		spec := kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, &logs)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		detachLeftoverBackups(ctx, spec, "")
		if !strings.Contains(logs.String(), "Failed to look for leftover KV bucket backups") {
			t.Fatalf("log = %s", logs.String())
		}
	})
	t.Run("more backups than are inspected", func(t *testing.T) {
		js := startJetStream(t)
		ctx := context.Background()
		for i := 0; i < maxLeftoverBackups+2; i++ {
			name := fmt.Sprintf("locks-backup-20260101T000000Z-%016x", i)
			if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, Storage: jetstream.MemoryStorage}); err != nil {
				t.Fatal(err)
			}
		}
		var logs bytes.Buffer
		detachLeftoverBackups(ctx, kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, &logs), "")
		if !strings.Contains(logs.String(), "Too many KV bucket backups to inspect them all") {
			t.Fatalf("log = %s", logs.String())
		}
	})
}

// TestOpen_DetachesLeftoversWhenTheCreateFails: the leftovers are detached
// once the stream is deleted, also when the create after it fails; the next
// Start finds no mismatch and would never detach them.
func TestOpen_DetachesLeftoversWhenTheCreateFails(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	fileBucket(t, js, "locks", 2)
	const leftover = "locks-backup-20260101T000000Z-0123456789abcdef"
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: leftover, Storage: jetstream.FileStorage,
		Sources: []*jetstream.StreamSource{{Name: "locks"}}}); err != nil {
		t.Fatal(err)
	}
	finished := ""
	spec := kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, nil)
	create, creates := spec.Create, 0
	spec.Create = func(ctx context.Context) (jetstream.KeyValue, error) {
		if creates++; creates == 2 { // the create after the delete
			return nil, errors.New("no suitable peers")
		}
		return create(ctx)
	}
	spec.AfterCopy = func(_ context.Context, backup string) error {
		if backup == leftover {
			finished = backup
		}
		return nil
	}
	if _, err := Open(ctx, spec); err == nil || !strings.Contains(err.Error(), "no suitable peers") {
		t.Fatalf("Open = %v, want the failed create", err)
	}
	stream, err := js.Stream(ctx, "KV_"+leftover)
	if err != nil {
		t.Fatal(err)
	}
	if got := stream.CachedInfo().Config.Sources; len(got) != 0 {
		t.Fatalf("the leftover backup still copies %d stream(s) after the failed create", len(got))
	}
	if finished != leftover {
		t.Fatal("the detached leftover was not finished like a backup (AfterCopy)")
	}
}

// TestCompleteBackup_DetachedByAnotherInstance: an instance whose backup was
// detached by another one that finished the repair first takes the bucket as
// repaired instead of failing Start with "has no source".
func TestCompleteBackup_DetachedByAnotherInstance(t *testing.T) {
	js := startJetStream(t)
	ctx := context.Background()
	fileBucket(t, js, "locks", 2)
	stream, err := js.Stream(ctx, "KV_locks")
	if err != nil {
		t.Fatal(err)
	}
	info := stream.CachedInfo()
	// The backup of this instance, already detached by the other one.
	const backup = "locks-backup-20260101T000000Z-0123456789abcdef"
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: backup, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}

	t.Run("the bucket is still as it was: an error", func(t *testing.T) {
		spec := kvSpec(js, StreamAPI{JS: js}, "locks", Options{Recreate: true}, nil)
		repaired, err := completeBackup(ctx, spec, backup, info)
		if repaired || err == nil || !strings.Contains(err.Error(), "has no source") {
			t.Fatalf("completeBackup = %v, %v", repaired, err)
		}
	})
	t.Run("the other instance repaired the bucket", func(t *testing.T) {
		// The first check of a round still sees the old stream; the check
		// made when the source is found gone sees the repair.
		lookups := 0
		streams := fakeStreams{StreamAPI: StreamAPI{JS: js}, info: func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
			if lookups++; lookups == 1 {
				return info, nil
			}
			return nil, jetstream.ErrStreamNotFound
		}}
		spec := kvSpec(js, streams, "locks", Options{Recreate: true}, nil)
		repaired, err := completeBackup(ctx, spec, backup, info)
		if !repaired || err != nil {
			t.Fatalf("completeBackup = %v, %v, want repaired", repaired, err)
		}
	})
}
