package kvjetstream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// createFileBucket creates bucket with file storage and one key, the state
// another client's defaults leave behind.
func createFileBucket(t *testing.T, js jetstream.JetStream, bucket string) {
	t.Helper()
	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: bucket, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func streamStorage(t *testing.T, js jetstream.JetStream, bucket string) (jetstream.StorageType, uint64) {
	t.Helper()
	s, err := js.Stream(context.Background(), kvStreamPrefix+bucket)
	if err != nil {
		t.Fatal(err)
	}
	info := s.CachedInfo()
	return info.Config.Storage, info.State.Msgs
}

func startPlugin(t *testing.T, conn *nats.Conn, cfg BucketConfig, opts ...Option) (*PluginModule, error) {
	t.Helper()
	m, err := New(Config{Buckets: []BucketConfig{cfg}}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	m.SetEventBus(newRealEventBus(conn))
	return m, m.Start(context.Background())
}

func TestStart_StorageMismatch(t *testing.T) {
	t.Run("recreated when opted in", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileBucket(t, js, "locks")
		var logs bytes.Buffer
		m, err := startPlugin(t, conn, BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true},
			WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if got, msgs := streamStorage(t, js, "locks"); got != jetstream.MemoryStorage || msgs != 0 {
			t.Fatalf("stream storage=%v msgs=%d, want memory and empty", got, msgs)
		}
		if m.Bucket("locks") == nil {
			t.Fatal("bucket not available after recreation")
		}
		line := logs.String()
		for _, want := range []string{"level=WARN", "bucket=locks", "existing_storage=File", "configured_storage=Memory", "messages=1"} {
			if !strings.Contains(line, want) {
				t.Errorf("log %q lacks %q", line, want)
			}
		}
	})

	t.Run("Start fails without the option and the bucket is kept", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileBucket(t, js, "sessions")
		_, err := startPlugin(t, conn, BucketConfig{Name: "sessions", Storage: MemoryStorage})
		if !errors.Is(err, ErrStorageMismatch) {
			t.Fatalf("err = %v, want ErrStorageMismatch", err)
		}
		for _, want := range []string{`bucket "sessions"`, "File storage", "Memory storage", "KV_sessions", "RecreateOnStorageMismatch"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		if got, msgs := streamStorage(t, js, "sessions"); got != jetstream.FileStorage || msgs != 1 {
			t.Fatalf("stream storage=%v msgs=%d, want the original file stream with its key", got, msgs)
		}
	})

	t.Run("a matching bucket is left alone", func(t *testing.T) {
		conn, js := setupTestNATSWithJetStream(t)
		createFileBucket(t, js, "data")
		m, err := startPlugin(t, conn, BucketConfig{Name: "data", Storage: FileStorage, RecreateOnStorageMismatch: true})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		value, err := m.Bucket("data").GetWithContext(context.Background(), "k")
		if err != nil || string(value) != "v" {
			t.Fatalf("existing key = %q, %v", value, err)
		}
	})
}

// fakeStreams wraps the real API to inject lookup and delete outcomes and to
// count deletes.
type fakeStreams struct {
	streamAPI
	info        func(ctx context.Context, name string) (*jetstream.StreamInfo, error) // nil: real lookup
	deleteErr   error
	deleteFirst bool // delete for real, then report deleteErr (a lost race)
	deletes     *int
}

func (f fakeStreams) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	if f.info != nil {
		return f.info(ctx, name)
	}
	return f.streamAPI.StreamInfo(ctx, name)
}

func (f fakeStreams) DeleteStream(ctx context.Context, name string) error {
	if f.deletes != nil {
		*f.deletes++
	}
	if f.deleteFirst {
		if err := f.streamAPI.DeleteStream(ctx, name); err != nil {
			return err
		}
	}
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if !f.deleteFirst {
		return f.streamAPI.DeleteStream(ctx, name)
	}
	return nil
}

func newMismatchModule(t *testing.T, cfg BucketConfig) (*PluginModule, jetstream.JetStream) {
	t.Helper()
	_, js := setupTestNATSWithJetStream(t)
	createFileBucket(t, js, cfg.Name)
	m, err := New(Config{Buckets: []BucketConfig{cfg}}, WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	m.js = js
	return m, js
}

func TestOpenBucket_DeleteOutcomes(t *testing.T) {
	cfg := BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true}

	t.Run("a stream deleted by another instance first is a success", func(t *testing.T) {
		m, js := newMismatchModule(t, cfg)
		_, err := m.openBucket(context.Background(), fakeStreams{streamAPI: streamAPI{js: js}, deleteErr: jetstream.ErrStreamNotFound, deleteFirst: true}, cfg)
		if err != nil {
			t.Fatalf("openBucket: %v", err)
		}
		if got, _ := streamStorage(t, js, cfg.Name); got != jetstream.MemoryStorage {
			t.Fatalf("storage = %v, want memory", got)
		}
	})
	t.Run("a failed delete fails with context", func(t *testing.T) {
		m, js := newMismatchModule(t, cfg)
		_, err := m.openBucket(context.Background(), fakeStreams{streamAPI: streamAPI{js: js}, deleteErr: errors.New("permission denied")}, cfg)
		if err == nil || !strings.Contains(err.Error(), "failed to delete stream KV_locks") || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("err = %v", err)
		}
	})
}

// TestOpenBucket_NeverDeletes covers every path after a failed create that
// must not delete anything.
func TestOpenBucket_NeverDeletes(t *testing.T) {
	optIn := BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true}
	tests := []struct {
		name    string
		cfg     BucketConfig
		info    func(real streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error)
		wantErr error // nil: the original create error is returned
	}{
		{
			name:    "mismatch without the option",
			cfg:     BucketConfig{Name: "locks", Storage: MemoryStorage},
			info:    func(streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) { return nil },
			wantErr: ErrStorageMismatch,
		},
		{
			name: "lookup fails",
			cfg:  optIn,
			info: func(streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) {
				return func(context.Context, string) (*jetstream.StreamInfo, error) { return nil, errors.New("timeout") }
			},
		},
		{
			name: "lookup returns no info",
			cfg:  optIn,
			info: func(streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) {
				return func(context.Context, string) (*jetstream.StreamInfo, error) { return nil, nil }
			},
		},
		{
			name: "storage matches (the create failed for another reason)",
			cfg:  optIn,
			info: func(streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) {
				return func(context.Context, string) (*jetstream.StreamInfo, error) {
					return &jetstream.StreamInfo{Config: jetstream.StreamConfig{Storage: jetstream.MemoryStorage}}, nil
				}
			},
		},
		{
			name: "not a plain KV stream (a mirror)",
			cfg:  optIn,
			info: func(real streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) {
				return func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
					info, err := real.StreamInfo(ctx, name)
					if err != nil {
						return nil, err
					}
					info.Config.Mirror = &jetstream.StreamSource{Name: "KV_upstream"}
					return info, nil
				}
			},
			wantErr: ErrStorageMismatch,
		},
		{
			name: "not a plain KV stream (other subjects)",
			cfg:  optIn,
			info: func(real streamAPI) func(context.Context, string) (*jetstream.StreamInfo, error) {
				return func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
					info, err := real.StreamInfo(ctx, name)
					if err != nil {
						return nil, err
					}
					info.Config.Subjects = []string{"orders.>"}
					return info, nil
				}
			},
			wantErr: ErrStorageMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, js := newMismatchModule(t, tt.cfg)
			real := streamAPI{js: js}
			deletes := 0
			_, err := m.openBucket(context.Background(), fakeStreams{streamAPI: real, info: tt.info(real), deletes: &deletes}, tt.cfg)
			if err == nil {
				t.Fatal("openBucket succeeded")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && errors.Is(err, ErrStorageMismatch) {
				t.Fatalf("err = %v, want the original create error", err)
			}
			if deletes != 0 {
				t.Fatalf("DeleteStream called %d times", deletes)
			}
			if got, msgs := streamStorage(t, js, tt.cfg.Name); got != jetstream.FileStorage || msgs != 1 {
				t.Fatalf("stream storage=%v msgs=%d, want the original file stream with its key", got, msgs)
			}
		})
	}
}

// TestOpenBucket_RepairedByAnotherInstance: when another instance recreates
// the bucket between this instance's failed create and its lookup, the
// bucket is opened, not reported as failed, and nothing is deleted.
func TestOpenBucket_RepairedByAnotherInstance(t *testing.T) {
	for _, between := range []string{"after the other's create", "between the other's delete and create"} {
		t.Run(between, func(t *testing.T) {
			cfg := BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true}
			m, js := newMismatchModule(t, cfg)
			real := streamAPI{js: js}
			deletes := 0
			info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
				if err := real.DeleteStream(ctx, name); err != nil {
					return nil, err
				}
				if between == "between the other's delete and create" {
					return nil, jetstream.ErrStreamNotFound
				}
				if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: cfg.Name, Storage: jetstream.MemoryStorage}); err != nil {
					return nil, err
				}
				return real.StreamInfo(ctx, name)
			}
			if _, err := m.openBucket(context.Background(), fakeStreams{streamAPI: real, info: info, deletes: &deletes}, cfg); err != nil {
				t.Fatalf("openBucket: %v", err)
			}
			if deletes != 0 {
				t.Fatalf("DeleteStream called %d times", deletes)
			}
			if got, _ := streamStorage(t, js, cfg.Name); got != jetstream.MemoryStorage {
				t.Fatalf("storage = %v, want memory", got)
			}
		})
	}
}

// TestStart_DefaultLoggerResolvedAtStart: without WithLogger, the plugin logs
// to the slog.Default in effect at Start, not the one at New. It replaces the
// global default, so it must not run in parallel with other tests.
func TestStart_DefaultLoggerResolvedAtStart(t *testing.T) {
	conn, js := setupTestNATSWithJetStream(t)
	createFileBucket(t, js, "presence")
	m, err := New(Config{Buckets: []BucketConfig{{Name: "presence", Storage: MemoryStorage, RecreateOnStorageMismatch: true}}})
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
	if !strings.Contains(logs.String(), "Recreating KV bucket") || !strings.Contains(logs.String(), "module=kv-jetstream") {
		t.Fatalf("the default logger set after New did not receive the warning: %q", logs.String())
	}
}

// TestOpenBucket_OtherCreateFailureNeverDeletes: a create that failed for a
// reason other than the server's invalid configuration rejection is not
// fixed by deleting the stream, so nothing is deleted even when the storage
// types differ and the bucket opted in.
func TestOpenBucket_OtherCreateFailureNeverDeletes(t *testing.T) {
	cfg := BucketConfig{Name: "locks", Storage: MemoryStorage, RecreateOnStorageMismatch: true}
	m, js := newMismatchModule(t, cfg)
	real := streamAPI{js: js}
	deletes := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the create fails with context.Canceled, not an API error
	info := func(_ context.Context, name string) (*jetstream.StreamInfo, error) {
		return real.StreamInfo(context.Background(), name)
	}
	_, err := m.openBucket(ctx, fakeStreams{streamAPI: real, info: info, deletes: &deletes}, cfg)
	if !errors.Is(err, ErrStorageMismatch) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrStorageMismatch wrapping context.Canceled", err)
	}
	if deletes != 0 {
		t.Fatalf("DeleteStream called %d times", deletes)
	}
	if got, msgs := streamStorage(t, js, cfg.Name); got != jetstream.FileStorage || msgs != 1 {
		t.Fatalf("stream storage=%v msgs=%d, want the original file stream with its key", got, msgs)
	}
}

func TestWithLogger_NilRejected(t *testing.T) {
	if _, err := New(Config{Buckets: []BucketConfig{{Name: "cache"}}}, WithLogger(nil)); err == nil {
		t.Fatal("New with WithLogger(nil) succeeded")
	}
}
