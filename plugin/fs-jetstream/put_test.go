package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ChunkedReader hands out chunks of 128 KiB (the chunk size of an object) and,
// after the given number of them, calls Cancel or returns Err. It never ends
// by itself.
type ChunkedReader struct {
	After  int
	Cancel context.CancelFunc
	Err    error
	reads  int
}

func (r *ChunkedReader) Read(p []byte) (int, error) {
	if r.reads >= r.After && r.After > 0 {
		if r.Err != nil {
			return 0, r.Err
		}
		if r.Cancel != nil {
			r.Cancel()
		}
	}
	r.reads++
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// BucketMessages returns the number of messages and bytes the stream of an
// object store bucket holds: chunks and metadata, referred to or not.
func BucketMessages(t *testing.T, js jetstream.JetStream, bucket string) (uint64, uint64) {
	t.Helper()
	stream, err := js.Stream(context.Background(), "OBJ_"+bucket)
	if err != nil {
		t.Fatalf("stream of bucket %s: %v", bucket, err)
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatalf("stream info of bucket %s: %v", bucket, err)
	}
	return info.State.Msgs, info.State.Bytes
}

// NewPutTestBackend returns a backend on a new bucket.
func NewPutTestBackend(t *testing.T, cfg BucketConfig) (*JetStreamBackend, jetstream.JetStream) {
	t.Helper()
	_, js := setupTestNATSWithJetStream(t)
	store := createTestObjectStore(t, js, cfg)
	return NewJetStreamBackend(cfg.Name, store, cfg.TTL, nil), js
}

// An upload that is given up must leave no chunks in the bucket: they are
// invisible to List, Delete and Reset and count towards the bucket's limits.
func TestJetStreamBackend_AbandonedUploadLeavesNoChunks(t *testing.T) {
	readErr := errors.New("connection reset")
	tests := []struct {
		name    string
		put     func(b *JetStreamBackend) error
		wantErr error
	}{
		{
			name: "context cancelled during the upload",
			put: func(b *JetStreamBackend) error {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				_, err := b.PutReaderWithContext(ctx, "report", &ChunkedReader{After: 20, Cancel: cancel}, 0)
				return err
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline passes during the upload",
			put: func(b *JetStreamBackend) error {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				_, err := b.PutReaderWithContext(ctx, "report", io.LimitReader(&ChunkedReader{}, 512<<20), 0)
				return err
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "reader fails",
			put: func(b *JetStreamBackend) error {
				_, err := b.PutReaderWithContext(context.Background(), "report", &ChunkedReader{After: 20, Err: readErr}, 0)
				return err
			},
			wantErr: readErr,
		},
		{
			name: "context already ended",
			put: func(b *JetStreamBackend) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return b.SetWithContext(ctx, "report", bytes.Repeat([]byte("y"), 1<<20), 0)
			},
			wantErr: context.Canceled,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := fmt.Sprintf("abandoned-%d", i)
			b, js := NewPutTestBackend(t, BucketConfig{Name: bucket})

			err := tt.put(b)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Put error = %v, want %v", err, tt.wantErr)
			}
			if msgs, size := BucketMessages(t, js, bucket); msgs != 0 {
				t.Errorf("bucket holds %d messages (%d bytes) after the abandoned upload, want none", msgs, size)
			}
			if data, err := b.Get("report"); err != nil || data != nil {
				t.Errorf("Get after the abandoned upload = %d bytes, %v; want no object", len(data), err)
			}
		})
	}
}

// Abandoned uploads used to fill a bucket with MaxBytes until every Put was
// rejected, and Reset did not help.
func TestJetStreamBackend_AbandonedUploadsDoNotExhaustBucket(t *testing.T) {
	b, js := NewPutTestBackend(t, BucketConfig{Name: "quota", MaxBytes: 8 << 20})
	for range 10 {
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := b.PutReaderWithContext(ctx, "avatar", &ChunkedReader{After: 8, Cancel: cancel}, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned upload error = %v, want context.Canceled", err)
		}
		cancel()
	}
	if msgs, size := BucketMessages(t, js, "quota"); msgs != 0 {
		t.Fatalf("bucket holds %d messages (%d bytes) after 10 abandoned uploads, want none", msgs, size)
	}
	if err := b.Set("legit", bytes.Repeat([]byte("y"), 1<<20), 0); err != nil {
		t.Errorf("Put after the abandoned uploads failed: %v", err)
	}
}

// An upload that replaces an object removes the chunks of the version it
// found. Concurrent uploads of one name all found the same one, so all but
// one left their chunks behind.
func TestJetStreamBackend_ConcurrentUploadsOfOneNameLeaveOneObject(t *testing.T) {
	b, js := NewPutTestBackend(t, BucketConfig{Name: "concurrent"})
	data := bytes.Repeat([]byte("z"), 1<<20)

	if err := b.Set("same", data, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	oneObject, _ := BucketMessages(t, js, "concurrent")

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- b.Set("same", data, 0)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Put: %v", err)
		}
	}

	if msgs, size := BucketMessages(t, js, "concurrent"); msgs != oneObject {
		t.Errorf("bucket holds %d messages (%d bytes) after concurrent uploads of one name, want %d (one object)", msgs, size, oneObject)
	}
	got, err := b.Get("same")
	if err != nil || !bytes.Equal(got, data) {
		t.Errorf("Get = %d bytes, %v; want the uploaded object", len(got), err)
	}
}

// An upload whose data was read completely is finished although its context
// ended meanwhile: giving it up then would leave its chunks.
func TestJetStreamBackend_UploadFinishesWhenContextEndsAfterLastRead(t *testing.T) {
	b, _ := NewPutTestBackend(t, BucketConfig{Name: "finished"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := bytes.Repeat([]byte("d"), 300<<10)

	info, err := b.PutReaderWithContext(ctx, "doc", &CancelAtEOFReader{Reader: bytes.NewReader(data), Cancel: cancel}, 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Errorf("stored %d bytes, want %d", info.Size, len(data))
	}
	got, err := b.Get("doc")
	if err != nil || !bytes.Equal(got, data) {
		t.Errorf("Get = %d bytes, %v; want the uploaded object", len(got), err)
	}
}

// CancelAtEOFReader calls Cancel when it returns the end of its data.
type CancelAtEOFReader struct {
	Reader io.Reader
	Cancel context.CancelFunc
}

func (r *CancelAtEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		r.Cancel()
	}
	return n, err
}

func TestKeyedMutex(t *testing.T) {
	var locks KeyedMutex
	unlockA, err := locks.Lock(context.Background(), "a")
	if err != nil {
		t.Fatalf("Lock(a): %v", err)
	}

	// Another key is not blocked.
	unlockB, err := locks.Lock(context.Background(), "b")
	if err != nil {
		t.Fatalf("Lock(b): %v", err)
	}
	unlockB()

	// The same key waits, and gives up with its context.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := locks.Lock(ctx, "a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock(a) while held = %v, want context.DeadlineExceeded", err)
	}

	// A waiter gets the lock when it is released.
	got := make(chan error, 1)
	go func() {
		unlock, err := locks.Lock(context.Background(), "a")
		if err == nil {
			unlock()
		}
		got <- err
	}()
	unlockA()
	select {
	case err := <-got:
		if err != nil {
			t.Errorf("waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not get the lock")
	}

	// Nothing is kept for keys nobody holds or waits for.
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.locks) != 0 {
		t.Errorf("%d locks kept after all were released", len(locks.locks))
	}
}
