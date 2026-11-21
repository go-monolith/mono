package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The object store client publishes an upload's chunks and its metadata
// without waiting. A bucket without room for a chunk rejected it and stored
// the small metadata message behind it, so an upload that was reported as
// failed had replaced the object (issue #87).

// FilledQuotaBackend returns a backend on a bucket of 256 KiB that holds the
// object "a" of 100 KiB and, when full is set, "b" of 100 KiB as well, and
// the content of "a".
func FilledQuotaBackend(t *testing.T, full bool) (*JetStreamBackend, jetstream.JetStream, []byte) {
	t.Helper()
	b, js := NewPutTestBackend(t, BucketConfig{Name: "quota", MaxBytes: 256 << 10})
	content := bytes.Repeat([]byte("a"), 100<<10)
	if err := b.Set("a", content, 0); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if full {
		if err := b.Set("b", bytes.Repeat([]byte("b"), 100<<10), 0); err != nil {
			t.Fatalf("Set b: %v", err)
		}
	}
	return b, js, content
}

// AssertObjectUntouched fails unless the object "a" still has its content and
// digest and the bucket holds the messages it held.
func AssertObjectUntouched(t *testing.T, b *JetStreamBackend, js jetstream.JetStream, content []byte, digest string, msgs, size uint64) {
	t.Helper()
	info, err := b.Stat("a")
	if err != nil || info.Digest != digest {
		t.Errorf("Stat after the rejected upload = %+v, %v; want digest %s", info, err, digest)
	}
	if data, err := b.Get("a"); err != nil || !bytes.Equal(data, content) {
		t.Errorf("Get after the rejected upload = %d bytes, %v; want the %d bytes stored before", len(data), err, len(content))
	}
	if gotMsgs, gotSize := BucketMessages(t, js, "quota"); gotMsgs != msgs || gotSize != size {
		t.Errorf("bucket holds %d messages (%d bytes) after the rejected upload, was %d (%d)", gotMsgs, gotSize, msgs, size)
	}
}

func TestUploadObject_RejectedOverwriteKeepsTheObject(t *testing.T) {
	tests := []struct {
		name string
		full bool
		size int
	}{
		// One chunk without room: the metadata used to be sent before the
		// rejection came.
		{"one chunk", true, 100 << 10},
		// Room for the first chunk of three: it is stored, the second is
		// rejected, and the first must be removed again.
		{"a tail that does not fit", false, 300 << 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, js, content := FilledQuotaBackend(t, tt.full)
			before, err := b.Stat("a")
			if err != nil {
				t.Fatal(err)
			}
			msgs, size := BucketMessages(t, js, "quota")

			begin := time.Now()
			err = b.Set("a", bytes.Repeat([]byte("n"), tt.size), 0)
			if waited := time.Since(begin); waited > UploadAckTimeout/2 {
				t.Errorf("the rejected upload took %s: it waited for an outcome it already had", waited)
			}
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("overwrite in a full bucket: err = %v, want the stream's rejection", err)
			}
			AssertObjectUntouched(t, b, js, content, before.Digest, msgs, size)
		})
	}
}

func TestUploadObject_RejectedNewNameLeavesNoObject(t *testing.T) {
	b, js, _ := FilledQuotaBackend(t, true)
	msgs, size := BucketMessages(t, js, "quota")
	if err := b.Set("c", bytes.Repeat([]byte("c"), 100<<10), 0); err == nil {
		t.Fatal("an upload into a full bucket succeeded")
	}
	list, err := b.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range list {
		if object.Name == "c" {
			t.Errorf("the rejected upload is listed: %+v", object)
		}
	}
	if info, err := b.Stat("c"); err == nil {
		t.Errorf("Stat of the rejected upload = %+v, want not found", info)
	}
	if gotMsgs, gotSize := BucketMessages(t, js, "quota"); gotMsgs != msgs || gotSize != size {
		t.Errorf("bucket holds %d messages (%d bytes), was %d (%d)", gotMsgs, gotSize, msgs, size)
	}
}

// RawMetadata returns the metadata message of an object as the stream holds
// it, with the object's version id replaced.
func RawMetadata(t *testing.T, js jetstream.JetStream, bucket, name, id string) (string, nats.Header) {
	t.Helper()
	stream, err := js.Stream(context.Background(), objStreamPrefix+bucket)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetLastMsgForSubject(context.Background(), MetaSubject(bucket, name))
	if err != nil {
		t.Fatalf("metadata message of %s: %v", name, err)
	}
	return strings.ReplaceAll(string(msg.Data), id, "<id>"), msg.Header
}

// The metadata an upload writes is what the object store client writes for
// the same object, so every client reads it the same way.
func TestUploadObject_MetadataMatchesTheObjectStoreClient(t *testing.T) {
	b, js := NewPutTestBackend(t, BucketConfig{Name: "format"})
	ctx := context.Background()
	store, err := js.ObjectStore(ctx, "format")
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("0123456789"), 40<<10) // 400 KiB, four chunks
	meta := jetstream.ObjectMeta{
		Name:        "reports/2026 q1.pdf",
		Description: "quarterly report",
		Headers:     nats.Header{"Content-Type": []string{"application/pdf"}},
		Metadata:    map[string]string{"owner": "finance"},
	}
	for _, tt := range []struct {
		name string
		meta jetstream.ObjectMeta
		data io.Reader
	}{
		{"content", meta, bytes.NewReader(content)},
		{"no reader", jetstream.ObjectMeta{Name: "empty"}, nil},
		{"own chunk size", jetstream.ObjectMeta{Name: "small-chunks", Opts: &jetstream.ObjectMetaOptions{ChunkSize: 1 << 10}}, bytes.NewReader(content[:10<<10])},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var seeker io.Seeker
			if s, ok := tt.data.(io.Seeker); ok {
				seeker = s
			}
			fromClient, err := store.Put(ctx, tt.meta, tt.data)
			if err != nil {
				t.Fatalf("object store Put: %v", err)
			}
			wantData, wantHeader := RawMetadata(t, js, "format", tt.meta.Name, fromClient.NUID)

			if seeker != nil {
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
			}
			uploaded, err := b.PutObject(ctx, tt.meta, tt.data)
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			gotData, gotHeader := RawMetadata(t, js, "format", tt.meta.Name, uploaded.NUID)
			if gotData != wantData {
				t.Errorf("metadata message\n got %s\nwant %s", gotData, wantData)
			}
			if gotHeader.Get(jetstream.MsgRollup) != wantHeader.Get(jetstream.MsgRollup) {
				t.Errorf("rollup header = %q, want %q", gotHeader.Get(jetstream.MsgRollup), wantHeader.Get(jetstream.MsgRollup))
			}
			if uploaded.Size != fromClient.Size || uploaded.Chunks != fromClient.Chunks || uploaded.Digest != fromClient.Digest {
				t.Errorf("returned info = %d bytes, %d chunks, %s; the client's = %d, %d, %s",
					uploaded.Size, uploaded.Chunks, uploaded.Digest, fromClient.Size, fromClient.Chunks, fromClient.Digest)
			}
		})
	}

	// The object store client reads the upload back, and the chunks of the
	// version it replaced are gone.
	result, err := store.Get(ctx, meta.Name)
	if err != nil {
		t.Fatalf("object store Get: %v", err)
	}
	data, err := io.ReadAll(result)
	if err != nil || !bytes.Equal(data, content) {
		t.Fatalf("object store Get = %d bytes, %v; want the %d uploaded", len(data), err, len(content))
	}
	// 4 chunks of the report, none of the empty object, 10 of the last one.
	if chunks := ChunkMessages(t, js, "format"); chunks != 14 {
		t.Errorf("bucket holds %d chunk messages, want 14: the replaced versions' chunks were not removed", chunks)
	}
}

func TestUploadObject_RejectsWhatTheObjectStoreClientRejects(t *testing.T) {
	b, _ := NewPutTestBackend(t, BucketConfig{Name: "invalid"})
	ctx := context.Background()
	if _, err := b.PutObject(ctx, jetstream.ObjectMeta{}, strings.NewReader("x")); !errors.Is(err, jetstream.ErrBadObjectMeta) {
		t.Errorf("empty name: err = %v, want ErrBadObjectMeta", err)
	}
	link := jetstream.ObjectMeta{Name: "l", Opts: &jetstream.ObjectMetaOptions{Link: &jetstream.ObjectLink{Bucket: "invalid", Name: "x"}}}
	if _, err := b.PutObject(ctx, link, strings.NewReader("x")); !errors.Is(err, jetstream.ErrLinkNotAllowed) {
		t.Errorf("link: err = %v, want ErrLinkNotAllowed", err)
	}
}

// MetaPublishJS fails the publish of metadata messages with Err, after
// handing the message to the stream when Deliver is set.
type MetaPublishJS struct {
	jetstream.JetStream
	Err     error
	Deliver bool
}

func (m *MetaPublishJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if !strings.Contains(msg.Subject, ".M.") {
		return m.JetStream.PublishMsg(ctx, msg, opts...)
	}
	if m.Deliver {
		if _, err := m.JetStream.PublishMsg(ctx, msg, opts...); err != nil {
			return nil, err
		}
	}
	return nil, m.Err
}

func TestUploadObject_MetadataPublishThatFails(t *testing.T) {
	rejected := &jetstream.APIError{Code: 503, ErrorCode: 10077, Description: "maximum bytes exceeded"}
	tests := []struct {
		name       string
		err        error
		deliver    bool
		wantStored bool
		wantChunks uint64
		wantWarn   string
	}{
		{name: "rejected by the stream: the chunks are removed", err: rejected},
		{name: "outcome unknown, the message was stored: the upload succeeded", err: nats.ErrTimeout, deliver: true, wantStored: true, wantChunks: 1},
		{name: "outcome unknown, the message is not there: the chunks are kept", err: nats.ErrTimeout, wantChunks: 1, wantWarn: "outcome of an object's metadata message is unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, js := setupTestNATSWithJetStream(t)
			store := createTestObjectStore(t, js, BucketConfig{Name: "outcome"})
			logs := &bytes.Buffer{}
			failing := &MetaPublishJS{JetStream: js, Err: tt.err, Deliver: tt.deliver}
			b := NewJetStreamBackendWithJetStream(failing, "outcome", store, 0, slog.New(slog.NewTextHandler(logs, nil)))

			err := b.Set("report", []byte("content"), 0)
			if tt.wantStored {
				if err != nil {
					t.Fatalf("Set = %v, want success: the metadata was stored", err)
				}
				if data, err := b.Get("report"); err != nil || string(data) != "content" {
					t.Errorf("Get = %q, %v", data, err)
				}
			} else {
				if !errors.Is(err, tt.err) {
					t.Fatalf("Set = %v, want %v", err, tt.err)
				}
				if data, err := b.Get("report"); err != nil || data != nil {
					t.Errorf("Get after the failed upload = %q, %v; want no object", data, err)
				}
			}
			if chunks := ChunkMessages(t, js, "outcome"); chunks != tt.wantChunks {
				t.Errorf("bucket holds %d chunk messages, want %d", chunks, tt.wantChunks)
			}
			if tt.wantWarn != "" && !strings.Contains(logs.String(), tt.wantWarn) {
				t.Errorf("log does not say %q:\n%s", tt.wantWarn, logs.String())
			}
			if tt.wantWarn == "" && strings.Contains(logs.String(), "level=WARN") {
				t.Errorf("unexpected warning:\n%s", logs.String())
			}
		})
	}
}

// TestFuture is a publish acknowledgement that is delivered by the test.
type TestFuture struct {
	OkCh  chan *jetstream.PubAck
	ErrCh chan error
}

// NewTestFuture returns a future nothing was delivered to yet.
func NewTestFuture() *TestFuture {
	return &TestFuture{OkCh: make(chan *jetstream.PubAck, 1), ErrCh: make(chan error, 1)}
}

func (f *TestFuture) Ok() <-chan *jetstream.PubAck { return f.OkCh }
func (f *TestFuture) Err() <-chan error            { return f.ErrCh }
func (f *TestFuture) Msg() *nats.Msg               { return nil }

func TestAwaitAck(t *testing.T) {
	acked := NewTestFuture()
	acked.OkCh <- &jetstream.PubAck{}
	if err := AwaitAck(acked, time.Second); err != nil {
		t.Errorf("acknowledged: %v", err)
	}
	rejectedErr := errors.New("maximum bytes exceeded")
	rejected := NewTestFuture()
	rejected.ErrCh <- rejectedErr
	if err := AwaitAck(rejected, time.Second); !errors.Is(err, rejectedErr) {
		t.Errorf("rejected: err = %v", err)
	}
	if err := AwaitAck(NewTestFuture(), 20*time.Millisecond); !errors.Is(err, ErrUploadAckTimeout) {
		t.Errorf("silent: err = %v, want ErrUploadAckTimeout", err)
	}
}

func TestSettleAcks(t *testing.T) {
	acked, rejected, late := NewTestFuture(), NewTestFuture(), NewTestFuture()
	acked.OkCh <- &jetstream.PubAck{}
	rejected.ErrCh <- errors.New("rejected")
	go func() {
		time.Sleep(50 * time.Millisecond)
		late.OkCh <- &jetstream.PubAck{}
	}()
	begin := time.Now()
	SettleAcks([]jetstream.PubAckFuture{acked, rejected, late}, 5*time.Second)
	if waited := time.Since(begin); waited < 40*time.Millisecond || waited > 2*time.Second {
		t.Errorf("waited %s, want until the last acknowledgement came", waited)
	}
	begin = time.Now()
	SettleAcks([]jetstream.PubAckFuture{NewTestFuture(), NewTestFuture()}, 30*time.Millisecond)
	if waited := time.Since(begin); waited > time.Second {
		t.Errorf("waited %s for acknowledgements that never came, want the timeout once", waited)
	}
}

// A backend without a JetStream context still stores objects, through the
// object store client.
func TestNewJetStreamBackend_UploadsThroughTheObjectStoreClient(t *testing.T) {
	_, js := setupTestNATSWithJetStream(t)
	store := createTestObjectStore(t, js, BucketConfig{Name: "plain"})
	b := NewJetStreamBackend("plain", store, 0, nil)
	if err := b.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if data, err := b.Get("k"); err != nil || string(data) != "v" {
		t.Fatalf("Get = %q, %v", data, err)
	}
}
