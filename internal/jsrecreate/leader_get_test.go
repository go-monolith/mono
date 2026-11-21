package jsrecreate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RequestCounter counts the requests a client sends to subject.
func RequestCounter(t *testing.T, nc *nats.Conn, subject string) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	sub, err := nc.Subscribe(subject, func(*nats.Msg) { n.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Errorf("unsubscribe: %v", err)
		}
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return &n
}

func TestHoldsMsgAfter(t *testing.T) {
	js, nc := startJetStreamConn(t)
	ctx := context.Background()
	fileBucket(t, js, "locks", 3) // sequences 1 to 3
	direct := RequestCounter(t, nc, "$JS.API.DIRECT.GET.>")
	leader := RequestCounter(t, nc, "$JS.API.STREAM.MSG.GET.>")

	for _, tc := range []struct {
		name  string
		after uint64
		want  bool
	}{
		{"before the first message", 0, true},
		{"before the last message", 2, true},
		{"at the last message", 3, false},
		{"past the last message", 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := HoldsMsgAfter(ctx, js, "KV_locks", tc.after)
			if err != nil || got != tc.want {
				t.Fatalf("HoldsMsgAfter(%d) = %v, %v; want %v", tc.after, got, err, tc.want)
			}
		})
	}

	t.Run("removed messages at the end are not counted", func(t *testing.T) {
		// A bucket's stream denies message deletes; a plain one stands in
		// for messages that expired.
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "tail", Subjects: []string{"tail.>"}})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, err := js.Publish(ctx, "tail.x", nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.DeleteMsg(ctx, 3); err != nil {
			t.Fatal(err)
		}
		if got, err := HoldsMsgAfter(ctx, js, "tail", 1); err != nil || !got {
			t.Fatalf("HoldsMsgAfter(1) = %v, %v; want true", got, err)
		}
		if got, err := HoldsMsgAfter(ctx, js, "tail", 2); err != nil || got {
			t.Fatalf("HoldsMsgAfter(2) = %v, %v; want false", got, err)
		}
	})

	t.Run("asked of the leader, not with a direct get", func(t *testing.T) {
		// A bucket's stream allows direct gets, which any replica answers.
		stream, err := js.Stream(ctx, "KV_locks")
		if err != nil {
			t.Fatal(err)
		}
		if !stream.CachedInfo().Config.AllowDirect {
			t.Fatal("the bucket's stream does not allow direct gets: the test proves nothing")
		}
		deadline := time.Now().Add(2 * time.Second)
		for leader.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if leader.Load() == 0 || direct.Load() != 0 {
			t.Fatalf("message get requests = %d, direct get requests = %d; want only the first", leader.Load(), direct.Load())
		}
	})

	t.Run("a stream that does not exist is an error", func(t *testing.T) {
		_, err := HoldsMsgAfter(ctx, js, "KV_missing", 0)
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("err = %v, want stream not found", err)
		}
	})

	t.Run("nobody answering is no responders", func(t *testing.T) {
		other, err := jetstream.NewWithAPIPrefix(nc, "nobody.api")
		if err != nil {
			t.Fatal(err)
		}
		_, err = HoldsMsgAfter(ctx, other, "KV_locks", 0)
		if !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("err = %v, want no responders", err)
		}
	})
}

// A copy that is short of the bucket's last sequence asks the leader whether
// anything is left: on a replicated bucket a direct get may be answered by a
// replica that is behind, and the bucket would be deleted without its tail.
func TestOpen_TailCheckAsksTheLeader(t *testing.T) {
	js, nc := startJetStreamConn(t)
	fileBucket(t, js, "locks", 2)
	direct := RequestCounter(t, nc, "$JS.API.DIRECT.GET.KV_locks")
	leader := RequestCounter(t, nc, "$JS.API.STREAM.MSG.GET.KV_locks")
	real := StreamAPI{JS: js}
	// As first read, the stream held 10 more messages, which have expired
	// since: the copy ends short of the last sequence.
	reads := 0
	info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
		i, err := real.StreamInfo(ctx, name)
		if err == nil {
			i.State.LastSeq += 10
			if reads++; reads == 1 {
				i.State.Msgs += 10
			}
		}
		return i, err
	}
	if _, err := Open(context.Background(), kvSpec(js, fakeStreams{StreamAPI: real, info: info}, "locks", Options{Recreate: true}, nil)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if leader.Load() == 0 || direct.Load() != 0 {
		t.Fatalf("message get requests = %d, direct get requests = %d; want only the first", leader.Load(), direct.Load())
	}
}

func TestAPIPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts jetstream.JetStreamOptions
		want string
	}{
		{"default", jetstream.JetStreamOptions{}, "$JS.API."},
		{"domain", jetstream.JetStreamOptions{Domain: "hub"}, "$JS.hub.API."},
		{"prefix", jetstream.JetStreamOptions{APIPrefix: "my.api"}, "my.api."},
		{"prefix with a dot", jetstream.JetStreamOptions{APIPrefix: "my.api."}, "my.api."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := APIPrefix(tc.opts); got != tc.want {
				t.Fatalf("APIPrefix = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLeaderNotAnswering(t *testing.T) {
	live := context.Background()
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"no responders", live, nats.ErrNoResponders, true},
		{"the request timed out, the caller has time left", live, context.DeadlineExceeded, true},
		{"a client timeout, the caller has time left", live, nats.ErrTimeout, true},
		{"the caller's own context ended", ended, context.DeadlineExceeded, false},
		{"JetStream temporarily unavailable", live, &jetstream.APIError{Code: 503, ErrorCode: ErrCodeClusterNotAvailable}, true},
		{"wrapped", live, fmt.Errorf("read: %w", &jetstream.APIError{ErrorCode: ErrCodeClusterNotAvailable}), true},
		{"stream not found", live, &jetstream.APIError{Code: 404, ErrorCode: jetstream.JSErrCodeStreamNotFound}, false},
		{"another error", live, errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := LeaderNotAnswering(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("LeaderNotAnswering(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A request nobody answers ends after MsgGetTimeout, not with the caller's
// much longer copy timeout: a stream without a leader stays silent.
func TestHoldsMsgAfter_BoundsARequestNobodyAnswers(t *testing.T) {
	_, nc := startJetStreamConn(t)
	// A subscriber that never replies stands in for the servers of a stream
	// without a leader.
	sub, err := nc.SubscribeSync("silent.api.STREAM.MSG.GET.KV_locks")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Errorf("unsubscribe: %v", err)
		}
	})
	silent, err := jetstream.NewWithAPIPrefix(nc, "silent.api")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*MsgGetTimeout+time.Minute)
	defer cancel()
	started := time.Now()
	_, err = HoldsMsgAfter(ctx, silent, "KV_locks", 0)
	if !LeaderNotAnswering(ctx, err) {
		t.Fatalf("err = %v, want one that is asked again", err)
	}
	if waited := time.Since(started); waited < MsgGetTimeout/2 || waited > MsgGetTimeout+3*time.Second {
		t.Fatalf("waited %s, want about %s", waited, MsgGetTimeout)
	}
}

func TestTailContradicted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  jetstream.StreamState
		copied uint64
		want   bool
	}{
		{"empty stream", jetstream.StreamState{Msgs: 0, FirstSeq: 13, LastSeq: 12}, 2, false},
		{"everything held was copied", jetstream.StreamState{Msgs: 2, FirstSeq: 1, LastSeq: 12}, 2, false},
		{"first message is the last one copied", jetstream.StreamState{Msgs: 1, FirstSeq: 2, LastSeq: 12}, 2, false},
		{"messages only after what was copied", jetstream.StreamState{Msgs: 3, FirstSeq: 10, LastSeq: 12}, 2, true},
		{"nothing copied, messages held", jetstream.StreamState{Msgs: 1, FirstSeq: 1, LastSeq: 1}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := TailContradicted(tc.state, tc.copied); got != tc.want {
				t.Fatalf("TailContradicted = %v, want %v", got, tc.want)
			}
		})
	}
}

// UnansweredAPI is a JetStream client whose message get requests go to an
// API prefix nobody answers, as while the stream has no leader.
type UnansweredAPI struct {
	jetstream.JetStream
}

// Options returns the options of the client with another API prefix.
func (u UnansweredAPI) Options() jetstream.JetStreamOptions {
	opts := u.JetStream.Options()
	opts.APIPrefix = "nobody.api"
	return opts
}

// While nobody answers whether anything is left, the copy does not count as
// complete: the question is asked again until the copy timeout, and then
// nothing is deleted.
func TestOpen_TailCheckWithoutALeaderDeletesNothing(t *testing.T) {
	js := startJetStream(t)
	fileBucket(t, js, "locks", 2)
	real := StreamAPI{JS: js}
	// The copy ends short of the last sequence the stream was read with.
	info := func(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
		i, err := real.StreamInfo(ctx, name)
		if err == nil {
			i.State.LastSeq += 10
		}
		return i, err
	}
	deletes := 0
	spec := kvSpec(UnansweredAPI{JetStream: js}, fakeStreams{StreamAPI: real, info: info, deletes: &deletes}, "locks", Options{Recreate: true}, nil)
	spec.CopyTimeout = time.Second
	started := time.Now()
	_, err := Open(context.Background(), spec)
	if !errors.Is(err, errMismatch) || !strings.Contains(err.Error(), "backup failed") {
		t.Fatalf("err = %v, want a failed backup", err)
	}
	// The rounds went on until the copy timeout: a first answer taken as an
	// error would have ended them at once.
	if waited := time.Since(started); waited < spec.CopyTimeout*8/10 {
		t.Fatalf("Open returned after %s: the question was not asked again (%v)", waited, err)
	}
	if deletes != 0 {
		t.Fatalf("deletes = %d", deletes)
	}
	if b := backups(t, js, "locks"); len(b) != 0 {
		t.Fatalf("incomplete backup left: %v", b)
	}
}
