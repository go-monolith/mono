package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/pkg/types"
)

// AckRecorder stands for a JetStream message: it records the acknowledgement
// calls made on it, in order.
type AckRecorder struct {
	mu          sync.Mutex
	Calls       []string
	ProgressErr error
}

func (a *AckRecorder) record(call string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Calls = append(a.Calls, call)
}

// Ack records an Ack.
func (a *AckRecorder) Ack() error { a.record("ack"); return nil }

// Nak records a Nak.
func (a *AckRecorder) Nak() error { a.record("nak"); return nil }

// InProgress records an in-progress signal.
func (a *AckRecorder) InProgress() error { a.record("progress"); return a.ProgressErr }

// Recorded returns the calls so far.
func (a *AckRecorder) Recorded() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.Calls...)
}

func cronEntry(handler types.CronHandler) *types.ServiceEntry {
	return &types.ServiceEntry{Name: "rollup", ModuleName: "reports", Subject: "services.reports.rollup", CronHandler: handler}
}

// TestDispatchCronTick_SignalsProgress: while the handler runs the occurrence
// is reported as in progress, and nothing is signalled after it was
// acknowledged.
func TestDispatchCronTick_SignalsProgress(t *testing.T) {
	tests := []struct {
		name     string
		handler  types.CronHandler
		wantLast string
	}{
		{"handler succeeds", func(context.Context, *types.Msg) error { time.Sleep(120 * time.Millisecond); return nil }, "ack"},
		{"handler fails", func(context.Context, *types.Msg) error { time.Sleep(120 * time.Millisecond); return errors.New("boom") }, "nak"},
		{"handler panics", func(context.Context, *types.Msg) error { time.Sleep(120 * time.Millisecond); panic("boom") }, "nak"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lm, _, _ := newBareManager(t, newMockEventBus())
			lm.cronProgressInterval = 20 * time.Millisecond
			rec := &AckRecorder{}
			lm.dispatchCronTick(context.Background(), cronEntry(tt.handler), &types.Msg{NatsMsg: rec})
			calls := rec.Recorded()
			if len(calls) < 3 {
				t.Fatalf("calls = %v, want several in-progress signals and %s", calls, tt.wantLast)
			}
			for _, c := range calls[:len(calls)-1] {
				if c != "progress" {
					t.Fatalf("calls = %v, want only in-progress signals before %s", calls, tt.wantLast)
				}
			}
			if calls[len(calls)-1] != tt.wantLast {
				t.Fatalf("last call = %s, want %s (calls %v)", calls[len(calls)-1], tt.wantLast, calls)
			}
			time.Sleep(60 * time.Millisecond)
			if after := rec.Recorded(); len(after) != len(calls) {
				t.Fatalf("signals after the acknowledgement: %v", after[len(calls):])
			}
		})
	}

	t.Run("a fast handler sends none", func(t *testing.T) {
		lm, _, _ := newBareManager(t, newMockEventBus())
		rec := &AckRecorder{}
		lm.dispatchCronTick(context.Background(), cronEntry(func(context.Context, *types.Msg) error { return nil }), &types.Msg{NatsMsg: rec})
		if calls := rec.Recorded(); len(calls) != 1 || calls[0] != "ack" {
			t.Fatalf("calls = %v, want one ack", calls)
		}
		if lm.cronProgressEvery() != defaultCronAckWait/3 {
			t.Fatalf("default interval = %v, want a third of AckWait (%v)", lm.cronProgressEvery(), defaultCronAckWait)
		}
	})

	t.Run("a failed signal is logged once", func(t *testing.T) {
		lm, logger, _ := newBareManager(t, newMockEventBus())
		lm.cronProgressInterval = 10 * time.Millisecond
		rec := &AckRecorder{ProgressErr: errors.New("connection closed")}
		lm.dispatchCronTick(context.Background(), cronEntry(func(context.Context, *types.Msg) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		}), &types.Msg{NatsMsg: rec})
		if n := logger.countWarnsContaining("Cron in-progress signal failed"); n != 1 {
			t.Fatalf("%d warnings for failed signals, want 1", n)
		}
	})
}

// TestKeepCronOccurrence_NoSecondDelivery runs against a real server: with
// the signal, a handler that outlives the consumer's AckWait several times
// over keeps its occurrence, so a second puller (another instance) gets
// nothing; without it, the second puller gets the occurrence while the first
// is still at work.
func TestKeepCronOccurrence_NoSecondDelivery(t *testing.T) {
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
	ctx := context.Background()
	const ackWait = 500 * time.Millisecond

	run := func(t *testing.T, name string, signal bool) (secondDeliveries int64) {
		t.Helper()
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".tick"}})
		if err != nil {
			t.Fatal(err)
		}
		cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
			Durable: "cron", AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait, MaxDeliver: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, name+".tick", []byte("tick")); err != nil {
			t.Fatal(err)
		}
		first, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var msg jetstream.Msg
		for m := range first.Messages() {
			msg = m
		}
		if msg == nil {
			t.Fatal("the occurrence was not delivered")
		}

		// The handler runs for five times AckWait; meanwhile another puller asks.
		handlerDone := make(chan struct{})
		var other atomic.Int64
		go func() {
			for {
				select {
				case <-handlerDone:
					return
				default:
				}
				batch, err := cons.Fetch(1, jetstream.FetchMaxWait(200*time.Millisecond))
				if err != nil {
					continue
				}
				for range batch.Messages() {
					other.Add(1)
				}
			}
		}()
		stop := func() {}
		if signal {
			stop = KeepCronOccurrence(eventbus.WrapJetStreamMsg(msg), ackWait/3, &mockLogger{})
		}
		time.Sleep(5 * ackWait)
		stop()
		close(handlerDone)
		if err := msg.Ack(); err != nil {
			t.Fatal(err)
		}
		return other.Load()
	}

	if n := run(t, "WITHOUT", false); n == 0 {
		t.Fatal("control: without the signal the occurrence was not delivered a second time, so the test proves nothing")
	}
	if n := run(t, "WITH", true); n != 0 {
		t.Fatalf("the occurrence was delivered %d more time(s) while its handler ran", n)
	}
}
