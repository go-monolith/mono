package monotest_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/monotest"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestEventBusPublishSubscribeWildcards(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()

	var exact, star, gt, other atomic.Int32
	must := func(_ types.Subscription, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(bus.Subscribe("events.order.v1.created", func(context.Context, *types.Msg) { exact.Add(1) }))
	must(bus.Subscribe("events.*.v1.created", func(context.Context, *types.Msg) { star.Add(1) }))
	must(bus.Subscribe("events.order.>", func(context.Context, *types.Msg) { gt.Add(1) }))
	must(bus.Subscribe("events.payment.>", func(context.Context, *types.Msg) { other.Add(1) }))

	if err := bus.Publish("events.order.v1.created", []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return exact.Load() == 1 && star.Load() == 1 && gt.Load() == 1 })
	if other.Load() != 0 {
		t.Error("non-matching subscription received the message")
	}
	if got := bus.Published(); len(got) != 1 || got[0] != "events.order.v1.created" {
		t.Errorf("Published = %v", got)
	}
	if got := bus.Subscribed(); len(got) != 4 {
		t.Errorf("Subscribed = %v", got)
	}
}

func TestEventBusQueueGroupDeliversToOneMember(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()

	var a, b atomic.Int32
	if _, err := bus.QueueSubscribe("services.m.jobs", "workers", func(context.Context, *types.Msg) { a.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.QueueSubscribe("services.m.jobs", "workers", func(context.Context, *types.Msg) { b.Add(1) }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := bus.Publish("services.m.jobs", nil); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return a.Load()+b.Load() == 10 })
	if a.Load() == 0 || b.Load() == 0 {
		t.Errorf("queue group should spread messages: a=%d b=%d", a.Load(), b.Load())
	}
}

func TestEventBusRequestReply(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()

	if _, err := bus.Subscribe("services.m.echo", func(_ context.Context, msg *types.Msg) {
		_ = bus.Publish(msg.Reply, append([]byte("re:"), msg.Data...))
	}); err != nil {
		t.Fatal(err)
	}
	reply, err := bus.Request("services.m.echo", []byte("hi"), time.Second)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(reply.Data) != "re:hi" {
		t.Errorf("reply = %q", reply.Data)
	}

	// No responder on a service subject: ErrServiceUnavailable, like the framework.
	_, err = bus.Request("services.nobody.home", nil, time.Second)
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
	// No responder on another subject: still unavailable.
	if _, err := bus.Request("custom.subject", nil, time.Second); !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}

	// A responder that never answers: the context decides.
	if _, err := bus.Subscribe("services.m.silent", func(context.Context, *types.Msg) {}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := bus.RequestWithContext(ctx, "services.m.silent", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	// Reply inboxes do not show in Published.
	for _, s := range bus.Published() {
		if len(s) > 6 && s[:7] == "_INBOX." {
			t.Errorf("inbox subject recorded: %s", s)
		}
	}
}

func TestEventBusSyncAndChanSubscriptions(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()

	sync1, err := bus.SubscribeSync("events.a.b.c")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan *types.Msg, 1)
	chanSub, err := bus.ChanSubscribe("events.a.b.c", ch)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishMsg(&types.Msg{Subject: "events.a.b.c", Data: []byte("d"), Header: types.Header{"H": {"v"}}}); err != nil {
		t.Fatal(err)
	}
	msg, err := sync1.NextMsg(time.Second)
	if err != nil || string(msg.Data) != "d" || msg.Header.Get("H") != "v" {
		t.Fatalf("NextMsg = %v, %v", msg, err)
	}
	select {
	case m := <-ch:
		if string(m.Data) != "d" {
			t.Errorf("chan msg = %q", m.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("chan subscription got nothing")
	}
	if !sync1.IsValid() || sync1.Subject() != "events.a.b.c" || sync1.Queue() != "" {
		t.Error("subscription accessors wrong")
	}
	if err := chanSub.Unsubscribe(); err != nil || chanSub.IsValid() {
		t.Error("Unsubscribe must invalidate")
	}
	if _, err := sync1.NextMsg(10 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected timeout, got %v", err)
	}
	if err := sync1.Drain(); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.EventStream(); !errors.Is(err, monotest.ErrJetStreamNotAvailable) {
		t.Errorf("EventStream = %v", err)
	}
}

func TestEventBusOrderPerSubscriptionAndPanicRecovery(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()
	var panics atomic.Int32
	bus.PanicHandler = func(subject string, value any, stack []byte) {
		if subject == "events.x.y.z" && value == "boom" && len(stack) > 0 {
			panics.Add(1)
		}
	}

	var mu sync.Mutex
	var got []string
	if _, err := bus.Subscribe("events.x.y.z", func(_ context.Context, m *types.Msg) {
		if string(m.Data) == "boom" {
			panic("boom")
		}
		mu.Lock()
		got = append(got, string(m.Data))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"1", "boom", "2", "3"} {
		if err := bus.Publish("events.x.y.z", []byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if got[0] != "1" || got[1] != "2" || got[2] != "3" {
		t.Errorf("order = %v", got)
	}
	if panics.Load() != 1 || bus.Panics() != 1 {
		t.Errorf("panic handler calls = %d, Panics() = %d", panics.Load(), bus.Panics())
	}
}

func TestEventBusInvalidArguments(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()
	if err := bus.PublishMsg(nil); err == nil {
		t.Error("nil msg must fail")
	}
	if err := bus.Publish("", nil); err == nil {
		t.Error("empty subject must fail")
	}
	if _, err := bus.Subscribe("x", nil); err == nil {
		t.Error("nil handler must fail")
	}
	if _, err := bus.QueueSubscribe("x", "q", nil); err == nil {
		t.Error("nil handler must fail")
	}
	if _, err := bus.ChanSubscribe("x", nil); err == nil {
		t.Error("nil channel must fail")
	}
	if _, err := bus.RequestMsgWithContext(context.Background(), nil); err == nil {
		t.Error("nil request must fail")
	}
}
