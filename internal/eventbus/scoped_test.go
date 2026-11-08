package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/boundary"
	"github.com/go-monolith/mono/internal/logger"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

func newScopedForTest(t *testing.T, strict bool) (types.EventBus, *boundary.Recorder) {
	t.Helper()
	conn := setupTestNATS(t)
	inner, err := NewEventBus(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rec := boundary.New("order", []string{"inventory"}, []string{"kv-store"}, nil, strict, logger.NewDefaultLogger())
	return NewScoped(inner, rec), rec
}

func TestScopedTarget(t *testing.T) {
	cases := []struct {
		subject string
		module  string
		ok      bool
	}{
		{"services.inventory.check-stock", "inventory", true},
		{"services.inventory.check-stock.extra", "inventory", true},
		{"services.>", ">", true},
		{"services.*.check", "*", true},
		{"services.", "", false},
		{"events.order.v1.created", "", false},
		{"_mono.x.y", "", false},
		{"anything", "", false},
	}
	for _, tc := range cases {
		module, ok := target(tc.subject)
		if module != tc.module || ok != tc.ok {
			t.Errorf("target(%q) = (%q, %v), want (%q, %v)", tc.subject, module, ok, tc.module, tc.ok)
		}
	}
}

func TestScopedAllowsOwnDeclaredPluginAndEvents(t *testing.T) {
	bus, rec := newScopedForTest(t, true)

	// A responder on each allowed target so Request gets an answer.
	for _, subj := range []string{"services.order.place", "services.inventory.check-stock", "services.kv-store.get"} {
		if _, err := bus.(*scopedEventBusWithConn).inner.Subscribe(subj, func(_ context.Context, m *types.Msg) {
			_ = bus.Publish(m.Reply, []byte("ok"))
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, subj := range []string{"services.order.place", "services.inventory.check-stock", "services.kv-store.get"} {
		if _, err := bus.Request(subj, []byte("x"), 2*time.Second); err != nil {
			t.Errorf("Request(%s): %v", subj, err)
		}
		if err := bus.Publish(subj, []byte("x")); err != nil {
			t.Errorf("Publish(%s): %v", subj, err)
		}
	}
	if err := bus.Publish("events.order.v1.created", []byte("x")); err != nil {
		t.Errorf("events must pass: %v", err)
	}
	if _, err := bus.Subscribe("events.inventory.v1.restocked", func(context.Context, *types.Msg) {}); err != nil {
		t.Errorf("event subscription must pass: %v", err)
	}
	if _, err := bus.Subscribe("services.order.place", func(context.Context, *types.Msg) {}); err != nil {
		t.Errorf("own service subscription must pass: %v", err)
	}
	if len(rec.Report().Violations) != 0 {
		t.Errorf("no violations expected: %+v", rec.Report().Violations)
	}
}

func TestScopedRejectsUndeclaredInStrictMode(t *testing.T) {
	bus, _ := newScopedForTest(t, true)
	ctx := context.Background()
	msg := &types.Msg{Subject: "services.payment.process", Data: []byte("x")}

	checks := map[string]func() error{
		"Publish":               func() error { return bus.Publish("services.payment.process", nil) },
		"PublishMsg":            func() error { return bus.PublishMsg(msg) },
		"Request":               func() error { _, err := bus.Request("services.payment.process", nil, time.Second); return err },
		"RequestWithContext":    func() error { _, err := bus.RequestWithContext(ctx, "services.payment.process", nil); return err },
		"RequestMsgWithContext": func() error { _, err := bus.RequestMsgWithContext(ctx, msg); return err },
		"Wildcard":              func() error { return bus.Publish("services.>", nil) },
		"Subscribe": func() error {
			_, err := bus.Subscribe("services.inventory.check-stock", func(context.Context, *types.Msg) {})
			return err
		},
		"SubscribeSync": func() error { _, err := bus.SubscribeSync("services.inventory.check-stock"); return err },
		"QueueSubscribe": func() error {
			_, err := bus.QueueSubscribe("services.inventory.check-stock", "q", func(context.Context, *types.Msg) {})
			return err
		},
		"QueueSubscribeSync": func() error { _, err := bus.QueueSubscribeSync("services.inventory.check-stock", "q"); return err },
		"ChanSubscribe": func() error {
			_, err := bus.ChanSubscribe("services.inventory.check-stock", make(chan *types.Msg))
			return err
		},
	}
	for name, call := range checks {
		if err := call(); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Errorf("%s: expected ErrModuleBoundaryViolation, got %v", name, err)
		}
	}
}

func TestScopedRecordsAndPassesThroughInNonStrictMode(t *testing.T) {
	bus, rec := newScopedForTest(t, false)
	// Publish to an undeclared module goes through (no responder needed).
	if err := bus.Publish("services.payment.process", []byte("x")); err != nil {
		t.Fatalf("non-strict publish must pass through: %v", err)
	}
	// Subscribing to a dependency's service subject goes through too.
	if _, err := bus.Subscribe("services.inventory.check-stock", func(context.Context, *types.Msg) {}); err != nil {
		t.Fatalf("non-strict subscribe must pass through: %v", err)
	}
	rep := rec.Report()
	if len(rep.Violations) != 2 {
		t.Fatalf("Violations = %+v", rep.Violations)
	}
	if rep.Violations[0].Kind != types.BoundaryViolationForeignServiceSubscription || rep.Violations[0].Dependency != "inventory" {
		t.Errorf("first = %+v", rep.Violations[0])
	}
	if rep.Violations[1].Kind != types.BoundaryViolationUndeclaredServiceSubject || rep.Violations[1].Subject != "services.payment.process" {
		t.Errorf("second = %+v", rep.Violations[1])
	}
}

func TestScopedKeepsConnAndPassthroughs(t *testing.T) {
	bus, _ := newScopedForTest(t, true)
	withConn, ok := bus.(types.EventBusWithConn[*nats.Conn])
	if !ok || withConn.Conn() == nil {
		t.Fatal("scoped bus over a NATS bus must expose Conn()")
	}
	if _, err := bus.EventStream(); err != nil {
		t.Errorf("EventStream passthrough: %v", err)
	}
	bus.SetRuntimeContext(context.Background()) // must not panic
	if err := bus.PublishMsg(&types.Msg{Subject: "events.order.v1.created", Data: []byte("x")}); err != nil {
		t.Errorf("PublishMsg passthrough: %v", err)
	}

	// A bus without Conn() yields a scoped bus without Conn().
	plain := NewScoped(&noConnBus{}, boundary.New("m", nil, nil, nil, false, nil))
	if _, ok := plain.(types.EventBusWithConn[*nats.Conn]); ok {
		t.Error("scoped bus must not claim Conn() the inner bus lacks")
	}
}

// noConnBus is a minimal EventBus without a NATS connection.
type noConnBus struct{ types.EventBus }
