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
	rec := boundary.New("order", []string{"inventory"}, []string{"kv-store"}, strict, logger.NewDefaultLogger())
	return NewScoped(inner, rec), rec
}

func TestScopedTarget(t *testing.T) {
	cases := []struct {
		subject string
		module  string
		service string
		ok      bool
	}{
		{"services.inventory.check-stock", "inventory", "check-stock", true},
		{"services.inventory.check-stock.extra", "inventory", "", true},
		{"services.>", ">", "", true},
		{"services.*.check", "*", "check", true},
		{"services.", "", "", false},
		{"events.order.v1.created", "", "", false},
		{"_mono.x.y", "", "", false},
		{"anything", "", "", false},
	}
	for _, tc := range cases {
		module, service, ok := target(tc.subject)
		if module != tc.module || service != tc.service || ok != tc.ok {
			t.Errorf("target(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.subject, module, service, ok, tc.module, tc.service, tc.ok)
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

// A module that implements ServiceConsumerModule is held to ConsumeServices()
// on the bus as on the dependency's container: a send to a service subject of
// a declared dependency is a use of the contract, or an undeclared use.
func TestScopedHoldsServiceSubjectsToTheConsumedList(t *testing.T) {
	reserve := types.BaseServiceDefinition{ModuleName: "inventory", Name: "reserve", Type: types.ServiceTypeQueueGroup}
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	ctx := context.Background()

	t.Run("non-strict: attributed or recorded, and sent", func(t *testing.T) {
		bus, rec := newScopedForTest(t, false)
		if err := rec.DeclareConsumed([]types.BaseServiceDefinition{reserve, checkStock}); err != nil {
			t.Fatal(err)
		}
		// Declared: counted as a use of the contract, so it is not unused.
		if err := bus.Publish("services.inventory.reserve", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if err := bus.PublishMsg(&types.Msg{Subject: "services.inventory.reserve"}); err != nil {
			t.Fatalf("PublishMsg: %v", err)
		}
		// Not declared: recorded, and the publish still goes through.
		if err := bus.Publish("services.inventory.restock", []byte("x")); err != nil {
			t.Fatalf("non-strict publish must pass through: %v", err)
		}
		// The module's own subjects, plugins and sub-subjects are not held to
		// the list.
		for _, subject := range []string{"services.order.place", "services.kv-store.get", "services.inventory.reserve.control"} {
			if err := bus.Publish(subject, []byte("x")); err != nil {
				t.Fatalf("Publish(%s): %v", subject, err)
			}
		}

		rep := rec.Report()
		if len(rep.Observed) != 1 || rep.Observed[0].Service != "reserve" || rep.Observed[0].Lookups != 2 || rep.Observed[0].Type != types.ServiceTypeQueueGroup {
			t.Errorf("Observed = %+v", rep.Observed)
		}
		if len(rep.UnusedContracts) != 1 || rep.UnusedContracts[0] != checkStock {
			t.Errorf("UnusedContracts = %+v", rep.UnusedContracts)
		}
		if len(rep.Violations) != 1 || rep.Violations[0].Kind != types.BoundaryViolationUndeclaredServiceUse ||
			rep.Violations[0].Dependency != "inventory" || rep.Violations[0].Subject != "services.inventory.restock" {
			t.Errorf("Violations = %+v", rep.Violations)
		}
	})

	t.Run("strict: an undeclared service is rejected on every send path", func(t *testing.T) {
		bus, rec := newScopedForTest(t, true)
		if err := rec.DeclareConsumed([]types.BaseServiceDefinition{reserve}); err != nil {
			t.Fatal(err)
		}
		if err := bus.Publish("services.inventory.reserve", []byte("x")); err != nil {
			t.Fatalf("declared contract: %v", err)
		}
		msg := &types.Msg{Subject: "services.inventory.restock"}
		checks := map[string]func() error{
			"Publish":               func() error { return bus.Publish("services.inventory.restock", nil) },
			"PublishMsg":            func() error { return bus.PublishMsg(msg) },
			"Request":               func() error { _, err := bus.Request("services.inventory.restock", nil, time.Second); return err },
			"RequestWithContext":    func() error { _, err := bus.RequestWithContext(ctx, "services.inventory.restock", nil); return err },
			"RequestMsgWithContext": func() error { _, err := bus.RequestMsgWithContext(ctx, msg); return err },
		}
		for name, call := range checks {
			if err := call(); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
				t.Errorf("%s: expected ErrModuleBoundaryViolation, got %v", name, err)
			}
		}
	})

	t.Run("a module that declares nothing is not held to a list", func(t *testing.T) {
		bus, rec := newScopedForTest(t, true)
		if err := bus.Publish("services.inventory.restock", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if rep := rec.Report(); len(rep.Observed) != 0 || len(rep.Violations) != 0 {
			t.Errorf("report = %+v", rep)
		}
	})
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
	plain := NewScoped(&noConnBus{}, boundary.New("m", nil, nil, false, nil))
	if _, ok := plain.(types.EventBusWithConn[*nats.Conn]); ok {
		t.Error("scoped bus must not claim Conn() the inner bus lacks")
	}
}

// noConnBus is a minimal EventBus without a NATS connection.
type noConnBus struct{ types.EventBus }
