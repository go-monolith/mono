package container

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/internal/boundary"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// newProviderContainer returns a bound "inventory" container with one service
// of each client-facing kind registered.
func newProviderContainer(t *testing.T) types.ServiceContainer {
	t.Helper()
	c := newContractContainer(t, "inventory")
	if err := c.RegisterRequestReplyService("check-stock", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterQueueGroupService("restock", types.QGHP{QueueGroup: "w", Handler: func(context.Context, *types.Msg) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterChannelService("feed", make(chan *types.Msg, 1), make(chan *types.Msg, 1)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDependencyViewAttributesLookups(t *testing.T) {
	rec := boundary.New("order", []string{"inventory"}, nil, nil, false, &mockLogger{})
	view := NewDependencyView(newProviderContainer(t), "inventory", rec)

	for i := 0; i < 3; i++ {
		if _, err := view.GetRequestReplyService("check-stock"); err != nil {
			t.Fatalf("GetRequestReplyService: %v", err)
		}
	}
	if _, err := view.GetQueueGroupService("restock"); err != nil {
		t.Fatalf("GetQueueGroupService: %v", err)
	}
	if _, _, err := view.GetChannelService("feed", "order"); err != nil {
		t.Fatalf("GetChannelService: %v", err)
	}
	if _, err := view.GetRequestReplyService("missing"); !errors.Is(err, monoerrors.ErrServiceNotFound) {
		t.Fatalf("expected ErrServiceNotFound, got %v", err)
	}
	if _, err := view.GetStreamConsumerService("check-stock"); err == nil {
		t.Fatal("expected type error for wrong kind")
	}
	if !view.Has("check-stock") || view.Has("missing") || len(view.Entries()) != 3 {
		t.Error("Has/Entries must delegate")
	}

	rep := rec.Report()
	if len(rep.Observed) != 3 {
		t.Fatalf("Observed = %+v", rep.Observed)
	}
	byService := map[string]types.ServiceUse{}
	for _, u := range rep.Observed {
		byService[u.Service] = u
	}
	if byService["check-stock"].Lookups != 3 || byService["check-stock"].Type != types.ServiceTypeRequestReply {
		t.Errorf("check-stock = %+v", byService["check-stock"])
	}
	if byService["restock"].Type != types.ServiceTypeQueueGroup || byService["feed"].Type != types.ServiceTypeChannel {
		t.Errorf("types = %+v", byService)
	}
	if len(rep.UnusedDependencies) != 0 || len(rep.Violations) != 0 {
		t.Errorf("unexpected report: %+v", rep)
	}
}

func TestDependencyViewChannelConsumerMismatch(t *testing.T) {
	t.Run("non-strict passes the name through and records it", func(t *testing.T) {
		rec := boundary.New("order", []string{"inventory"}, nil, nil, false, &mockLogger{})
		inner := newProviderContainer(t)
		view := NewDependencyView(inner, "inventory", rec)
		if _, _, err := view.GetChannelService("feed", "someone-else"); err != nil {
			t.Fatalf("GetChannelService: %v", err)
		}
		// The call went through unchanged: the channel exists under the
		// name the caller passed (targeted replies keep working as before).
		entry := findEntry(inner, "feed")
		entry.ConsumerMu.RLock()
		_, forOrder := entry.ConsumerChannels["order"]
		_, forOther := entry.ConsumerChannels["someone-else"]
		entry.ConsumerMu.RUnlock()
		if forOrder || !forOther {
			t.Errorf("non-strict must delegate unchanged: order=%v other=%v", forOrder, forOther)
		}
		rep := rec.Report()
		if len(rep.Violations) != 1 || rep.Violations[0].Kind != types.BoundaryViolationChannelConsumerMismatch || rep.Violations[0].Subject != "someone-else" {
			t.Fatalf("Violations = %+v", rep.Violations)
		}
	})
	t.Run("strict rejects", func(t *testing.T) {
		rec := boundary.New("order", []string{"inventory"}, nil, nil, true, &mockLogger{})
		view := NewDependencyView(newProviderContainer(t), "inventory", rec)
		if _, _, err := view.GetChannelService("feed", "someone-else"); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
		func() {
			defer func() {
				r := recover()
				err, _ := r.(error)
				if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
					t.Fatalf("strict MustGetChannelService must panic with ErrModuleBoundaryViolation, got %v", r)
				}
			}()
			view.MustGetChannelService("feed", "someone-else")
		}()
		if in, out := view.MustGetChannelService("feed", "order"); in == nil || out == nil {
			t.Fatal("MustGetChannelService with the right consumer must work")
		}
	})
}

func findEntry(c types.ServiceContainer, name string) *types.ServiceEntry {
	for _, e := range c.Entries() {
		if e.Name == name {
			return e
		}
	}
	return nil
}

// mutations lists every mutating call on a view with a label, returning the
// error (void methods return nil).
func mutations(view types.ServiceContainer) map[string]func() error {
	rr := func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }
	return map[string]func() error{
		"BindModule":           func() error { return view.BindModule(&mockModule{name: "x"}) },
		"SetEventBus":          func() error { view.SetEventBus(&mockEventBus{}); return nil },
		"SetOptimisticWindow":  func() error { view.SetQueueGroupOptimisticWindow(1); return nil },
		"SetMiddlewareChain":   func() error { view.SetMiddlewareChain(nil); return nil },
		"StartChannelRouters":  func() error { view.StartChannelRouters(context.Background()); return nil },
		"RegisterChannel":      func() error { return view.RegisterChannelService("c", make(chan *types.Msg), make(chan *types.Msg)) },
		"RegisterRequestReply": func() error { return view.RegisterRequestReplyService("rr", rr) },
		"RegisterQueueGroup": func() error {
			return view.RegisterQueueGroupService("qg", types.QGHP{QueueGroup: "w", Handler: func(context.Context, *types.Msg) error { return nil }})
		},
		"RegisterStreamConsumer": func() error {
			return view.RegisterStreamConsumerService("sc", types.StreamConsumerConfig{}, func(context.Context, []*types.Msg) error { return nil })
		},
		"RegisterCron": func() error {
			return view.RegisterCronService("cron", types.CronServiceConfig{Schedule: "@every 1m"}, func(context.Context, *types.Msg) error { return nil })
		},
		"Unregister": func() error { return view.Unregister("check-stock") },
		"RegisterContract": func() error {
			return view.(types.ContractRegistrar).RegisterContract(types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply})
		},
	}
}

func TestDependencyViewMutationsStrict(t *testing.T) {
	rec := boundary.New("order", []string{"inventory"}, nil, nil, true, &mockLogger{})
	inner := newProviderContainer(t)
	view := NewDependencyView(inner, "inventory", rec)
	before := len(inner.Entries())

	for name, call := range mutations(view) {
		err := call()
		switch name {
		case "SetEventBus", "SetOptimisticWindow", "SetMiddlewareChain", "StartChannelRouters":
			if err != nil {
				t.Errorf("%s: void method must not return an error", name)
			}
		default:
			if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
				t.Errorf("%s: expected ErrModuleBoundaryViolation, got %v", name, err)
			}
		}
	}
	if len(inner.Entries()) != before || !inner.Has("check-stock") {
		t.Error("strict mode must not touch the dependency's container")
	}
	if findEntry(inner, "check-stock").Contract != nil {
		t.Error("strict mode must not record a contract on the dependency")
	}
}

func TestDependencyViewMutationsNonStrictDelegate(t *testing.T) {
	logger := &mockLogger{}
	rec := boundary.New("order", []string{"inventory"}, nil, nil, false, logger)
	inner := newProviderContainer(t)
	view := NewDependencyView(inner, "inventory", rec)

	if err := view.RegisterRequestReplyService("extra", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("non-strict register must delegate, got %v", err)
	}
	if !inner.Has("extra") {
		t.Error("registration did not reach the dependency's container")
	}
	if err := view.Unregister("extra"); err != nil || inner.Has("extra") {
		t.Errorf("non-strict Unregister must delegate: err=%v has=%v", err, inner.Has("extra"))
	}
	if err := view.(types.ContractRegistrar).RegisterContract(types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}); err != nil {
		t.Errorf("non-strict RegisterContract must delegate, got %v", err)
	}
	view.SetQueueGroupOptimisticWindow(0) // void: delegated, recorded

	rep := rec.Report()
	if len(rep.Violations) != 1 || rep.Violations[0].Kind != types.BoundaryViolationRegisterOnDependency || rep.Violations[0].Count != 4 {
		t.Fatalf("Violations = %+v", rep.Violations)
	}
	if !logger.hasWarnedAbout("boundary violation") {
		t.Error("expected a warning")
	}
}

// BenchmarkGetRequestReplyService compares a lookup through the dependency view
// (attributed, counted) with one on the raw container, to keep the view's cost
// in the noise of the request path.
func BenchmarkGetRequestReplyService(b *testing.B) {
	c := NewServiceContainer(&mockLogger{})
	if err := c.BindModule(&mockModule{name: "inventory"}); err != nil {
		b.Fatal(err)
	}
	c.SetEventBus(&mockEventBus{})
	if err := c.RegisterRequestReplyService("check-stock", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }); err != nil {
		b.Fatal(err)
	}
	view := NewDependencyView(c, "inventory", boundary.New("order", []string{"inventory"}, nil, nil, false, nil))

	b.ReportAllocs()
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.GetRequestReplyService("check-stock"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("view", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := view.GetRequestReplyService("check-stock"); err != nil {
				b.Fatal(err)
			}
		}
	})
	// A consumer that implements ServiceConsumerModule: the lookup is also
	// checked against its declared contracts.
	declaring := NewDependencyView(c, "inventory",
		boundary.New("order", []string{"inventory"}, nil, consumedCheckStock, false, nil).EnforceConsumed())
	b.Run("view-declared", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := declaring.GetRequestReplyService("check-stock"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// consumedCheckStock is the only contract the consumer declares in the
// undeclared-use tests below.
var consumedCheckStock = []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}}

func TestDependencyViewUndeclaredUseNonStrict(t *testing.T) {
	logger := &mockLogger{}
	rec := boundary.New("order", []string{"inventory"}, nil, consumedCheckStock, false, logger).EnforceConsumed()
	view := NewDependencyView(newProviderContainer(t), "inventory", rec)

	if _, err := view.GetRequestReplyService("check-stock"); err != nil {
		t.Fatalf("declared contract: %v", err)
	}
	// Not in ConsumeServices: recorded, and the lookup still succeeds.
	client, err := view.GetQueueGroupService("restock")
	if err != nil || client == nil {
		t.Fatalf("non-strict lookup must go through, got client=%v err=%v", client, err)
	}
	// Channel services have no contract and are not checked.
	if _, _, err := view.GetChannelService("feed", "order"); err != nil {
		t.Fatalf("GetChannelService: %v", err)
	}

	rep := rec.Report()
	if len(rep.Violations) != 1 {
		t.Fatalf("Violations = %+v", rep.Violations)
	}
	v := rep.Violations[0]
	if v.Kind != types.BoundaryViolationUndeclaredServiceUse || v.Dependency != "inventory" || v.Subject != "services.inventory.restock" || v.Count != 1 {
		t.Errorf("violation = %+v", v)
	}
	if len(rep.Observed) != 3 {
		t.Errorf("a non-strict undeclared lookup is still observed: %+v", rep.Observed)
	}
	if len(rep.UnusedContracts) != 0 {
		t.Errorf("UnusedContracts = %+v", rep.UnusedContracts)
	}
	if !logger.hasWarnedAbout("boundary violation") {
		t.Error("expected a warning")
	}
}

func TestDependencyViewUndeclaredUseStrict(t *testing.T) {
	rec := boundary.New("order", []string{"inventory"}, nil, consumedCheckStock, true, &mockLogger{}).EnforceConsumed()
	view := NewDependencyView(newProviderContainer(t), "inventory", rec)

	if _, err := view.GetRequestReplyService("check-stock"); err != nil {
		t.Fatalf("declared contract: %v", err)
	}
	if client, err := view.GetQueueGroupService("restock"); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) || client != nil {
		t.Fatalf("expected ErrModuleBoundaryViolation and no client, got client=%v err=%v", client, err)
	}
	// A service that does not exist is reported as undeclared, not as missing:
	// the module asked for something it did not declare.
	if _, err := view.GetRequestReplyService("missing"); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
	if _, err := view.GetStreamConsumerService("audit"); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
	if _, _, err := view.GetChannelService("feed", "order"); err != nil {
		t.Fatalf("channel services are not checked: %v", err)
	}
	if rep := rec.Report(); len(rep.Observed) != 2 {
		t.Errorf("only the allowed lookups are observed: %+v", rep.Observed)
	}
}

func TestDependencyViewWithoutConsumeServicesIsNotChecked(t *testing.T) {
	// The consumer does not implement ServiceConsumerModule: no EnforceConsumed.
	rec := boundary.New("order", []string{"inventory"}, nil, nil, true, &mockLogger{})
	view := NewDependencyView(newProviderContainer(t), "inventory", rec)
	if _, err := view.GetRequestReplyService("check-stock"); err != nil {
		t.Fatalf("GetRequestReplyService: %v", err)
	}
	if _, err := view.GetQueueGroupService("restock"); err != nil {
		t.Fatalf("GetQueueGroupService: %v", err)
	}
}

func TestDependencyViewEntriesAreDescriptions(t *testing.T) {
	inner := newProviderContainer(t)
	cronConfig := types.CronServiceConfig{Schedule: "@every 1m", Payload: []byte("tick")}
	if err := inner.RegisterCronService("sweep", cronConfig, func(context.Context, *types.Msg) error { return nil }); err != nil {
		t.Fatal(err)
	}
	streamConfig := types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "AUDIT"}, Fetch: types.FetchConfig{BatchSize: 7}}
	if err := inner.RegisterStreamConsumerService("audit", streamConfig, func(context.Context, []*types.Msg) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := inner.(types.ContractRegistrar).RegisterContract(consumedCheckStock[0]); err != nil {
		t.Fatal(err)
	}
	rec := boundary.New("order", []string{"inventory"}, nil, nil, false, nil)
	view := NewDependencyView(inner, "inventory", rec)
	if _, err := view.GetStreamConsumerService("audit"); err != nil {
		t.Fatalf("GetStreamConsumerService: %v", err)
	}
	if got := rec.Report().Observed; len(got) != 1 || got[0].Type != types.ServiceTypeStreamConsumer {
		t.Errorf("Observed = %+v", got)
	}

	real := map[string]*types.ServiceEntry{}
	for _, e := range inner.Entries() {
		real[e.Name] = e
	}
	entries := view.Entries()
	if len(entries) != len(real) {
		t.Fatalf("Entries = %d, want %d", len(entries), len(real))
	}
	for _, e := range entries {
		r := real[e.Name]
		if r == nil {
			t.Fatalf("unknown entry %q", e.Name)
		}
		if e == r {
			t.Errorf("%s: the view must not hand out the provider's entry", e.Name)
		}
		if e.Type != r.Type || e.Subject != r.Subject || e.ModuleName != r.ModuleName || !e.Created.Equal(r.Created) || e.QueueGroup != r.QueueGroup {
			t.Errorf("%s: identity not copied: %+v", e.Name, e)
		}
		if e.RequestHandler != nil || e.StreamConsumerHandler != nil || e.CronHandler != nil ||
			e.InChannel != nil || e.OutChannel != nil || e.ConsumerChannels != nil {
			t.Errorf("%s: handlers and channels must be left out", e.Name)
		}
		for _, pair := range e.QueueHandlers {
			if pair.Handler != nil {
				t.Errorf("%s: queue group handler must be left out", e.Name)
			}
		}
	}

	byName := map[string]*types.ServiceEntry{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	if qg := byName["restock"]; len(qg.QueueHandlers) != 1 || qg.QueueHandlers[0].QueueGroup != "w" {
		t.Errorf("queue groups must be described: %+v", qg.QueueHandlers)
	}
	if c := byName["check-stock"].Contract; c == nil || *c != consumedCheckStock[0] || c == real["check-stock"].Contract {
		t.Errorf("contract must be copied: %+v", c)
	}
	stream := byName["audit"]
	if stream.StreamConsumerConfig == nil || stream.StreamConsumerConfig == real["audit"].StreamConsumerConfig ||
		stream.StreamConsumerConfig.Stream.Name != "AUDIT" || stream.StreamConsumerConfig.Fetch.BatchSize != 7 {
		t.Fatalf("stream configuration must be described by a copy: %+v", stream.StreamConsumerConfig)
	}
	stream.StreamConsumerConfig.Fetch.BatchSize = 99
	if real["audit"].StreamConsumerConfig.Fetch.BatchSize != 7 {
		t.Error("changing the described stream configuration must not reach the provider")
	}
	cron := byName["sweep"]
	if cron.CronConfig == nil || cron.CronConfig.Schedule != real["sweep"].CronConfig.Schedule || string(cron.CronConfig.Payload) != "tick" {
		t.Fatalf("cron configuration must be described: %+v", cron.CronConfig)
	}

	// Changing a description changes nothing for the provider.
	byName["check-stock"].RequestHandler = func(context.Context, *types.Msg) ([]byte, error) { return []byte("hijacked"), nil }
	byName["check-stock"].Contract.Name = "changed"
	cron.CronConfig.Schedule = "@every 1s"
	cron.CronConfig.Payload[0] = 'T'
	if real["check-stock"].Contract.Name != "check-stock" || real["sweep"].CronConfig.Schedule != cronConfig.Schedule ||
		string(real["sweep"].CronConfig.Payload) != "tick" {
		t.Error("a description must not share state with the provider's entry")
	}
	if again := view.Entries(); len(again) != len(real) {
		t.Errorf("each call must return fresh descriptions, got %d", len(again))
	}
}
