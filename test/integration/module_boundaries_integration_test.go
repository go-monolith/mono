//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// boundaryModule depends on "stock", keeps its dependency container and its
// event bus, and registers one request-reply service of its own.
type boundaryModule struct {
	testModule
	stock mono.ServiceContainer
}

func (m *boundaryModule) Dependencies() []string { return []string{"stock"} }
func (m *boundaryModule) SetDependencyServiceContainer(dep string, c mono.ServiceContainer) {
	if dep == "stock" {
		m.stock = c
	}
}

func newBoundaryApp(t *testing.T, opts ...mono.MonoFrameworkOption) (mono.MonoApplication, *contractProvider, *boundaryModule, *testModule) {
	t.Helper()
	fw := newWorkerPoolApp(t, opts...)
	stock := &contractProvider{testModule: testModule{name: "stock"}, restocked: make(chan string, 1)}
	// payment is not a dependency of shop; it answers "check" so that a
	// non-strict bypass request gets a reply.
	payment := &testModule{name: "payment"}
	payment.registerServicesFunc = func(c mono.ServiceContainer) error {
		return c.RegisterRequestReplyService("check", func(_ context.Context, m *mono.Msg) ([]byte, error) { return m.Data, nil })
	}
	shop := &boundaryModule{testModule: testModule{name: "shop"}}
	for _, m := range []mono.Module{stock, payment, shop} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Name(), err)
		}
	}
	return fw, stock, shop, payment
}

func TestModuleBoundaries_ReportObservedAndViolations(t *testing.T) {
	fw, _, shop, _ := newBoundaryApp(t)
	startWorkerPoolApp(t, fw)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Through the port: attributed lookups.
	for i := 0; i < 3; i++ {
		if _, err := checkStockContract.Call(ctx, shop.stock, stockRequest{ProductID: "a"}); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}
	// Around the port: a raw request to a module that is not a dependency.
	// Non-strict: it goes through (payment answers) and is recorded.
	if _, err := shop.eventBus.Request("services.payment.check", []byte(`{"product_id":"x"}`), 5*time.Second); err != nil {
		t.Fatalf("non-strict request must go through: %v", err)
	}
	// Mutating the dependency's container: recorded and delegated.
	if err := shop.stock.RegisterRequestReplyService("sneaky", func(_ context.Context, m *mono.Msg) ([]byte, error) { return m.Data, nil }); err != nil {
		t.Fatalf("non-strict register must go through: %v", err)
	}

	var shopReport *types.ModuleBoundaryReport
	for _, r := range fw.ModuleBoundaries() {
		if r.Module == "shop" {
			r := r
			shopReport = &r
		}
	}
	if shopReport == nil {
		t.Fatal("no report for shop")
	}
	if len(shopReport.Observed) != 1 || shopReport.Observed[0].Module != "stock" || shopReport.Observed[0].Service != "check" || shopReport.Observed[0].Lookups != 3 {
		t.Errorf("Observed = %+v", shopReport.Observed)
	}
	if len(shopReport.Violations) != 2 {
		t.Fatalf("Violations = %+v", shopReport.Violations)
	}
	kinds := map[types.BoundaryViolationKind]types.BoundaryViolation{}
	for _, v := range shopReport.Violations {
		kinds[v.Kind] = v
	}
	if v, ok := kinds[types.BoundaryViolationUndeclaredServiceSubject]; !ok || v.Dependency != "payment" || v.Subject != "services.payment.check" {
		t.Errorf("undeclared subject violation = %+v", v)
	}
	if v, ok := kinds[types.BoundaryViolationRegisterOnDependency]; !ok || v.Dependency != "stock" {
		t.Errorf("register violation = %+v", v)
	}
	// The host accessor returns the module's scoped bus.
	if fw.EventBus("shop") != shop.eventBus {
		t.Error("EventBus(shop) must return the bus handed to the module")
	}
	if fw.EventBus("") == nil || fw.EventBus("nobody") == nil {
		t.Error("EventBus must fall back to the shared bus")
	}
}

func TestModuleBoundaries_StrictRejects(t *testing.T) {
	fw, _, shop, _ := newBoundaryApp(t, mono.WithStrictModuleBoundaries())
	startWorkerPoolApp(t, fw)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The port still works.
	if _, err := checkStockContract.Call(ctx, shop.stock, stockRequest{ProductID: "a"}); err != nil {
		t.Fatalf("Call through the port: %v", err)
	}
	// Around the port: rejected without reaching NATS.
	if _, err := shop.eventBus.Request("services.payment.check", []byte("{}"), time.Second); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
	if _, err := shop.eventBus.Subscribe("services.stock.check", func(context.Context, *mono.Msg) {}); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation on foreign subscription, got %v", err)
	}
	if err := shop.stock.RegisterRequestReplyService("sneaky", func(_ context.Context, m *mono.Msg) ([]byte, error) { return m.Data, nil }); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation on register, got %v", err)
	}
	if fw.Services("stock").Has("sneaky") {
		t.Error("strict mode must not touch the dependency's container")
	}
	// Own and declared subjects, and events, pass.
	if err := shop.eventBus.Publish("services.stock.restock", []byte(`{"product_id":"m"}`)); err != nil {
		t.Errorf("declared dependency subject must pass: %v", err)
	}
	if err := shop.eventBus.Publish("events.shop.v1.opened", []byte("{}")); err != nil {
		t.Errorf("event subject must pass: %v", err)
	}
	for _, r := range fw.ModuleBoundaries() {
		if r.Module == "shop" && len(r.Violations) != 0 {
			t.Errorf("strict run must record no violations, got %+v", r.Violations)
		}
	}
}

func TestModuleBoundaries_NilWhenNotRunning(t *testing.T) {
	fw, _, _, _ := newBoundaryApp(t)
	if fw.ModuleBoundaries() != nil {
		t.Fatal("ModuleBoundaries must be nil before Start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if len(fw.ModuleBoundaries()) == 0 {
		t.Fatal("ModuleBoundaries must report while running")
	}
	if err := fw.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if fw.ModuleBoundaries() != nil {
		t.Fatal("ModuleBoundaries must be nil after Stop")
	}
}

// channelProvider registers a channel service and echoes targeted replies.
type channelProvider struct {
	testModule
	in, out chan *mono.Msg
}

func (p *channelProvider) RegisterServices(c mono.ServiceContainer) error {
	p.in, p.out = make(chan *mono.Msg, 8), make(chan *mono.Msg, 8)
	go func() {
		for msg := range p.in {
			p.out <- &mono.Msg{Subject: "feed", Reply: msg.Reply, Data: append([]byte("echo:"), msg.Data...)}
		}
	}()
	return c.RegisterChannelService("feed", p.in, p.out)
}

type channelConsumer struct {
	testModule
	feed mono.ServiceContainer
}

func (m *channelConsumer) Dependencies() []string { return []string{"feeder"} }
func (m *channelConsumer) SetDependencyServiceContainer(dep string, c mono.ServiceContainer) {
	m.feed = c
}

func TestModuleBoundaries_ChannelConsumerMismatch(t *testing.T) {
	fw := newWorkerPoolApp(t)
	provider := &channelProvider{testModule: testModule{name: "feeder"}}
	consumer := &channelConsumer{testModule: testModule{name: "reader"}}
	for _, m := range []mono.Module{provider, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatal(err)
		}
	}
	startWorkerPoolApp(t, fw)

	// A wrong consumer name still works (non-strict), is recorded with the
	// name passed, and targeted replies reach the name the caller used.
	in, out, err := consumer.feed.GetChannelService("feed", "Reader-Old")
	if err != nil {
		t.Fatalf("GetChannelService: %v", err)
	}
	in <- &mono.Msg{Subject: "feed", Reply: "Reader-Old", Data: []byte("hi")}
	select {
	case got := <-out:
		if string(got.Data) != "echo:hi" {
			t.Fatalf("reply = %q", got.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("targeted reply did not reach the consumer channel")
	}
	for _, r := range fw.ModuleBoundaries() {
		if r.Module != "reader" {
			continue
		}
		if len(r.Violations) != 1 || r.Violations[0].Kind != types.BoundaryViolationChannelConsumerMismatch || r.Violations[0].Subject != "Reader-Old" {
			t.Fatalf("Violations = %+v", r.Violations)
		}
		if len(r.Observed) != 1 || r.Observed[0].Type != types.ServiceTypeChannel {
			t.Fatalf("Observed = %+v", r.Observed)
		}
	}
}
