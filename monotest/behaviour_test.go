package monotest_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/monotest"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }

func TestDispatcherErrorHeadersAndPanics(t *testing.T) {
	bus := monotest.NewEventBus()
	bus.PanicHandler = func(string, any, []byte) {}
	c := monotest.NewContainer("svc", bus)
	if err := c.RegisterRequestReplyService("typed-error", func(context.Context, *types.Msg) ([]byte, error) {
		return nil, &validationError{msg: "bad input"}
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterRequestReplyService("boom", func(context.Context, *types.Msg) ([]byte, error) {
		panic("kaboom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterRequestReplyService("nil-reply", func(context.Context, *types.Msg) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	client, err := c.GetRequestReplyService("typed-error")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Call(ctx, nil)
	var remote *monoerrors.RemoteError
	if !errors.As(err, &remote) || !strings.Contains(err.Error(), "bad input") || !strings.Contains(err.Error(), "validation") {
		t.Fatalf("expected a RemoteError of type validation, got %v", err)
	}

	client, err = c.GetRequestReplyService("boom")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Call(ctx, nil); !errors.Is(err, monoerrors.ErrHandlerPanic) {
		t.Fatalf("expected ErrHandlerPanic, got %v", err)
	}
	if c.Calls("boom") != 1 {
		t.Errorf("a panicking handler still counts as a run")
	}

	// A nil response is not answered, as in the framework: the caller times out.
	client, err = c.GetRequestReplyService("nil-reply")
	if err != nil {
		t.Fatal(err)
	}
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := client.Call(shortCtx, nil); err == nil {
		t.Fatal("expected a timeout for a nil response")
	}
}

func TestQueueGroupPanicIsRecovered(t *testing.T) {
	bus := monotest.NewEventBus()
	c := monotest.NewContainer("svc", bus)
	var n atomic.Int32
	if err := c.RegisterQueueGroupService("jobs", types.QGHP{QueueGroup: "w", Handler: func(_ context.Context, m *types.Msg) error {
		if string(m.Data) == "boom" {
			panic("boom")
		}
		n.Add(1)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := c.GetQueueGroupService("jobs")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"boom", "ok"} {
		if err := client.Send(context.Background(), []byte(d)); err != nil {
			t.Fatalf("Send(%s): %v", d, err)
		}
	}
	waitFor(t, func() bool { return c.Calls("jobs") == 2 })
	if n.Load() != 1 {
		t.Errorf("handler runs after the panic = %d", n.Load())
	}
}

func TestDispatchStartsChannelRoutersOnce(t *testing.T) {
	bus := monotest.NewEventBus()
	c := monotest.NewContainer("svc", bus)
	in, out := make(chan *types.Msg, 8), make(chan *types.Msg, 8)
	if err := c.RegisterChannelService("feed", in, out); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := c.Dispatch(ctx); err != nil {
			t.Fatal(err)
		}
	}
	consumerIn, consumerOut, err := c.GetChannelService("feed", "reader")
	if err != nil {
		t.Fatal(err)
	}
	// With one router, every message reaches the consumer in order.
	go func() {
		for msg := range in {
			out <- &types.Msg{Subject: "feed", Reply: msg.Reply, Data: msg.Data}
		}
	}()
	for i := 0; i < 5; i++ {
		consumerIn <- &types.Msg{Subject: "feed", Reply: "reader", Data: []byte{byte('0' + i)}}
	}
	for i := 0; i < 5; i++ {
		select {
		case m := <-consumerOut:
			if m.Data[0] != byte('0'+i) {
				t.Fatalf("message %d out of order: %s", i, m.Data)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("message %d not routed", i)
		}
	}
}

// slowModule's queue group handler blocks until released; Stop must wait for it.
type slowModule struct {
	release  chan struct{}
	finished atomic.Bool
	stopped  atomic.Bool
}

func (m *slowModule) Name() string { return "slow" }
func (m *slowModule) RegisterServices(c types.ServiceContainer) error {
	return c.RegisterQueueGroupService("work", types.QGHP{QueueGroup: "w", Handler: func(context.Context, *types.Msg) error {
		<-m.release
		m.finished.Store(true)
		return nil
	}})
}
func (m *slowModule) Start(context.Context) error { return nil }
func (m *slowModule) Stop(context.Context) error {
	if !m.finished.Load() {
		return errors.New("stopped while a handler was still running")
	}
	m.stopped.Store(true)
	return nil
}

func TestHarnessStopWaitsForInFlightHandlers(t *testing.T) {
	m := &slowModule{release: make(chan struct{})}
	h := monotest.New(t, m)
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := h.Container().GetQueueGroupService("work")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- h.Stop(context.Background()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before the handler finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(m.release)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !m.stopped.Load() {
		t.Error("module not stopped")
	}
}

// startCaller calls a dependency service from Start, which the framework does
// not support (subscriptions are created after every module started).
type startCaller struct {
	inventory types.ServiceContainer
}

func (m *startCaller) Name() string           { return "eager" }
func (m *startCaller) Dependencies() []string { return []string{"inventory"} }
func (m *startCaller) SetDependencyServiceContainer(_ string, c types.ServiceContainer) {
	m.inventory = c
}
func (m *startCaller) Start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err := checkStock.Call(ctx, m.inventory, stockReq{ProductID: "x"})
	return err
}
func (m *startCaller) Stop(context.Context) error { return nil }

func TestHarnessDispatchesAfterStartLikeTheFramework(t *testing.T) {
	bus := monotest.NewEventBus()
	h := monotest.New(t, &startCaller{}, monotest.WithDependency("inventory", newInventory(t, bus)))
	err := h.Start(context.Background())
	if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
		t.Fatalf("a call from Start must find no responder, got %v", err)
	}
	_ = h.Stop(context.Background())
}

// eventLookupConsumer finds the event through the registry instead of
// importing the emitter's definition.
type eventLookupConsumer struct {
	got atomic.Int32
}

func (m *eventLookupConsumer) Name() string                { return "lookup-consumer" }
func (m *eventLookupConsumer) Start(context.Context) error { return nil }
func (m *eventLookupConsumer) Stop(context.Context) error  { return nil }
func (m *eventLookupConsumer) RegisterEventConsumers(reg types.EventRegistry) error {
	def, ok := reg.GetEventByName("Placed", "v1", "order")
	if !ok {
		return errors.New("event order.Placed v1 not found in registry")
	}
	return reg.RegisterEventConsumer(def, func(context.Context, *types.Msg) error { m.got.Add(1); return nil }, m)
}

func TestHarnessWithEmittedEvents(t *testing.T) {
	m := &eventLookupConsumer{}
	if err := monotest.New(t, m).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "not found in registry") {
		t.Fatalf("without WithEmittedEvents the lookup must fail, got %v", err)
	}
	h := monotest.New(t, m, monotest.WithEmittedEvents(orderEvent.ToBase()))
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Stop(context.Background()) }()
	if err := orderEvent.Publish(h.EventBus(), stockReq{ProductID: "p"}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return m.got.Load() == 1 })
}

// pluginWithService is a plugin exposing one request-reply service.
type pluginWithService struct {
	container types.ServiceContainer
}

func (p *pluginWithService) Name() string                          { return "kv-store" }
func (p *pluginWithService) Start(context.Context) error           { return nil }
func (p *pluginWithService) Stop(context.Context) error            { return nil }
func (p *pluginWithService) SetContainer(c types.ServiceContainer) { p.container = c }
func (p *pluginWithService) Container() types.ServiceContainer     { return p.container }
func (p *pluginWithService) RegisterServices(c types.ServiceContainer) error {
	return c.RegisterRequestReplyService("lookup", func(_ context.Context, m *types.Msg) ([]byte, error) { return m.Data, nil })
}

var lookupContract = helper.RequestReplyService[stockReq, stockResp]("kv-store", "lookup")

// pluginUser consumes the plugin's service and calls it through the bus.
type pluginUser struct {
	plugin types.PluginModule
	bus    types.EventBus
}

func (m *pluginUser) Name() string                             { return "plugin-user" }
func (m *pluginUser) SetPlugin(_ string, p types.PluginModule) { m.plugin = p }
func (m *pluginUser) SetEventBus(bus types.EventBus)           { m.bus = bus }
func (m *pluginUser) ConsumeServices() []types.BaseServiceDefinition {
	return []types.BaseServiceDefinition{lookupContract.ToBase()}
}
func (m *pluginUser) Start(context.Context) error { return nil }
func (m *pluginUser) Stop(context.Context) error  { return nil }

func TestHarnessPlugins(t *testing.T) {
	bus := monotest.NewEventBus()
	plugin := &pluginWithService{}

	t.Run("plugin without container is explained", func(t *testing.T) {
		h := monotest.New(t, &pluginUser{}, monotest.WithPlugin("kv", plugin))
		err := h.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), "SetContainer") {
			t.Fatalf("expected a hint about SetContainer, got %v", err)
		}
	})

	pc := monotest.NewContainer("kv-store", bus)
	plugin.SetContainer(pc)
	if err := plugin.RegisterServices(pc); err != nil {
		t.Fatal(err)
	}
	user := &pluginUser{}
	h := monotest.New(t, user, monotest.WithPlugin("kv", plugin), monotest.WithEventBus(bus))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = h.Stop(context.Background()) }()
	if user.plugin != plugin {
		t.Fatal("plugin not injected")
	}
	// The plugin's container is dispatched and its subjects are allowed.
	if _, err := user.bus.Request("services.kv-store.lookup", []byte("x"), time.Second); err != nil {
		t.Fatalf("request to the plugin: %v", err)
	}
	if pc.Calls("lookup") != 1 || len(h.Report().Violations) != 0 {
		t.Errorf("calls=%d violations=%+v", pc.Calls("lookup"), h.Report().Violations)
	}
}

func TestEventBusClosed(t *testing.T) {
	bus := monotest.NewEventBus()
	bus.Close()
	if err := bus.Publish("events.a.b.c", nil); !errors.Is(err, monotest.ErrEventBusClosed) {
		t.Errorf("Publish after Close = %v", err)
	}
	if _, err := bus.Subscribe("events.a.b.c", func(context.Context, *types.Msg) {}); !errors.Is(err, monotest.ErrEventBusClosed) {
		t.Errorf("Subscribe after Close = %v", err)
	}
	if _, err := bus.Request("services.a.b", nil, time.Second); !errors.Is(err, monotest.ErrEventBusClosed) {
		t.Errorf("Request after Close = %v", err)
	}
}

func TestEventBusWildcardEdgeCasesAndCopies(t *testing.T) {
	bus := monotest.NewEventBus()
	defer bus.Close()
	var short, long atomic.Int32
	if _, err := bus.Subscribe("events.order.>", func(_ context.Context, m *types.Msg) {
		if m.Subject == "events.order" {
			short.Add(1)
		} else {
			long.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish("events.order", nil); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish("events.order.v1.created", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return long.Load() == 1 })
	if short.Load() != 0 {
		t.Error("'>' must require at least one trailing token")
	}

	// Each subscription gets its own copy of the message.
	seen := make(chan *types.Msg, 2)
	for i := 0; i < 2; i++ {
		if _, err := bus.Subscribe("events.copy.v1.x", func(_ context.Context, m *types.Msg) { seen <- m }); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.PublishMsg(&types.Msg{Subject: "events.copy.v1.x", Header: types.Header{"H": {"v"}}}); err != nil {
		t.Fatal(err)
	}
	a, b := <-seen, <-seen
	if a == b {
		t.Error("subscriptions must not share one *Msg")
	}
	if bus.Panics() != 0 {
		t.Error("no panics expected")
	}
}

func TestCheckModuleRuleFourAndStopErrors(t *testing.T) {
	bus := monotest.NewEventBus()
	inv := monotest.NewContainer("inventory", bus) // consumed services missing
	rec := runCheck(t, &orderModule{}, monotest.WithDependency("inventory", inv))
	if !rec.fatal || !rec.has("rule 4:") || !rec.has("consumed service contract rejected") {
		t.Fatalf("expected a fatal rule 4 failure, got %v", rec.errors)
	}

	rec = runCheck(t, &stopFailModule{}, monotest.WithStopTimeout(time.Second))
	if !rec.has("rule 8: Stop failed") {
		t.Fatalf("expected a Stop failure, got %v", rec.errors)
	}

	rec = runCheck(t, &stopFailModule{ok: true}, monotest.WithExercise(func(context.Context, *monotest.Harness) error {
		return errors.New("exercise broke")
	}))
	if !rec.has("exercise failed: exercise broke") {
		t.Fatalf("expected the exercise error, got %v", rec.errors)
	}
}

type stopFailModule struct{ ok bool }

func (m *stopFailModule) Name() string                { return "stopper" }
func (m *stopFailModule) Start(context.Context) error { return nil }
func (m *stopFailModule) Stop(ctx context.Context) error {
	if _, has := ctx.Deadline(); !has {
		return errors.New("Stop must receive a context with a deadline")
	}
	if m.ok {
		return nil
	}
	return errors.New("cannot stop")
}

func TestCheckModuleIgnoresExercisePublishesForRuleSeven(t *testing.T) {
	bus := monotest.NewEventBus()
	rec := runCheck(t, &orderModule{}, monotest.WithDependency("inventory", newInventory(t, bus)),
		monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
			// A consumed event published by the test must not count as an
			// undeclared emission of the module.
			if err := h.EventBus().Publish("events.payment.v1.completed", nil); err != nil {
				return err
			}
			return exercise(ctx, h)
		}))
	if len(rec.errors) != 0 {
		t.Fatalf("unexpected failures: %v", rec.errors)
	}
}
