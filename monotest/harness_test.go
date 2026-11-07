package monotest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/monotest"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// Contracts of the stand-in "inventory" provider.
type stockReq struct {
	ProductID string `json:"product_id"`
}

type stockResp struct {
	Stock int `json:"stock"`
}

var (
	checkStock = helper.RequestReplyService[stockReq, stockResp]("inventory", "check-stock")
	restock    = helper.QueueGroupService[stockReq]("inventory", "restock")
	orderEvent = helper.EventDefinition[stockReq]("order", "Placed", "v1")
)

// orderModule is a well-behaved module under test: it depends on inventory,
// declares what it consumes, exposes one service, emits one event.
type orderModule struct {
	inventory  types.ServiceContainer
	bus        types.EventBus
	stopped    bool
	bypass     bool // publish around the port
	mutate     bool // register on the dependency
	undeclared bool // emit an undeclared event
	mu         sync.Mutex
}

func (m *orderModule) Name() string           { return "order" }
func (m *orderModule) Dependencies() []string { return []string{"inventory"} }
func (m *orderModule) SetDependencyServiceContainer(dep string, c types.ServiceContainer) {
	if dep == "inventory" {
		m.inventory = c
	}
}
func (m *orderModule) SetEventBus(bus types.EventBus) { m.bus = bus }
func (m *orderModule) ConsumeServices() []types.BaseServiceDefinition {
	return []types.BaseServiceDefinition{checkStock.ToBase(), restock.ToBase()}
}
func (m *orderModule) EmitEvents() []types.BaseEventDefinition {
	return []types.BaseEventDefinition{orderEvent.ToBase()}
}
func (m *orderModule) Start(context.Context) error { return nil }
func (m *orderModule) Stop(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	return nil
}

var placeOrder = helper.RequestReplyService[stockReq, stockResp]("order", "place")

func (m *orderModule) RegisterServices(c types.ServiceContainer) error {
	return placeOrder.Register(c, func(ctx context.Context, req stockReq, _ *types.Msg) (stockResp, error) {
		if req.ProductID == "fail" {
			return stockResp{}, errors.New("cannot place")
		}
		if m.mutate {
			_ = m.inventory.RegisterRequestReplyService("sneaky", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil })
		}
		if m.bypass {
			_, _ = m.bus.Request("services.payment.charge", nil, 50*time.Millisecond)
		}
		resp, err := checkStock.Call(ctx, m.inventory, req)
		if err != nil {
			return stockResp{}, err
		}
		if err := restock.Send(ctx, m.inventory, req); err != nil {
			return stockResp{}, err
		}
		if m.undeclared {
			_ = m.bus.Publish("events.order.v1.shadow", nil)
		}
		_ = orderEvent.Publish(m.bus, req, nil)
		return resp, nil
	})
}

func newInventory(t *testing.T, bus *monotest.EventBus) *monotest.Container {
	t.Helper()
	inv := monotest.NewContainer("inventory", bus)
	if err := checkStock.Register(inv, func(_ context.Context, req stockReq, _ *types.Msg) (stockResp, error) {
		return stockResp{Stock: len(req.ProductID)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := restock.Register(inv, types.TypedQGHP[stockReq]{QueueGroup: "w", Handler: func(context.Context, stockReq, *types.Msg) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	return inv
}

func TestHarnessStartCallStop(t *testing.T) {
	bus := monotest.NewEventBus()
	inv := newInventory(t, bus)
	m := &orderModule{}
	h := monotest.New(t, m, monotest.WithDependency("inventory", inv))
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp := monotest.Call[stockReq, stockResp](t, h, "place", stockReq{ProductID: "laptop"})
	if resp.Stock != len("laptop") {
		t.Errorf("Stock = %d", resp.Stock)
	}
	if inv.Calls("check-stock") != 1 {
		t.Errorf("check-stock calls = %d", inv.Calls("check-stock"))
	}
	waitFor(t, func() bool { return inv.Calls("restock") == 1 })

	// Through the contract as well, and the handler error comes back.
	ctx := context.Background()
	if _, err := placeOrder.Call(ctx, h.Container(), stockReq{ProductID: "fail"}); err == nil || !strings.Contains(err.Error(), "cannot place") {
		t.Fatalf("expected handler error, got %v", err)
	}

	rep := h.Report()
	if len(rep.Observed) != 2 || len(rep.UnusedDependencies) != 0 || len(rep.Violations) != 0 {
		t.Errorf("report = %+v", rep)
	}
	if len(rep.Consumed) != 2 || len(rep.Declared) != 1 {
		t.Errorf("declared/consumed = %+v", rep)
	}
	found := false
	for _, s := range h.Published() {
		if s == orderEvent.Subject {
			found = true
		}
	}
	if !found {
		t.Errorf("event not published: %v", h.Published())
	}
	if h.Dependency("inventory") != inv || h.Dependency("none") != nil || h.Module() != m || h.EventBus() != bus {
		t.Error("accessors wrong")
	}

	if err := h.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !m.stopped {
		t.Error("module not stopped")
	}
	if err := h.Stop(context.Background()); err != nil {
		t.Errorf("second Stop must be a no-op: %v", err)
	}
}

func TestHarnessStartFailures(t *testing.T) {
	t.Run("missing dependency container", func(t *testing.T) {
		h := monotest.New(t, &orderModule{})
		err := h.Start(context.Background())
		if !errors.Is(err, monoerrors.ErrMissingDependency) || !strings.Contains(err.Error(), "WithDependency") {
			t.Fatalf("expected a hint about WithDependency, got %v", err)
		}
		_ = h.Stop(context.Background())
	})
	t.Run("consumed service not registered on the stand-in", func(t *testing.T) {
		bus := monotest.NewEventBus()
		inv := monotest.NewContainer("inventory", bus) // nothing registered
		h := monotest.New(t, &orderModule{}, monotest.WithDependency("inventory", inv))
		err := h.Start(context.Background())
		if !errors.Is(err, monoerrors.ErrServiceNotFound) || !strings.Contains(err.Error(), "StubRequestReply") {
			t.Fatalf("expected ErrServiceNotFound with a hint, got %v", err)
		}
	})
	t.Run("consumed service type mismatch", func(t *testing.T) {
		bus := monotest.NewEventBus()
		inv := monotest.NewContainer("inventory", bus)
		_ = monotest.StubQueueGroup(inv, "check-stock", func(context.Context, stockReq) error { return nil })
		_ = monotest.StubQueueGroup(inv, "restock", func(context.Context, stockReq) error { return nil })
		h := monotest.New(t, &orderModule{}, monotest.WithDependency("inventory", inv))
		if err := h.Start(context.Background()); !errors.Is(err, monoerrors.ErrServiceContractMismatch) {
			t.Fatalf("expected ErrServiceContractMismatch, got %v", err)
		}
	})
	t.Run("containers on different buses", func(t *testing.T) {
		inv := newInventory(t, monotest.NewEventBus())
		h := monotest.New(t, &orderModule{}, monotest.WithDependency("inventory", inv), monotest.WithEventBus(monotest.NewEventBus()))
		if err := h.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "different EventBus") {
			t.Fatalf("expected bus mismatch error, got %v", err)
		}
	})
	t.Run("double start", func(t *testing.T) {
		bus := monotest.NewEventBus()
		h := monotest.New(t, &orderModule{}, monotest.WithDependency("inventory", newInventory(t, bus)))
		if err := h.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = h.Stop(context.Background()) }()
		if err := h.Start(context.Background()); err == nil {
			t.Fatal("second Start must fail")
		}
	})
}

func TestHarnessStrictRejectsBypass(t *testing.T) {
	bus := monotest.NewEventBus()
	h := monotest.New(t, &orderModule{bypass: true}, monotest.WithDependency("inventory", newInventory(t, bus)), monotest.Strict())
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Stop(context.Background()) }()
	// The bypass request fails inside the handler but the handler ignores it;
	// the report stays clean in strict mode and the call still succeeds.
	resp := monotest.Call[stockReq, stockResp](t, h, "place", stockReq{ProductID: "ab"})
	if resp.Stock != 2 {
		t.Errorf("Stock = %d", resp.Stock)
	}
	if n := len(h.Report().Violations); n != 0 {
		t.Errorf("strict mode records nothing, got %d violations", n)
	}
	for _, s := range h.Published() {
		if s == "services.payment.charge" {
			t.Error("strict mode must not let the bypass reach the bus")
		}
	}
}

func TestStubsAndContainerAccessors(t *testing.T) {
	bus := monotest.NewEventBus()
	c := monotest.NewContainer("kv", bus)
	if c.ModuleName() != "kv" || c.EventBus() != bus {
		t.Fatal("accessors wrong")
	}
	if err := monotest.StubRequestReply(c, "get", func(_ context.Context, req stockReq) (stockResp, error) {
		if req.ProductID == "" {
			return stockResp{}, fmt.Errorf("empty")
		}
		return stockResp{Stock: 7}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got stockReq
	if err := monotest.StubQueueGroup(c, "put", func(_ context.Context, p stockReq) error {
		mu.Lock()
		defer mu.Unlock()
		got = p
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Dispatch(context.Background()); err != nil { // idempotent
		t.Fatal(err)
	}
	ctx := context.Background()
	var resp stockResp
	if err := helper.CallRequestReplyService(ctx, c, "get", jsonM, jsonU, stockReq{ProductID: "x"}, &resp); err != nil || resp.Stock != 7 {
		t.Fatalf("call = %+v, %v", resp, err)
	}
	err := helper.CallRequestReplyService(ctx, c, "get", jsonM, jsonU, stockReq{}, &resp)
	var remote *monoerrors.RemoteError
	if !errors.As(err, &remote) || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected a RemoteError carrying the handler message, got %v", err)
	}
	if err := helper.SendQueueGroupService(ctx, c, "put", jsonM, stockReq{ProductID: "p"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return c.Calls("put") == 1 })
	mu.Lock()
	gotID := got.ProductID
	mu.Unlock()
	if gotID != "p" || c.Calls("get") != 2 || c.Calls("none") != 0 {
		t.Errorf("calls: got=%q get=%d", gotID, c.Calls("get"))
	}
}

func TestNewContainerPanicsOnEmptyName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	monotest.NewContainer("", monotest.NewEventBus())
}

func jsonM(v any) ([]byte, error) { return jsonMarshal(v) }
func jsonU(d []byte, v any) error { return jsonUnmarshal(d, v) }
