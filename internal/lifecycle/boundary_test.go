package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// boundaryConsumer looks services up on its dependencies in Start and may
// publish on its scoped event bus.
type boundaryConsumer struct {
	mockModule
	eventBus  types.EventBus
	onStart   func(m *boundaryConsumer) error
	contracts []types.BaseServiceDefinition
}

func (m *boundaryConsumer) SetEventBus(bus types.EventBus) { m.eventBus = bus }
func (m *boundaryConsumer) ConsumeServices() []types.BaseServiceDefinition {
	return m.contracts
}
func (m *boundaryConsumer) Start(ctx context.Context) error {
	if err := m.mockModule.Start(ctx); err != nil {
		return err
	}
	if m.onStart != nil {
		return m.onStart(m)
	}
	return nil
}

func newBoundaryManager(t *testing.T, strict bool, modules ...types.Module) (*lifecycleManager, *mockLogger) {
	t.Helper()
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	for _, m := range modules {
		if err := reg.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Name(), err)
		}
	}
	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), newMockEventBus(), nil, logger, 0).(*lifecycleManager)
	lm.SetStrictModuleBoundaries(strict)
	return lm, logger
}

func TestModuleBoundariesReport(t *testing.T) {
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory", "payment"}},
		contracts:  []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}},
		onStart: func(m *boundaryConsumer) error {
			for i := 0; i < 2; i++ {
				if _, err := m.depContainers["inventory"].GetRequestReplyService("check-stock"); err != nil {
					return err
				}
			}
			// payment is declared but never looked up.
			return nil
		},
	}
	lm, logger := newBoundaryManager(t, false, newProvider("inventory"), newProvider("payment"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	reports := lm.ModuleBoundaries()
	if len(reports) != 3 || reports[0].Module != "inventory" || reports[1].Module != "order" || reports[2].Module != "payment" {
		t.Fatalf("reports = %+v", reports)
	}
	order := reports[1]
	if len(order.Declared) != 2 || len(order.Consumed) != 1 {
		t.Errorf("declared/consumed = %v / %v", order.Declared, order.Consumed)
	}
	if len(order.Observed) != 1 || order.Observed[0].Module != "inventory" || order.Observed[0].Lookups != 2 {
		t.Errorf("Observed = %+v", order.Observed)
	}
	if len(order.UnusedDependencies) != 1 || order.UnusedDependencies[0] != "payment" {
		t.Errorf("UnusedDependencies = %v", order.UnusedDependencies)
	}
	if len(order.Violations) != 0 {
		t.Errorf("Violations = %+v", order.Violations)
	}
	if consumer.eventBus == nil {
		t.Fatal("event bus not set")
	}
	if lm.ModuleEventBus("order") != consumer.eventBus || lm.ModuleEventBus("nope") != nil {
		t.Error("ModuleEventBus must return the bus handed to the module")
	}

	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !logger.hasEntryContaining("INFO: Declared dependency never used") || !logger.hasEntryContaining("payment") {
		t.Error("expected an Info line about the unused dependency")
	}
}

func TestModuleBoundariesNonStrictRecordsViolations(t *testing.T) {
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
		onStart: func(m *boundaryConsumer) error {
			// Register on a dependency: recorded, delegated.
			if err := m.depContainers["inventory"].RegisterRequestReplyService("sneaky", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }); err != nil {
				return err
			}
			// Bypass the port with a raw publish to an undeclared module.
			return m.eventBus.Publish("services.payment.process", []byte("x"))
		},
	}
	lm, logger := newBoundaryManager(t, false, newProvider("inventory"), newProvider("payment"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var order types.ModuleBoundaryReport
	for _, r := range lm.ModuleBoundaries() {
		if r.Module == "order" {
			order = r
		}
	}
	if len(order.Violations) != 2 {
		t.Fatalf("Violations = %+v", order.Violations)
	}
	if order.Violations[0].Kind != types.BoundaryViolationRegisterOnDependency || order.Violations[1].Kind != types.BoundaryViolationUndeclaredServiceSubject {
		t.Errorf("kinds = %+v", order.Violations)
	}
	if !lm.GetServiceContainer("inventory").Has("sneaky") {
		t.Error("non-strict registration must have gone through")
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !logger.hasWarnContaining("Module boundary violations recorded") {
		t.Error("expected a Warn summary at Stop")
	}
}

func TestModuleBoundariesStrictFailsStart(t *testing.T) {
	t.Run("register on dependency", func(t *testing.T) {
		consumer := &boundaryConsumer{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			onStart: func(m *boundaryConsumer) error {
				return m.depContainers["inventory"].RegisterRequestReplyService("sneaky", func(context.Context, *types.Msg) ([]byte, error) { return nil, nil })
			},
		}
		lm, _ := newBoundaryManager(t, true, newProvider("inventory"), consumer)
		err := lm.Start(context.Background())
		if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
		if lm.GetServiceContainer("inventory").Has("sneaky") {
			t.Error("strict mode must not touch the dependency's container")
		}
	})
	t.Run("undeclared service subject", func(t *testing.T) {
		consumer := &boundaryConsumer{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			onStart: func(m *boundaryConsumer) error {
				return m.eventBus.Publish("services.payment.process", []byte("x"))
			},
		}
		lm, _ := newBoundaryManager(t, true, newProvider("inventory"), newProvider("payment"), consumer)
		if err := lm.Start(context.Background()); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})
	t.Run("declared targets pass", func(t *testing.T) {
		consumer := &boundaryConsumer{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			onStart: func(m *boundaryConsumer) error {
				if err := m.eventBus.Publish("services.inventory.check-stock", []byte("x")); err != nil {
					return err
				}
				if err := m.eventBus.Publish("services.order.own", []byte("x")); err != nil {
					return err
				}
				return m.eventBus.Publish("events.order.v1.created", []byte("x"))
			},
		}
		lm, _ := newBoundaryManager(t, true, newProvider("inventory"), consumer)
		if err := lm.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

func TestModuleBoundariesPluginSubjectsAllowed(t *testing.T) {
	plugin := &pluginWithService{mockEventBusAwarePluginModule{name: "kv-store"}}
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order"},
		onStart: func(m *boundaryConsumer) error {
			return m.eventBus.Publish("services.kv-store.lookup", []byte("x"))
		},
	}
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	if err := pluginReg.Register(plugin, "kv"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(consumer); err != nil {
		t.Fatal(err)
	}
	lm := NewLifecycleManager(reg, pluginReg, newMockEventBus(), nil, logger, 0).(*lifecycleManager)
	lm.SetStrictModuleBoundaries(true)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("plugin service subjects must be allowed: %v", err)
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(lm.ModuleBoundaries()) != 0 || lm.ModuleEventBus("order") != nil {
		t.Error("Stop must clear the boundary reports and scoped buses")
	}
}

func TestModuleBoundariesStrictFailureIsRolledBack(t *testing.T) {
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
		onStart: func(m *boundaryConsumer) error {
			return m.eventBus.Publish("services.payment.process", []byte("x"))
		},
	}
	provider := newProvider("inventory")
	lm, _ := newBoundaryManager(t, true, provider, consumer)
	if err := lm.Start(context.Background()); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
	if !provider.stopCalled {
		t.Error("the started dependency must be rolled back")
	}
	for _, r := range lm.ModuleBoundaries() {
		if r.Module == "order" {
			t.Error("a module whose start failed must not be reported")
		}
	}
	if lm.ModuleEventBus("order") != nil {
		t.Error("the failed module's scoped bus must be dropped")
	}
}
