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
			// On the bus too, a service of a dependency must be consumed.
			contracts: []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}},
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

// A module that implements ServiceConsumerModule declares everything it
// consumes: a lookup outside ConsumeServices is recorded (non-strict) and the
// contracts it never used are reported.
func TestModuleBoundariesUndeclaredServiceUseRecorded(t *testing.T) {
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory", "payment"}},
		contracts:  []types.BaseServiceDefinition{checkStock},
		onStart: func(m *boundaryConsumer) error {
			// payment/check-stock is registered by the provider but not declared here.
			_, err := m.depContainers["payment"].GetRequestReplyService("check-stock")
			return err
		},
	}
	lm, logger := newBoundaryManager(t, false, newProvider("inventory"), newProvider("payment"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("non-strict Start must succeed: %v", err)
	}
	var order types.ModuleBoundaryReport
	for _, r := range lm.ModuleBoundaries() {
		if r.Module == "order" {
			order = r
		}
	}
	if len(order.Violations) != 1 {
		t.Fatalf("Violations = %+v", order.Violations)
	}
	v := order.Violations[0]
	if v.Kind != types.BoundaryViolationUndeclaredServiceUse || v.Dependency != "payment" || v.Subject != "services.payment.check-stock" {
		t.Errorf("violation = %+v", v)
	}
	if len(order.Observed) != 1 || order.Observed[0].Module != "payment" {
		t.Errorf("the lookup went through and must be observed: %+v", order.Observed)
	}
	if len(order.UnusedContracts) != 1 || order.UnusedContracts[0] != checkStock {
		t.Errorf("UnusedContracts = %+v", order.UnusedContracts)
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !logger.hasWarnContaining("Module boundary violations recorded") {
		t.Error("expected a Warn summary at Stop")
	}
	if !logger.hasEntryContaining("INFO: Consumed service contract never used during this run") {
		t.Error("expected an Info line about the unused contract")
	}
}

func TestModuleBoundariesUndeclaredServiceUseStrict(t *testing.T) {
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory", "payment"}},
		contracts:  []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}},
		onStart: func(m *boundaryConsumer) error {
			if _, err := m.depContainers["inventory"].GetRequestReplyService("check-stock"); err != nil {
				return err
			}
			_, err := m.depContainers["payment"].GetRequestReplyService("check-stock")
			return err
		},
	}
	lm, _ := newBoundaryManager(t, true, newProvider("inventory"), newProvider("payment"), consumer)
	err := lm.Start(context.Background())
	if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
}

// plainConsumer does not implement ServiceConsumerModule: its lookups are
// observed but never checked against a list, even in strict mode.
type plainConsumer struct {
	mockModule
}

func (m *plainConsumer) Start(ctx context.Context) error {
	if err := m.mockModule.Start(ctx); err != nil {
		return err
	}
	_, err := m.depContainers["inventory"].GetRequestReplyService("check-stock")
	return err
}

func TestModuleBoundariesWithoutConsumeServicesNotChecked(t *testing.T) {
	consumer := &plainConsumer{mockModule: mockModule{name: "order", dependencies: []string{"inventory"}}}
	lm, _ := newBoundaryManager(t, true, newProvider("inventory"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, r := range lm.ModuleBoundaries() {
		if r.Module == "order" && (len(r.Observed) != 1 || len(r.UnusedContracts) != 0) {
			t.Errorf("report = %+v", r)
		}
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// reportOf returns the boundary report of module and fails the test when the
// manager has none, so that an assertion on it cannot be skipped.
func reportOf(t *testing.T, lm *lifecycleManager, module string) types.ModuleBoundaryReport {
	t.Helper()
	for _, r := range lm.ModuleBoundaries() {
		if r.Module == module {
			return r
		}
	}
	t.Fatalf("no boundary report for module %q", module)
	return types.ModuleBoundaryReport{}
}

// wiredConsumer builds its ConsumeServices() list from the dependency
// containers it was handed, as a module with one list per adapter does, and
// may look a service up while it is being wired.
type wiredConsumer struct {
	mockModule
	wired        []string
	consumeCalls int
	lookUpOnWire string
	onStart      func(m *wiredConsumer) error
}

func (m *wiredConsumer) SetDependencyServiceContainer(name string, c types.ServiceContainer) {
	m.mockModule.SetDependencyServiceContainer(name, c)
	m.wired = append(m.wired, name)
	if m.lookUpOnWire != "" {
		// The client is kept for later; the error does not matter here.
		_, _ = c.GetQueueGroupService(m.lookUpOnWire)
	}
}

func (m *wiredConsumer) ConsumeServices() []types.BaseServiceDefinition {
	m.consumeCalls++
	defs := make([]types.BaseServiceDefinition, 0, len(m.wired))
	for _, dep := range m.wired {
		defs = append(defs, types.BaseServiceDefinition{ModuleName: dep, Name: "check-stock", Type: types.ServiceTypeRequestReply})
	}
	return defs
}

func (m *wiredConsumer) Start(ctx context.Context) error {
	if err := m.mockModule.Start(ctx); err != nil {
		return err
	}
	if m.onStart != nil {
		return m.onStart(m)
	}
	return nil
}

// The list validated at step 1.5 is the list the lookups are checked against:
// ConsumeServices() is called once, after the dependency containers are set,
// so a list built from the wiring is complete when it is enforced.
func TestModuleBoundariesConsumedListIsReadOnceAfterWiring(t *testing.T) {
	consumer := &wiredConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory", "payment"}},
		onStart: func(m *wiredConsumer) error {
			for _, dep := range []string{"inventory", "payment"} {
				if _, err := m.depContainers[dep].GetRequestReplyService("check-stock"); err != nil {
					return err
				}
			}
			return nil
		},
	}
	lm, _ := newBoundaryManager(t, true, newProvider("inventory"), newProvider("payment"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("strict Start must succeed for a list built from the wiring: %v", err)
	}
	if consumer.consumeCalls != 1 {
		t.Errorf("ConsumeServices() was called %d time(s), want 1", consumer.consumeCalls)
	}
	if r := reportOf(t, lm, "order"); len(r.Consumed) != 2 || len(r.UnusedContracts) != 0 || len(r.Violations) != 0 {
		t.Errorf("order report = %+v", r)
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// A service looked up in SetDependencyServiceContainer is checked when the
// list is declared: strict mode fails the start, non-strict records it.
func TestModuleBoundariesLookupWhileWiredIsChecked(t *testing.T) {
	t.Run("strict", func(t *testing.T) {
		consumer := &wiredConsumer{mockModule: mockModule{name: "order", dependencies: []string{"inventory"}}, lookUpOnWire: "on-event"}
		provider := newProvider("inventory")
		lm, _ := newBoundaryManager(t, true, provider, consumer)
		err := lm.Start(context.Background())
		if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) || !errors.Is(err, monoerrors.ErrModuleStartFailed) {
			t.Fatalf("expected a start failure wrapping ErrModuleBoundaryViolation, got %v", err)
		}
		if consumer.startCalled {
			t.Error("the module must not be started")
		}
		if !provider.stopCalled {
			t.Error("the started dependency must be rolled back")
		}
	})
	t.Run("non-strict", func(t *testing.T) {
		consumer := &wiredConsumer{mockModule: mockModule{name: "order", dependencies: []string{"inventory"}}, lookUpOnWire: "on-event"}
		lm, _ := newBoundaryManager(t, false, newProvider("inventory"), consumer)
		if err := lm.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		r := reportOf(t, lm, "order")
		if len(r.Violations) != 1 || r.Violations[0].Kind != types.BoundaryViolationUndeclaredServiceUse ||
			r.Violations[0].Subject != "services.inventory.on-event" || r.Violations[0].Count != 1 {
			t.Errorf("Violations = %+v", r.Violations)
		}
		if err := lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

// Probing for an optional service that the dependency does not provide is not
// an undeclared use: the module could not declare it (the start would fail).
func TestModuleBoundariesMissingServiceProbeIsNotAViolation(t *testing.T) {
	consumer := &boundaryConsumer{
		mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
		contracts:  []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}},
		onStart: func(m *boundaryConsumer) error {
			if _, err := m.depContainers["inventory"].GetRequestReplyService("check-stock-v2"); !errors.Is(err, monoerrors.ErrServiceNotFound) {
				return errors.New("expected ErrServiceNotFound for the optional service")
			}
			_, err := m.depContainers["inventory"].GetRequestReplyService("check-stock")
			return err
		},
	}
	lm, logger := newBoundaryManager(t, false, newProvider("inventory"), consumer)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if r := reportOf(t, lm, "order"); len(r.Violations) != 0 || len(r.Observed) != 1 {
		t.Errorf("only the lookup that succeeded leaves a trace: %+v", r)
	}
	if logger.hasWarnContaining("Module boundary violation") {
		t.Error("a probe for a missing service must not be warned about")
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ConsumeServices() also covers the event bus: a send to a service subject of
// a declared dependency is a use of its contract or an undeclared use.
func TestModuleBoundariesConsumedListCoversTheEventBus(t *testing.T) {
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	newConsumer := func() *boundaryConsumer {
		return &boundaryConsumer{
			mockModule: mockModule{name: "order", dependencies: []string{"inventory"}},
			contracts:  []types.BaseServiceDefinition{checkStock},
			onStart: func(m *boundaryConsumer) error {
				if err := m.eventBus.Publish("services.inventory.check-stock", []byte("x")); err != nil {
					return err
				}
				return m.eventBus.Publish("services.inventory.on-event", []byte("x"))
			},
		}
	}
	t.Run("non-strict", func(t *testing.T) {
		lm, _ := newBoundaryManager(t, false, newProvider("inventory"), newConsumer())
		if err := lm.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		r := reportOf(t, lm, "order")
		if len(r.UnusedContracts) != 0 || len(r.UnusedDependencies) != 0 {
			t.Errorf("a contract reached over the bus is used: %+v", r)
		}
		if len(r.Violations) != 1 || r.Violations[0].Kind != types.BoundaryViolationUndeclaredServiceUse ||
			r.Violations[0].Subject != "services.inventory.on-event" {
			t.Errorf("Violations = %+v", r.Violations)
		}
		if err := lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
	t.Run("strict", func(t *testing.T) {
		lm, _ := newBoundaryManager(t, true, newProvider("inventory"), newConsumer())
		if err := lm.Start(context.Background()); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})
}
