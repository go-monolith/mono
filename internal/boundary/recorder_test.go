package boundary_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/go-monolith/mono/internal/boundary"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

type recLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *recLogger) Debug(string, ...any) {}
func (l *recLogger) Info(string, ...any)  {}
func (l *recLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	b.WriteString(msg)
	for _, a := range args {
		b.WriteString(" ")
		switch v := a.(type) {
		case string:
			b.WriteString(v)
		default:
			b.WriteString("?")
		}
	}
	l.warns = append(l.warns, b.String())
}
func (l *recLogger) Error(string, ...any)           {}
func (l *recLogger) With(...any) types.Logger       { return l }
func (l *recLogger) WithModule(string) types.Logger { return l }
func (l *recLogger) WithError(error) types.Logger   { return l }
func (l *recLogger) count() int                     { l.mu.Lock(); defer l.mu.Unlock(); return len(l.warns) }

func TestRecorderAllowed(t *testing.T) {
	r := boundary.New("order", []string{"inventory", "payment", "inventory"}, []string{"kv-store"}, nil, false, nil)
	for _, m := range []string{"order", "inventory", "payment", "kv-store"} {
		if !r.Allowed(m) {
			t.Errorf("%s should be allowed", m)
		}
	}
	if r.Allowed("shipping") {
		t.Error("shipping should not be allowed")
	}
	if got := r.Report().Declared; len(got) != 2 || got[0] != "inventory" || got[1] != "payment" {
		t.Errorf("Declared = %v (duplicates dropped, order kept)", got)
	}
	if r.Module() != "order" || r.Strict() {
		t.Error("module or strict flag wrong")
	}
}

func TestRecorderLookupsAndUnused(t *testing.T) {
	consumed := []types.BaseServiceDefinition{{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}}
	r := boundary.New("order", []string{"inventory", "payment", "notification"}, nil, consumed, false, nil)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
		}()
	}
	wg.Wait()
	r.Lookup("payment", "process", types.ServiceTypeRequestReply)
	r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)

	rep := r.Report()
	if len(rep.Observed) != 3 {
		t.Fatalf("Observed = %+v", rep.Observed)
	}
	// sorted by module then service
	if rep.Observed[0].Service != "check-stock" || rep.Observed[0].Lookups != 50 || rep.Observed[0].Type != types.ServiceTypeRequestReply {
		t.Errorf("first use = %+v", rep.Observed[0])
	}
	if rep.Observed[1].Service != "reserve" || rep.Observed[2].Module != "payment" {
		t.Errorf("order wrong: %+v", rep.Observed)
	}
	if len(rep.UnusedDependencies) != 1 || rep.UnusedDependencies[0] != "notification" {
		t.Errorf("UnusedDependencies = %v", rep.UnusedDependencies)
	}
	if len(rep.Consumed) != 1 || rep.Consumed[0] != consumed[0] {
		t.Errorf("Consumed = %+v", rep.Consumed)
	}
	if len(rep.Violations) != 0 {
		t.Errorf("Violations = %+v", rep.Violations)
	}
}

func TestRecorderViolationNonStrict(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, logger)

	for i := 0; i < 3; i++ {
		if err := r.Violation(types.BoundaryViolationUndeclaredServiceSubject, "shipping", "services.shipping.quote"); err != nil {
			t.Fatalf("non-strict Violation must return nil, got %v", err)
		}
	}
	if err := r.Violation(types.BoundaryViolationRegisterOnDependency, "inventory", ""); err != nil {
		t.Fatalf("non-strict Violation must return nil, got %v", err)
	}
	if logger.count() != 2 {
		t.Errorf("expected one warning per distinct violation, got %d", logger.count())
	}

	rep := r.Report()
	if len(rep.Violations) != 2 {
		t.Fatalf("Violations = %+v", rep.Violations)
	}
	// sorted by kind: register-on-dependency < undeclared-service-subject
	if rep.Violations[0].Kind != types.BoundaryViolationRegisterOnDependency || rep.Violations[0].Count != 1 {
		t.Errorf("first violation = %+v", rep.Violations[0])
	}
	v := rep.Violations[1]
	if v.Kind != types.BoundaryViolationUndeclaredServiceSubject || v.Dependency != "shipping" || v.Subject != "services.shipping.quote" || v.Count != 3 || v.First.IsZero() {
		t.Errorf("second violation = %+v", v)
	}
}

func TestRecorderViolationStrict(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", nil, nil, nil, true, logger)
	err := r.Violation(types.BoundaryViolationForeignServiceSubscription, "payment", "services.payment.process")
	if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
		t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
	}
	if !strings.Contains(err.Error(), "order") || !strings.Contains(err.Error(), "services.payment.process") {
		t.Errorf("error should name module and subject: %v", err)
	}
	if len(r.Report().Violations) != 0 || logger.count() != 0 {
		t.Error("strict mode must neither record nor warn")
	}
}

func TestRecorderReportIsSnapshot(t *testing.T) {
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, nil)
	rep := r.Report()
	rep.Declared[0] = "changed"
	if r.Report().Declared[0] != "inventory" {
		t.Error("Report must copy Declared")
	}
	if rep.Observed == nil || rep.UnusedDependencies == nil || rep.UnusedContracts == nil || rep.Violations == nil {
		t.Error("slices must be non-nil for JSON consumers")
	}
	encoded, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{`"observed":[]`, `"unused_dependencies":[`, `"unused_contracts":[]`, `"violations":[]`} {
		if !strings.Contains(string(encoded), want) {
			t.Errorf("JSON report should contain %s: %s", want, encoded)
		}
	}
}

func TestRecorderCheckUse(t *testing.T) {
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	consumed := []types.BaseServiceDefinition{checkStock}

	t.Run("not enforced: every lookup is allowed", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory"}, nil, consumed, false, nil)
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Fatalf("CheckUse = %v", err)
		}
		if got := r.Report().Violations; len(got) != 0 {
			t.Errorf("Violations = %+v", got)
		}
	})

	t.Run("enforced, non-strict: recorded and let through", func(t *testing.T) {
		logger := &recLogger{}
		r := boundary.New("order", []string{"inventory"}, nil, consumed, false, logger).EnforceConsumed()
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); err != nil {
			t.Fatalf("declared contract: CheckUse = %v", err)
		}
		// Same name, other kind: not the declared contract.
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeQueueGroup); err != nil {
			t.Fatalf("non-strict CheckUse must return nil, got %v", err)
		}
		for i := 0; i < 2; i++ {
			if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
				t.Fatalf("non-strict CheckUse must return nil, got %v", err)
			}
		}
		// Channel services have no contract type and are never checked.
		if err := r.CheckUse("inventory", "feed", types.ServiceTypeChannel); err != nil {
			t.Fatalf("channel: CheckUse = %v", err)
		}

		got := r.Report().Violations
		if len(got) != 2 {
			t.Fatalf("Violations = %+v", got)
		}
		// sorted by subject: check-stock < reserve
		if got[0].Kind != types.BoundaryViolationUndeclaredServiceUse || got[0].Dependency != "inventory" ||
			got[0].Subject != "services.inventory.check-stock" || got[0].Count != 1 {
			t.Errorf("first violation = %+v", got[0])
		}
		if got[1].Subject != "services.inventory.reserve" || got[1].Count != 2 {
			t.Errorf("second violation = %+v", got[1])
		}
		if logger.count() != 2 {
			t.Errorf("expected one warning per distinct violation, got %d", logger.count())
		}
	})

	t.Run("enforced, strict: rejected", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory"}, nil, consumed, true, nil).EnforceConsumed()
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); err != nil {
			t.Fatalf("declared contract: CheckUse = %v", err)
		}
		err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply)
		if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
		if !strings.Contains(err.Error(), "undeclared-service-use") || !strings.Contains(err.Error(), "services.inventory.reserve") {
			t.Errorf("error should name the kind and the service subject: %v", err)
		}
	})

	t.Run("enforced with an empty list: every lookup is undeclared", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory"}, nil, nil, true, nil).EnforceConsumed()
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})
}

func TestRecorderUnusedContracts(t *testing.T) {
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	reserve := types.BaseServiceDefinition{ModuleName: "inventory", Name: "reserve", Type: types.ServiceTypeQueueGroup}
	charge := types.BaseServiceDefinition{ModuleName: "payment", Name: "charge", Type: types.ServiceTypeRequestReply}
	// A plugin contract: lookups on plugin containers are not observed, so it
	// must never be reported as unused.
	kvGet := types.BaseServiceDefinition{ModuleName: "kv-store", Name: "get", Type: types.ServiceTypeRequestReply}
	consumed := []types.BaseServiceDefinition{checkStock, reserve, charge, kvGet, reserve}

	r := boundary.New("order", []string{"inventory", "payment"}, []string{"kv-store"}, consumed, false, nil).EnforceConsumed()
	if got := r.Report().UnusedContracts; len(got) != 3 || got[0] != checkStock || got[1] != reserve || got[2] != charge {
		t.Fatalf("before any lookup, UnusedContracts = %+v (declaration order, no duplicates, no plugin contract)", got)
	}

	r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
	r.Lookup("payment", "charge", types.ServiceTypeRequestReply)
	if got := r.Report().UnusedContracts; len(got) != 1 || got[0] != reserve {
		t.Fatalf("UnusedContracts = %+v", got)
	}

	r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
	if got := r.Report().UnusedContracts; len(got) != 0 {
		t.Fatalf("UnusedContracts = %+v", got)
	}
}

// A violation repeated from many goroutines is counted exactly and warned
// about once.
func TestRecorderViolationConcurrent(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, logger).EnforceConsumed()

	const goroutines, each = 16, 200
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
					t.Errorf("non-strict CheckUse must return nil, got %v", err)
					return
				}
				_ = r.Report()
			}
		}()
	}
	wg.Wait()

	got := r.Report().Violations
	if len(got) != 1 || got[0].Count != goroutines*each || got[0].First.IsZero() {
		t.Fatalf("Violations = %+v, want one with count %d", got, goroutines*each)
	}
	if logger.count() != 1 {
		t.Errorf("expected exactly one warning, got %d", logger.count())
	}
}

// A repeated undeclared use is on the request path of a non-strict module
// with an incomplete ConsumeServices(): it must not build the subject again.
func TestRecorderCheckUseRepeatedDoesNotAllocate(t *testing.T) {
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, nil).EnforceConsumed()
	if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
		t.Fatalf("CheckUse = %v", err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Errorf("CheckUse = %v", err)
		}
	})
	if allocs != 0 {
		t.Errorf("a repeated undeclared use allocated %.0f time(s) per call, want 0", allocs)
	}
	// The same name with another kind is the same violation (same subject).
	if err := r.CheckUse("inventory", "reserve", types.ServiceTypeQueueGroup); err != nil {
		t.Fatalf("CheckUse = %v", err)
	}
	got := r.Report().Violations
	if len(got) != 1 || got[0].Subject != "services.inventory.reserve" || got[0].Count != 103 {
		t.Fatalf("Violations = %+v, want one with count 103 (AllocsPerRun adds a warm-up call)", got)
	}
}

// An undeclared use and the same violation reported directly are one entry.
func TestRecorderCheckUseSharesTheEntryWithViolation(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, logger).EnforceConsumed()
	if err := r.Violation(types.BoundaryViolationUndeclaredServiceUse, "inventory", "services.inventory.reserve"); err != nil {
		t.Fatalf("Violation = %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Fatalf("CheckUse = %v", err)
		}
	}
	got := r.Report().Violations
	if len(got) != 1 || got[0].Count != 3 {
		t.Fatalf("Violations = %+v, want one with count 3", got)
	}
	if logger.count() != 1 {
		t.Errorf("expected one warning, got %d", logger.count())
	}
}

func BenchmarkCheckUseRepeatedUndeclared(b *testing.B) {
	r := boundary.New("order", []string{"inventory"}, nil, nil, false, nil).EnforceConsumed()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			b.Fatal(err)
		}
	}
}

// plainModule implements types.Module only.
type plainModule struct{ name string }

func (m *plainModule) Name() string                { return m.name }
func (m *plainModule) Start(context.Context) error { return nil }
func (m *plainModule) Stop(context.Context) error  { return nil }

// dependentModule declares dependencies but not what it consumes.
type dependentModule struct {
	plainModule
	deps []string
}

func (m *dependentModule) Dependencies() []string                                       { return m.deps }
func (m *dependentModule) SetDependencyServiceContainer(string, types.ServiceContainer) {}

// consumerModule declares dependencies and the contracts it consumes.
type consumerModule struct {
	dependentModule
	consumed []types.BaseServiceDefinition
}

func (m *consumerModule) ConsumeServices() []types.BaseServiceDefinition { return m.consumed }

// pluginModule is a plugin known by its module name.
type pluginModule struct{ plainModule }

func (p *pluginModule) SetContainer(types.ServiceContainer) {}
func (p *pluginModule) Container() types.ServiceContainer   { return nil }

func TestForModule(t *testing.T) {
	checkStock := types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	// Plugins are registered under an alias; the module name is what counts.
	plugins := map[string]types.PluginModule{"storage": &pluginModule{plainModule{name: "kv-store"}}}

	t.Run("a module without dependencies", func(t *testing.T) {
		r := boundary.ForModule(&plainModule{name: "order"}, nil, false, nil)
		rep := r.Report()
		if r.Module() != "order" || r.Strict() || len(rep.Declared) != 0 || len(rep.Consumed) != 0 {
			t.Errorf("recorder: module %q strict %v, report %+v", r.Module(), r.Strict(), rep)
		}
		if r.Allowed("inventory") {
			t.Error("an undeclared module must not be allowed")
		}
	})

	t.Run("dependencies and plugins are allowed; lookups are not checked", func(t *testing.T) {
		r := boundary.ForModule(&dependentModule{plainModule{name: "order"}, []string{"inventory", "payment", "inventory"}}, plugins, true, nil)
		if !r.Strict() {
			t.Error("strict must be passed on")
		}
		if got := r.Report().Declared; len(got) != 2 || got[0] != "inventory" || got[1] != "payment" {
			t.Errorf("Declared = %v", got)
		}
		for _, name := range []string{"order", "inventory", "payment", "kv-store"} {
			if !r.Allowed(name) {
				t.Errorf("%s should be allowed", name)
			}
		}
		if r.Allowed("storage") {
			t.Error("a plugin alias is not a module name")
		}
		// The module does not implement ServiceConsumerModule.
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Errorf("CheckUse = %v", err)
		}
	})

	t.Run("a module that declares what it consumes is held to the list", func(t *testing.T) {
		m := &consumerModule{dependentModule{plainModule{name: "order"}, []string{"inventory"}}, []types.BaseServiceDefinition{checkStock}}
		r := boundary.ForModule(m, plugins, true, nil)
		if got := r.Report().Consumed; len(got) != 1 || got[0] != checkStock {
			t.Errorf("Consumed = %v", got)
		}
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); err != nil {
			t.Errorf("declared contract: CheckUse = %v", err)
		}
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Errorf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})

	t.Run("an empty list is a complete list", func(t *testing.T) {
		m := &consumerModule{dependentModule{plainModule{name: "order"}, []string{"inventory"}}, nil}
		r := boundary.ForModule(m, nil, true, nil)
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Errorf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})
}
