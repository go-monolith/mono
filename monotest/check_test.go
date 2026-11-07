package monotest_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/monotest"
	"github.com/go-monolith/mono/pkg/types"
)

// recordingTB captures the failures CheckModule reports.
type recordingTB struct {
	testing.TB
	mu     sync.Mutex
	errors []string
	fatal  bool
}

func (r *recordingTB) Helper()             {}
func (r *recordingTB) Log(...any)          {}
func (r *recordingTB) Logf(string, ...any) {}
func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, sprintf(format, args...))
}
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.errors = append(r.errors, sprintf(format, args...))
	r.fatal = true
	r.mu.Unlock()
	panic(fatalSentinel{})
}
func (r *recordingTB) Fatal(args ...any) { r.Fatalf("%v", args...) }

type fatalSentinel struct{}

func sprintf(format string, args ...any) string {
	return strings.TrimSpace(fmtSprintf(format, args...))
}

func runCheck(t *testing.T, module types.Module, opts ...monotest.Option) *recordingTB {
	t.Helper()
	rec := &recordingTB{TB: t}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(fatalSentinel); !ok {
					panic(r)
				}
			}
		}()
		monotest.CheckModule(rec, module, opts...)
	}()
	return rec
}

func (r *recordingTB) has(substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errors {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func exercise(ctx context.Context, h *monotest.Harness) error {
	_, err := placeOrder.Call(ctx, h.Container(), stockReq{ProductID: "ab"})
	return err
}

func TestCheckModuleConformingModulePasses(t *testing.T) {
	bus := monotest.NewEventBus()
	rec := runCheck(t, &orderModule{}, monotest.WithDependency("inventory", newInventory(t, bus)), monotest.WithExercise(exercise))
	if len(rec.errors) != 0 {
		t.Fatalf("unexpected failures: %v", rec.errors)
	}
}

func TestCheckModuleReportsViolations(t *testing.T) {
	bus := monotest.NewEventBus()
	rec := runCheck(t, &orderModule{bypass: true, mutate: true, undeclared: true},
		monotest.WithDependency("inventory", newInventory(t, bus)), monotest.WithExercise(exercise))
	for _, want := range []string{
		"rule 5: boundary violation register-on-dependency",
		"rule 5: boundary violation undeclared-service-subject on payment (services.payment.charge)",
		"rule 7: event published on \"events.order.v1.shadow\" is not in EmitEvents()",
	} {
		if !rec.has(want) {
			t.Errorf("missing failure %q in %v", want, rec.errors)
		}
	}
	if rec.fatal {
		t.Error("violations must not be fatal")
	}
}

func TestCheckModuleUnusedDependency(t *testing.T) {
	bus := monotest.NewEventBus()
	// No exercise: the adapters never run, so inventory is never looked up.
	rec := runCheck(t, &orderModule{}, monotest.WithDependency("inventory", newInventory(t, bus)))
	if !rec.has("rule 6: dependency \"inventory\" is declared but no service of it was looked up") || !rec.has("WithExercise") {
		t.Fatalf("expected the unused-dependency failure with a hint, got %v", rec.errors)
	}
	rec = runCheck(t, &orderModule{}, monotest.WithDependency("inventory", newInventory(t, bus)), monotest.WithOrderingOnlyDependency("inventory"))
	if rec.has("rule 6") {
		t.Fatalf("ordering-only dependency must be exempt, got %v", rec.errors)
	}
}

func TestCheckModuleMissingStandIn(t *testing.T) {
	rec := runCheck(t, &orderModule{})
	if !rec.fatal || !rec.has("rule 2: no stand-in container for dependency \"inventory\"") {
		t.Fatalf("expected a fatal rule 2 failure, got %v", rec.errors)
	}
}

// badModule breaks the cheap rules: bad name, self and duplicate dependency,
// no services although it is a provider, slow Stop.
type badModule struct{}

func (badModule) Name() string           { return "Bad_Module" }
func (badModule) Dependencies() []string { return []string{"Bad_Module", "dep", "dep"} }
func (badModule) SetDependencyServiceContainer(string, types.ServiceContainer) {
}
func (badModule) RegisterServices(types.ServiceContainer) error { return nil }
func (badModule) Start(context.Context) error                   { return nil }
func (badModule) Stop(ctx context.Context) error {
	time.Sleep(200 * time.Millisecond)
	return nil
}

func TestCheckModuleCheapRules(t *testing.T) {
	bus := monotest.NewEventBus()
	dep := monotest.NewContainer("dep", bus)
	self := monotest.NewContainer("Bad_Module", bus)
	rec := runCheck(t, badModule{},
		monotest.WithDependency("dep", dep), monotest.WithDependency("Bad_Module", self),
		monotest.WithOrderingOnlyDependency("dep"), monotest.WithOrderingOnlyDependency("Bad_Module"),
		monotest.WithStopTimeout(50*time.Millisecond))
	for _, want := range []string{
		"rule 1: Name() \"Bad_Module\" must be kebab-case",
		"rule 2: Dependencies() lists the module itself",
		"rule 2: Dependencies() lists \"dep\" twice",
		"rule 3: the module implements ServiceProviderModule but RegisterServices registered no service",
		"rule 8: Stop did not return within 50ms",
	} {
		if !rec.has(want) {
			t.Errorf("missing failure %q in %v", want, rec.errors)
		}
	}
}

// badEventModule declares an event with an invalid subject.
type badEventModule struct{}

func (badEventModule) Name() string                { return "bad-event" }
func (badEventModule) SetEventBus(types.EventBus)  {}
func (badEventModule) Start(context.Context) error { return nil }
func (badEventModule) Stop(context.Context) error  { return nil }
func (badEventModule) EmitEvents() []types.BaseEventDefinition {
	return []types.BaseEventDefinition{{ModuleName: "bad-event", Name: "X", Subject: "not-an-event", Version: "v1"}}
}

func TestCheckModuleInvalidEventSubject(t *testing.T) {
	rec := runCheck(t, badEventModule{})
	if !rec.has("rule 7: emitted event bad-event.X has an invalid subject") {
		t.Fatalf("expected rule 7 failure, got %v", rec.errors)
	}
	// The framework's registry rejects it too, so Start fails as well.
	if !rec.has("rule 8: Start failed") {
		t.Fatalf("expected Start failure, got %v", rec.errors)
	}
}

func TestCheckModuleNilModule(t *testing.T) {
	rec := runCheck(t, nil)
	if !rec.fatal || !rec.has("module cannot be nil") {
		t.Fatalf("got %v", rec.errors)
	}
}
