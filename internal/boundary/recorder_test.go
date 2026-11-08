package boundary_test

import (
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
	if rep.Observed == nil || rep.UnusedDependencies == nil || rep.Violations == nil {
		t.Error("slices must be non-nil for JSON consumers")
	}
}
