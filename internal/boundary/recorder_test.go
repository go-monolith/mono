package boundary_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	r := boundary.New("order", []string{"inventory", "payment", "inventory"}, []string{"kv-store"}, false, nil)
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
	r := boundary.New("order", []string{"inventory", "payment", "notification"}, nil, false, nil)

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
	// No list was declared: nothing is consumed and nothing is checked.
	if rep.Consumed != nil {
		t.Errorf("Consumed = %+v", rep.Consumed)
	}
	if len(rep.Violations) != 0 {
		t.Errorf("Violations = %+v", rep.Violations)
	}
}

func TestRecorderViolationNonStrict(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", []string{"inventory"}, nil, false, logger)

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
	r := boundary.New("order", nil, nil, true, logger)
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
	r := boundary.New("order", []string{"inventory"}, nil, false, nil)
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

// declare builds a recorder whose consumed list is consumed.
func declare(t testing.TB, r *boundary.Recorder, consumed ...types.BaseServiceDefinition) *boundary.Recorder {
	t.Helper()
	if err := r.DeclareConsumed(consumed); err != nil {
		t.Fatalf("DeclareConsumed = %v", err)
	}
	return r
}

var (
	checkStock = types.BaseServiceDefinition{ModuleName: "inventory", Name: "check-stock", Type: types.ServiceTypeRequestReply}
	reserveQG  = types.BaseServiceDefinition{ModuleName: "inventory", Name: "reserve", Type: types.ServiceTypeQueueGroup}
)

func TestRecorderUndeclaredUse(t *testing.T) {
	t.Run("no list declared: every lookup is allowed", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory"}, nil, true, nil)
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Fatalf("CheckUse = %v", err)
		}
		r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
		if got := r.Report().Violations; len(got) != 0 {
			t.Errorf("Violations = %+v", got)
		}
	})

	t.Run("non-strict: recorded by the successful lookup, never before it", func(t *testing.T) {
		logger := &recLogger{}
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, logger), checkStock)

		// The check before the lookup never rejects and records nothing: a
		// probe for a service that turns out not to exist is not a violation.
		for _, name := range []string{"check-stock", "reserve", "missing"} {
			if err := r.CheckUse("inventory", name, types.ServiceTypeRequestReply); err != nil {
				t.Fatalf("non-strict CheckUse(%s) = %v", name, err)
			}
		}
		if got := r.Report().Violations; len(got) != 0 {
			t.Fatalf("CheckUse must not record: %+v", got)
		}

		r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
		// Same name, other kind: not the declared contract.
		r.Lookup("inventory", "feed-orders", types.ServiceTypeQueueGroup)
		for i := 0; i < 2; i++ {
			r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
		}
		// Channel services have no contract type and are never checked.
		r.Lookup("inventory", "feed", types.ServiceTypeChannel)

		got := r.Report().Violations
		if len(got) != 2 {
			t.Fatalf("Violations = %+v", got)
		}
		// sorted by subject: feed-orders < reserve
		if got[0].Kind != types.BoundaryViolationUndeclaredServiceUse || got[0].Dependency != "inventory" ||
			got[0].Subject != "services.inventory.feed-orders" || got[0].Count != 1 {
			t.Errorf("first violation = %+v", got[0])
		}
		if got[1].Subject != "services.inventory.reserve" || got[1].Count != 2 {
			t.Errorf("second violation = %+v", got[1])
		}
		if logger.count() != 2 {
			t.Errorf("expected one warning per distinct violation, got %d", logger.count())
		}
	})

	t.Run("a contract of another kind does not declare the service", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, nil), checkStock)
		r.Lookup("inventory", "check-stock", types.ServiceTypeQueueGroup)
		if got := r.Report().Violations; len(got) != 1 || got[0].Subject != "services.inventory.check-stock" {
			t.Errorf("Violations = %+v", got)
		}
	})

	t.Run("strict: rejected before the lookup, nothing recorded", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, true, nil), checkStock)
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
		if err := r.CheckUse("inventory", "feed", types.ServiceTypeChannel); err != nil {
			t.Fatalf("channel: CheckUse = %v", err)
		}
		if got := r.Report().Violations; len(got) != 0 {
			t.Errorf("strict mode records nothing: %+v", got)
		}
	})

	t.Run("an empty list is a complete list", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, true, nil))
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})
}

// The list is declared after the dependency containers are set, so a service
// looked up in SetDependencyServiceContainer is checked when the list arrives.
func TestRecorderDeclareConsumedChecksEarlierLookups(t *testing.T) {
	t.Run("non-strict: recorded with the lookups made so far", func(t *testing.T) {
		logger := &recLogger{}
		r := boundary.New("order", []string{"inventory"}, nil, false, logger)
		r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
		for i := 0; i < 3; i++ {
			r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
		}
		r.Lookup("inventory", "feed", types.ServiceTypeChannel)
		if got := r.Report().Violations; len(got) != 0 {
			t.Fatalf("before the list is declared nothing is a violation: %+v", got)
		}

		declare(t, r, checkStock)
		got := r.Report().Violations
		if len(got) != 1 || got[0].Subject != "services.inventory.reserve" || got[0].Count != 3 {
			t.Fatalf("Violations = %+v, want reserve with count 3", got)
		}
		if logger.count() != 1 {
			t.Errorf("expected one warning, got %d", logger.count())
		}
		// Later lookups keep counting on the same entry.
		r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
		if got := r.Report().Violations; len(got) != 1 || got[0].Count != 4 {
			t.Fatalf("Violations = %+v, want count 4", got)
		}
	})

	t.Run("strict: the declaration fails and names the first use", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory", "payment"}, nil, true, nil)
		r.Lookup("payment", "charge", types.ServiceTypeRequestReply)
		r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
		r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
		err := r.DeclareConsumed([]types.BaseServiceDefinition{checkStock})
		if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
		if !strings.Contains(err.Error(), "services.inventory.reserve") {
			t.Errorf("the first undeclared use (by module, then service) should be named: %v", err)
		}
	})

	t.Run("declared once", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, nil), checkStock)
		if err := r.DeclareConsumed(nil); !errors.Is(err, boundary.ErrConsumedAlreadyDeclared) {
			t.Fatalf("expected ErrConsumedAlreadyDeclared, got %v", err)
		}
		if got := r.Report().Consumed; len(got) != 1 || got[0] != checkStock {
			t.Errorf("the first list must be kept: %+v", got)
		}
	})

	t.Run("the list is copied", func(t *testing.T) {
		list := []types.BaseServiceDefinition{checkStock}
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, nil), list...)
		list[0].Name = "changed"
		if got := r.Report().Consumed; got[0] != checkStock {
			t.Errorf("Consumed = %+v", got)
		}
	})
}

// Declaring the list while lookups are running checks every use exactly once:
// the violation count equals the number of undeclared lookups.
func TestRecorderDeclareConsumedConcurrentWithLookups(t *testing.T) {
	r := boundary.New("order", []string{"inventory"}, nil, false, nil)
	const goroutines, each = 8, 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < each; j++ {
				r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
				r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
			}
		}()
	}
	close(start)
	declare(t, r, checkStock)
	wg.Wait()

	rep := r.Report()
	if len(rep.Violations) != 1 || rep.Violations[0].Subject != "services.inventory.reserve" {
		t.Fatalf("Violations = %+v", rep.Violations)
	}
	// Lookups that ran before the declaration are added when it arrives, the
	// others as they happen; a lookup between the two is counted once.
	if got := rep.Violations[0].Count; got != goroutines*each {
		t.Errorf("violation count = %d, want %d", got, goroutines*each)
	}
}

func TestRecorderSubjectUse(t *testing.T) {
	t.Run("no list declared: bus sends are not checked or attributed", func(t *testing.T) {
		r := boundary.New("order", []string{"inventory"}, nil, true, nil)
		if err := r.SubjectUse("inventory", "reserve", "services.inventory.reserve"); err != nil {
			t.Fatalf("SubjectUse = %v", err)
		}
		if rep := r.Report(); len(rep.Observed) != 0 || len(rep.Violations) != 0 {
			t.Errorf("report = %+v", rep)
		}
	})

	t.Run("a declared contract is counted as used, whatever its kind", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, true, nil), checkStock, reserveQG)
		for i := 0; i < 2; i++ {
			if err := r.SubjectUse("inventory", "reserve", "services.inventory.reserve"); err != nil {
				t.Fatalf("SubjectUse = %v", err)
			}
		}
		rep := r.Report()
		if len(rep.Observed) != 1 || rep.Observed[0].Service != "reserve" || rep.Observed[0].Type != types.ServiceTypeQueueGroup || rep.Observed[0].Lookups != 2 {
			t.Errorf("Observed = %+v", rep.Observed)
		}
		if len(rep.UnusedContracts) != 1 || rep.UnusedContracts[0] != checkStock {
			t.Errorf("UnusedContracts = %+v", rep.UnusedContracts)
		}
	})

	t.Run("an undeclared service is a violation with the subject sent to", func(t *testing.T) {
		logger := &recLogger{}
		r := declare(t, boundary.New("order", []string{"inventory"}, []string{"kv-store"}, false, logger), checkStock)
		for i := 0; i < 2; i++ {
			if err := r.SubjectUse("inventory", "reserve", "services.inventory.reserve"); err != nil {
				t.Fatalf("non-strict SubjectUse must return nil, got %v", err)
			}
		}
		// The module itself and plugins are not held to the list.
		for _, module := range []string{"order", "kv-store"} {
			if err := r.SubjectUse(module, "get", "services."+module+".get"); err != nil {
				t.Fatalf("SubjectUse(%s) = %v", module, err)
			}
		}
		rep := r.Report()
		if len(rep.Violations) != 1 || rep.Violations[0].Kind != types.BoundaryViolationUndeclaredServiceUse ||
			rep.Violations[0].Subject != "services.inventory.reserve" || rep.Violations[0].Count != 2 {
			t.Fatalf("Violations = %+v", rep.Violations)
		}
		if len(rep.Observed) != 0 {
			t.Errorf("an undeclared send is not a lookup: %+v", rep.Observed)
		}
		if logger.count() != 1 {
			t.Errorf("expected one warning, got %d", logger.count())
		}
	})

	t.Run("strict: rejected", func(t *testing.T) {
		r := declare(t, boundary.New("order", []string{"inventory"}, nil, true, nil), checkStock)
		err := r.SubjectUse("inventory", "reserve", "services.inventory.reserve")
		if !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) || !strings.Contains(err.Error(), "undeclared-service-use") {
			t.Fatalf("expected an undeclared-service-use ErrModuleBoundaryViolation, got %v", err)
		}
	})
}

func TestRecorderUnusedContracts(t *testing.T) {
	charge := types.BaseServiceDefinition{ModuleName: "payment", Name: "charge", Type: types.ServiceTypeRequestReply}
	// A plugin contract: lookups on plugin containers are not observed, so it
	// must never be reported as unused.
	kvGet := types.BaseServiceDefinition{ModuleName: "kv-store", Name: "get", Type: types.ServiceTypeRequestReply}

	r := declare(t, boundary.New("order", []string{"inventory", "payment"}, []string{"kv-store"}, false, nil),
		checkStock, reserveQG, charge, kvGet, reserveQG)
	if got := r.Report().UnusedContracts; len(got) != 3 || got[0] != checkStock || got[1] != reserveQG || got[2] != charge {
		t.Fatalf("before any lookup, UnusedContracts = %+v (declaration order, no duplicates, no plugin contract)", got)
	}

	r.Lookup("inventory", "check-stock", types.ServiceTypeRequestReply)
	r.Lookup("payment", "charge", types.ServiceTypeRequestReply)
	if got := r.Report().UnusedContracts; len(got) != 1 || got[0] != reserveQG {
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
	r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, logger))

	const goroutines, each = 16, 200
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
				_ = r.Report()
			}
		}()
	}
	wg.Wait()

	rep := r.Report()
	if got := rep.Violations; len(got) != 1 || got[0].Count != goroutines*each || got[0].First.IsZero() {
		t.Fatalf("Violations = %+v, want one with count %d", got, goroutines*each)
	}
	if got := rep.Observed; len(got) != 1 || got[0].Lookups != goroutines*each {
		t.Fatalf("Observed = %+v, want one with %d lookups", got, goroutines*each)
	}
	if logger.count() != 1 {
		t.Errorf("expected exactly one warning, got %d", logger.count())
	}
}

// A repeated undeclared use is on the request path of a non-strict module
// with an incomplete ConsumeServices(): it must not build the subject again.
func TestRecorderRepeatedUndeclaredLookupDoesNotAllocate(t *testing.T) {
	r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, nil))
	r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
	allocs := testing.AllocsPerRun(100, func() {
		r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
	})
	if allocs != 0 {
		t.Errorf("a repeated undeclared use allocated %.0f time(s) per call, want 0", allocs)
	}
	got := r.Report().Violations
	if len(got) != 1 || got[0].Subject != "services.inventory.reserve" || got[0].Count != 102 {
		t.Fatalf("Violations = %+v, want one with count 102 (AllocsPerRun adds a warm-up call)", got)
	}
}

// An undeclared lookup and the same service reached over the bus are one entry.
func TestRecorderUndeclaredLookupSharesTheEntryWithTheBus(t *testing.T) {
	logger := &recLogger{}
	r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, logger))
	if err := r.SubjectUse("inventory", "reserve", "services.inventory.reserve"); err != nil {
		t.Fatalf("SubjectUse = %v", err)
	}
	for i := 0; i < 2; i++ {
		r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
	}
	got := r.Report().Violations
	if len(got) != 1 || got[0].Count != 3 {
		t.Fatalf("Violations = %+v, want one with count 3", got)
	}
	if logger.count() != 1 {
		t.Errorf("expected one warning, got %d", logger.count())
	}
}

// A module that crosses a boundary with a new subject on every request must
// not grow the report or the log without limit: past the bound the violations
// are counted together, per kind.
func TestRecorderBoundsDistinctViolations(t *testing.T) {
	logger := &recLogger{}
	r := declare(t, boundary.New("order", []string{"inventory"}, nil, false, logger))
	const extra = 10
	for i := 0; i < boundary.MaxViolationsForTest+extra; i++ {
		subject := fmt.Sprintf("services.inventory.tenant-%d", i)
		if err := r.SubjectUse("inventory", fmt.Sprintf("tenant-%d", i), subject); err != nil {
			t.Fatalf("SubjectUse = %v", err)
		}
	}
	// A subject that was folded stays folded, and another kind has its own
	// overflow entry.
	if err := r.SubjectUse("inventory", "tenant-late", "services.inventory.tenant-late"); err != nil {
		t.Fatalf("SubjectUse = %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := r.Violation(types.BoundaryViolationUndeclaredServiceSubject, "payment", "services.payment.charge"); err != nil {
			t.Fatalf("Violation = %v", err)
		}
	}

	got := r.Report().Violations
	if len(got) != boundary.MaxViolationsForTest+2 {
		t.Fatalf("%d violations kept, want %d and one overflow entry per kind", len(got), boundary.MaxViolationsForTest)
	}
	overflow := map[types.BoundaryViolationKind]int64{}
	for _, v := range got {
		if v.Dependency == boundary.OverflowTarget && v.Subject == boundary.OverflowTarget {
			overflow[v.Kind] = v.Count
		}
	}
	if overflow[types.BoundaryViolationUndeclaredServiceUse] != extra+1 || overflow[types.BoundaryViolationUndeclaredServiceSubject] != 2 {
		t.Errorf("overflow counts = %v", overflow)
	}
	if logger.count() != boundary.MaxViolationsForTest+2 {
		t.Errorf("%d warnings, want one per kept entry (%d)", logger.count(), boundary.MaxViolationsForTest+2)
	}

	// A violation kept before the bound was reached still counts on its own.
	if err := r.SubjectUse("inventory", "tenant-0", "services.inventory.tenant-0"); err != nil {
		t.Fatalf("SubjectUse = %v", err)
	}
	for _, v := range r.Report().Violations {
		if v.Subject == "services.inventory.tenant-0" && v.Count != 2 {
			t.Errorf("tenant-0 count = %d, want 2", v.Count)
		}
	}
}

// In strict mode an undeclared lookup is rejected by CheckUse. One that passed
// the check just before the list was declared is not lost: it is recorded.
func TestRecorderStrictLookupThatPassedTheCheckBeforeTheListIsRecorded(t *testing.T) {
	r := boundary.New("order", []string{"inventory"}, nil, true, nil)
	if err := r.CheckUse("inventory", "reserve", types.ServiceTypeQueueGroup); err != nil {
		t.Fatalf("no list yet: CheckUse = %v", err)
	}
	declare(t, r, checkStock)
	r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
	if got := r.Report().Violations; len(got) != 1 || got[0].Subject != "services.inventory.reserve" || got[0].Count != 1 {
		t.Fatalf("Violations = %+v", got)
	}
}

func TestRecorderEntriesDescribed(t *testing.T) {
	logger := &recLogger{}
	r := boundary.New("order", []string{"inventory"}, nil, false, logger)
	r.EntriesDescribed("inventory")
	if logger.count() != 1 || !strings.Contains(logger.warns[0], "descriptions") || !strings.Contains(logger.warns[0], "inventory") {
		t.Errorf("warnings = %v", logger.warns)
	}
	if got := r.Report().Violations; len(got) != 0 {
		t.Errorf("reading the entries is not a violation: %+v", got)
	}
	// Without a logger it is a no-op.
	boundary.New("order", nil, nil, false, nil).EntriesDescribed("inventory")
}

func BenchmarkLookupRepeatedUndeclared(b *testing.B) {
	r := declare(b, boundary.New("order", []string{"inventory"}, nil, false, nil))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Lookup("inventory", "reserve", types.ServiceTypeRequestReply)
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

// consumerModule declares dependencies and the contracts it consumes. Like a
// module that builds its list from what it was wired with, it reports the
// list only once its dependency containers are set.
type consumerModule struct {
	dependentModule
	consumed []types.BaseServiceDefinition
	wired    bool
	calls    int
}

func (m *consumerModule) SetDependencyServiceContainer(string, types.ServiceContainer) {
	m.wired = true
}

func (m *consumerModule) ConsumeServices() []types.BaseServiceDefinition {
	m.calls++
	if !m.wired {
		return nil
	}
	return m.consumed
}

// pluginModule is a plugin known by its module name.
type pluginModule struct {
	plainModule
	container types.ServiceContainer
}

func (p *pluginModule) SetContainer(c types.ServiceContainer) { p.container = c }
func (p *pluginModule) Container() types.ServiceContainer     { return p.container }

func TestForModule(t *testing.T) {
	// Plugins are registered under an alias; the module name is what counts.
	plugins := map[string]types.PluginModule{"storage": &pluginModule{plainModule: plainModule{name: "kv-store"}}}

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
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeRequestReply); err != nil {
			t.Errorf("CheckUse = %v", err)
		}
	})

	t.Run("the consumed list is not read when the recorder is built", func(t *testing.T) {
		m := &consumerModule{dependentModule: dependentModule{plainModule{name: "order"}, []string{"inventory"}}, consumed: []types.BaseServiceDefinition{checkStock}}
		r := boundary.ForModule(m, plugins, true, nil)
		if m.calls != 0 {
			t.Errorf("ConsumeServices() was called %d time(s) before the dependency containers were set", m.calls)
		}
		if got := r.Report().Consumed; len(got) != 0 {
			t.Errorf("Consumed = %v", got)
		}
	})
}

// stubContainer is a provider container that only lists its entries.
type stubContainer struct {
	types.ServiceContainer
	entries []*types.ServiceEntry
}

func (c *stubContainer) Entries() []*types.ServiceEntry { return c.entries }

// providersOf returns a ProviderLookup over dependency and plugin containers.
func providersOf(deps, plugins map[string]types.ServiceContainer) boundary.ProviderLookup {
	return func(module string) (types.ServiceContainer, bool, error) {
		if c, ok := plugins[module]; ok {
			if c == nil {
				return nil, true, errors.New("plugin has no container")
			}
			return c, true, nil
		}
		if c, ok := deps[module]; ok {
			return c, false, nil
		}
		return nil, false, nil
	}
}

func TestResolveConsumed(t *testing.T) {
	inventory := &stubContainer{entries: []*types.ServiceEntry{
		nil,
		{Name: "check-stock", Type: types.ServiceTypeRequestReply, ModuleName: "inventory"},
		{Name: "reserve", Type: types.ServiceTypeQueueGroup, ModuleName: "inventory"},
	}}
	kv := &stubContainer{entries: []*types.ServiceEntry{{Name: "get", Type: types.ServiceTypeRequestReply, ModuleName: "kv-store"}}}
	kvGet := types.BaseServiceDefinition{ModuleName: "kv-store", Name: "get", Type: types.ServiceTypeRequestReply}
	deps := map[string]types.ServiceContainer{"inventory": inventory}
	plugins := map[string]types.ServiceContainer{"kv-store": kv}

	newConsumer := func(consumed ...types.BaseServiceDefinition) *consumerModule {
		return &consumerModule{dependentModule: dependentModule{plainModule{name: "order"}, []string{"inventory", "shipping"}}, consumed: consumed}
	}
	newRecorder := func(m types.Module, strict bool) *boundary.Recorder {
		return boundary.New(m.Name(), []string{"inventory", "shipping"}, []string{"kv-store"}, strict, &recLogger{})
	}

	t.Run("a module that declares nothing is not checked", func(t *testing.T) {
		m := &dependentModule{plainModule{name: "order"}, []string{"inventory"}}
		r := newRecorder(m, true)
		if err := r.ResolveConsumed(m, providersOf(deps, plugins)); err != nil {
			t.Fatalf("ResolveConsumed = %v", err)
		}
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeQueueGroup); err != nil {
			t.Errorf("CheckUse = %v", err)
		}
	})

	// The list a module builds from what it was wired with is read once, after
	// the wiring, and that one list is both validated and enforced.
	t.Run("one call, after the containers are set, validated and enforced", func(t *testing.T) {
		m := newConsumer(checkStock, kvGet)
		r := newRecorder(m, true)
		m.SetDependencyServiceContainer("inventory", inventory)
		if err := r.ResolveConsumed(m, providersOf(deps, plugins)); err != nil {
			t.Fatalf("ResolveConsumed = %v", err)
		}
		if m.calls != 1 {
			t.Errorf("ConsumeServices() was called %d time(s), want 1", m.calls)
		}
		if got := r.Report().Consumed; len(got) != 2 || got[0] != checkStock || got[1] != kvGet {
			t.Errorf("Consumed = %+v", got)
		}
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); err != nil {
			t.Errorf("declared contract: CheckUse = %v", err)
		}
		if err := r.CheckUse("inventory", "reserve", types.ServiceTypeQueueGroup); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Errorf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})

	t.Run("an empty list is a complete list", func(t *testing.T) {
		m := newConsumer()
		r := newRecorder(m, true)
		m.SetDependencyServiceContainer("inventory", inventory)
		if err := r.ResolveConsumed(m, providersOf(deps, plugins)); err != nil {
			t.Fatalf("ResolveConsumed = %v", err)
		}
		if err := r.CheckUse("inventory", "check-stock", types.ServiceTypeRequestReply); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Errorf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})

	t.Run("strict: a service looked up while being wired must be in the list", func(t *testing.T) {
		m := newConsumer(checkStock)
		r := newRecorder(m, true)
		m.SetDependencyServiceContainer("inventory", inventory)
		r.Lookup("inventory", "reserve", types.ServiceTypeQueueGroup)
		if err := r.ResolveConsumed(m, providersOf(deps, plugins)); !errors.Is(err, monoerrors.ErrModuleBoundaryViolation) {
			t.Fatalf("expected ErrModuleBoundaryViolation, got %v", err)
		}
	})

	rejected := []struct {
		name     string
		contract types.BaseServiceDefinition
		deps     map[string]types.ServiceContainer
		plugins  map[string]types.ServiceContainer
		is       error
		contains string
	}{
		{name: "no module name", contract: types.BaseServiceDefinition{Name: "x", Type: types.ServiceTypeRequestReply}, contains: "incomplete"},
		{name: "no service name", contract: types.BaseServiceDefinition{ModuleName: "inventory", Type: types.ServiceTypeRequestReply}, contains: "incomplete"},
		{name: "the module itself", contract: types.BaseServiceDefinition{ModuleName: "order", Name: "place", Type: types.ServiceTypeRequestReply}, contains: "names the module itself"},
		{name: "not a dependency", contract: types.BaseServiceDefinition{ModuleName: "payment", Name: "charge", Type: types.ServiceTypeRequestReply},
			is: monoerrors.ErrMissingDependency, contains: "requires module 'payment' in Dependencies()"},
		{name: "a dependency without a container", contract: types.BaseServiceDefinition{ModuleName: "shipping", Name: "quote", Type: types.ServiceTypeRequestReply},
			is: monoerrors.ErrMissingDependency, contains: "dependency 'shipping' has no container"},
		{name: "a plugin without a container", contract: kvGet, plugins: map[string]types.ServiceContainer{"kv-store": nil}, contains: "plugin has no container"},
		{name: "service not registered", contract: types.BaseServiceDefinition{ModuleName: "inventory", Name: "missing", Type: types.ServiceTypeRequestReply},
			is: monoerrors.ErrServiceNotFound},
		{name: "another kind of service", contract: types.BaseServiceDefinition{ModuleName: "inventory", Name: "reserve", Type: types.ServiceTypeRequestReply},
			is: monoerrors.ErrServiceContractMismatch},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			m := newConsumer(checkStock, tc.contract)
			r := newRecorder(m, false)
			m.SetDependencyServiceContainer("inventory", inventory)
			d, p := deps, plugins
			if tc.deps != nil {
				d = tc.deps
			}
			if tc.plugins != nil {
				p = tc.plugins
			}
			err := r.ResolveConsumed(m, providersOf(d, p))
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("error %v should wrap %v", err, tc.is)
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error %q should contain %q", err, tc.contains)
			}
			// A rejected list is not enforced: the module start fails.
			if got := r.Report().Consumed; len(got) != 0 {
				t.Errorf("Consumed = %+v", got)
			}
		})
	}
}
