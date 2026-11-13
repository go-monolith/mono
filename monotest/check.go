package monotest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// CheckModule starts module in a Harness built from opts, runs the exercise
// given with WithExercise, stops the module, and reports every Ports and
// Adapters rule the module breaks as a separate test error:
//
//  1. Name() is non-empty and kebab-case.
//  2. Dependencies() has no self reference and no duplicates, and every entry
//     has a stand-in container (WithDependency).
//  3. A ServiceProviderModule registers at least one service, each with a
//     valid services.<module>.<service> subject.
//  4. Every ConsumeServices() contract names a declared dependency (or a
//     plugin) whose stand-in registered the service with the same type.
//  5. The module neither mutates a dependency's container, nor addresses the
//     service subject of an undeclared module on the event bus, nor, when it
//     implements ServiceConsumerModule, looks up a dependency service that is
//     missing from ConsumeServices() (no boundary violations recorded).
//  6. Every declared dependency is looked up at least once during Start or the
//     exercise, unless excluded with WithOrderingOnlyDependency.
//  7. An EventEmitterModule declares events with valid subjects, and every
//     events.* subject the module itself publishes during the run is one it
//     declared (publishes by stand-ins or by the exercise do not count).
//  8. Start then Stop succeed, and Stop returns within WithStopTimeout
//     (default 5s) when given a context with that deadline.
//  9. Every ConsumeServices() contract of a declared dependency is looked up
//     at least once during Start or the exercise, unless excluded with
//     WithUnexercisedContract. With rule 5 this makes the list exact: nothing
//     used is undeclared and nothing declared is unused.
//
// Rules 2, 4 and 8 fail the test at once (the module could not be started or
// stopped); the others are collected so one run shows every deviation. The
// harness runs non-strict whatever the options say, since the rules need the
// violations recorded rather than rejected.
func CheckModule(tb testing.TB, module types.Module, opts ...Option) {
	tb.Helper()
	if module == nil {
		tb.Fatal("monotest.CheckModule: module cannot be nil")
		return
	}
	h := New(tb, module, opts...)
	h.strict = false
	name := module.Name()

	// Rule 1: name.
	if name == "" {
		tb.Errorf("rule 1: Name() must not be empty")
	} else if err := monoerrors.ValidateServiceSubject(types.ServiceSubject(name, "probe")); err != nil {
		tb.Errorf("rule 1: Name() %q must be kebab-case (lowercase letters, digits, single hyphens): %v", name, err)
	}

	// Rule 2: dependencies.
	if depMod, ok := module.(types.DependentModule); ok {
		seen := make(map[string]struct{})
		for _, dep := range depMod.Dependencies() {
			if dep == name {
				tb.Errorf("rule 2: Dependencies() lists the module itself (%q)", dep)
			}
			if _, dup := seen[dep]; dup {
				tb.Errorf("rule 2: Dependencies() lists %q twice", dep)
			}
			seen[dep] = struct{}{}
			if _, ok := h.deps[dep]; !ok {
				tb.Fatalf("rule 2: no stand-in container for dependency %q; add monotest.WithDependency(%q, monotest.NewContainer(%q, bus)) and register the services the module consumes on it", dep, dep, dep)
				return
			}
		}
	}

	// Rule 7a: emitted event definitions (checked before Start, which the
	// framework's event registry would fail on an invalid subject).
	var declaredEvents map[string]struct{}
	if emitter, ok := module.(types.EventEmitterModule); ok {
		declaredEvents = make(map[string]struct{})
		for _, def := range emitter.EmitEvents() {
			if err := monoerrors.ValidateEventDefinitionSubject(def.Subject); err != nil {
				tb.Errorf("rule 7: emitted event %s.%s has an invalid subject %q: %v", def.ModuleName, def.Name, def.Subject, err)
			}
			declaredEvents[def.Subject] = struct{}{}
		}
	}

	// Start (rules 4 and 8a).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := h.Start(ctx); err != nil {
		// Clean up first: Fatalf does not return.
		if stopErr := h.Stop(context.Background()); stopErr != nil {
			tb.Logf("cleanup after failed Start: %v", stopErr)
		}
		if errors.Is(err, errConsumedContract) {
			tb.Fatalf("rule 4: %v", err)
		} else {
			tb.Fatalf("rule 8: Start failed: %v", err)
		}
		return
	}

	// Rule 3: provided services.
	if _, ok := module.(types.ServiceProviderModule); ok {
		entries := h.Container().Entries()
		if len(entries) == 0 {
			tb.Errorf("rule 3: the module implements ServiceProviderModule but RegisterServices registered no service")
		}
		for _, e := range entries {
			if err := monoerrors.ValidateServiceSubject(e.Subject); err != nil {
				tb.Errorf("rule 3: service %q has an invalid subject: %v", e.Name, err)
			}
		}
	}

	// Exercise.
	if h.exercise != nil {
		exCtx, exCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := h.exercise(exCtx, h); err != nil {
			tb.Errorf("exercise failed: %v", err)
		}
		exCancel()
	}

	// Rule 5: boundary violations.
	report := h.Report()
	for _, v := range report.Violations {
		where := v.Dependency
		if v.Subject != "" {
			where += " (" + v.Subject + ")"
		}
		tb.Errorf("rule 5: boundary violation %s on %s, %d time(s): %s", v.Kind, where, v.Count, violationHint(v.Kind))
	}

	// Rule 6: unused dependencies.
	for _, dep := range report.UnusedDependencies {
		if h.orderingOnly[dep] {
			continue
		}
		hint := "exercise the module with monotest.WithExercise so that its adapters call the dependency, or mark it with monotest.WithOrderingOnlyDependency(%q) if it only has to start first"
		tb.Errorf("rule 6: dependency %q is declared but no service of it was looked up; "+hint, dep, dep)
	}

	// Rule 9: consumed contracts never looked up.
	for _, def := range report.UnusedContracts {
		if h.unexercised[def] {
			continue
		}
		tb.Errorf("rule 9: consumed contract %s is in ConsumeServices() but was never looked up; "+
			"call it from monotest.WithExercise, remove it from ConsumeServices() if the module no longer uses it, "+
			"or exclude it with monotest.WithUnexercisedContract", def)
	}

	// Rule 7b: the events the module itself published are declared.
	if declaredEvents != nil {
		reported := make(map[string]struct{})
		for _, subject := range h.ModulePublished() {
			if !strings.HasPrefix(subject, "events.") {
				continue
			}
			if _, ok := declaredEvents[subject]; ok {
				continue
			}
			if _, done := reported[subject]; done {
				continue
			}
			reported[subject] = struct{}{}
			tb.Errorf("rule 7: event published on %q is not in EmitEvents()", subject)
		}
	}

	// Rule 8b: Stop within the timeout, with a context carrying that deadline
	// (the framework always hands Stop a live deadline).
	stopCtx, stopCancel := context.WithTimeout(context.Background(), h.stopTimeout)
	defer stopCancel()
	done := make(chan error, 1)
	go func() { done <- h.Stop(stopCtx) }()
	select {
	case err := <-done:
		if err != nil {
			tb.Errorf("rule 8: Stop failed: %v", err)
		}
	case <-time.After(h.stopTimeout + 100*time.Millisecond):
		tb.Fatalf("rule 8: Stop did not return within %s", h.stopTimeout)
	}
}

func violationHint(kind types.BoundaryViolationKind) string {
	switch kind {
	case types.BoundaryViolationRegisterOnDependency:
		return "a module may only look services up on a dependency's container; register services on its own container in RegisterServices"
	case types.BoundaryViolationChannelConsumerMismatch:
		return "pass the module's own name as consumerModule to GetChannelService"
	case types.BoundaryViolationUndeclaredServiceSubject:
		return "call the service through the dependency container received in SetDependencyServiceContainer (declare the dependency) instead of the raw event bus"
	case types.BoundaryViolationForeignServiceSubscription:
		return "a module must not subscribe to another module's service subject"
	case types.BoundaryViolationUndeclaredServiceUse:
		return "add the service's contract to ConsumeServices(): a module that declares what it consumes must declare all of it"
	default:
		return fmt.Sprintf("unknown violation kind %q", kind)
	}
}
