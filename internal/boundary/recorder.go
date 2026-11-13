// Package boundary records how a module uses the boundaries it declared: the
// dependency services it looks up and the crossings it attempts outside its
// declared dependencies. One Recorder exists per started module; the
// dependency container views (internal/container) and the module-scoped event
// bus (internal/eventbus) report to it, and the lifecycle manager turns it
// into a types.ModuleBoundaryReport.
//
// Recording a lookup is on the request path (every helper.Call* looks the
// service up once), so it is a read-locked map lookup plus an atomic increment,
// with no allocation after the first lookup of a service; checking it against
// the consumed contracts (CheckUse) adds one lock-free map lookup. A repeated
// violation takes the same path (read lock, atomic increment), because in
// non-strict mode a module that keeps crossing a boundary does so on every
// request; only the first occurrence of a violation takes the write lock. A
// repeated undeclared service use is found by module and service name, so its
// subject is built once and that path does not allocate either.
package boundary

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// Recorder collects the boundary observations of one module.
type Recorder struct {
	module   string
	strict   bool
	logger   types.Logger
	declared []string            // deduplicated Dependencies(), in order
	allowed  map[string]struct{} // declared dependencies and plugin names
	consumed []types.BaseServiceDefinition

	// consumedSet indexes consumed; enforceConsumed makes a lookup outside it
	// a violation (see EnforceConsumed). Both are fixed before the recorder is
	// shared, so CheckUse reads them without a lock.
	consumedSet     map[types.BaseServiceDefinition]struct{}
	enforceConsumed bool

	usesMu sync.RWMutex
	uses   map[useKey]*use

	mu         sync.RWMutex
	violations map[violationKey]*violation
	// undeclared indexes the undeclared-service-use entries of violations by
	// module and service, so that CheckUse finds a repeated one without
	// building its subject. Guarded by mu.
	undeclared map[useKey]*violation
}

type useKey struct {
	module, service string
}

type use struct {
	typ   types.ServiceType
	count atomic.Int64
}

type violationKey struct {
	kind                types.BoundaryViolationKind
	dependency, subject string
}

type violation struct {
	count atomic.Int64
	first time.Time
}

// ForModule creates the recorder of module as the framework builds it: from
// the module's Dependencies(), the names of the registered plugins and its
// ConsumeServices(). A module that implements ServiceConsumerModule declares
// all it consumes (even with an empty list), so its lookups are checked
// against the list; a module that does not is not checked. The lifecycle
// manager and the monotest harness both build their recorders here, so a
// module is held to the same rules in a test as in the application.
func ForModule(module types.Module, plugins map[string]types.PluginModule, strict bool, logger types.Logger) *Recorder {
	var declared []string
	if dependent, ok := module.(types.DependentModule); ok {
		declared = dependent.Dependencies()
	}
	var consumed []types.BaseServiceDefinition
	consumer, declaresConsumed := module.(types.ServiceConsumerModule)
	if declaresConsumed {
		consumed = consumer.ConsumeServices()
	}
	pluginNames := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		pluginNames = append(pluginNames, plugin.Name())
	}
	rec := New(module.Name(), declared, pluginNames, consumed, strict, logger)
	if declaresConsumed {
		rec.EnforceConsumed()
	}
	return rec
}

// New creates the recorder of module. declared are the module's dependencies
// (duplicates are dropped, order kept) and plugins the names of the registered
// plugin modules; both are allowed targets of service subjects. consumed is
// what the module reported through ServiceConsumerModule, kept for the
// report and, after EnforceConsumed, checked against the module's lookups.
// With strict true, Violation returns an error instead of recording.
func New(module string, declared, plugins []string, consumed []types.BaseServiceDefinition, strict bool, logger types.Logger) *Recorder {
	r := &Recorder{
		module:      module,
		strict:      strict,
		logger:      logger,
		allowed:     make(map[string]struct{}, len(declared)+len(plugins)),
		consumed:    consumed,
		consumedSet: make(map[types.BaseServiceDefinition]struct{}, len(consumed)),
		uses:        make(map[useKey]*use),
		violations:  make(map[violationKey]*violation),
		undeclared:  make(map[useKey]*violation),
	}
	for _, def := range consumed {
		r.consumedSet[def] = struct{}{}
	}
	for _, dep := range declared {
		if _, dup := r.allowed[dep]; dup {
			continue
		}
		r.allowed[dep] = struct{}{}
		r.declared = append(r.declared, dep)
	}
	for _, p := range plugins {
		r.allowed[p] = struct{}{}
	}
	return r
}

// Module returns the module the recorder belongs to.
func (r *Recorder) Module() string { return r.module }

// Strict reports whether violations are rejected rather than recorded.
func (r *Recorder) Strict() bool { return r.strict }

// Allowed reports whether module is a legitimate target of a service subject
// for this module: itself, a declared dependency or a plugin.
func (r *Recorder) Allowed(module string) bool {
	if module == r.module {
		return true
	}
	_, ok := r.allowed[module]
	return ok
}

// EnforceConsumed makes the consumed list given to New complete: from then on
// CheckUse reports a lookup of a service that is not in it. ForModule calls it
// for a module that implements ServiceConsumerModule (even when the list is
// empty), and not otherwise, so a module that declares nothing is not
// checked. It is part of building the recorder: it must be called before the
// recorder is handed to a dependency view, and returns the recorder for
// chaining.
func (r *Recorder) EnforceConsumed() *Recorder {
	r.enforceConsumed = true
	return r
}

// CheckUse reports whether the module may look up service of kind typ on
// dependency. When the consumed list is enforced (EnforceConsumed) and holds no
// contract with that module, name and type, the lookup is an
// undeclared-service-use violation: rejected with ErrModuleBoundaryViolation
// in strict mode, recorded and let through (nil) otherwise. Channel services
// are never checked, as they have no contract type. It is on the request path
// and costs one map lookup; a repeated undeclared use in non-strict mode costs
// a second one under the read lock and an atomic increment, without
// allocating.
func (r *Recorder) CheckUse(dependency, service string, typ types.ServiceType) error {
	if !r.enforceConsumed || typ == types.ServiceTypeChannel {
		return nil
	}
	if _, ok := r.consumedSet[types.BaseServiceDefinition{ModuleName: dependency, Name: service, Type: typ}]; ok {
		return nil
	}
	if r.strict {
		return r.Violation(types.BoundaryViolationUndeclaredServiceUse, dependency, types.ServiceSubject(dependency, service))
	}

	// The violation is keyed by the service subject, which module and service
	// determine, so a repeated one is found without building the subject.
	key := useKey{module: dependency, service: service}
	r.mu.RLock()
	v, seen := r.undeclared[key]
	r.mu.RUnlock()
	if seen {
		v.count.Add(1)
		return nil
	}
	v = r.record(types.BoundaryViolationUndeclaredServiceUse, dependency, types.ServiceSubject(dependency, service))
	r.mu.Lock()
	r.undeclared[key] = v
	r.mu.Unlock()
	return nil
}

// Lookup records a successful lookup of service on dependency.
func (r *Recorder) Lookup(dependency, service string, typ types.ServiceType) {
	key := useKey{module: dependency, service: service}
	r.usesMu.RLock()
	u, ok := r.uses[key]
	r.usesMu.RUnlock()
	if ok {
		u.count.Add(1)
		return
	}
	r.usesMu.Lock()
	u, ok = r.uses[key]
	if !ok {
		u = &use{typ: typ}
		r.uses[key] = u
	}
	// Counted before the lock is released, as in Violation: a concurrent
	// Report never shows a service looked up zero times.
	u.count.Add(1)
	r.usesMu.Unlock()
}

// Violation records a boundary crossing of kind against dependency. subject
// is the NATS subject of a bus operation, the service subject of an undeclared
// service use, the consumer name of a channel-consumer mismatch, and empty
// otherwise. In strict mode nothing is recorded and
// ErrModuleBoundaryViolation is returned for the caller to propagate; otherwise
// the crossing is counted, logged once per (kind, dependency, subject) at Warn,
// and nil is returned so the operation can go through.
func (r *Recorder) Violation(kind types.BoundaryViolationKind, dependency, subject string) error {
	if r.strict {
		return monoerrors.WrapModuleBoundaryViolation(r.module, kind, dependency, subject)
	}
	r.record(kind, dependency, subject)
	return nil
}

// record counts one occurrence of a violation, logs its first occurrence and
// returns its entry.
func (r *Recorder) record(kind types.BoundaryViolationKind, dependency, subject string) *violation {
	key := violationKey{kind: kind, dependency: dependency, subject: subject}

	r.mu.RLock()
	v, seen := r.violations[key]
	r.mu.RUnlock()
	if seen {
		v.count.Add(1)
		return v
	}

	r.mu.Lock()
	v, seen = r.violations[key]
	if !seen {
		v = &violation{first: time.Now()}
		r.violations[key] = v
	}
	// Counted before the lock is released, so that a concurrent Report never
	// shows a violation that happened zero times.
	v.count.Add(1)
	r.mu.Unlock()

	// Only the caller that created the entry logs, so the warning is written
	// once however many goroutines cross the boundary at the same time.
	if !seen && r.logger != nil {
		args := []any{"module", r.module, "kind", string(kind), "dependency", dependency}
		if subject != "" {
			args = append(args, "subject", subject)
		}
		r.logger.Warn("Module boundary violation (recorded; enable WithStrictModuleBoundaries to reject)", args...)
	}
	return v
}

// Report returns a snapshot of the observations as a ModuleBoundaryReport.
func (r *Recorder) Report() types.ModuleBoundaryReport {
	report := types.ModuleBoundaryReport{
		Module:             r.module,
		Declared:           append([]string(nil), r.declared...),
		Consumed:           append([]types.BaseServiceDefinition(nil), r.consumed...),
		Observed:           []types.ServiceUse{},
		UnusedDependencies: []string{},
		UnusedContracts:    []types.BaseServiceDefinition{},
		Violations:         []types.BoundaryViolation{},
	}

	used := make(map[string]struct{})
	usedServices := make(map[useKey]struct{})
	r.usesMu.RLock()
	for key, u := range r.uses {
		used[key.module] = struct{}{}
		usedServices[key] = struct{}{}
		report.Observed = append(report.Observed, types.ServiceUse{
			Module: key.module, Service: key.service, Type: u.typ, Lookups: u.count.Load(),
		})
	}
	r.usesMu.RUnlock()
	sort.Slice(report.Observed, func(i, j int) bool {
		a, b := report.Observed[i], report.Observed[j]
		if a.Module != b.Module {
			return a.Module < b.Module
		}
		return a.Service < b.Service
	})

	for _, dep := range r.declared {
		if _, ok := used[dep]; !ok {
			report.UnusedDependencies = append(report.UnusedDependencies, dep)
		}
	}

	// Consumed contracts never looked up, in declaration order. Only those of
	// declared dependencies: plugin containers are not views, so a lookup on
	// one is never observed.
	declared := make(map[string]struct{}, len(r.declared))
	for _, dep := range r.declared {
		declared[dep] = struct{}{}
	}
	listed := make(map[types.BaseServiceDefinition]struct{}, len(r.consumed))
	for _, def := range r.consumed {
		if _, ok := declared[def.ModuleName]; !ok {
			continue
		}
		if _, dup := listed[def]; dup {
			continue
		}
		listed[def] = struct{}{}
		if _, ok := usedServices[useKey{module: def.ModuleName, service: def.Name}]; !ok {
			report.UnusedContracts = append(report.UnusedContracts, def)
		}
	}

	r.mu.RLock()
	for key, v := range r.violations {
		report.Violations = append(report.Violations, types.BoundaryViolation{
			Kind: key.kind, Dependency: key.dependency, Subject: key.subject, Count: v.count.Load(), First: v.first,
		})
	}
	r.mu.RUnlock()
	sort.Slice(report.Violations, func(i, j int) bool {
		a, b := report.Violations[i], report.Violations[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Dependency != b.Dependency {
			return a.Dependency < b.Dependency
		}
		return a.Subject < b.Subject
	})
	return report
}
