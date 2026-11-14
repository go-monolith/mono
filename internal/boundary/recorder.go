// Package boundary records how a module uses the boundaries it declared: the
// dependency services it looks up and the crossings it attempts outside its
// declared dependencies. One Recorder exists per started module; the
// dependency container views (internal/container) and the module-scoped event
// bus (internal/eventbus) report to it, and the lifecycle manager turns it
// into a types.ModuleBoundaryReport.
//
// Recording a lookup is on the request path (every helper.Call* looks the
// service up once), so it is a read-locked map lookup plus an atomic increment,
// with no allocation after the first lookup of a service. Whether a service is
// outside the consumed contracts is decided once, when it is first looked up,
// and kept with the recorded use, so a repeated undeclared use costs one more
// atomic increment and builds no subject. A repeated violation of any other
// kind takes the same kind of path (read lock, atomic increment), because in
// non-strict mode a module that keeps crossing a boundary does so on every
// request; only the first occurrence of a violation takes the write lock.
package boundary

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// Recorder collects the boundary observations of one module.
type Recorder struct {
	module      string
	strict      bool
	logger      types.Logger
	declared    []string            // deduplicated Dependencies(), in order
	declaredSet map[string]struct{} // the same, indexed
	allowed     map[string]struct{} // declared dependencies and plugin names

	// consumed is the module's ConsumeServices() list, nil until
	// DeclareConsumed: a module that does not implement ServiceConsumerModule
	// never declares one and is not checked. It is stored with usesMu held, so
	// that every use is checked against it exactly once, and read without a
	// lock.
	consumed atomic.Pointer[consumedContracts]

	usesMu sync.RWMutex
	uses   map[useKey]*use

	mu         sync.RWMutex
	violations map[violationKey]*violation
}

// consumedContracts is a ConsumeServices() list with its indexes. It is not
// changed after it is built.
type consumedContracts struct {
	list []types.BaseServiceDefinition
	set  map[types.BaseServiceDefinition]struct{}
	// byName gives the type of the first contract with a module and service
	// name, for a send on the event bus, whose subject does not carry a type.
	byName map[useKey]types.ServiceType
}

type useKey struct {
	module, service string
}

type use struct {
	typ   types.ServiceType
	count atomic.Int64
	// undeclared is the undeclared-service-use violation of this use, nil
	// when the service is in the consumed list or the list is not enforced.
	// Guarded by usesMu.
	undeclared *violation
}

type violationKey struct {
	kind                types.BoundaryViolationKind
	dependency, subject string
}

type violation struct {
	count atomic.Int64
	first time.Time
}

// ProviderLookup returns the service container of the module that provides a
// consumed contract, for ResolveConsumed. plugin reports whether the module is
// a registered plugin, which may be consumed without being in Dependencies().
// err says why a provider that should have a container has none; a nil
// container without an error is reported as a missing dependency container.
type ProviderLookup func(module string) (container types.ServiceContainer, plugin bool, err error)

// ForModule creates the recorder of module as the framework builds it: from
// the module's Dependencies() and the names of the registered plugins. The
// lifecycle manager and the monotest harness both build their recorders here,
// after the plugins are injected, and both declare the module's
// ConsumeServices() list with ResolveConsumed after its dependency containers
// are set, so a module is held to the same rules in a test as in the
// application.
func ForModule(module types.Module, plugins map[string]types.PluginModule, strict bool, logger types.Logger) *Recorder {
	var declared []string
	if dependent, ok := module.(types.DependentModule); ok {
		declared = dependent.Dependencies()
	}
	pluginNames := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		pluginNames = append(pluginNames, plugin.Name())
	}
	return New(module.Name(), declared, pluginNames, strict, logger)
}

// New creates the recorder of module. declared are the module's dependencies
// (duplicates are dropped, order kept) and plugins the names of the registered
// plugin modules; both are allowed targets of service subjects. With strict
// true, a violation is rejected with an error instead of being recorded. What
// the module consumes is declared afterwards, with DeclareConsumed.
func New(module string, declared, plugins []string, strict bool, logger types.Logger) *Recorder {
	r := &Recorder{
		module:      module,
		strict:      strict,
		logger:      logger,
		declaredSet: make(map[string]struct{}, len(declared)),
		allowed:     make(map[string]struct{}, len(declared)+len(plugins)),
		uses:        make(map[useKey]*use),
		violations:  make(map[violationKey]*violation),
	}
	for _, dep := range declared {
		if _, dup := r.declaredSet[dep]; dup {
			continue
		}
		r.declaredSet[dep] = struct{}{}
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

// ResolveConsumed is step 1.5 of a module start, shared by the lifecycle
// manager and the monotest harness. For a module that implements
// ServiceConsumerModule it calls ConsumeServices() once, validates every
// contract against what its provider registered, and makes that same list the
// one the module's lookups are checked against (DeclareConsumed), so the list
// validated and the list enforced cannot differ. It must run after the
// dependency containers are set: a module may build its list from what it was
// wired with. A module that does not implement the interface is not checked
// and nil is returned.
//
// For each contract: the providing module must be one of the module's declared
// dependencies or a registered plugin (plugins are injected into every
// UsePluginModule and are not part of the dependency graph), that provider
// must have registered the service, and the registered service must be of the
// contract's type. Dependencies and plugins start before the module, so their
// registrations are complete here. The error wraps ErrMissingDependency,
// ErrServiceNotFound, ErrServiceContractMismatch or, from DeclareConsumed,
// ErrModuleBoundaryViolation.
func (r *Recorder) ResolveConsumed(module types.Module, providers ProviderLookup) error {
	consumer, ok := module.(types.ServiceConsumerModule)
	if !ok {
		return nil
	}
	contracts := consumer.ConsumeServices()
	if err := r.validateConsumed(contracts, providers); err != nil {
		return err
	}
	return r.DeclareConsumed(contracts)
}

// validateConsumed checks contracts against the providers' registrations.
func (r *Recorder) validateConsumed(contracts []types.BaseServiceDefinition, providers ProviderLookup) error {
	for _, def := range contracts {
		if def.ModuleName == "" || def.Name == "" {
			return fmt.Errorf("consumed service contract %s is incomplete (module and service name are required)", def)
		}
		if def.ModuleName == r.module {
			return fmt.Errorf("consumed service contract %s names the module itself", def)
		}

		provider, plugin, err := providers(def.ModuleName)
		if !plugin {
			if _, ok := r.declaredSet[def.ModuleName]; !ok {
				return fmt.Errorf("%w: consumed service contract %s requires module '%s' in Dependencies()",
					monoerrors.ErrMissingDependency, def, def.ModuleName)
			}
		}
		if err != nil {
			return fmt.Errorf("consumed service contract %s: %w", def, err)
		}
		if provider == nil {
			return fmt.Errorf("%w: consumed service contract %s: dependency '%s' has no container",
				monoerrors.ErrMissingDependency, def, def.ModuleName)
		}

		var entry *types.ServiceEntry
		for _, candidate := range provider.Entries() {
			if candidate != nil && candidate.Name == def.Name {
				entry = candidate
				break
			}
		}
		if entry == nil {
			return fmt.Errorf("consumed service contract %s: %w", def, &monoerrors.ServiceError{
				ServiceName: def.Name, ModuleName: def.ModuleName, ServiceType: def.Type,
				Err: monoerrors.ErrServiceNotFound,
			})
		}
		if entry.Type != def.Type {
			return monoerrors.WrapServiceContractMismatch(def, def.ModuleName, entry.Type)
		}
		if r.logger != nil {
			r.logger.Debug("Consumed service contract validated",
				"module", r.module, "provider", def.ModuleName, "service", def.Name,
				"type", types.FormatServiceType(def.Type), "contract_backed", entry.Contract != nil)
		}
	}
	return nil
}

// ErrConsumedAlreadyDeclared is returned by DeclareConsumed when the consumed
// list of the recorder was declared before.
var ErrConsumedAlreadyDeclared = errors.New("boundary: consumed services already declared")

// DeclareConsumed makes consumed the complete list of what the module
// consumes: from then on a lookup of a request-reply, queue group or stream
// consumer service of a dependency that is not in it (same module, name and
// type) is an undeclared-service-use violation, as is a send on the event bus
// to such a service's subject. An empty list is a list: every use is then
// undeclared. A recorder whose list is never declared checks nothing.
//
// It may be called at any time, once, and is safe while the recorder is in
// use: services the module looked up before (in SetDependencyServiceContainer,
// which runs before the list is known) are checked now. In strict mode such a
// use that is not in the list is returned as ErrModuleBoundaryViolation and
// the list is not installed (the module start fails); otherwise it is recorded
// with the number of lookups made so far.
func (r *Recorder) DeclareConsumed(consumed []types.BaseServiceDefinition) error {
	contracts := &consumedContracts{
		list:   slices.Clone(consumed),
		set:    make(map[types.BaseServiceDefinition]struct{}, len(consumed)),
		byName: make(map[useKey]types.ServiceType, len(consumed)),
	}
	for _, def := range consumed {
		contracts.set[def] = struct{}{}
		key := useKey{module: def.ModuleName, service: def.Name}
		if _, dup := contracts.byName[key]; !dup {
			contracts.byName[key] = def.Type
		}
	}

	r.usesMu.Lock()
	if r.consumed.Load() != nil {
		r.usesMu.Unlock()
		return fmt.Errorf("%w for module %q", ErrConsumedAlreadyDeclared, r.module)
	}
	// The uses recorded before the list was known, in a fixed order.
	var earlier []useKey
	for key, u := range r.uses {
		if contracts.undeclared(key.module, key.service, u.typ) {
			earlier = append(earlier, key)
		}
	}
	sort.Slice(earlier, func(i, j int) bool {
		if earlier[i].module != earlier[j].module {
			return earlier[i].module < earlier[j].module
		}
		return earlier[i].service < earlier[j].service
	})
	if r.strict && len(earlier) > 0 {
		r.usesMu.Unlock()
		first := earlier[0]
		return monoerrors.WrapModuleBoundaryViolation(r.module, types.BoundaryViolationUndeclaredServiceUse,
			first.module, types.ServiceSubject(first.module, first.service))
	}
	var created []violationKey
	for _, key := range earlier {
		u := r.uses[key]
		vKey := violationKey{kind: types.BoundaryViolationUndeclaredServiceUse, dependency: key.module, subject: types.ServiceSubject(key.module, key.service)}
		var isNew bool
		u.undeclared, vKey, isNew = r.count(vKey, u.count.Load())
		if isNew {
			created = append(created, vKey)
		}
	}
	r.consumed.Store(contracts)
	r.usesMu.Unlock()

	// Logged without the lock held: the logger is application code.
	for _, vKey := range created {
		r.warnFirst(vKey)
	}
	return nil
}

// undeclared reports whether a use of service of kind typ on module is
// outside the list. Channel services have no contract type and never are.
func (c *consumedContracts) undeclared(module, service string, typ types.ServiceType) bool {
	if c == nil || typ == types.ServiceTypeChannel {
		return false
	}
	_, ok := c.set[types.BaseServiceDefinition{ModuleName: module, Name: service, Type: typ}]
	return !ok
}

// CheckUse reports whether the module may look up service of kind typ on
// dependency, before the lookup is made. It rejects in strict mode only: when
// the consumed list is declared and holds no contract with that module, name
// and type, the lookup fails with ErrModuleBoundaryViolation, whether or not
// the service exists. Without strict mode it returns nil and the undeclared
// use is recorded by Lookup, once the lookup succeeded, so a probe for a
// service that does not exist is not a violation. Channel services are never
// checked. It is on the request path and costs nothing without strict mode,
// and one lock-free map lookup with it.
func (r *Recorder) CheckUse(dependency, service string, typ types.ServiceType) error {
	if !r.strict || !r.consumed.Load().undeclared(dependency, service, typ) {
		return nil
	}
	return monoerrors.WrapModuleBoundaryViolation(r.module, types.BoundaryViolationUndeclaredServiceUse,
		dependency, types.ServiceSubject(dependency, service))
}

// Lookup records a successful lookup of service on dependency. When the
// consumed list is declared and the service is not in it, the lookup also
// counts as an undeclared-service-use violation (strict mode rejected it in
// CheckUse before it was made), logged once.
func (r *Recorder) Lookup(dependency, service string, typ types.ServiceType) {
	key := useKey{module: dependency, service: service}
	// Counted with the read lock held: DeclareConsumed takes the write lock
	// to add the lookups made so far to a violation, so it sees each lookup
	// either wholly before or wholly after, never between the two counters.
	r.usesMu.RLock()
	if u, ok := r.uses[key]; ok {
		u.count.Add(1)
		if u.undeclared != nil {
			u.undeclared.count.Add(1)
		}
		r.usesMu.RUnlock()
		return
	}
	r.usesMu.RUnlock()

	var (
		vKey    violationKey
		created bool
	)
	r.usesMu.Lock()
	u, ok := r.uses[key]
	if !ok {
		u = &use{typ: typ}
		// In strict mode CheckUse rejected an undeclared lookup, unless the
		// list was declared between that check and this call (a goroutine
		// looking services up while the module is being wired): that lookup
		// went through, so it is at least recorded.
		if r.consumed.Load().undeclared(dependency, service, typ) {
			vKey = violationKey{kind: types.BoundaryViolationUndeclaredServiceUse, dependency: dependency, subject: types.ServiceSubject(dependency, service)}
			u.undeclared, vKey, created = r.count(vKey, 1)
		}
		r.uses[key] = u
	} else if u.undeclared != nil {
		u.undeclared.count.Add(1)
	}
	// Counted before the lock is released, as in count: a concurrent Report
	// never shows a service looked up zero times.
	u.count.Add(1)
	r.usesMu.Unlock()

	if created {
		r.warnFirst(vKey)
	}
}

// SubjectUse checks a publish or request the module makes on its event bus to
// subject, the service subject services.<dependency>.<service>. It applies
// only to a declared dependency of a module whose consumed list is declared
// (a plugin, the module itself and a module that declares nothing are not
// checked): a service with a contract in the list is counted as a use of that
// contract, like a lookup, so a module that reaches a contract over the bus
// does not have it reported as unused; any other service is an
// undeclared-service-use violation, rejected with ErrModuleBoundaryViolation
// in strict mode and recorded otherwise. The subject does not carry the kind
// of service, so a contract of any type with that module and name declares it.
// An undeclared send is recorded whether or not the service exists (a publish
// cannot tell), under the subject sent to; the number of distinct violations
// kept is bounded (see maxViolations).
func (r *Recorder) SubjectUse(dependency, service, subject string) error {
	contracts := r.consumed.Load()
	if contracts == nil {
		return nil
	}
	if _, ok := r.declaredSet[dependency]; !ok {
		return nil
	}
	if typ, ok := contracts.byName[useKey{module: dependency, service: service}]; ok {
		r.Lookup(dependency, service, typ)
		return nil
	}
	return r.Violation(types.BoundaryViolationUndeclaredServiceUse, dependency, subject)
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
	key := violationKey{kind: kind, dependency: dependency, subject: subject}
	if _, key, created := r.count(key, 1); created {
		r.warnFirst(key)
	}
	return nil
}

// maxViolations bounds the distinct violations (kind, dependency, subject) a
// recorder keeps. The subject of a bus operation is chosen by the module, so a
// module that crosses a boundary with a subject built per request (a tenant or
// an id in it) would otherwise grow the report, and the log, without limit.
// Past the bound a new violation is counted in one entry per kind, with
// OverflowTarget as its dependency and subject.
const maxViolations = 1024

// OverflowTarget is the Dependency and the Subject of the entry that counts,
// per kind, the violations a recorder no longer keeps apart (see
// maxViolations).
const OverflowTarget = "*"

// count adds n occurrences of the violation key and returns its entry and
// the key it is kept under: key itself, or the overflow key of its kind once
// the recorder holds maxViolations entries. created is true for the caller
// that made the entry: that caller logs the violation (warnFirst), so the
// warning is written once however many goroutines cross the boundary at the
// same time.
func (r *Recorder) count(key violationKey, n int64) (v *violation, kept violationKey, created bool) {
	overflow := violationKey{kind: key.kind, dependency: OverflowTarget, subject: OverflowTarget}

	r.mu.RLock()
	v, seen := r.violations[key]
	if !seen && len(r.violations) >= maxViolations {
		// A repeated overflow stays on the read path too.
		key = overflow
		v, seen = r.violations[key]
	}
	r.mu.RUnlock()
	if seen {
		v.count.Add(n)
		return v, key, false
	}

	r.mu.Lock()
	v, seen = r.violations[key]
	if !seen && key != overflow && len(r.violations) >= maxViolations {
		key = overflow
		v, seen = r.violations[key]
	}
	if !seen {
		v = &violation{first: time.Now()}
		r.violations[key] = v
	}
	// Counted before the lock is released, so that a concurrent Report never
	// shows a violation that happened zero times.
	v.count.Add(n)
	r.mu.Unlock()
	return v, key, !seen
}

// warnFirst logs the first occurrence of a violation.
func (r *Recorder) warnFirst(key violationKey) {
	if r.logger == nil {
		return
	}
	args := []any{"module", r.module, "kind", string(key.kind), "dependency", key.dependency}
	if key.subject != "" {
		args = append(args, "subject", key.subject)
	}
	r.logger.Warn("Module boundary violation (recorded; enable WithStrictModuleBoundaries to reject)", args...)
}

// EntriesDescribed logs that the module read Entries() on the container of
// dependency and got descriptions: copies without the handlers and channels.
// Reading them is not a violation (a consumer may inspect what a dependency
// provides), but a module written against the entries themselves finds nil
// where it expects a handler or a channel, so the first read is pointed out.
// The dependency view calls it once per dependency.
func (r *Recorder) EntriesDescribed(dependency string) {
	if r.logger == nil {
		return
	}
	r.logger.Warn("Entries() of a dependency container returns descriptions without handlers and channels; "+
		"reach its services through Get* (the host reads the real entries with MonoFramework.Services)",
		"module", r.module, "dependency", dependency)
}

// Report returns a snapshot of the observations as a ModuleBoundaryReport.
func (r *Recorder) Report() types.ModuleBoundaryReport {
	report := types.ModuleBoundaryReport{
		Module:             r.module,
		Declared:           append([]string(nil), r.declared...),
		Observed:           []types.ServiceUse{},
		UnusedDependencies: []string{},
		UnusedContracts:    []types.BaseServiceDefinition{},
		Violations:         []types.BoundaryViolation{},
	}

	var consumed []types.BaseServiceDefinition
	if contracts := r.consumed.Load(); contracts != nil {
		consumed = contracts.list
		report.Consumed = slices.Clone(consumed)
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
	listed := make(map[types.BaseServiceDefinition]struct{}, len(consumed))
	for _, def := range consumed {
		if _, ok := r.declaredSet[def.ModuleName]; !ok {
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
