// Package boundary records how a module uses the boundaries it declared: the
// dependency services it looks up and the crossings it attempts outside its
// declared dependencies. One Recorder exists per started module; the
// dependency container views (internal/container) and the module-scoped event
// bus (internal/eventbus) report to it, and the lifecycle manager turns it
// into a types.ModuleBoundaryReport.
//
// Recording a lookup is on the request path (every helper.Call* looks the
// service up once), so it is a read-locked map lookup plus an atomic increment,
// with no allocation after the first lookup of a service. Violations are rare
// and take a mutex.
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

	usesMu sync.RWMutex
	uses   map[useKey]*use

	mu         sync.Mutex
	violations map[violationKey]*violation
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
	count int64
	first time.Time
}

// New creates the recorder of module. declared are the module's dependencies
// (duplicates are dropped, order kept) and plugins the names of the registered
// plugin modules; both are allowed targets of service subjects. consumed is
// what the module reported through ServiceConsumerModule, kept for the
// report. With strict true, Violation returns an error instead of recording.
func New(module string, declared, plugins []string, consumed []types.BaseServiceDefinition, strict bool, logger types.Logger) *Recorder {
	r := &Recorder{
		module:     module,
		strict:     strict,
		logger:     logger,
		allowed:    make(map[string]struct{}, len(declared)+len(plugins)),
		consumed:   consumed,
		uses:       make(map[useKey]*use),
		violations: make(map[violationKey]*violation),
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
	r.usesMu.Unlock()
	u.count.Add(1)
}

// Violation records a boundary crossing of kind against dependency (subject
// is set for bus operations). In strict mode nothing is recorded and
// ErrModuleBoundaryViolation is returned for the caller to propagate; otherwise
// the crossing is counted, logged once per (kind, dependency, subject) at Warn,
// and nil is returned so the operation can go through.
func (r *Recorder) Violation(kind types.BoundaryViolationKind, dependency, subject string) error {
	if r.strict {
		return monoerrors.WrapModuleBoundaryViolation(r.module, kind, dependency, subject)
	}
	key := violationKey{kind: kind, dependency: dependency, subject: subject}

	r.mu.Lock()
	v, seen := r.violations[key]
	if !seen {
		v = &violation{first: time.Now()}
		r.violations[key] = v
	}
	v.count++
	r.mu.Unlock()

	if !seen && r.logger != nil {
		args := []any{"module", r.module, "kind", string(kind), "dependency", dependency}
		if subject != "" {
			args = append(args, "subject", subject)
		}
		r.logger.Warn("Module boundary violation (recorded; enable WithStrictModuleBoundaries to reject)", args...)
	}
	return nil
}

// Report returns a snapshot of the observations as a ModuleBoundaryReport.
func (r *Recorder) Report() types.ModuleBoundaryReport {
	report := types.ModuleBoundaryReport{
		Module:             r.module,
		Declared:           append([]string(nil), r.declared...),
		Consumed:           append([]types.BaseServiceDefinition(nil), r.consumed...),
		Observed:           []types.ServiceUse{},
		UnusedDependencies: []string{},
		Violations:         []types.BoundaryViolation{},
	}

	used := make(map[string]struct{})
	r.usesMu.RLock()
	for key, u := range r.uses {
		used[key.module] = struct{}{}
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

	r.mu.Lock()
	for key, v := range r.violations {
		report.Violations = append(report.Violations, types.BoundaryViolation{
			Kind: key.kind, Dependency: key.dependency, Subject: key.subject, Count: v.count, First: v.first,
		})
	}
	r.mu.Unlock()
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
