package monotest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/boundary"
	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/internal/eventbus"
	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// ErrNotStarted is returned by Harness methods that need a started harness.
var ErrNotStarted = errors.New("monotest: harness not started")

// errConsumedContract marks a Start failure caused by a ConsumeServices()
// contract (CheckModule reports it under rule 4).
var errConsumedContract = errors.New("consumed service contract rejected")

// Option configures a Harness (and CheckModule, which runs one).
type Option func(*Harness)

// WithDependency supplies the stand-in container of dependency name. Every
// name in the module's Dependencies() needs one; the containers must share
// one EventBus.
func WithDependency(name string, c *Container) Option {
	return func(h *Harness) { h.deps[name] = c }
}

// WithPlugin injects plugin under alias into a UsePluginModule and allows the
// plugin's module name as a target of service subjects and consumed contracts.
// The plugin is not started by the harness: give it a container first
// (plugin.SetContainer(monotest.NewContainer(plugin.Name(), bus))), register
// its services, and start it if its services need that. A *Container returned
// by plugin.Container() is dispatched with the module's services.
func WithPlugin(alias string, plugin types.PluginModule) Option {
	return func(h *Harness) { h.plugins[alias] = plugin }
}

// WithEmittedEvents registers event definitions of other modules on the
// harness's event registry before RegisterEventConsumers runs, for consumers
// that look events up with GetEventByName instead of importing the emitter's
// definition.
func WithEmittedEvents(defs ...types.BaseEventDefinition) Option {
	return func(h *Harness) { h.emitted = append(h.emitted, defs...) }
}

// WithEventBus uses bus instead of the dependencies' bus (or a new one).
func WithEventBus(bus *EventBus) Option {
	return func(h *Harness) { h.bus = bus }
}

// Strict makes boundary violations fail the call (as WithStrictModuleBoundaries
// does in the framework) instead of being recorded in Report. CheckModule
// ignores it: it needs the recording to report violations.
func Strict() Option {
	return func(h *Harness) { h.strict = true }
}

// WithExercise runs fn after Start in CheckModule, so that the module's
// adapters actually look services up and publish events. Adapters look
// services up at call time, so without an exercise most "uses" are invisible.
func WithExercise(fn func(ctx context.Context, h *Harness) error) Option {
	return func(h *Harness) { h.exercise = fn }
}

// WithOrderingOnlyDependency excludes name from the "declared but unused"
// check of CheckModule: the dependency exists only to start before the module.
func WithOrderingOnlyDependency(name string) Option {
	return func(h *Harness) { h.orderingOnly[name] = true }
}

// WithUnexercisedContract excludes def from the "consumed but never looked
// up" check of CheckModule (rule 9), for a contract the exercise cannot reach
// in a unit test (for example one called only from a JetStream handler).
func WithUnexercisedContract(def types.BaseServiceDefinition) Option {
	return func(h *Harness) { h.unexercised[def] = true }
}

// WithStopTimeout bounds, in Stop and CheckModule, the wait for handlers in
// flight and for the module's Stop (default 5s).
func WithStopTimeout(d time.Duration) Option {
	return func(h *Harness) {
		if d > 0 {
			h.stopTimeout = d
		}
	}
}

// Harness starts one module through the framework's lifecycle order without
// NATS: plugins, dependency containers (as the same read-only attributed views
// the framework hands out), consumed-contract validation, the module-scoped
// event bus, RegisterServices, RegisterEventConsumers, Start; then, as the
// framework does only after every module started, the services of the module
// and of its dependencies are dispatched on the in-memory bus and its event
// consumers subscribed. Stop drains the subscriptions and waits for handlers
// in flight, cancels the runtime context, then stops the module.
type Harness struct {
	tb      testing.TB
	module  types.Module
	bus     *EventBus
	deps    map[string]*Container
	plugins map[string]types.PluginModule
	emitted []types.BaseEventDefinition
	strict  bool
	logger  *testLogger

	exercise     func(ctx context.Context, h *Harness) error
	orderingOnly map[string]bool
	unexercised  map[types.BaseServiceDefinition]bool
	stopTimeout  time.Duration

	rec        *boundary.Recorder
	container  *Container
	recording  *recordingBus
	scoped     types.EventBus
	registry   types.EventRegistry
	eventSubs  []*subscription
	runtimeCtx context.Context
	cancel     context.CancelFunc
	started    bool
}

// New creates a harness for module. tb may be nil outside tests.
func New(tb testing.TB, module types.Module, opts ...Option) *Harness {
	h := &Harness{
		tb:           tb,
		module:       module,
		deps:         make(map[string]*Container),
		plugins:      make(map[string]types.PluginModule),
		orderingOnly: make(map[string]bool),
		unexercised:  make(map[types.BaseServiceDefinition]bool),
		stopTimeout:  5 * time.Second,
		logger:       newTestLogger(tb),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.bus == nil {
		for _, c := range h.deps {
			h.bus = c.bus
			break
		}
	}
	if h.bus == nil {
		h.bus = NewEventBus()
	}
	return h
}

// Module returns the module under test.
func (h *Harness) Module() types.Module { return h.module }

// EventBus returns the in-memory bus (unscoped) the harness runs on.
func (h *Harness) EventBus() *EventBus { return h.bus }

// Container returns the module's own container, bound to the module, with its
// services dispatched. Call the module's services through it. Nil before Start.
func (h *Harness) Container() *Container { return h.container }

// Dependency returns the stand-in container supplied for name, or nil.
func (h *Harness) Dependency(name string) *Container { return h.deps[name] }

// Report returns the module's boundary report so far. Before Start it names
// the module and lists nothing; its lists of observations are empty, not nil,
// as in every report.
func (h *Harness) Report() types.ModuleBoundaryReport {
	if h.rec == nil {
		return types.ModuleBoundaryReport{
			Module:             h.module.Name(),
			Declared:           []string{},
			Observed:           []types.ServiceUse{},
			UnusedDependencies: []string{},
			UnusedContracts:    []types.BaseServiceDefinition{},
			Violations:         []types.BoundaryViolation{},
		}
	}
	return h.rec.Report()
}

// Published returns every subject published on the bus so far, by anyone.
func (h *Harness) Published() []string { return h.bus.Published() }

// ModulePublished returns the subjects the module itself published through the
// event bus it received in SetEventBus (events, raw service requests), in
// order. Publishes by stand-ins, by the exercise or by the test are not
// included.
func (h *Harness) ModulePublished() []string {
	if h.recording == nil {
		return nil
	}
	return h.recording.subjects()
}

// Logs returns the lines the framework-side logger wrote (warnings included).
func (h *Harness) Logs() []string { return h.logger.Lines() }

// Start runs the lifecycle order for the module. It fails like the framework
// would: a missing stand-in container, a consumed contract without a provider,
// a strict-mode boundary violation, or an error from the module itself.
func (h *Harness) Start(ctx context.Context) (err error) {
	if h.started {
		return fmt.Errorf("monotest: harness already started")
	}
	if h.module == nil {
		return fmt.Errorf("monotest: module cannot be nil")
	}
	name := h.module.Name()
	if name == "" {
		return monoerrors.WrapInvalidModule(h.module, "module name cannot be empty")
	}
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapModulePanic(name, "start", r)
		}
	}()

	for depName, c := range h.deps {
		if c.bus != h.bus {
			return fmt.Errorf("monotest: dependency %q uses a different EventBus than the harness; create every container on the same bus", depName)
		}
	}

	// The runtime context handlers receive; cancelled at the end of Stop.
	h.runtimeCtx, h.cancel = context.WithCancel(context.Background())
	h.bus.SetRuntimeContext(h.runtimeCtx)

	// Step 0: the module's own container.
	own, err := newContainer(h.module, h.bus, h.logger.WithModule(name))
	if err != nil {
		return monoerrors.WrapModuleStartFailed(name, err)
	}
	h.container = own

	// Boundary recorder, built by the constructor the framework uses, so the
	// module is held to the same rules here as in the application.
	h.rec = boundary.ForModule(h.module, h.plugins, h.strict, h.logger)

	// Step 0.5: plugins.
	if user, ok := h.module.(types.UsePluginModule); ok {
		aliases := make([]string, 0, len(h.plugins))
		for alias := range h.plugins {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
		for _, alias := range aliases {
			user.SetPlugin(alias, h.plugins[alias])
		}
	}

	// Step 1: dependency containers, through the framework's read-only views.
	if depMod, ok := h.module.(types.DependentModule); ok {
		seen := make(map[string]struct{})
		for _, depName := range depMod.Dependencies() {
			if _, dup := seen[depName]; dup {
				continue
			}
			seen[depName] = struct{}{}
			c, ok := h.deps[depName]
			if !ok {
				return monoerrors.WrapModuleStartFailed(name, fmt.Errorf(
					"%w: no stand-in container for dependency %q; add monotest.WithDependency(%q, monotest.NewContainer(%q, bus))",
					monoerrors.ErrMissingDependency, depName, depName, depName))
			}
			depMod.SetDependencyServiceContainer(depName, container.NewDependencyView(c.ServiceContainer, depName, h.rec))
		}
	}

	// Step 1.5: consumed contracts, with the framework's rules.
	if consumer, ok := h.module.(types.ServiceConsumerModule); ok {
		var declared []string
		if depMod, ok := h.module.(types.DependentModule); ok {
			declared = depMod.Dependencies()
		}
		pluginByName := make(map[string]types.PluginModule, len(h.plugins))
		for _, p := range h.plugins {
			pluginByName[p.Name()] = p
		}
		if err := h.validateConsumed(name, declared, consumer.ConsumeServices(), pluginByName); err != nil {
			return monoerrors.WrapModuleStartFailed(name, fmt.Errorf("%w: %w", errConsumedContract, err))
		}
	}

	// Step 2: module-scoped event bus, over a recorder of the module's own
	// publishes.
	h.recording = &recordingBus{EventBus: h.bus}
	h.scoped = eventbus.NewScoped(h.recording, h.rec)
	if aware, ok := h.module.(types.EventBusAwareModule); ok {
		aware.SetEventBus(h.scoped)
	}

	// Step 3: services.
	if provider, ok := h.module.(types.ServiceProviderModule); ok {
		if err := provider.RegisterServices(own); err != nil {
			return monoerrors.WrapModuleStartFailed(name, err)
		}
	}

	// Step 4: events (definitions the module emits, consumers it registers).
	h.registry = registry.NewEventRegistry(h.logger)
	for _, def := range h.emitted {
		if err := h.registry.RegisterEvent(def); err != nil {
			return monoerrors.WrapModuleStartFailed(name, fmt.Errorf("register event %s.%s from WithEmittedEvents: %w", def.ModuleName, def.Name, err))
		}
	}
	if emitter, ok := h.module.(types.EventEmitterModule); ok {
		for _, def := range emitter.EmitEvents() {
			if err := h.registry.RegisterEvent(def); err != nil {
				return monoerrors.WrapModuleStartFailed(name, fmt.Errorf("register emitted event %s.%s: %w", def.ModuleName, def.Name, err))
			}
		}
	}
	if consumer, ok := h.module.(types.EventConsumerModule); ok {
		if err := consumer.RegisterEventConsumers(h.registry); err != nil {
			return monoerrors.WrapModuleStartFailed(name, err)
		}
	}

	// Step 5: Start.
	if err := h.module.Start(ctx); err != nil {
		return monoerrors.WrapModuleStartFailed(name, err)
	}
	h.started = true

	// Step 6: subscriptions, as the framework creates them after every module
	// started: the module's services, its dependencies' and plugins' services,
	// and its event consumers.
	if err := own.Dispatch(h.runtimeCtx); err != nil {
		return err
	}
	for depName, c := range h.deps {
		if err := c.Dispatch(h.runtimeCtx); err != nil {
			return fmt.Errorf("monotest: dependency %q: %w", depName, err)
		}
	}
	for _, p := range h.plugins {
		if c, ok := p.Container().(*Container); ok {
			if err := c.Dispatch(h.runtimeCtx); err != nil {
				return fmt.Errorf("monotest: plugin %q: %w", p.Name(), err)
			}
		}
	}
	for _, entry := range h.registry.Entries() {
		handler := entry.Handler
		subject := entry.EventDef.Subject
		sub, err := h.bus.subscribe(subject, entry.QueueGroup, func(ctx context.Context, msg *types.Msg) {
			if err := invokeEventConsumer(ctx, handler, msg); err != nil {
				h.logger.Error("monotest: event consumer error", "subject", subject, "error", err)
			}
		}, false)
		if err != nil {
			return fmt.Errorf("monotest: subscribe event consumer %s: %w", subject, err)
		}
		h.eventSubs = append(h.eventSubs, sub)
	}
	return nil
}

// invokeEventConsumer runs an event consumer handler, turning a panic into an
// error.
func invokeEventConsumer(ctx context.Context, handler types.EventConsumerHandler, msg *types.Msg) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &handlerPanicError{value: r}
		}
	}()
	return handler(ctx, msg)
}

// validateConsumed mirrors the framework's step 1.5.
func (h *Harness) validateConsumed(name string, declared []string, consumed []types.BaseServiceDefinition, plugins map[string]types.PluginModule) error {
	if len(consumed) == 0 {
		return nil
	}
	declaredSet := make(map[string]struct{}, len(declared))
	for _, d := range declared {
		declaredSet[d] = struct{}{}
	}
	for _, def := range consumed {
		if def.ModuleName == "" || def.Name == "" {
			return fmt.Errorf("consumed service contract %s is incomplete (module and service name are required)", def)
		}
		if def.ModuleName == name {
			return fmt.Errorf("consumed service contract %s names the module itself", def)
		}
		var provider types.ServiceContainer
		var hint string
		if p, ok := plugins[def.ModuleName]; ok {
			provider = p.Container()
			if provider == nil {
				return fmt.Errorf("consumed service contract %s: plugin %q has no container; call plugin.SetContainer(monotest.NewContainer(%q, bus)) and register its services before the harness starts",
					def, def.ModuleName, def.ModuleName)
			}
			hint = "register it on the plugin's container"
		} else {
			if _, ok := declaredSet[def.ModuleName]; !ok {
				return fmt.Errorf("%w: consumed service contract %s requires module '%s' in Dependencies()",
					monoerrors.ErrMissingDependency, def, def.ModuleName)
			}
			c, ok := h.deps[def.ModuleName]
			if !ok {
				return fmt.Errorf("%w: consumed service contract %s: no stand-in container for '%s'",
					monoerrors.ErrMissingDependency, def, def.ModuleName)
			}
			provider = c.ServiceContainer
			hint = "register it on the stand-in container, e.g. with the provider's contract or monotest.StubRequestReply"
		}
		var entry *types.ServiceEntry
		for _, e := range provider.Entries() {
			if e.Name == def.Name {
				entry = e
				break
			}
		}
		if entry == nil {
			return fmt.Errorf("consumed service contract %s: %w (%s)", def,
				&monoerrors.ServiceError{ServiceName: def.Name, ModuleName: def.ModuleName, ServiceType: def.Type, Err: monoerrors.ErrServiceNotFound}, hint)
		}
		if entry.Type != def.Type {
			return monoerrors.WrapServiceContractMismatch(def, def.ModuleName, entry.Type)
		}
	}
	return nil
}

// Stop drains the subscriptions and waits (up to the stop timeout) for the
// handlers in flight, cancels the runtime context, then stops the module with
// ctx. It is safe to call after a failed Start (it then only cleans up) and to
// call twice.
func (h *Harness) Stop(ctx context.Context) (err error) {
	for _, s := range h.eventSubs {
		if err := s.drain(h.stopTimeout); err != nil {
			h.logger.Warn("monotest: drain failed", "subject", s.Subject(), "error", err)
		}
	}
	h.eventSubs = nil
	if h.container != nil {
		h.container.close(h.stopTimeout)
	}
	for _, c := range h.deps {
		c.close(h.stopTimeout)
	}
	for _, p := range h.plugins {
		if c, ok := p.Container().(*Container); ok {
			c.close(h.stopTimeout)
		}
	}
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	if !h.started {
		return nil
	}
	h.started = false
	defer func() {
		if r := recover(); r != nil {
			err = monoerrors.WrapModulePanic(h.module.Name(), "stop", r)
		}
	}()
	if err := h.module.Stop(ctx); err != nil {
		return monoerrors.WrapModuleStopFailed(h.module.Name(), err)
	}
	return nil
}

// Call invokes the request-reply service name of the module under test through
// its own container, with JSON serialization, and fails the test on error.
func Call[Req any, Resp any](tb testing.TB, h *Harness, name string, req Req) Resp {
	tb.Helper()
	var resp Resp
	if h.container == nil || !h.started {
		tb.Fatalf("monotest.Call(%s): %v", name, ErrNotStarted)
		return resp
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := helper.CallRequestReplyService(ctx, h.container, name, jsonMarshal, jsonUnmarshal, req, &resp); err != nil {
		tb.Fatalf("monotest.Call(%s): %v", name, err)
	}
	return resp
}

// recordingBus records the subjects published through it (the module's own
// publishes) and delegates everything to the in-memory bus.
type recordingBus struct {
	*EventBus
	mu   sync.Mutex
	subs []string
}

func (r *recordingBus) record(subject string) {
	if strings.HasPrefix(subject, "_INBOX.") {
		return
	}
	r.mu.Lock()
	r.subs = append(r.subs, subject)
	r.mu.Unlock()
}

func (r *recordingBus) subjects() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.subs...)
}

func (r *recordingBus) Publish(subject string, data []byte) error {
	r.record(subject)
	return r.EventBus.Publish(subject, data)
}

func (r *recordingBus) PublishMsg(msg *types.Msg) error {
	if msg != nil {
		r.record(msg.Subject)
	}
	return r.EventBus.PublishMsg(msg)
}

func (r *recordingBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	r.record(subject)
	return r.EventBus.Request(subject, data, timeout)
}

func (r *recordingBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	r.record(subject)
	return r.EventBus.RequestWithContext(ctx, subject, data)
}

func (r *recordingBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	if msg != nil {
		r.record(msg.Subject)
	}
	return r.EventBus.RequestMsgWithContext(ctx, msg)
}
