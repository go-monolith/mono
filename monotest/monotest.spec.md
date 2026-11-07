# monotest

Test support for mono modules without NATS. Implements the "monotest harness" item of the "Module Boundaries (Ports and Adapters)" section of `docs/spec/foundation.md`.

## Purpose

- Let a module be started and exercised in a unit test with the same lifecycle order, the same read-only dependency views and the same module-scoped event bus the framework uses, so a test observes what production would.
- Give every module one conformance test (`CheckModule`) that fails on the Ports and Adapters deviations the framework can detect.

## Components

| Component | Role |
|-----------|------|
| `EventBus` | In-memory `types.EventBus`: subject matching with `*` and `>`, queue groups (one member per group, round-robin), request-reply through `_INBOX.monotest.N` reply subjects, per-subscription ordered delivery on its own goroutine, panic recovery. `EventStream()` returns `ErrJetStreamNotAvailable`. Records published and subscribed subjects. |
| `Container` | The framework's real container (`internal/container.New`) bound to a module, plus a dispatcher that subscribes request-reply handlers (reply payload, or `Mono-Error*` headers on a handler error, `handler-panic` type on a recovered panic) and queue group handlers (ACK first, then handler) on the bus. Counts handler runs per service (`Calls`). Stream consumer and cron services are registered but not dispatched; channel services are served by the container's own routers. |
| `StubRequestReply`, `StubQueueGroup` | JSON stubs for a stand-in container when the provider's contract is not at hand. |
| `Harness` | Runs one module in the framework's order: own container, boundary recorder (`internal/boundary`), plugins (sorted by alias), dependency views (`internal/container.NewDependencyView`), consumed-contract validation (same rules as the lifecycle's step 1.5), scoped bus (`internal/eventbus.NewScoped`) over a recorder of the module's own publishes, `RegisterServices`, events (`WithEmittedEvents` and `EmitEvents()` registered on a real `internal/registry` event registry), `RegisterEventConsumers`, `Start`; only then, as the framework creates subscriptions after every module started, the module's, dependencies' and plugins' services are dispatched and the event consumers subscribed. Handlers receive a runtime context the harness owns. `Stop` drains the subscriptions and waits (bounded by the stop timeout) for handlers in flight, cancels the runtime context, then stops the module. `Report()` is the module's `types.ModuleBoundaryReport`; `ModulePublished()` the subjects the module itself published. |
| `Call[Req, Resp]` | Calls a service of the module under test through its own container with JSON. |
| `CheckModule` | Conformance rules 1–8 (see `check.go` godoc). Rules 2, 4 and 8 are fatal (the module could not be run); the rest are collected. |

## Rules

- `monotest` lives at the repository root (beside `middleware/`, `plugin/`) because it imports `internal/*`; `pkg/` stays free of `internal/` imports.
- Non-strict by default so `CheckModule` can collect every violation in one run; `Strict()` reproduces `WithStrictModuleBoundaries`.
- Every stand-in container and the harness must share one `EventBus`; the harness adopts the dependencies' bus when none is given.
- Rule 6 ("declared but unused") needs `WithExercise` in practice, since adapters look services up at call time; `WithOrderingOnlyDependency` exempts a start-ordering dependency.
- Emitted event definitions are validated before `Start` because the event registry rejects an invalid subject and the failure would otherwise be reported as a start failure.
- Rule 7 looks only at the module's own publishes (`ModulePublished`), so a consumed event the test publishes on the bus is not reported as an undeclared emission.
- Rule 8 hands `Stop` a context with the stop timeout as deadline, as the framework always gives modules a live deadline; `CheckModule` forces non-strict mode so violations are recorded rather than rejected inside the module.
- A call to a dependency service from `Start` finds no responder (`ErrServiceUnavailable`), exactly as in production, because dispatch happens after `Start`.
- A request-reply handler returning a nil response gets no reply (the caller times out), as in the framework; the harness logs why.
- The bus delivers a copy of each message per subscription, requires at least one token after `>`, refuses publishes and subscriptions after `Close`, and reports handler panics through `PanicHandler` and `Panics()`.

## Not covered

- JetStream: stream consumer, event stream consumer and cron services, `EventStream`.
- Middleware chains and worker pools: handlers run directly on the bus's subscription goroutine.
- Request deadlines headers are sent by the real clients but not enforced by the dispatcher.
