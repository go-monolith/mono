# monotest

Test support for mono modules without NATS. Implements the "monotest harness" item of the "Module Boundaries (Ports and Adapters)" section of `docs/spec/foundation.md`.

## Purpose

- Let a module be started and exercised in a unit test with the same lifecycle order, the same read-only dependency views and the same module-scoped event bus the framework uses, so a test observes what production would.
- Give every module one conformance test (`CheckModule`) that fails on the Ports and Adapters deviations the framework can detect.
- Give every application one test (`CheckImports`) for the contract package import rule, which no runtime check can see.

## Components

| Component | Role |
|-----------|------|
| `EventBus` | In-memory `types.EventBus`: subject matching with `*` and `>`, queue groups (one member per group, round-robin), request-reply through `_INBOX.monotest.N` reply subjects, per-subscription ordered delivery on its own goroutine, panic recovery. `EventStream()` returns `ErrJetStreamNotAvailable`. Records published and subscribed subjects. |
| `Container` | The framework's real container (`internal/container.New`) bound to a module, plus a dispatcher that subscribes request-reply handlers (reply payload, or `Mono-Error*` headers on a handler error, `handler-panic` type on a recovered panic) and queue group handlers (ACK first, then handler) on the bus. Counts handler runs per service (`Calls`). Stream consumer and cron services are registered but not dispatched; channel services are served by the container's own routers. |
| `StubRequestReply`, `StubQueueGroup` | JSON stubs for a stand-in container when the provider's contract is not at hand. |
| `Harness` | Runs one module in the framework's order: own container, boundary recorder (`internal/boundary`), plugins (sorted by alias), dependency views (`internal/container.NewDependencyView`), consumed-contract validation (same rules as the lifecycle's step 1.5), scoped bus (`internal/eventbus.NewScoped`) over a recorder of the module's own publishes, `RegisterServices`, events (`WithEmittedEvents` and `EmitEvents()` registered on a real `internal/registry` event registry), `RegisterEventConsumers`, `Start`; only then, as the framework creates subscriptions after every module started, the module's, dependencies' and plugins' services are dispatched and the event consumers subscribed. Handlers receive a runtime context the harness owns. `Stop` drains the subscriptions and waits (bounded by the stop timeout) for handlers in flight, cancels the runtime context, then stops the module. `Report()` is the module's `types.ModuleBoundaryReport`; `ModulePublished()` the subjects the module itself published. |
| `Call[Req, Resp]` | Calls a service of the module under test through its own container with JSON. |
| `CheckModule` | Conformance rules 1–9 (see `check.go` godoc). Rules 2, 4 and 8 are fatal (the module could not be run); the rest are collected. |
| `CheckImports`, `ImportViolations` | The contract package import rule on the import graph (`imports.go`). Runs `go list` in the test's directory (the root directory of the application) on `./...` and applies two rules: `contract-import` (a contract package imports only the standard library, `pkg/types`, `pkg/helper` and other contract packages) and `module-import` (a package outside a module imports only that module's contract package). `ImportViolations` returns the findings without a test. |

## Rules

- `monotest` lives at the repository root (beside `middleware/`, `plugin/`) because it imports `internal/*`; `pkg/` stays free of `internal/` imports.
- Non-strict by default so `CheckModule` can collect every violation in one run; `Strict()` reproduces `WithStrictModuleBoundaries`.
- Every stand-in container and the harness must share one `EventBus`; the harness adopts the dependencies' bus when none is given.
- Rule 6 ("declared but unused") needs `WithExercise` in practice, since adapters look services up at call time; `WithOrderingOnlyDependency` exempts a start-ordering dependency.
- Emitted event definitions are validated before `Start` because the event registry rejects an invalid subject and the failure would otherwise be reported as a start failure.
- Rule 7 looks only at the module's own publishes (`ModulePublished`), so a consumed event the test publishes on the bus is not reported as an undeclared emission.
- Rule 5 includes `undeclared-service-use`: the harness enforces the consumed list exactly as the lifecycle does (`Recorder.EnforceConsumed` when the module implements `ServiceConsumerModule`).
- Rule 9 ("consumed but never looked up") reads `ModuleBoundaryReport.UnusedContracts`, so it covers contracts of declared dependencies only (plugin containers are not observed); `WithUnexercisedContract` exempts a contract the exercise cannot reach. Rules 5 and 9 together make `ConsumeServices()` exact.
- Rule 8 hands `Stop` a context with the stop timeout as deadline, as the framework always gives modules a live deadline; `CheckModule` forces non-strict mode so violations are recorded rather than rejected inside the module.
- A call to a dependency service from `Start` finds no responder (`ErrServiceUnavailable`), exactly as in production, because dispatch happens after `Start`.
- A request-reply handler returning a nil response gets no reply (the caller times out), as in the framework; the harness logs why.
- The bus delivers a copy of each message per subscription, requires at least one token after `>`, refuses publishes and subscriptions after `Close`, and reports handler panics through `PanicHandler` and `Panics()`.

## Import rule (`CheckImports`)

- A **module** is a package of the main module (or workspace) that has a `contract` sub-package, together with every package below it; `WithModuleRoots` adds modules without one. The last `contract` path element decides, so `<module>/contract/v2` is a contract package of `<module>`. Only path elements below the Go module path count, so a Go module named `.../contract` is not one big contract package. A dependency's `contract` directory does not make it a module.
- Module roots are collected from every package of the application: besides the `go list -deps` of the patterns, every package of the main Go module (every workspace module in workspace mode) is listed for its path (`go list -m`, then `go list -e -find ./...` in each module directory). A check limited to one module's packages (`WithImportPatterns("./order/...")`) therefore knows the other modules even when nothing it depends on imports their contract. Imports are checked for the matched packages only.
- A check that applied a rule to no package is an error ("no package was checked"), not a pass: patterns that match nothing (`go list` only warns), or only `package main` and host packages, which is what `./...` matches beside a `cmd/<app>/main.go`. The test belongs in the root directory of the application, or uses `WithImportDir`.
- With nested modules the innermost module owns a package. Packages of one module import each other freely.
- `package main` and `WithHostPackages` are exempt from `module-import`: the host constructs and registers the modules. Contract packages are checked by `contract-import` only.
- `WithAllowedContractImports` extends what a contract package may import (a serialization runtime, a shared value type). The default follows the documented rule strictly.
- Imports of test files are not listed (`go list` without `-test`), so an integration test may wire real modules.
- It shells out to `go list` (standard tooling, no `golang.org/x/tools` dependency). `ImportViolations` takes a context (`CheckImports` passes `tb.Context()`), bounded to two minutes; a listing failure or an ended context is fatal in `CheckImports` and an error in `ImportViolations`, never a silent pass.
- Packages are listed for the default build context: files behind build tags or for another operating system are not checked.
- The pure rule logic (`findImportViolations`) is separate from the `go list` call and unit-tested with synthetic package lists; `testdata/imports` is a small application (its own Go module, standard library only) with known violations. Tests that list it set `GOWORK=off` and clear `GOFLAGS`, and skip when the fixture is absent (a module zip leaves nested modules out).

## Not covered

- JetStream: stream consumer, event stream consumer and cron services, `EventStream`.
- Imports of test files, and a module reaching another through an interface value handed to it by the host (no import to see).
- Middleware chains and worker pools: handlers run directly on the bus's subscription goroutine.
- Request deadlines headers are sent by the real clients but not enforced by the dispatcher.
