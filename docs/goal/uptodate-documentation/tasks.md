# Implementation Plan

Goal: [uptodate-documentation](./goal.md)
Constraints: [constraints.md](./constraints.md)

**Custom Instructions**: Focus on recently modified refactoring on module path - the module path was changed from `github.com/go-monolith/mono/v1` to `github.com/go-monolith/mono` in commit b2fdfcd.

**Analysis Summary**: The `/v1` module path references have been successfully removed from all documentation. However, several documentation issues remain that need to be addressed to achieve the goal of accurate, up-to-date documentation.

## Task List

- [x] 1. Fix broken documentation links in example READMEs

  - Fix `examples/multi-module/README.md` line 411: change `../../docs/spec/monolith-framework/design.md` to `../../docs/spec/foundation.md`
  - Fix `examples/multi-module/README.md` line 412: change `../../pkg/mono/module.go` to `../../pkg/types/module.go` (types are in `pkg/types/`, not `pkg/mono/`)
  - Fix `examples/basic/README.md` line 151: change `../../pkg/mono/module.go` to `../../pkg/types/module.go`
  - Fix `examples/basic/README.md` line 152: change `../../pkg/mono/framework.go` to `../../pkg/types/framework.go`
  - Verify all fixed links point to existing files
  - **Addresses Success Criterion**: All documentation matches the current implementation with no outdated information
  - _Requirements: Accurate documentation links_

- [x] 2. Remove outdated version reference in official documentation

  - Edit `docs/official/README.md` line 6: remove or update the "v1.x" reference
  - Current text: `These docs are for **Monolith Framework v1.x**.`
  - Update to: `These docs are for the **Monolith Framework**. [GitHub Repository](https://github.com/go-monolith/mono)`
  - This aligns with the unversioned module path `github.com/go-monolith/mono`
  - **Addresses Success Criterion**: Official framework docs accurately describe current functionality
  - _Requirements: No version confusion for users_

- [x] 3. Update architecture diagram in root package godoc

  - Edit `doc.go` lines 51-54 to update the Framework Layer diagram
  - Current: Shows "Logger" and "(Structured Logging)" in the third box
  - Update to: Show "EventRegistry" and "(EDA & Consumers)" to match other updated documentation
  - This is a godoc comment change only (no code logic changes)
  - Verify the updated diagram is consistent with `README.md` and `docs/architect/mono-framework-architecture.md`
  - **Addresses Success Criterion**: Every public API has comprehensive godoc comments explaining purpose
  - _Requirements: Accurate architecture representation in pkg.go.dev_

- [x] 4. Verify and commit pending documentation changes

  - Review the uncommitted changes in git status for documentation files:
    - `README.md` - Architecture diagram update (Logger → EventRegistry)
    - `docs/architect/mono-framework-architecture.md` - Architecture diagram update
    - `docs/official/README.md` - GitHub link addition
    - `docs/official/core-concepts/architecture.md` - Component updates
    - `docs/official/getting-started/installation.md` - Import pattern clarification
  - Verify each change is correct and consistent
  - Stage all documentation changes together
  - **Addresses Success Criterion**: All documentation matches the current implementation
  - _Dependencies: 1, 2, 3_
  - _Requirements: Documentation consistency across all files_

- [x] 5. Run examples to verify documentation accuracy

  - Run `make run-example-basic` to verify basic example works
  - Run `make run-example-2` to verify multi-module example works
  - Verify example code in documentation matches actual runnable examples
  - Check that import paths in example code use `github.com/go-monolith/mono` (not `/v1`)
  - **Addresses Success Criterion**: Examples in examples/ directory are accurate, working, and demonstrate current API usage
  - _Dependencies: 1_
  - _Requirements: All examples compile and run correctly_

<!-- New task added on 2026-01-03 - Per user request to add EventRegistry API documentation -->

- [x] 6. Create EventRegistry API reference documentation

  - Create `docs/official/api/eventregistry.md` similar to `docs/official/api/container.md`
  - Include comprehensive method signatures with parameters and return types:
    - `RegisterEvent(def BaseEventDefinition) error`
    - `GetEventByName(name, version, moduleName string) (BaseEventDefinition, bool)`
    - `GetEventsByModule(moduleName string) []BaseEventDefinition`
    - `GetAllEvents() []BaseEventDefinition`
    - `RegisterEventConsumer(eventDef, handler, module, queueGroup...) error`
    - `RegisterEventStreamConsumer(eventDef, config, handler, module) error`
    - `Entries() []EventConsumerEntry`
  - Add detailed examples for each method (similar to container.md examples)
  - Include configuration tables and best practices section
  - Update `docs/official/SUMMARY.md` to add the new page under API Reference section
  - Reference: Interface definition at `pkg/types/event.go:476`
  - **Addresses Success Criterion**: Every public API has comprehensive godoc comments explaining purpose, parameters, return values
  - _Requirements: Complete API documentation for EventRegistry similar to ServiceContainer_

<!-- New tasks added on 2026-01-03 - Address remaining broken documentation references -->

- [x] 7. Fix broken godoc references to non-existent design.md

  - Update godoc comments in `pkg/types/` files that reference `docs/spec/monolith-framework/design.md`
  - Files to update:
    - `pkg/types/framework.go:12` - change to `docs/spec/foundation.md`
    - `pkg/types/module.go:19` - change to `docs/spec/foundation.md`
    - `pkg/types/container.go:17` - change to `docs/spec/foundation.md`
    - `pkg/types/eventbus.go:15` - change to `docs/spec/foundation.md`
    - `pkg/types/logger.go:8` - change to `docs/spec/foundation.md`
    - `pkg/types/middleware.go:25` - change to `docs/spec/foundation.md`
  - Verify `docs/spec/foundation.md` exists and is the correct target
  - These are godoc comment changes only (no code logic changes)
  - **Addresses Success Criterion**: All documentation matches the current implementation with no outdated information
  - _Requirements: Accurate godoc references_

- [x] 8. Fix broken links in analytics example README

  - Update `examples/analytics/README.md` line 356: change `../../docs/spec/monolith-framework/design.md` to `../../docs/spec/foundation.md`
  - Update `examples/analytics/README.md` line 357: change `../../pkg/mono/module.go` to `../../pkg/types/module.go`
  - Verify all fixed links point to existing files
  - **Addresses Success Criterion**: All documentation matches the current implementation with no outdated information
  - _Requirements: Accurate documentation links_

- [x] 9. Fix broken links in benchmark README

  - Update `bench/README.md` line 328: change `../docs/spec/monolith-framework/requirements.md` to `../docs/spec/foundation.md`
  - Update `bench/README.md` line 329: change `../docs/spec/monolith-framework/design.md#performance-considerations` to `../docs/spec/foundation.md#performance-targets`
  - Verify all fixed links point to existing files/sections
  - **Addresses Success Criterion**: All documentation matches the current implementation with no outdated information
  - _Requirements: Accurate documentation links_

- [x] 10. Verify and commit all documentation fixes

  - Run `git status` to review all pending documentation changes
  - Verify each change is correct and consistent
  - Stage all documentation changes together
  - Create commit with descriptive message
  - **Addresses Success Criterion**: All documentation matches the current implementation
  - _Dependencies: 7, 8, 9_
  - _Requirements: Documentation consistency across all files_
