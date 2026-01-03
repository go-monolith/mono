# Implementation Plan

Goal: [uptodate-documentation](./goal.md)
Constraints: [constraints.md](./constraints.md)

**Custom Instructions**: Focus on recently modified refactoring on module path - the module path was changed from `github.com/go-monolith/mono/v1` to `github.com/go-monolith/mono` in commit b2fdfcd.

**Analysis Summary**: The `/v1` module path references have been successfully removed from all documentation. However, several documentation issues remain that need to be addressed to achieve the goal of accurate, up-to-date documentation.

## Task List

- [ ] 1. Fix broken documentation links in example READMEs

  - Fix `examples/multi-module/README.md` line 411: change `../../docs/spec/monolith-framework/design.md` to `../../docs/spec/foundation.md`
  - Fix `examples/multi-module/README.md` line 412: change `../../pkg/mono/module.go` to `../../pkg/types/module.go` (types are in `pkg/types/`, not `pkg/mono/`)
  - Fix `examples/basic/README.md` line 151: change `../../pkg/mono/module.go` to `../../pkg/types/module.go`
  - Fix `examples/basic/README.md` line 152: change `../../pkg/mono/framework.go` to `../../pkg/types/framework.go`
  - Verify all fixed links point to existing files
  - **Addresses Success Criterion**: All documentation matches the current implementation with no outdated information
  - _Requirements: Accurate documentation links_

- [ ] 2. Remove outdated version reference in official documentation

  - Edit `docs/official/README.md` line 6: remove or update the "v1.x" reference
  - Current text: `These docs are for **Monolith Framework v1.x**.`
  - Update to: `These docs are for the **Monolith Framework**. [GitHub Repository](https://github.com/go-monolith/mono)`
  - This aligns with the unversioned module path `github.com/go-monolith/mono`
  - **Addresses Success Criterion**: Official framework docs accurately describe current functionality
  - _Requirements: No version confusion for users_

- [ ] 3. Update architecture diagram in root package godoc

  - Edit `doc.go` lines 51-54 to update the Framework Layer diagram
  - Current: Shows "Logger" and "(Structured Logging)" in the third box
  - Update to: Show "EventRegistry" and "(EDA & Consumers)" to match other updated documentation
  - This is a godoc comment change only (no code logic changes)
  - Verify the updated diagram is consistent with `README.md` and `docs/architect/mono-framework-architecture.md`
  - **Addresses Success Criterion**: Every public API has comprehensive godoc comments explaining purpose
  - _Requirements: Accurate architecture representation in pkg.go.dev_

- [ ] 4. Verify and commit pending documentation changes

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

- [ ] 5. Run examples to verify documentation accuracy

  - Run `make run-example-basic` to verify basic example works
  - Run `make run-example-2` to verify multi-module example works
  - Verify example code in documentation matches actual runnable examples
  - Check that import paths in example code use `github.com/go-monolith/mono` (not `/v1`)
  - **Addresses Success Criterion**: Examples in examples/ directory are accurate, working, and demonstrate current API usage
  - _Dependencies: 1_
  - _Requirements: All examples compile and run correctly_
