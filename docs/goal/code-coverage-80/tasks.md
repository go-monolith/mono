# Implementation Plan

Goal: [code-coverage-80](./goal.md)
Constraints: [constraints.md](./constraints.md)

**Custom Instructions:** Focus on newly added features/changes in the last 10 commits

**Summary of Codebase Analysis:**
- Overall coverage: ~93% (exceeds 80% target)
- Recent commits (#3 error propagation, #2 rename, #1 initial framework) are well-tested
- Primary gap: `plugin/kv-jetstream` at 84.0% (adapter wrapper functions have 0% coverage)

## Milestones

1. Milestone 1 - KV JetStream Adapter Coverage
   - Tasks: 1-3
   - Notes: Focus on adapter wrapper functions that delegate to `*WithContext()` methods

## Task List

- [x] 1. Add tests for kv-jetstream adapter basic operations (Get, Set, Delete)
  - Add tests for `Get()`, `Set()`, `Delete()` wrapper methods in `plugin/kv-jetstream/adapter.go`
  - These methods delegate to their `*WithContext()` counterparts with `context.Background()`
  - Verify correct delegation and error propagation
  - Target: Move coverage from 0% to 100% for these 3 functions
  - File: `plugin/kv-jetstream/adapter_test.go`
  - Note: Current tests only cover `*WithContext()` variants, not the convenience wrappers
  - _Requirements: Overall coverage 80%+, meaningful tests_

- [x] 2. Add tests for kv-jetstream adapter reset and close operations
  - Add tests for `Reset()`, `ResetWithContext()`, `Close()` methods in `plugin/kv-jetstream/adapter.go`
  - Test successful reset/close scenarios
  - Test error propagation when backend fails
  - Target: Move coverage from 0% to 100% for these functions
  - File: `plugin/kv-jetstream/adapter_test.go`
  - _Dependencies: 1_
  - _Requirements: Overall coverage 80%+, meaningful tests_

- [x] 3. Add tests for kv-jetstream adapter advanced operations (Watch, Create, Update, Purge, Keys, Status, GetEntry, PutWithRevision)
  - Add tests for remaining adapter wrapper methods with 0% coverage
  - Focus on: `Watch()`, `Create()`, `Update()`, `Purge()`, `Keys()`, `Status()`, `GetEntry()`, `PutWithRevision()`
  - Verify each method correctly delegates to its `*WithContext()` counterpart
  - Use table-driven tests where appropriate
  - Target: Move plugin/kv-jetstream from 84% to 95%+
  - File: `plugin/kv-jetstream/adapter_test.go`
  - _Dependencies: 2_
  - _Requirements: Overall coverage 80%+, meaningful tests_
