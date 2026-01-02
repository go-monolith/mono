# Constraints

These constraints define the hard boundaries for achieving the goal "Code Coverage 80%" defined in [goal.md](./goal.md).

## Tech Stack & Libraries

| Category | Allowed | Not Allowed |
|----------|---------|-------------|
| Language | Go 1.21+ | Any other language |
| Testing | Go standard `testing` package, `testify` for assertions | External mocking frameworks (mockery, gomock) |
| Libraries | Standard library only | Third-party test utilities |

## Git & Cadence

- **Git Branching Strategy**: N/A (no commits)
- **Git Commit Frequency**: **Strictly no git commits** - all changes are made without committing
- **OKR Review Cadence**: N/A
- **Human Executive Check-in Frequency**: N/A

## Security & Compliance

- Tests must not expose sensitive data or credentials
- Test fixtures must use mock/fake data only

## Performance Targets

| Metric | Target | Current |
|--------|--------|---------|
| Overall Code Coverage | ≥88% | 80-95% (estimated) |
| Per-Package Minimum | ≥80% | Varies |

## "Don't Touch" Areas

**CRITICAL CONSTRAINT**: Do NOT modify any existing implementation code.

- ❌ **All source files** - No modifications to any `.go` files that are not test files
- ❌ **API signatures** - No changes to public or internal API signatures
- ❌ **Package structure** - No restructuring or moving of packages

**Only allowed modifications**:
- ✅ Files ending with `_test.go` (test files)
- ✅ Test helper files (e.g., `testutil.go`, `mock_*.go` within test packages)
- ✅ New test fixtures and test data files

## Definition of "Safe Change"

- ✅ All existing tests pass (both unit-test and integration-test)
- ✅ New tests are added only in `*_test.go` files
- ✅ No modifications to implementation code
- ✅ Tests are meaningful and cover real scenarios
- ✅ Test code follows Go testing conventions

## Additional Constraints

- Focus on testing public APIs and exported functions first
- Prioritize testing error paths and edge cases over happy paths (which are likely already covered)
- Use table-driven tests where appropriate for better coverage of multiple scenarios
- Document any areas that cannot reach 80% coverage with justification

---

*These constraints are part of the Goal Driven Development (GDD) process.*
