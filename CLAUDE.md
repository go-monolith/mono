# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Foundation

[@docs/spec/foundation.md](docs/spec/foundation.md) describe the foundational specification of this project, including architecture, core interfaces, and design principles.

## Common Commands

Refer to `./Makefile` for full list of commands.

## Quick start

```bash
# Configure project and install dependencies
./configure.sh
make install
make check
# Run unit tests & integration tests
make test
make test-integration
# Run examples
make run-example-2
```

## Spec-Driven Development

When working on source files, read the corresponding `.spec.md` file (if exist) first:

- `api_handler.go` -> read `api_handler.spec.md`
- `auth_test.go` -> read `auth.spec.md`

Refer to the original framework features spec in `docs/spec/` for requirements, high-level design and architecture.

### Coding Standards

- **Go Standard Formatting (gofmt)**: All code must be formatted using gofmt. No exceptions. Code that doesn't pass gofmt will not be accepted. Additionally, all code must pass go vet static analysis checks.

- **Comprehensive godoc Comments**: All exported (public) APIs must have clear, complete godoc comments that explain what the API does, its parameters, return values, and any error conditions. Complex APIs should include usage examples in godoc format. Private (unexported) code should also be commented where the purpose is not immediately obvious.

- **Error Handling Best Practices**: Follow Go error handling conventions rigorously. Errors must be wrapped with context using fmt.Errorf with %w verb or equivalent. Library code must never panic - panics should only occur for truly unrecoverable programmer errors. Sentinel errors should be exported for caller inspection. Consider using custom error types for rich error information.

- **Interface-Driven Design**: Prefer small, focused interfaces over large ones or concrete types in public APIs. Follow the "accept interfaces, return structs" principle where appropriate. Interfaces should describe behavior, not implementation details.

## Importance Points
<importance>
- In this framework type interface `mono.MonoApplication` is an alias for `types.MonoFramework`. Must use `MonoApplication` in examples and public documentation.
- **Import Pattern**: Use the root package import `"github.com/go-monolith/mono/v1"` when writing integration tests in `test/integration/` and examples in `examples/`. Helper functions are available from `github.com/go-monolith/mono/v1/pkg/helper`.
- When writing code in `internal/`, you MUST use types from `pkg/types` package to avoid circular dependencies. Strictly no import of root package in `internal/`.
- DO NOT add new alias in `/exports.go` unless explicitly approved by the project maintainer.
</importance>