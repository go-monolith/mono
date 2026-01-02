# Development Guide

This guide is for contributors and developers working on the Mono Framework codebase itself. For using the framework in your applications, see the [official documentation](docs/official/README.md).

## Prerequisites

- Go 1.25 or higher
- Git
- Make (recommended)

## Quick Setup

```bash
# Clone the repository
git clone https://github.com/go-monolith/mono.git
cd mono

# Configure project and install dependencies
./configure.sh
make install

# Verify setup
make check
```

## Development Commands

### Testing

```bash
make test               # Run unit tests (excludes integration tests)
make test-short         # Run short tests (skip timing-sensitive)
make test-integration   # Run integration tests only
make test-all           # Run all tests (unit + integration)
make test-coverage      # Generate coverage report (opens coverage.html)
```

### Code Quality

```bash
make fmt                # Format code (gofmt + goimports)
make vet                # Run go vet
make lint               # Run golangci-lint
make lint-fix           # Run linter with auto-fix
make check              # Run all quality checks (fmt, vet, lint, test-short)
```

### Building

```bash
make build              # Build all packages
make clean              # Clean build artifacts and caches
```

### Running Examples

```bash
make run-example-1      # Basic module example
make run-example-2      # Multi-module with services
make run-example-3      # Analytics with channel services
make run-example-4      # Event emitter pattern
```

### Benchmarks

```bash
make bench              # Run all benchmarks
make bench-inprocess    # In-process benchmarks only
make bench-socket       # Socket benchmarks only
make bench-multi-module # Multi-module benchmarks only
make bench-all-save-json # Run benchmarks and save JSON results
```

### Module Management

```bash
make mod-tidy           # Tidy go.mod and go.sum
make mod-download       # Download dependencies
make mod-verify         # Verify dependencies
```

## Project Structure

```
mono-framework/
├── pkg/                        # PUBLIC API PACKAGES
│   ├── types/                  # Core interfaces & type definitions
│   ├── errors/                 # Error handling utilities
│   └── helper/                 # Helper functions
│
├── internal/                   # INTERNAL IMPLEMENTATIONS (not for import)
│   ├── app/                    # Framework lifecycle
│   ├── container/              # ServiceContainer implementation
│   ├── eventbus/               # EventBus implementation
│   ├── lifecycle/              # Module lifecycle management
│   ├── logger/                 # Logger implementation
│   ├── middleware/             # Middleware chain execution
│   ├── nats/                   # NATS server management
│   └── registry/               # Module and event registries
│
├── middleware/                 # BUILT-IN MIDDLEWARE
│   ├── accesslog/              # Access logging
│   ├── audit/                  # Audit logging
│   └── requestid/              # Request ID injection
│
├── plugin/                     # BUILT-IN PLUGINS
│   ├── fs-jetstream/           # File storage plugin
│   └── kv-jetstream/           # Key-value storage plugin
│
├── examples/                   # EXAMPLE APPLICATIONS
│   ├── basic/                  # Hello World
│   ├── multi-module/           # Order system with dependencies
│   ├── analytics/              # Channel services
│   └── event-emitter/          # Event pub/sub
│
├── test/integration/           # Integration tests
├── bench/                      # Performance benchmarks
└── docs/                       # Documentation
    ├── official/               # GitBook documentation
    └── spec/                  # Design specifications
```

## Coding Standards

### Go Conventions

- **Formatting**: All code must pass `gofmt` and `go vet`
- **Linting**: All code must pass `golangci-lint`
- **Naming**: Follow Go naming conventions (CamelCase for exports, camelCase for unexported)

### Documentation

- All exported APIs must have comprehensive godoc comments
- Comments should explain purpose, parameters, return values, and error conditions
- Complex APIs should include usage examples

### Error Handling

- Wrap errors with context using `fmt.Errorf` with `%w` verb
- Library code must never panic (except for truly unrecoverable programmer errors)
- Use sentinel errors for caller inspection

### Interface Design

- Prefer small, focused interfaces over large ones
- Follow "accept interfaces, return structs" principle
- Interfaces should describe behavior, not implementation

### Import Rules

- Use root package `github.com/go-monolith/mono` in `examples/` and `test/integration/`
- Use `pkg/types` in `internal/` packages to avoid circular dependencies
- Never import root package from `internal/`

## Testing Guidelines

### Unit Tests

- Located alongside source files as `*_test.go`
- Cover happy paths, error cases, and edge conditions
- Use table-driven tests where appropriate

### Integration Tests

- Located in `test/integration/`
- Test module interactions through NATS
- Validate end-to-end scenarios

### Coverage Target

- Maintain >80% code coverage for public APIs
- See current coverage at [docs/official/extra/code-coverage.md](docs/official/extra/code-coverage.md)

## Pre-commit Hooks

The project uses pre-commit hooks that run automatically:

```bash
make pre-commit         # Manually run pre-commit checks
```

This runs: `fmt`, `vet`, `test-short`, `lint`

## Contributing

For contribution guidelines, PR process, and commit conventions, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

For security policies and reporting vulnerabilities, see [SECURITY.md](SECURITY.md).
