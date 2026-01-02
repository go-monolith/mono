# Contributing to Mono Framework

Thank you for your interest in contributing to the Mono Framework! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Go 1.25 or higher
- Git
- Make (optional, for convenience)

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/mono-framework.git
   cd mono-framework
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/go-monolith/mono.git
   ```
4. Install dependencies:
   ```bash
   go mod download
   ```

## Development Workflow

### Creating a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### Making Changes

1. Make your changes
2. Run tests: `go test ./...`
3. Run linter: `make lint` or `golangci-lint run`
4. Format code: `gofmt -s -w .`
5. Build: `go build ./...`

### Commit Guidelines

- Use conventional commits format: `type(scope): description`
- Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
- Reference issues: `#123`
- Keep commits focused and atomic

Examples:
```
feat(eventbus): add JetStream support
fix(logger): correct log level filtering
docs(readme): update installation instructions
test(registry): add dependency resolution tests
```

### Pre-commit Checks

The project uses pre-commit hooks that run automatically:
- Code formatting (gofmt)
- Go vet
- Tests
- Linter (golangci-lint)

All checks must pass before committing.

## Code Standards

### Go Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use `gofmt` for formatting
- Keep functions small and focused
- Use meaningful variable names

### Documentation

- Add godoc comments for all exported types and functions
- Include usage examples in godoc
- Update README.md for user-facing changes

### Testing

- Write tests for new functionality
- Maintain >80% code coverage
- Use table-driven tests where appropriate
- Test edge cases and error conditions

### Error Handling

- Use typed errors from `pkg/mono/errors.go`
- Wrap errors with context
- Return errors, don't panic (except in truly unrecoverable situations)

## Pull Request Process

1. **Update Documentation**: Ensure all changes are documented
2. **Add Tests**: Include tests for new functionality
3. **Update Changelog**: Add entry to CHANGELOG.md
4. **Run Full Test Suite**: `go test ./...`
5. **Submit PR**: 
   - Clear title and description
   - Link related issues
   - Request review from maintainers

### PR Review Criteria

PRs will be reviewed for:
- Code quality and style
- Test coverage
- Documentation completeness
- Performance implications
- Security considerations
- Breaking changes (must be justified)

## Project Structure

```
mono-framework/
├── pkg/mono/           # Public API
├── internal/           # Internal implementations
├── examples/           # Usage examples
├── docs/               # Documentation
│   └── spec/          # Design specifications
└── tests/              # Integration tests
```

## Testing Guidelines

### Unit Tests

- File: `*_test.go` in same package
- Naming: `TestFunctionName`
- Use subtests with `t.Run()`

### Integration Tests

- Directory: `tests/`
- Use in-memory NATS for testing
- Clean up resources in `defer`

### Benchmarks

- File: `*_test.go`
- Naming: `BenchmarkFunctionName`
- Focus on performance-critical paths

## Issue Guidelines

### Reporting Bugs

Include:
- Go version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Stack trace (if applicable)

### Feature Requests

Include:
- Use case description
- Proposed API (if applicable)
- Alternatives considered
- Breaking changes (if any)

## Security

- Report security issues privately to maintainers
- Do not open public issues for security vulnerabilities
- See SECURITY.md for details

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

## Questions?

- Open a discussion for questions
- Join our community chat (if available)
- Check existing issues and documentation
