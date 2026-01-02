# Contributing to Mono Framework

Thank you for your interest in contributing to the Mono Framework! This document provides guidelines for contributing to the project.

## Development Setup

For development environment setup, project structure, coding standards, testing guidelines, and available commands, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Contribution Workflow

### Fork and Clone

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/mono.git
   cd mono
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/go-monolith/mono.git
   ```
4. Follow the setup instructions in [DEVELOPMENT.md](DEVELOPMENT.md)

### Creating a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

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

## Pull Request Process

1. **Update Documentation**: Ensure all changes are documented
2. **Add Tests**: Include tests for new functionality
3. **Run Unit Test**: `make test`
4. **Run Integration Test**: `make test-integration`
5. **Run Benchmarks**: `make bench-all-save-json`
6. **Submit PR**:
   - Clear title and description (follow our PR template)
   - Link related issues
   - Attach benchmark results
   - Request review from maintainers

### PR Review Criteria

PRs will be reviewed for:
- Code quality and style
- Test coverage
- Documentation completeness
- Performance implications
- Security considerations
- Breaking changes (must be justified)

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
