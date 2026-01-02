# Constraints

These constraints define the hard boundaries for achieving the goal "Uptodate Documentation" defined in [goal.md](./goal.md).

## Tech Stack & Libraries

| Category | Allowed | Not Allowed |
|----------|---------|-------------|
| Language | Go (godocs), Markdown | Other languages |
| Documentation | GitBook format, Markdown | External doc generators |
| Tools | Standard Go tooling (go doc, godoc) | Third-party doc tools |

## Git & Cadence

- **Git Branching Strategy**: No specific constraint
- **Git Commit Frequency**: No specific constraint
- **OKR Review Cadence**: N/A
- **Human Executive Check-in Frequency**: N/A

## Security & Compliance

- No sensitive information (credentials, tokens, internal URLs) in documentation
- No proprietary implementation details in public-facing docs

## Additional Instructions

- Go through recent git commits to identify areas where documentation may be outdated (up to 20 commits)
- Focus on changes that impact public APIs, configuration options, and usage patterns

## "Don't Touch" Areas

The following areas should NOT be modified during this documentation effort:

- `internal/` - All internal packages (no code modifications)
- `*_test.go` - All test files (no modifications)
- `pkg/` - Source code in pkg/ packages (documentation/godocs only)
- All source code files EXCEPT code in `examples/`

**Allowed modifications:**
- `examples/` - Example code can be updated to ensure accuracy
- `docs/` - All documentation files
- `README.md` - Root readme
- Godoc comments in source files (documentation only, no code logic changes)

## Definition of "Safe Change"

- ✅ No changes to framework code logic or behavior
- ✅ Documentation changes only (markdown, godoc comments)
- ✅ Examples compile and run correctly

## Additional Constraints

- GitBook documentation should follow GitBook's best practices as outlined in [document-with-gitbook-best-practices.md](/docs/analyst/document-with-gitbook-best-practices.md)
- Godoc comments must follow Go documentation conventions
- All code examples in `./examples` must be runnable. 
- All code examples in documentation must be verified for syntax & accuracy.
- All code examples for Module implementation using this framework must follow "Hexagonal Architecture" principles as outlined in [hexagonal-architecture.md](/docs/architect/hexagonal-architecture.md)
- Do not use "Service" in module names in examples or documentation. Use "Module" suffix instead (e.g., `OrderModule` not `OrderService`). This is to avoid confusion with inter-module communication service terminology or micro-services architecture.
- All anchors/links in markdown documentation must be verified to ensure they point to the correct and existing sections/files.

---

*These constraints are part of the Goal Driven Development (GDD) process.*
