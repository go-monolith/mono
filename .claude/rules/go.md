---
paths: **/*.go
---

# Go Code Style Guide

## 1. Formatting

- **MUST use `gofmt`:** All Go code must be formatted with `gofmt` (or `go fmt`). This is non-negotiable and automated.
- **Indentation:** Always use tabs for indentation (enforced by `gofmt`).
- **Line Length:** No strict line length limit. Let `gofmt` handle line wrapping.
- **Braces:** Mandatory for all control structures (`if`, `for`, `switch`). Opening brace on same line.

## 2. Naming

- **Naming Convention:** Use `MixedCaps` or `mixedCaps` for multi-word names. **Never use underscores.**
- **Exported vs. Unexported:**
  - Names starting with uppercase are exported (public): `Foo`, `FooBar`
  - Names starting with lowercase are unexported (private): `foo`, `fooBar`
- **Visibility Rules:**
  - When creating functions, structs, interfaces, enums in packages other than `package main`, **always make them public** (e.g., `func Foo()`, `type Foo struct`, `type Foo interface`)
  - Only make them private (lowercase) if user explicitly requests it
- **Package Names:** Short, concise, single-word, lowercase names.
- **Getters:** Do NOT use `Get` prefix. For a field `owner`, name the getter `Owner()`, not `GetOwner()`.
- **Interface Names:**
  - One-method interfaces: method name + `-er` suffix (e.g., `Reader`, `Writer`, `Stringer`)
  - Multi-method interfaces: descriptive name without `-er` suffix
- **Test Files:** Must end with `_test.go` in the same directory as source (e.g., `utils.go` → `utils_test.go`)

## 3. Control Structures

- **`if` statements:**
  - No parentheses around conditions
  - Braces are mandatory
  - Can include initialization: `if err := file.Chmod(0664); err != nil { ... }`
- **`for` loops:**
  - Go's only looping construct (no `while`)
  - Use `for...range` to iterate over slices, maps, strings, and channels
  - Standard forms: `for init; condition; post { }`, `for condition { }`, `for { }`
- **`switch` statements:**
  - More general than C
  - Cases do NOT fall through by default (use `fallthrough` explicitly if needed)
  - Can be used without expression for cleaner `if-else-if` chains
  - Can have initialization statement

## 4. Functions

- **Multiple Returns:** Use multiple return values, especially for `(result, error)` pattern.
- **Error Handling:**
  - Use error return pattern: `func foo() (string, error)`
  - `err` object MUST be checked immediately after function call
  - Handle errors appropriately (log, return, or wrap)
  - Do NOT discard errors with blank identifier `_`
  - Libraries should generally not `panic` (reserved for unrecoverable situations)
- **Named Result Parameters:** Can name return parameters for clarity and documentation.
- **`defer` statements:**
  - Schedules function call to run immediately before function returns
  - Use for cleanup tasks (closing files, unlocking mutexes, etc.)
  - Multiple defers execute in LIFO order
- **Context Pattern:**
  - Add `context.Context` as **first parameter** for functions that perform I/O or batch processing
  - Example: `func foo(ctx context.Context, name string, value int) error`
  - If `ctx` is not used within function, include it but use `_` to ignore: `func foo(_ context.Context, ...) error`
  - When calling functions that require `context.Context`, pass the received `ctx` down to them

## 5. Data Types and Structures

- **`new` vs. `make`:**
  - `new(T)`: Allocates zeroed memory for type `T`, returns pointer `*T`
  - `make(T, ...)`: Creates and initializes slices, maps, and channels ONLY. Returns initialized `T` (not pointer)
- **Slices:**
  - Preferred way to work with sequences (more flexible than arrays)
  - Use `make` to create: `make([]int, length, capacity)`
- **Maps:**
  - Use "comma ok" idiom to check key existence: `value, ok := myMap[key]`
  - Use `make` to create: `make(map[string]int)`
- **Interfaces:**
  - **Always use `any` instead of `interface{}`**
  - Types implement interfaces implicitly (no `implements` keyword needed)
  - Prefer many small interfaces over one large interface
  - Standard library pattern: single-method interfaces (e.g., `io.Reader`, `io.Writer`)

## 6. Concurrency

- **Philosophy:** Share memory by communicating, do NOT communicate by sharing memory.
- **Goroutines:**
  - Lightweight concurrent functions
  - Start with `go` keyword: `go functionName(args)`
- **Channels:**
  - Typed conduits for goroutine communication
  - Create with `make`: `ch := make(chan int)` or `ch := make(chan int, bufferSize)`
  - Use for synchronization and data passing between goroutines

## 7. Error Handling

- **`error` type:** Use built-in `error` interface as standard error handling mechanism.
- **Explicit Checking:** Always check errors explicitly, never ignore them.
- **Error Wrapping:** Use `fmt.Errorf` with `%w` verb to wrap errors and preserve context.
- **Panic:** Only use for truly exceptional, unrecoverable situations. Libraries should avoid panicking.

## 8. Unit Testing

- **Test Files:** End with `_test.go` in same directory as source code
  - Example: `utils.go` → `utils_test.go`
- **Test Functions:** Name with `Test` prefix: `func TestFunctionName(t *testing.T)`
- **Table-Driven Tests:** Prefer table-driven tests for multiple test cases
- **Test Helpers:** Mark helper functions with `t.Helper()` for better error messages

## 9. Code Review Requirements

- **Mandatory Review:** Always use subagent `gosu-mcp-core:gosu-code-reviewer` (run in background) after making changes to Go files
- **Bundling:** Try to bundle multiple related files for review at once (e.g., modify 3 files for a fix, then review all 3 together)
- **Fix Issues:** After code review completes, you MUST fix the files according to feedback before proceeding

---

*Based on [Effective Go](https://go.dev/doc/effective_go) and project conventions*
