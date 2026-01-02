# Makefile for mono-framework
include $(HOME)/.Makefile
# Variables
GO := go
GOLANGCI_LINT := golangci-lint
GOFMT := gofmt
GOIMPORTS := goimports
COVERAGE_FILE := coverage.out
COVERAGE_HTML := coverage.html

# Go build flags
BUILD_FLAGS := -v
TEST_FLAGS := -v -race
BENCH_FLAGS := -benchmem

.PHONY: help test test-short test-all test-coverage test-verbose lint lint-fix fmt vet build clean bench bench-json bench-json-inprocess bench-json-socket install-tools check mod-tidy mod-download mod-verify pre-commit test-integration

# Default target
.DEFAULT_GOAL := help

# Install development tools for Go
install:
	@echo "Installing development tools..."
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.7.2
	@$(GO) install golang.org/x/tools/cmd/goimports@latest
	@echo "✓ Development tools installed successfully"

# Run unit tests only (excludes test/ directory)
test:
	@echo "Running unit tests (up to 3 attempts)..."
	@pkgs="$$( $(GO) list ./... | grep -v /test )"; \
	for i in 1 2 3; do \
		echo "Attempt $$i/3"; \
		if $(GO) test $(TEST_FLAGS) $$pkgs; then \
			echo "Unit tests passed on attempt $$i"; \
			exit 0; \
		fi; \
	done; \
	echo "Unit tests failed after 3 attempts"; \
	exit 1

# Run short tests (skip timing-sensitive tests)
test-short:
	@echo "Running short unit tests..."
	@$(GO) test $(TEST_FLAGS) -short $(shell $(GO) list ./... | grep -v /test)

# Run tests with verbose output
test-verbose:
	@echo "Running unit tests with verbose output..."
	@$(GO) test -v -race -count=1 $(shell $(GO) list ./... | grep -v /test)

# Run integration tests only (test/ directory)
test-integration:
	@echo "Running integration tests..."
	@$(GO) test $(TEST_FLAGS) -tags=integration ./test/...

# Run all tests (unit + integration)
test-all:
	@echo "Running all tests (unit + integration)..."
	@$(GO) test $(TEST_FLAGS) ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@$(GO) test $(TEST_FLAGS) -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"
	@echo "View it with: open $(COVERAGE_HTML)"

# Run linter
lint:
	@echo "Running golangci-lint..."
	@$(GOLANGCI_LINT) run ./...

# Run linter with auto-fix
lint-fix:
	@echo "Running golangci-lint with auto-fix..."
	@$(GOLANGCI_LINT) run --fix ./...

# Format code
fmt:
	@echo "Formatting code..."
	@$(GOFMT) -s -w .
	@$(GOIMPORTS) -w .
	@echo "Code formatted successfully"

# Run go vet
vet:
	@echo "Running go vet..."
	@$(GO) vet ./...

# Build all packages
build:
	@echo "Building packages..."
	@$(GO) build $(BUILD_FLAGS) ./...
	@echo "Build completed successfully"

# Run all benchmarks
bench:
	@$(GO) test $(BENCH_FLAGS) ./bench/
# Run in-process benchmarks only
bench-inprocess:
	@$(GO) test -bench='BenchmarkInProcess' $(BENCH_FLAGS) ./bench/
# Run socket benchmarks only
bench-socket:
	@$(GO) test -bench='BenchmarkSocket' $(BENCH_FLAGS) ./bench/
# Run multi-module benchmarks only
bench-multi-module:
	@$(GO) test -bench='BenchmarkMultiModule' $(BENCH_FLAGS) ./bench/
# Run all benchmarks with JSON output
bench-all-save-json:
	@echo "Running all benchmarks with JSON output..."
	@$(GO) test $(BENCH_FLAGS) -bench='Benchmark' -json ./bench/ | tee /dev/tty | $(GO) run ./bench/cmd/benchparse
	@echo "Benchmark results written to mono_benchmark_result.json"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(COVERAGE_FILE) $(COVERAGE_HTML)
	@$(GO) clean -cache -testcache -modcache ./...
	@echo "Clean completed successfully"

# Run all quality checks
check: fmt vet lint test-short
	@echo "✓ All checks passed!"

# Tidy go.mod and go.sum
mod-tidy:
	@echo "Tidying go modules..."
	@$(GO) mod tidy
	@echo "Modules tidied successfully"

# Download dependencies
mod-download:
	@echo "Downloading dependencies..."
	@$(GO) mod download
	@echo "Dependencies downloaded successfully"

# Verify dependencies
mod-verify:
	@echo "Verifying dependencies..."
	@$(GO) mod verify
	@echo "Dependencies verified successfully"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@$(GO) install golang.org/x/tools/cmd/goimports@latest
	@echo "✓ Development tools installed successfully"

# Run example 1
run-example-1:
	@echo "Running example 1..."
	@cd examples/basic && $(GO) build $(BUILD_FLAGS) -o dist/ ./... 
	@examples/basic/dist/basic & PID=$$!; sleep 3; kill -2 "$$PID"; wait "$$PID" 2>/dev/null || true
# Run example 2
run-example-2:
	@echo "Running example 2..."
	@cd examples/multi-module && $(GO) build $(BUILD_FLAGS) -o dist/ ./... 
	@examples/multi-module/dist/multi-module & PID=$$!; sleep 5; kill -2 "$$PID"; wait "$$PID" 2>/dev/null || true
# Run example 3
run-example-3:
	@echo "Running example 3..."
	@cd examples/analytics && $(GO) build $(BUILD_FLAGS) -o dist/ ./... 
	@examples/analytics/dist/analytics & PID=$$!; sleep 5; kill -2 "$$PID"; wait "$$PID" 2>/dev/null || true

# Pre-commit checks (used by pre-commit hook)
pre-commit: fmt vet test-short lint
	@echo "✓ Pre-commit checks passed!"
