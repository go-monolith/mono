package mono_test

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// mockLogger is a shared mock logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(_ string, _ ...any)        {}
func (m *mockLogger) Info(_ string, _ ...any)         {}
func (m *mockLogger) Warn(_ string, _ ...any)         {}
func (m *mockLogger) Error(_ string, _ ...any)        {}
func (m *mockLogger) With(_ ...any) mono.Logger       { return m }
func (m *mockLogger) WithModule(_ string) mono.Logger { return m }
func (m *mockLogger) WithError(_ error) mono.Logger   { return m }

// Test-only error extraction utilities

// GetModuleError extracts ModuleError from err if present.
// This is a test utility - use IsModuleError() for production code.
func GetModuleError(err error) (*monoerrors.ModuleError, bool) {
	var moduleErr *monoerrors.ModuleError
	ok := errors.As(err, &moduleErr)
	return moduleErr, ok
}

// GetDependencyError extracts DependencyError from err if present.
// This is a test utility - use IsDependencyError() for production code.
func GetDependencyError(err error) (*monoerrors.DependencyError, bool) {
	var depErr *monoerrors.DependencyError
	ok := errors.As(err, &depErr)
	return depErr, ok
}

// GetTimeoutError extracts TimeoutError from err if present.
// This is a test utility - use IsTimeoutError() for production code.
func GetTimeoutError(err error) (*monoerrors.TimeoutError, bool) {
	var timeoutErr *monoerrors.TimeoutError
	ok := errors.As(err, &timeoutErr)
	return timeoutErr, ok
}

// Test-only panic recovery utilities

// RecoverToError converts a panic value to an error.
// Returns nil if panicValue is nil.
func RecoverToError(panicValue any) error {
	if panicValue == nil {
		return nil
	}

	if err, ok := panicValue.(error); ok {
		return err
	}

	return fmt.Errorf("panic: %v", panicValue)
}

// RecoverModulePanic recovers from a panic in a module operation and converts it to an error.
// This is a test utility for testing panic scenarios.
func RecoverModulePanic(moduleName, operation string) error {
	if r := recover(); r != nil {
		return monoerrors.WrapModulePanic(moduleName, operation, r)
	}
	return nil
}

// Test-only error creation utilities for testing

// NewModuleErrorForTest creates a ModuleError for testing purposes.
func NewModuleErrorForTest(moduleName, operation string, err error) *monoerrors.ModuleError {
	return &monoerrors.ModuleError{
		ModuleName: moduleName,
		Operation:  operation,
		Err:        err,
	}
}

// NewDependencyErrorForTest creates a DependencyError for testing purposes.
func NewDependencyErrorForTest(module, dependency string, chain []string, err error) *monoerrors.DependencyError {
	return &monoerrors.DependencyError{
		Module:     module,
		Dependency: dependency,
		Chain:      chain,
		Err:        err,
	}
}

// NewConfigurationErrorForTest creates a ConfigurationError for testing purposes.
func NewConfigurationErrorForTest(optionIndex int, optionName string, value any, err error) *monoerrors.ConfigurationError {
	return &monoerrors.ConfigurationError{
		OptionIndex: optionIndex,
		OptionName:  optionName,
		Value:       value,
		Err:         err,
	}
}

// CombineErrors combines multiple errors into a single error with newline separation.
// This is used internally by AggregateErrors.
// Returns nil if all errors are nil.
func CombineErrors(errs ...error) error {
	var nonNilErrs []error
	for _, err := range errs {
		if err != nil {
			nonNilErrs = append(nonNilErrs, err)
		}
	}

	if len(nonNilErrs) == 0 {
		return nil
	}

	if len(nonNilErrs) == 1 {
		return nonNilErrs[0]
	}

	var sb strings.Builder
	sb.WriteString("multiple errors occurred:\n")
	for i, err := range nonNilErrs {
		sb.WriteString(fmt.Sprintf("  [%d] %v\n", i+1, err))
	}
	return errors.New(strings.TrimRight(sb.String(), "\n"))
}
