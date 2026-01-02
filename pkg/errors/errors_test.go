package errors_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pkgerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// mockModule implements types.Module for testing
type mockModule struct {
	name string
}

func (m *mockModule) Name() string {
	return m.name
}

func (m *mockModule) Start(ctx context.Context) error {
	return nil
}

func (m *mockModule) Stop(ctx context.Context) error {
	return nil
}

// TestModuleError tests ModuleError type
func TestModuleError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method with operation", func(t *testing.T) {
		err := &pkgerrors.ModuleError{
			ModuleName: "test-module",
			Operation:  "start",
			Err:        baseErr,
		}

		errMsg := err.Error()
		expected := "module 'test-module': start failed: base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method without error", func(t *testing.T) {
		err := &pkgerrors.ModuleError{
			ModuleName: "test-module",
			Operation:  "start",
		}

		errMsg := err.Error()
		expected := "module 'test-module': start failed"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.ModuleError{
			ModuleName: "test-module",
			Operation:  "start",
			Err:        baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})
}

// TestServiceError tests ServiceError type
func TestServiceError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method", func(t *testing.T) {
		err := &pkgerrors.ServiceError{
			ServiceName: "test-service",
			ModuleName:  "test-module",
			ServiceType: types.ServiceTypeRequestReply,
			Err:         baseErr,
		}

		errMsg := err.Error()
		expected := "service 'test-service' (request_reply) in module 'test-module': base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method without error", func(t *testing.T) {
		err := &pkgerrors.ServiceError{
			ServiceName: "test-service",
			ModuleName:  "test-module",
			ServiceType: types.ServiceTypeChannel,
		}

		errMsg := err.Error()
		expected := "service 'test-service' (channel) in module 'test-module'"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.ServiceError{
			ServiceName: "test-service",
			Err:         baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})
}

// TestDependencyError tests DependencyError type
func TestDependencyError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method with chain", func(t *testing.T) {
		err := &pkgerrors.DependencyError{
			Module:     "module-a",
			Dependency: "module-b",
			Chain:      []string{"module-a", "module-b", "module-c"},
			Err:        baseErr,
		}

		errMsg := err.Error()
		expected := "module 'module-a' depends on 'module-b': base error (chain: module-a -> module-b -> module-c)"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method without chain", func(t *testing.T) {
		err := &pkgerrors.DependencyError{
			Module:     "module-a",
			Dependency: "module-b",
			Err:        baseErr,
		}

		errMsg := err.Error()
		expected := "module 'module-a' depends on 'module-b': base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.DependencyError{
			Err: baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})
}

// TestConfigurationError tests ConfigurationError type
func TestConfigurationError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method with value", func(t *testing.T) {
		err := &pkgerrors.ConfigurationError{
			OptionIndex: 1,
			OptionName:  "max-connections",
			Value:       "invalid",
			Err:         baseErr,
		}

		errMsg := err.Error()
		expected := "option 1 (max-connections) failed: base error (value: invalid)"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method without value", func(t *testing.T) {
		err := &pkgerrors.ConfigurationError{
			OptionIndex: 2,
			OptionName:  "timeout",
			Err:         baseErr,
		}

		errMsg := err.Error()
		expected := "option 2 (timeout) failed: base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.ConfigurationError{
			Err: baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})
}

// TestTimeoutError tests TimeoutError type
func TestTimeoutError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method", func(t *testing.T) {
		err := &pkgerrors.TimeoutError{
			Operation: "startup",
			Duration:  5 * time.Second,
			Err:       baseErr,
		}

		errMsg := err.Error()
		expected := "operation 'startup' timed out after 5s: base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method without error", func(t *testing.T) {
		err := &pkgerrors.TimeoutError{
			Operation: "shutdown",
			Duration:  10 * time.Second,
		}

		errMsg := err.Error()
		expected := "operation 'shutdown' timed out after 10s"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.TimeoutError{
			Err: baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})

	t.Run("Timeout method", func(t *testing.T) {
		err := &pkgerrors.TimeoutError{
			Operation: "test",
		}

		if !err.Timeout() {
			t.Error("Timeout() should return true")
		}
	})
}

// TestEventStreamError tests EventStreamError type
func TestEventStreamError(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("Error method with stream and subject", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			StreamName: "EVENTS",
			Subject:    "events.test.v1.created",
			Operation:  "publish",
			Err:        baseErr,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed for stream 'EVENTS' on subject 'events.test.v1.created': base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method with subject only", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			Subject:   "events.test.v1.created",
			Operation: "publish",
			Err:       baseErr,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed on subject 'events.test.v1.created': base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Unwrap method", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			Err: baseErr,
		}

		unwrapped := err.Unwrap()
		if unwrapped != baseErr {
			t.Errorf("expected unwrapped error to be baseErr, got %v", unwrapped)
		}
	})
}

// TestWrapModuleFunctions tests module error wrapper functions
func TestWrapModuleFunctions(t *testing.T) {
	t.Run("WrapInvalidModule with module", func(t *testing.T) {
		module := &mockModule{name: "test-module"}
		err := pkgerrors.WrapInvalidModule(module, "missing Name method")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var moduleErr *pkgerrors.ModuleError
		if !errors.As(err, &moduleErr) {
			t.Errorf("expected ModuleError, got %T", err)
		}
		if moduleErr.ModuleName != "test-module" {
			t.Errorf("expected module name 'test-module', got %q", moduleErr.ModuleName)
		}
	})

	t.Run("WrapInvalidModule with nil", func(t *testing.T) {
		err := pkgerrors.WrapInvalidModule(nil, "module is nil")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var moduleErr *pkgerrors.ModuleError
		if !errors.As(err, &moduleErr) {
			t.Errorf("expected ModuleError, got %T", err)
		}
		if moduleErr.ModuleName != "<nil>" {
			t.Errorf("expected module name '<nil>', got %q", moduleErr.ModuleName)
		}
	})

	t.Run("WrapModuleNotFound", func(t *testing.T) {
		err := pkgerrors.WrapModuleNotFound("missing-module")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var moduleErr *pkgerrors.ModuleError
		if !errors.As(err, &moduleErr) {
			t.Errorf("expected ModuleError, got %T", err)
		}
		if !errors.Is(err, pkgerrors.ErrModuleNotFound) {
			t.Error("expected error to wrap ErrModuleNotFound")
		}
	})

	t.Run("WrapModuleAlreadyRegistered", func(t *testing.T) {
		err := pkgerrors.WrapModuleAlreadyRegistered("duplicate-module")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModuleAlreadyRegistered) {
			t.Error("expected error to wrap ErrModuleAlreadyRegistered")
		}
	})

	t.Run("WrapModuleStartFailed", func(t *testing.T) {
		baseErr := errors.New("connection failed")
		err := pkgerrors.WrapModuleStartFailed("failed-module", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModuleStartFailed) {
			t.Error("expected error to wrap ErrModuleStartFailed")
		}
	})

	t.Run("WrapModuleStopFailed", func(t *testing.T) {
		baseErr := errors.New("cleanup failed")
		err := pkgerrors.WrapModuleStopFailed("failed-module", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModuleStopFailed) {
			t.Error("expected error to wrap ErrModuleStopFailed")
		}
	})

	t.Run("WrapModulePanic", func(t *testing.T) {
		panicValue := "panic value"
		err := pkgerrors.WrapModulePanic("panic-module", "start", panicValue)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModulePanic) {
			t.Error("expected error to wrap ErrModulePanic")
		}
	})
}

// TestWrapPluginFunctions tests plugin error wrapper functions
func TestWrapPluginFunctions(t *testing.T) {
	t.Run("WrapPluginNotFound", func(t *testing.T) {
		err := pkgerrors.WrapPluginNotFound("missing-plugin")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrPluginNotFound) {
			t.Error("expected error to wrap ErrPluginNotFound")
		}
	})

	t.Run("WrapPluginAlreadyRegistered", func(t *testing.T) {
		err := pkgerrors.WrapPluginAlreadyRegistered("duplicate-plugin")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrPluginAlreadyRegistered) {
			t.Error("expected error to wrap ErrPluginAlreadyRegistered")
		}
	})

	t.Run("WrapPluginStartFailed", func(t *testing.T) {
		baseErr := errors.New("init failed")
		err := pkgerrors.WrapPluginStartFailed("failed-plugin", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModuleStartFailed) {
			t.Error("expected error to wrap ErrModuleStartFailed")
		}
	})

	t.Run("WrapPluginStopFailed", func(t *testing.T) {
		baseErr := errors.New("cleanup failed")
		err := pkgerrors.WrapPluginStopFailed("failed-plugin", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrModuleStopFailed) {
			t.Error("expected error to wrap ErrModuleStopFailed")
		}
	})
}

// TestWrapServiceFunctions tests service error wrapper functions
func TestWrapServiceFunctions(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("WrapServiceError", func(t *testing.T) {
		err := pkgerrors.WrapServiceError("test-service", "test-module", types.ServiceTypeRequestReply, baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var serviceErr *pkgerrors.ServiceError
		if !errors.As(err, &serviceErr) {
			t.Errorf("expected ServiceError, got %T", err)
		}
		if serviceErr.ServiceName != "test-service" {
			t.Errorf("expected service name 'test-service', got %q", serviceErr.ServiceName)
		}
	})

	t.Run("WrapServiceNotFound", func(t *testing.T) {
		err := pkgerrors.WrapServiceNotFound("missing-service", "test-module")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrServiceNotFound) {
			t.Error("expected error to wrap ErrServiceNotFound")
		}
	})

	t.Run("WrapServiceAlreadyRegistered", func(t *testing.T) {
		err := pkgerrors.WrapServiceAlreadyRegistered("duplicate-service", "test-module", types.ServiceTypeQueueGroup)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrServiceAlreadyRegistered) {
			t.Error("expected error to wrap ErrServiceAlreadyRegistered")
		}
	})

	t.Run("WrapServiceUnavailable", func(t *testing.T) {
		err := pkgerrors.WrapServiceUnavailable("unavailable-service", "test-module", types.ServiceTypeRequestReply, baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrServiceUnavailable) {
			t.Error("expected error to wrap ErrServiceUnavailable")
		}
	})
}

// TestWrapDependencyFunctions tests dependency error wrapper functions
func TestWrapDependencyFunctions(t *testing.T) {
	t.Run("WrapMissingDependency", func(t *testing.T) {
		err := pkgerrors.WrapMissingDependency("module-a", "module-b")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrMissingDependency) {
			t.Error("expected error to wrap ErrMissingDependency")
		}

		var depErr *pkgerrors.DependencyError
		if errors.As(err, &depErr) {
			if depErr.Module != "module-a" {
				t.Errorf("expected module 'module-a', got %q", depErr.Module)
			}
			if depErr.Dependency != "module-b" {
				t.Errorf("expected dependency 'module-b', got %q", depErr.Dependency)
			}
		}
	})

	t.Run("WrapCircularDependency", func(t *testing.T) {
		chain := []string{"module-a", "module-b", "module-a"}
		err := pkgerrors.WrapCircularDependency(chain)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrCircularDependency) {
			t.Error("expected error to wrap ErrCircularDependency")
		}

		var depErr *pkgerrors.DependencyError
		if errors.As(err, &depErr) {
			if len(depErr.Chain) != 3 {
				t.Errorf("expected chain length 3, got %d", len(depErr.Chain))
			}
		}
	})
}

// TestWrapConfigurationFunctions tests configuration error wrapper functions
func TestWrapConfigurationFunctions(t *testing.T) {
	t.Run("WrapInvalidConfiguration", func(t *testing.T) {
		err := pkgerrors.WrapInvalidConfiguration(1, "port", "invalid", "must be between 1024 and 65535")
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrInvalidConfiguration) {
			t.Error("expected error to wrap ErrInvalidConfiguration")
		}

		var configErr *pkgerrors.ConfigurationError
		if errors.As(err, &configErr) {
			if configErr.OptionName != "port" {
				t.Errorf("expected option name 'port', got %q", configErr.OptionName)
			}
			if configErr.Value != "invalid" {
				t.Errorf("expected value 'invalid', got %v", configErr.Value)
			}
			if configErr.OptionIndex != 1 {
				t.Errorf("expected option index 1, got %d", configErr.OptionIndex)
			}
		}
	})
}

// TestWrapTimeoutFunctions tests timeout error wrapper functions
func TestWrapTimeoutFunctions(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("WrapTimeout", func(t *testing.T) {
		err := pkgerrors.WrapTimeout("module-startup", 10*time.Second, baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var timeoutErr *pkgerrors.TimeoutError
		if errors.As(err, &timeoutErr) {
			if timeoutErr.Operation != "module-startup" {
				t.Errorf("expected operation 'module-startup', got %q", timeoutErr.Operation)
			}
			if timeoutErr.Duration != 10*time.Second {
				t.Errorf("expected duration 10s, got %v", timeoutErr.Duration)
			}
			if !timeoutErr.Timeout() {
				t.Error("Timeout() should return true")
			}
		}
	})
}

// TestWrapEventStreamFunctions tests event stream error wrapper functions
func TestWrapEventStreamFunctions(t *testing.T) {
	baseErr := errors.New("base error")

	t.Run("WrapEventStreamError", func(t *testing.T) {
		err := pkgerrors.WrapEventStreamError("EVENTS", "events.test.v1.created", "publish", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var streamErr *pkgerrors.EventStreamError
		if !errors.As(err, &streamErr) {
			t.Errorf("expected EventStreamError, got %T", err)
		}
		if streamErr.Subject != "events.test.v1.created" {
			t.Errorf("expected subject 'events.test.v1.created', got %q", streamErr.Subject)
		}
		if streamErr.StreamName != "EVENTS" {
			t.Errorf("expected stream name 'EVENTS', got %q", streamErr.StreamName)
		}
	})

	t.Run("WrapEventStreamNotAvailable", func(t *testing.T) {
		err := pkgerrors.WrapEventStreamNotAvailable("events.test.v1.created", "publish", baseErr)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		var streamErr *pkgerrors.EventStreamError
		if !errors.As(err, &streamErr) {
			t.Errorf("expected EventStreamError, got %T", err)
		}
	})
}

// TestErrorCheckingUtilities tests Is* functions
func TestErrorCheckingUtilities(t *testing.T) {
	t.Run("IsModuleError", func(t *testing.T) {
		err := &pkgerrors.ModuleError{ModuleName: "test"}
		if !pkgerrors.IsModuleError(err) {
			t.Error("IsModuleError should return true for ModuleError")
		}

		regularErr := errors.New("not a module error")
		if pkgerrors.IsModuleError(regularErr) {
			t.Error("IsModuleError should return false for regular error")
		}
	})

	t.Run("IsServiceError", func(t *testing.T) {
		err := &pkgerrors.ServiceError{ServiceName: "test"}
		if !pkgerrors.IsServiceError(err) {
			t.Error("IsServiceError should return true for ServiceError")
		}

		regularErr := errors.New("not a service error")
		if pkgerrors.IsServiceError(regularErr) {
			t.Error("IsServiceError should return false for regular error")
		}
	})

	t.Run("IsDependencyError", func(t *testing.T) {
		err := &pkgerrors.DependencyError{Module: "test"}
		if !pkgerrors.IsDependencyError(err) {
			t.Error("IsDependencyError should return true for DependencyError")
		}

		regularErr := errors.New("not a dependency error")
		if pkgerrors.IsDependencyError(regularErr) {
			t.Error("IsDependencyError should return false for regular error")
		}
	})

	t.Run("IsConfigurationError", func(t *testing.T) {
		err := &pkgerrors.ConfigurationError{OptionName: "test"}
		if !pkgerrors.IsConfigurationError(err) {
			t.Error("IsConfigurationError should return true for ConfigurationError")
		}

		regularErr := errors.New("not a config error")
		if pkgerrors.IsConfigurationError(regularErr) {
			t.Error("IsConfigurationError should return false for regular error")
		}
	})

	t.Run("IsTimeoutError", func(t *testing.T) {
		err := &pkgerrors.TimeoutError{Operation: "test"}
		if !pkgerrors.IsTimeoutError(err) {
			t.Error("IsTimeoutError should return true for TimeoutError")
		}

		regularErr := errors.New("not a timeout error")
		if pkgerrors.IsTimeoutError(regularErr) {
			t.Error("IsTimeoutError should return false for regular error")
		}
	})

	t.Run("IsEventStreamError", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{Subject: "test"}
		if !pkgerrors.IsEventStreamError(err) {
			t.Error("IsEventStreamError should return true for EventStreamError")
		}

		regularErr := errors.New("not a stream error")
		if pkgerrors.IsEventStreamError(regularErr) {
			t.Error("IsEventStreamError should return false for regular error")
		}
	})
}

// TestGetErrorFunctions tests Get* accessor functions
func TestGetErrorFunctions(t *testing.T) {
	t.Run("GetServiceError", func(t *testing.T) {
		serviceErr := &pkgerrors.ServiceError{
			ServiceName: "test-service",
			ModuleName:  "test-module",
		}
		wrappedErr := fmt.Errorf("wrapped: %w", serviceErr)

		extracted, ok := pkgerrors.GetServiceError(wrappedErr)
		if !ok {
			t.Fatal("GetServiceError should extract ServiceError")
		}
		if extracted.ServiceName != "test-service" {
			t.Errorf("expected service name 'test-service', got %q", extracted.ServiceName)
		}

		_, ok = pkgerrors.GetServiceError(errors.New("not a service error"))
		if ok {
			t.Error("GetServiceError should return false for non-ServiceError")
		}
	})

	t.Run("GetConfigurationError", func(t *testing.T) {
		configErr := &pkgerrors.ConfigurationError{
			OptionName: "test-option",
			Value:      "test-value",
		}
		wrappedErr := fmt.Errorf("wrapped: %w", configErr)

		extracted, ok := pkgerrors.GetConfigurationError(wrappedErr)
		if !ok {
			t.Fatal("GetConfigurationError should extract ConfigurationError")
		}
		if extracted.OptionName != "test-option" {
			t.Errorf("expected option name 'test-option', got %q", extracted.OptionName)
		}

		_, ok = pkgerrors.GetConfigurationError(errors.New("not a config error"))
		if ok {
			t.Error("GetConfigurationError should return false for non-ConfigurationError")
		}
	})

	t.Run("GetEventStreamError", func(t *testing.T) {
		streamErr := &pkgerrors.EventStreamError{
			Subject:   "test-subject",
			Operation: "test-op",
		}
		wrappedErr := fmt.Errorf("wrapped: %w", streamErr)

		extracted, ok := pkgerrors.GetEventStreamError(wrappedErr)
		if !ok {
			t.Fatal("GetEventStreamError should extract EventStreamError")
		}
		if extracted.Subject != "test-subject" {
			t.Errorf("expected subject 'test-subject', got %q", extracted.Subject)
		}

		_, ok = pkgerrors.GetEventStreamError(errors.New("not a stream error"))
		if ok {
			t.Error("GetEventStreamError should return false for non-EventStreamError")
		}
	})
}

// TestAggregateErrors tests error aggregation
func TestAggregateErrors(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		err := pkgerrors.AggregateErrors([]error{})
		if err != nil {
			t.Errorf("expected nil for empty slice, got %v", err)
		}
	})

	t.Run("single error", func(t *testing.T) {
		singleErr := errors.New("single error")
		err := pkgerrors.AggregateErrors([]error{singleErr})
		if err != singleErr {
			t.Errorf("expected original error, got %v", err)
		}
	})

	t.Run("multiple errors", func(t *testing.T) {
		errs := []error{
			errors.New("error 1"),
			errors.New("error 2"),
			errors.New("error 3"),
		}
		err := pkgerrors.AggregateErrors(errs)
		if err == nil {
			t.Fatal("expected aggregated error, got nil")
		}

		errMsg := err.Error()
		// Check that error message contains all errors
		if !contains(errMsg, "error 1") || !contains(errMsg, "error 2") || !contains(errMsg, "error 3") {
			t.Errorf("aggregated error should contain all errors: %q", errMsg)
		}
		if !contains(errMsg, "multiple errors occurred") {
			t.Errorf("aggregated error should have header: %q", errMsg)
		}
	})

	t.Run("with nil errors", func(t *testing.T) {
		errs := []error{
			errors.New("error 1"),
			nil,
			errors.New("error 2"),
		}
		err := pkgerrors.AggregateErrors(errs)
		if err == nil {
			t.Fatal("expected aggregated error, got nil")
		}

		// Should skip nil errors
		errMsg := err.Error()
		if !contains(errMsg, "error 1") || !contains(errMsg, "error 2") {
			t.Errorf("aggregated error should contain non-nil errors: %q", errMsg)
		}
	})
}

// TestFormatDependencyChain tests dependency chain formatting
func TestFormatDependencyChain(t *testing.T) {
	t.Run("empty chain", func(t *testing.T) {
		result := pkgerrors.FormatDependencyChain([]string{})
		if result != "" {
			t.Errorf("expected empty string, got %q", result)
		}
	})

	t.Run("single module", func(t *testing.T) {
		result := pkgerrors.FormatDependencyChain([]string{"module-a"})
		if result != "module-a" {
			t.Errorf("expected 'module-a', got %q", result)
		}
	})

	t.Run("multiple modules", func(t *testing.T) {
		result := pkgerrors.FormatDependencyChain([]string{"module-a", "module-b", "module-c"})
		if result != "module-a -> module-b -> module-c" {
			t.Errorf("expected 'module-a -> module-b -> module-c', got %q", result)
		}
	})
}

// TestFormatFrameworkState tests framework state formatting
func TestFormatFrameworkState(t *testing.T) {
	tests := []struct {
		state    types.MonoFrameworkState
		expected string
	}{
		{types.StateCreated, "Created"},
		{types.StateStarting, "Starting"},
		{types.StateRunning, "Running"},
		{types.StateStopping, "Stopping"},
		{types.StateStopped, "Stopped"},
		{types.MonoFrameworkState(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := pkgerrors.FormatFrameworkState(tt.state)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// contains is a helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestDependencyErrorAdditionalCases tests additional DependencyError cases for better coverage
func TestDependencyErrorAdditionalCases(t *testing.T) {
	t.Run("Error method with chain but no error", func(t *testing.T) {
		err := &pkgerrors.DependencyError{
			Module:     "module-a",
			Dependency: "module-b",
			Chain:      []string{"module-a", "module-b", "module-c"},
			Err:        nil,
		}

		errMsg := err.Error()
		expected := "module 'module-a' depends on 'module-b' (chain: module-a -> module-b -> module-c)"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method with no chain and no error", func(t *testing.T) {
		err := &pkgerrors.DependencyError{
			Module:     "module-a",
			Dependency: "module-b",
			Err:        nil,
		}

		errMsg := err.Error()
		expected := "module 'module-a' depends on 'module-b'"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})
}

// TestEventStreamErrorAdditionalCases tests additional EventStreamError cases for better coverage
func TestEventStreamErrorAdditionalCases(t *testing.T) {
	t.Run("Error method with stream and subject but no error", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			StreamName: "EVENTS",
			Subject:    "events.test.v1.created",
			Operation:  "publish",
			Err:        nil,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed for stream 'EVENTS' on subject 'events.test.v1.created'"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method with subject only but no error", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			Subject:   "events.test.v1.created",
			Operation: "publish",
			Err:       nil,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed on subject 'events.test.v1.created'"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method with no stream or subject but with error", func(t *testing.T) {
		baseErr := errors.New("base error")
		err := &pkgerrors.EventStreamError{
			Operation: "publish",
			Err:       baseErr,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed: base error"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})

	t.Run("Error method with no stream, subject, or error", func(t *testing.T) {
		err := &pkgerrors.EventStreamError{
			Operation: "publish",
			Err:       nil,
		}

		errMsg := err.Error()
		expected := "event stream error: operation 'publish' failed"
		if errMsg != expected {
			t.Errorf("expected %q, got %q", expected, errMsg)
		}
	})
}

// TestWrapCircularDependencyEdgeCases tests edge cases for WrapCircularDependency
func TestWrapCircularDependencyEdgeCases(t *testing.T) {
	t.Run("WrapCircularDependency with empty chain", func(t *testing.T) {
		chain := []string{}
		err := pkgerrors.WrapCircularDependency(chain)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrCircularDependency) {
			t.Error("expected error to wrap ErrCircularDependency")
		}

		var depErr *pkgerrors.DependencyError
		if errors.As(err, &depErr) {
			if len(depErr.Chain) != 0 {
				t.Errorf("expected empty chain, got length %d", len(depErr.Chain))
			}
			if depErr.Module != "" {
				t.Errorf("expected empty Module, got %q", depErr.Module)
			}
			if depErr.Dependency != "" {
				t.Errorf("expected empty Dependency, got %q", depErr.Dependency)
			}
		}
	})

	t.Run("WrapCircularDependency with single element chain", func(t *testing.T) {
		chain := []string{"module-a"}
		err := pkgerrors.WrapCircularDependency(chain)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, pkgerrors.ErrCircularDependency) {
			t.Error("expected error to wrap ErrCircularDependency")
		}

		var depErr *pkgerrors.DependencyError
		if errors.As(err, &depErr) {
			// When chain length < 2, the function returns empty chain
			if len(depErr.Chain) != 0 {
				t.Errorf("expected empty chain for len < 2, got %d", len(depErr.Chain))
			}
		}
	})
}
