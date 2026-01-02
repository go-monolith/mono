package mono_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

// Test sentinel errors
func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedMsg    string
		shouldBeNonNil bool
	}{
		{"monoerrors.ErrModuleNotFound", monoerrors.ErrModuleNotFound, "module not found", true},
		{"monoerrors.ErrServiceNotFound", monoerrors.ErrServiceNotFound, "service not found", true},
		{"monoerrors.ErrModuleAlreadyRegistered", monoerrors.ErrModuleAlreadyRegistered, "module already registered", true},
		{"monoerrors.ErrServiceAlreadyRegistered", monoerrors.ErrServiceAlreadyRegistered, "service already registered", true},
		{"monoerrors.ErrCircularDependency", monoerrors.ErrCircularDependency, "circular dependency detected", true},
		{"monoerrors.ErrMissingDependency", monoerrors.ErrMissingDependency, "missing dependency", true},
		{"monoerrors.ErrServiceUnavailable", monoerrors.ErrServiceUnavailable, "service unavailable", true},
		{"monoerrors.ErrInvalidConfiguration", monoerrors.ErrInvalidConfiguration, "invalid configuration", true},
		{"monoerrors.ErrModuleStartFailed", monoerrors.ErrModuleStartFailed, "module start failed", true},
		{"monoerrors.ErrModuleStopFailed", monoerrors.ErrModuleStopFailed, "module stop failed", true},
		{"monoerrors.ErrModulePanic", monoerrors.ErrModulePanic, "module panic", true},
		{"monoerrors.ErrContainerNotBound", monoerrors.ErrContainerNotBound, "container not bound to module", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldBeNonNil && tt.err == nil {
				t.Errorf("%s should not be nil", tt.name)
			}
			if tt.err != nil && tt.err.Error() != tt.expectedMsg {
				t.Errorf("%s error message = %q, want %q", tt.name, tt.err.Error(), tt.expectedMsg)
			}
		})
	}
}

// Test ModuleError
func TestModuleError(t *testing.T) {
	t.Run("with wrapped error", func(t *testing.T) {
		err := NewModuleErrorForTest("auth", "start", monoerrors.ErrModuleStartFailed)
		expectedMsg := "module 'auth': start failed: module start failed"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("without wrapped error", func(t *testing.T) {
		err := NewModuleErrorForTest("auth", "start", nil)
		expectedMsg := "module 'auth': start failed"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		wrapped := errors.New("underlying error")
		err := NewModuleErrorForTest("auth", "start", wrapped)
		if err.Unwrap() != wrapped {
			t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), wrapped)
		}
	})

	t.Run("errors.Is through wrapping", func(t *testing.T) {
		err := NewModuleErrorForTest("auth", "start", monoerrors.ErrModuleStartFailed)
		if !errors.Is(err, monoerrors.ErrModuleStartFailed) {
			t.Error("errors.Is(err, monoerrors.ErrModuleStartFailed) = false, want true")
		}
	})
}

// Test monoerrors.WrapModuleNotFound
func TestWrapModuleNotFound(t *testing.T) {
	err := monoerrors.WrapModuleNotFound("payment")
	expectedMsg := "module 'payment': lookup failed: module not found"
	if err.Error() != expectedMsg {
		t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
	}

	if !errors.Is(err, monoerrors.ErrModuleNotFound) {
		t.Error("errors.Is(err, monoerrors.ErrModuleNotFound) = false, want true")
	}
}

// Test monoerrors.WrapModuleAlreadyRegistered
func TestWrapModuleAlreadyRegistered(t *testing.T) {
	err := monoerrors.WrapModuleAlreadyRegistered("order")
	expectedMsg := "module 'order': register failed: module already registered"
	if err.Error() != expectedMsg {
		t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
	}

	if !errors.Is(err, monoerrors.ErrModuleAlreadyRegistered) {
		t.Error("errors.Is(err, monoerrors.ErrModuleAlreadyRegistered) = false, want true")
	}
}

// Test monoerrors.WrapModuleStartFailed
func TestWrapModuleStartFailed(t *testing.T) {
	baseErr := errors.New("database connection failed")
	err := monoerrors.WrapModuleStartFailed("inventory", baseErr)

	if !errors.Is(err, monoerrors.ErrModuleStartFailed) {
		t.Error("errors.Is(err, monoerrors.ErrModuleStartFailed) = false, want true")
	}

	if !strings.Contains(err.Error(), "inventory") {
		t.Errorf("error message should contain module name 'inventory': %s", err.Error())
	}
}

// Test monoerrors.WrapModulePanic
func TestWrapModulePanic(t *testing.T) {
	tests := []struct {
		name       string
		panicValue any
	}{
		{"string panic", "null pointer dereference"},
		{"error panic", errors.New("some error")},
		{"struct panic", struct{ msg string }{msg: "test"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := monoerrors.WrapModulePanic("notification", "start", tt.panicValue)
			if !errors.Is(err, monoerrors.ErrModulePanic) {
				t.Error("errors.Is(err, monoerrors.ErrModulePanic) = false, want true")
			}
			if !strings.Contains(err.Error(), "notification") {
				t.Errorf("error message should contain module name: %s", err.Error())
			}
		})
	}
}

// Test monoerrors.ServiceError
func TestServiceError(t *testing.T) {
	t.Run("with wrapped error", func(t *testing.T) {
		err := monoerrors.WrapServiceError("payment-processor", "payment", mono.ServiceTypeRequestReply, monoerrors.ErrServiceNotFound)
		expectedMsg := "service 'payment-processor' (request_reply) in module 'payment': service not found"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("without wrapped error", func(t *testing.T) {
		err := monoerrors.WrapServiceError("payment-processor", "payment", mono.ServiceTypeChannel, nil)
		expectedMsg := "service 'payment-processor' (channel) in module 'payment'"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		wrapped := errors.New("underlying error")
		err := monoerrors.WrapServiceError("svc", "mod", mono.ServiceTypeQueueGroup, wrapped)
		serviceErr, ok := err.(*monoerrors.ServiceError)
		if !ok {
			t.Fatal("expected *monoerrors.ServiceError")
		}
		if serviceErr.Unwrap() != wrapped {
			t.Errorf("Unwrap() = %v, want %v", serviceErr.Unwrap(), wrapped)
		}
	})
}

// Test monoerrors.WrapServiceUnavailableWithType
func TestWrapServiceUnavailableWithType(t *testing.T) {
	t.Run("with RequestReply service type", func(t *testing.T) {
		baseErr := errors.New("nats: no responders available for request")
		err := monoerrors.WrapServiceUnavailable("check-stock", "inventory", mono.ServiceTypeRequestReply, baseErr)

		// Should wrap monoerrors.ErrServiceUnavailable
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Error("errors.Is(err, monoerrors.ErrServiceUnavailable) = false, want true")
		}

		// Should be a monoerrors.ServiceError
		serviceErr, ok := monoerrors.GetServiceError(err)
		if !ok {
			t.Fatal("expected monoerrors.ServiceError")
		}

		// Verify fields
		if serviceErr.ServiceName != "check-stock" {
			t.Errorf("ServiceName = %q, want 'check-stock'", serviceErr.ServiceName)
		}
		if serviceErr.ModuleName != "inventory" {
			t.Errorf("ModuleName = %q, want 'inventory'", serviceErr.ModuleName)
		}
		if serviceErr.ServiceType != mono.ServiceTypeRequestReply {
			t.Errorf("mono.ServiceType = %v, want mono.ServiceTypeRequestReply", serviceErr.ServiceType)
		}

		// Error message should contain all context
		errMsg := err.Error()
		if !strings.Contains(errMsg, "check-stock") {
			t.Errorf("error message should contain service name: %s", errMsg)
		}
		if !strings.Contains(errMsg, "inventory") {
			t.Errorf("error message should contain module name: %s", errMsg)
		}
		if !strings.Contains(errMsg, "request_reply") {
			t.Errorf("error message should contain service type: %s", errMsg)
		}
		if !strings.Contains(errMsg, "no responders") {
			t.Errorf("error message should contain underlying error: %s", errMsg)
		}
	})

	t.Run("with QueueGroup service type", func(t *testing.T) {
		baseErr := errors.New("connection closed")
		err := monoerrors.WrapServiceUnavailable("process-order", "order", mono.ServiceTypeQueueGroup, baseErr)

		serviceErr, ok := monoerrors.GetServiceError(err)
		if !ok {
			t.Fatal("expected monoerrors.ServiceError")
		}

		if serviceErr.ServiceType != mono.ServiceTypeQueueGroup {
			t.Errorf("mono.ServiceType = %v, want mono.ServiceTypeQueueGroup", serviceErr.ServiceType)
		}
	})

	t.Run("preserves original error in chain", func(t *testing.T) {
		baseErr := errors.New("original error")
		err := monoerrors.WrapServiceUnavailable("svc", "mod", mono.ServiceTypeRequestReply, baseErr)

		// The error message should contain the original error
		if !strings.Contains(err.Error(), "original error") {
			t.Errorf("error should contain original error message: %s", err.Error())
		}

		// Verify monoerrors.ErrServiceUnavailable is in the error chain
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Error("should preserve monoerrors.ErrServiceUnavailable in chain")
		}
	})
}

// Test DependencyError
func TestDependencyError(t *testing.T) {
	t.Run("with chain", func(t *testing.T) {
		chain := []string{"order", "inventory", "payment"}
		err := NewDependencyErrorForTest("order", "payment", chain, monoerrors.ErrCircularDependency)
		if !strings.Contains(err.Error(), "order -> inventory -> payment") {
			t.Errorf("error message should contain chain: %s", err.Error())
		}
	})

	t.Run("without chain", func(t *testing.T) {
		err := NewDependencyErrorForTest("order", "inventory", nil, monoerrors.ErrMissingDependency)
		expectedMsg := "module 'order' depends on 'inventory': missing dependency"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		err := NewDependencyErrorForTest("a", "b", nil, monoerrors.ErrMissingDependency)
		if err.Unwrap() != monoerrors.ErrMissingDependency {
			t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), monoerrors.ErrMissingDependency)
		}
	})
}

// Test monoerrors.WrapCircularDependency
func TestWrapCircularDependency(t *testing.T) {
	t.Run("with chain", func(t *testing.T) {
		chain := []string{"A", "B", "C", "A"}
		err := monoerrors.WrapCircularDependency(chain)

		if !errors.Is(err, monoerrors.ErrCircularDependency) {
			t.Error("errors.Is(err, monoerrors.ErrCircularDependency) = false, want true")
		}

		depErr, ok := GetDependencyError(err)
		if !ok {
			t.Fatal("expected DependencyError")
		}
		if depErr.Module != "A" {
			t.Errorf("Module = %q, want 'A'", depErr.Module)
		}
		if depErr.Dependency != "A" {
			t.Errorf("Dependency = %q, want 'A'", depErr.Dependency)
		}
	})

	t.Run("empty chain", func(t *testing.T) {
		err := monoerrors.WrapCircularDependency([]string{})
		if !errors.Is(err, monoerrors.ErrCircularDependency) {
			t.Error("errors.Is(err, monoerrors.ErrCircularDependency) = false, want true")
		}
	})
}

// Test ConfigurationError
func TestConfigurationError(t *testing.T) {
	t.Run("with value", func(t *testing.T) {
		err := NewConfigurationErrorForTest(2, "WithNATSPort", 4222, monoerrors.ErrInvalidConfiguration)
		if !strings.Contains(err.Error(), "option 2") {
			t.Errorf("error message should contain option index: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "WithNATSPort") {
			t.Errorf("error message should contain option name: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "4222") {
			t.Errorf("error message should contain value: %s", err.Error())
		}
	})

	t.Run("without value", func(t *testing.T) {
		err := NewConfigurationErrorForTest(1, "WithLogger", nil, errors.New("logger is nil"))
		expectedMsg := "option 1 (WithLogger) failed: logger is nil"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})
}

// Test TimeoutError
func TestTimeoutError(t *testing.T) {
	t.Run("with wrapped error", func(t *testing.T) {
		duration := 5 * time.Second
		baseErr := errors.New("context deadline exceeded")
		err := monoerrors.WrapTimeout("shutdown", duration, baseErr)
		if !strings.Contains(err.Error(), "shutdown") {
			t.Errorf("error message should contain operation: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "5s") {
			t.Errorf("error message should contain duration: %s", err.Error())
		}
	})
}

// Test monoerrors.EventStreamError
func TestEventStreamError(t *testing.T) {
	t.Run("with stream name and subject", func(t *testing.T) {
		baseErr := errors.New("nats: no responders available for request")
		err := monoerrors.WrapEventStreamError("ORDERS", "orders.new", "publish", baseErr)
		expectedMsg := "event stream error: operation 'publish' failed for stream 'ORDERS' on subject 'orders.new': nats: no responders available for request"
		if err.Error() != expectedMsg {
			t.Errorf("error message = %q, want %q", err.Error(), expectedMsg)
		}
	})

	t.Run("with subject only", func(t *testing.T) {
		baseErr := errors.New("nats: no responders available for request")
		err := monoerrors.WrapEventStreamNotAvailable("orders.new", "publish", baseErr)
		if !strings.Contains(err.Error(), "orders.new") {
			t.Errorf("error message should contain subject: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "publish") {
			t.Errorf("error message should contain operation: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "no stream configured") {
			t.Errorf("error message should contain 'no stream configured': %s", err.Error())
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		baseErr := errors.New("underlying error")
		err := monoerrors.WrapEventStreamError("ORDERS", "orders.new", "publish", baseErr)
		unwrapped := err.(*monoerrors.EventStreamError).Unwrap()
		if unwrapped != baseErr {
			t.Errorf("Unwrap() = %v, want %v", unwrapped, baseErr)
		}
	})
}

// Test error checking utilities
func TestErrorCheckingUtilities(t *testing.T) {
	t.Run("monoerrors.IsModuleError", func(t *testing.T) {
		err := NewModuleErrorForTest("auth", "start", nil)
		if !monoerrors.IsModuleError(err) {
			t.Error("monoerrors.IsModuleError(ModuleError) = false, want true")
		}
		if monoerrors.IsModuleError(errors.New("not a module error")) {
			t.Error("monoerrors.IsModuleError(generic error) = true, want false")
		}
	})

	t.Run("monoerrors.IsServiceError", func(t *testing.T) {
		err := monoerrors.WrapServiceError("svc", "mod", mono.ServiceTypeChannel, nil)
		if !monoerrors.IsServiceError(err) {
			t.Error("monoerrors.IsServiceError(monoerrors.ServiceError) = false, want true")
		}
		if monoerrors.IsServiceError(errors.New("not a service error")) {
			t.Error("monoerrors.IsServiceError(generic error) = true, want false")
		}
	})

	t.Run("monoerrors.IsDependencyError", func(t *testing.T) {
		err := NewDependencyErrorForTest("a", "b", nil, nil)
		if !monoerrors.IsDependencyError(err) {
			t.Error("monoerrors.IsDependencyError(DependencyError) = false, want true")
		}
		if monoerrors.IsDependencyError(errors.New("not a dependency error")) {
			t.Error("monoerrors.IsDependencyError(generic error) = true, want false")
		}
	})

	t.Run("monoerrors.IsConfigurationError", func(t *testing.T) {
		err := NewConfigurationErrorForTest(1, "opt", nil, nil)
		if !monoerrors.IsConfigurationError(err) {
			t.Error("monoerrors.IsConfigurationError(ConfigurationError) = false, want true")
		}
		if monoerrors.IsConfigurationError(errors.New("not a config error")) {
			t.Error("monoerrors.IsConfigurationError(generic error) = true, want false")
		}
	})

	t.Run("monoerrors.IsTimeoutError", func(t *testing.T) {
		err := monoerrors.WrapTimeout("op", time.Second, nil)
		if !monoerrors.IsTimeoutError(err) {
			t.Error("monoerrors.IsTimeoutError(TimeoutError) = false, want true")
		}
		if monoerrors.IsTimeoutError(errors.New("not a timeout error")) {
			t.Error("monoerrors.IsTimeoutError(generic error) = true, want false")
		}
	})

	t.Run("monoerrors.IsEventStreamError", func(t *testing.T) {
		err := monoerrors.WrapEventStreamNotAvailable("orders.new", "publish", errors.New("no responders"))
		if !monoerrors.IsEventStreamError(err) {
			t.Error("monoerrors.IsEventStreamError(monoerrors.EventStreamError) = false, want true")
		}
		if monoerrors.IsEventStreamError(errors.New("not an event stream error")) {
			t.Error("monoerrors.IsEventStreamError(generic error) = true, want false")
		}
	})
}

// Test error extraction utilities
func TestErrorExtractionUtilities(t *testing.T) {
	t.Run("GetModuleError", func(t *testing.T) {
		expected := NewModuleErrorForTest("test", "op", nil)
		moduleErr, ok := GetModuleError(expected)
		if !ok {
			t.Error("GetModuleError should return true for ModuleError")
		}
		if moduleErr.ModuleName != "test" {
			t.Errorf("ModuleName = %q, want 'test'", moduleErr.ModuleName)
		}

		_, ok = GetModuleError(errors.New("generic"))
		if ok {
			t.Error("GetModuleError should return false for non-ModuleError")
		}
	})

	t.Run("monoerrors.GetServiceError", func(t *testing.T) {
		expected := monoerrors.WrapServiceError("svc", "mod", mono.ServiceTypeChannel, nil)
		serviceErr, ok := monoerrors.GetServiceError(expected)
		if !ok {
			t.Error("monoerrors.GetServiceError should return true for monoerrors.ServiceError")
		}
		if serviceErr.ServiceName != "svc" {
			t.Errorf("ServiceName = %q, want 'svc'", serviceErr.ServiceName)
		}
	})

	t.Run("GetDependencyError", func(t *testing.T) {
		expected := NewDependencyErrorForTest("a", "b", []string{"a", "b"}, nil)
		depErr, ok := GetDependencyError(expected)
		if !ok {
			t.Error("GetDependencyError should return true for DependencyError")
		}
		if depErr.Module != "a" {
			t.Errorf("Module = %q, want 'a'", depErr.Module)
		}
	})

	t.Run("GetTimeoutError", func(t *testing.T) {
		expected := monoerrors.WrapTimeout("op", time.Second, nil)
		timeoutErr, ok := GetTimeoutError(expected)
		if !ok {
			t.Error("GetTimeoutError should return true for TimeoutError")
		}
		if timeoutErr.Operation != "op" {
			t.Errorf("Operation = %q, want 'op'", timeoutErr.Operation)
		}
	})

	t.Run("monoerrors.GetEventStreamError", func(t *testing.T) {
		expected := monoerrors.WrapEventStreamNotAvailable("orders.new", "publish", errors.New("no responders"))
		streamErr, ok := monoerrors.GetEventStreamError(expected)
		if !ok {
			t.Error("monoerrors.GetEventStreamError should return true for monoerrors.EventStreamError")
		}
		if streamErr.Subject != "orders.new" {
			t.Errorf("Subject = %q, want 'orders.new'", streamErr.Subject)
		}
		if streamErr.Operation != "publish" {
			t.Errorf("Operation = %q, want 'publish'", streamErr.Operation)
		}

		_, ok = monoerrors.GetEventStreamError(errors.New("generic"))
		if ok {
			t.Error("monoerrors.GetEventStreamError should return false for non-monoerrors.EventStreamError")
		}
	})
}

// Test CombineErrors
func TestCombineErrors(t *testing.T) {
	t.Run("no errors", func(t *testing.T) {
		err := CombineErrors()
		if err != nil {
			t.Errorf("CombineErrors() = %v, want nil", err)
		}
	})

	t.Run("all nil errors", func(t *testing.T) {
		err := CombineErrors(nil, nil, nil)
		if err != nil {
			t.Errorf("CombineErrors(nil, nil, nil) = %v, want nil", err)
		}
	})

	t.Run("single error", func(t *testing.T) {
		single := errors.New("single error")
		err := CombineErrors(single)
		if err != single {
			t.Errorf("CombineErrors(single) = %v, want %v", err, single)
		}
	})

	t.Run("multiple errors", func(t *testing.T) {
		err1 := errors.New("error 1")
		err2 := errors.New("error 2")
		err3 := errors.New("error 3")
		combined := CombineErrors(err1, err2, err3)

		if combined == nil {
			t.Fatal("CombineErrors should not return nil for multiple errors")
		}

		msg := combined.Error()
		if !strings.Contains(msg, "multiple errors occurred") {
			t.Errorf("error message should contain 'multiple errors occurred': %s", msg)
		}
		if !strings.Contains(msg, "error 1") {
			t.Errorf("error message should contain 'error 1': %s", msg)
		}
		if !strings.Contains(msg, "error 2") {
			t.Errorf("error message should contain 'error 2': %s", msg)
		}
		if !strings.Contains(msg, "error 3") {
			t.Errorf("error message should contain 'error 3': %s", msg)
		}
	})

	t.Run("mixed nil and non-nil errors", func(t *testing.T) {
		err1 := errors.New("error 1")
		err2 := errors.New("error 2")
		combined := CombineErrors(nil, err1, nil, err2, nil)

		if combined == nil {
			t.Fatal("CombineErrors should not return nil")
		}

		msg := combined.Error()
		if !strings.Contains(msg, "error 1") || !strings.Contains(msg, "error 2") {
			t.Errorf("error message should contain both errors: %s", msg)
		}
	})
}

// Test monoerrors.AggregateErrors
func TestAggregateErrors(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		err := monoerrors.AggregateErrors([]error{})
		if err != nil {
			t.Errorf("monoerrors.AggregateErrors([]) = %v, want nil", err)
		}
	})

	t.Run("nil slice", func(t *testing.T) {
		err := monoerrors.AggregateErrors(nil)
		if err != nil {
			t.Errorf("monoerrors.AggregateErrors(nil) = %v, want nil", err)
		}
	})

	t.Run("multiple errors", func(t *testing.T) {
		errs := []error{
			errors.New("error 1"),
			errors.New("error 2"),
			errors.New("error 3"),
		}
		combined := monoerrors.AggregateErrors(errs)
		if combined == nil {
			t.Fatal("monoerrors.AggregateErrors should not return nil")
		}
		if !strings.Contains(combined.Error(), "error 1") {
			t.Errorf("should contain 'error 1': %s", combined.Error())
		}
	})
}

// Test RecoverToError
func TestRecoverToError(t *testing.T) {
	t.Run("nil panic", func(t *testing.T) {
		err := RecoverToError(nil)
		if err != nil {
			t.Errorf("RecoverToError(nil) = %v, want nil", err)
		}
	})

	t.Run("error panic", func(t *testing.T) {
		original := errors.New("test error")
		err := RecoverToError(original)
		if err != original {
			t.Errorf("RecoverToError(error) = %v, want %v", err, original)
		}
	})

	t.Run("string panic", func(t *testing.T) {
		err := RecoverToError("panic message")
		if err == nil {
			t.Fatal("RecoverToError should not return nil for string panic")
		}
		if !strings.Contains(err.Error(), "panic message") {
			t.Errorf("error should contain panic message: %s", err.Error())
		}
	})

	t.Run("struct panic", func(t *testing.T) {
		type testStruct struct {
			msg string
		}
		panicVal := testStruct{msg: "test"}
		err := RecoverToError(panicVal)
		if err == nil {
			t.Fatal("RecoverToError should not return nil for struct panic")
		}
	})
}

// Test RecoverModulePanic in defer
func TestRecoverModulePanic(t *testing.T) {
	t.Run("function panics", func(t *testing.T) {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = monoerrors.WrapModulePanic("test-module", "test-op", r)
				}
			}()
			panic("test panic")
		}()

		if err == nil {
			t.Fatal("expected error from panic recovery")
		}
		if !errors.Is(err, monoerrors.ErrModulePanic) {
			t.Error("error should wrap monoerrors.ErrModulePanic")
		}
		if !strings.Contains(err.Error(), "test-module") {
			t.Errorf("error should contain module name: %s", err.Error())
		}
	})

	t.Run("function does not panic", func(t *testing.T) {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = monoerrors.WrapModulePanic("test-module", "test-op", r)
				}
			}()
			// no panic
		}()

		if err != nil {
			t.Errorf("expected no error when function doesn't panic, got %v", err)
		}
	})
}

// Test monoerrors.FormatDependencyChain
func TestFormatDependencyChain(t *testing.T) {
	tests := []struct {
		name     string
		chain    []string
		expected string
	}{
		{"empty chain", []string{}, ""},
		{"single item", []string{"A"}, "A"},
		{"two items", []string{"A", "B"}, "A -> B"},
		{"three items", []string{"A", "B", "C"}, "A -> B -> C"},
		{"circular chain", []string{"A", "B", "C", "A"}, "A -> B -> C -> A"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := monoerrors.FormatDependencyChain(tt.chain)
			if result != tt.expected {
				t.Errorf("monoerrors.FormatDependencyChain(%v) = %q, want %q", tt.chain, result, tt.expected)
			}
		})
	}
}

// Test mono.FormatServiceType
func TestFormatServiceType(t *testing.T) {
	tests := []struct {
		name        string
		serviceType mono.ServiceType
		expected    string
	}{
		{"Channel", mono.ServiceTypeChannel, "channel"},
		{"RequestReply", mono.ServiceTypeRequestReply, "request_reply"},
		{"QueueGroup", mono.ServiceTypeQueueGroup, "queue_group"},
		{"StreamConsumer", mono.ServiceTypeStreamConsumer, "stream_consumer"},
		{"Unknown", mono.ServiceType(999), "unknown(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mono.FormatServiceType(tt.serviceType)
			if result != tt.expected {
				t.Errorf("mono.FormatServiceType(%v) = %q, want %q", tt.serviceType, result, tt.expected)
			}
		})
	}
}

// Test formatFrameworkState
func TestFormatFrameworkState(t *testing.T) {
	tests := []struct {
		name     string
		state    mono.MonoFrameworkState
		expected string
	}{
		{"Created", mono.StateCreated, "Created"},
		{"Starting", mono.StateStarting, "Starting"},
		{"Running", mono.StateRunning, "Running"},
		{"Stopping", mono.StateStopping, "Stopping"},
		{"Stopped", mono.StateStopped, "Stopped"},
		{"Unknown", mono.MonoFrameworkState(999), "Unknown(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := monoerrors.FormatFrameworkState(tt.state)
			if result != tt.expected {
				t.Errorf("monoerrors.FormatFrameworkState(%v) = %q, want %q", tt.state, result, tt.expected)
			}
		})
	}
}

func TestEventDefinitionVersionTokenValidation(t *testing.T) {
	t.Run("panics when version token missing from explicit subject", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic when version token is missing from subject")
			}
		}()
		_ = helper.EventDefinition[struct{}]("order", "OrderCreated", "v1", "events.orders.created")
	})

	t.Run("does not panic when version token present in explicit subject", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()
		_ = helper.EventDefinition[struct{}]("order", "OrderCreated", "v1", "events.orders.v1.created")
	})

	t.Run("does not panic with auto-computed subject", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()
		eventDef := helper.EventDefinition[struct{}]("order", "OrderCreated", "v1")
		expected := "events.order.v1.order-created"
		if eventDef.ToBase().Subject != expected {
			t.Errorf("expected subject %q, got %q", expected, eventDef.ToBase().Subject)
		}
	})
}

// Test error wrapping chain with errors.Is and errors.As
func TestErrorWrappingChain(t *testing.T) {
	t.Run("multi-level wrapping with errors.Is", func(t *testing.T) {
		// Create a 3-level error chain
		wrapped1 := monoerrors.WrapModuleNotFound("auth")
		wrapped2 := NewModuleErrorForTest("system", "dependency-check", wrapped1)

		// errors.Is should work through the chain
		if !errors.Is(wrapped2, monoerrors.ErrModuleNotFound) {
			t.Error("errors.Is should find monoerrors.ErrModuleNotFound through wrapping chain")
		}
	})

	t.Run("multi-level wrapping with errors.As", func(t *testing.T) {
		// Create nested error
		innerErr := monoerrors.WrapServiceNotFound("payment", "order")
		outerErr := NewModuleErrorForTest("order", "service-lookup", innerErr)

		// Extract monoerrors.ServiceError using errors.As
		var serviceErr *monoerrors.ServiceError
		if !errors.As(outerErr, &serviceErr) {
			t.Fatal("errors.As should extract monoerrors.ServiceError from wrapping chain")
		}
		if serviceErr.ServiceName != "payment" {
			t.Errorf("ServiceName = %q, want 'payment'", serviceErr.ServiceName)
		}
	})
}
