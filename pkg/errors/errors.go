// Package errors provides error types, sentinel errors, and error wrapping utilities
// for the mono-framework. This package can be safely imported by both pkg/mono
// and internal packages without creating import cycles.
package errors

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// Sentinel errors for common error cases
var (
	// ErrModuleNotFound is returned when a module is not registered
	ErrModuleNotFound = errors.New("module not found")

	// ErrServiceNotFound is returned when a service is not registered
	ErrServiceNotFound = errors.New("service not found")

	// ErrModuleAlreadyRegistered is returned when attempting to register a duplicate module
	ErrModuleAlreadyRegistered = errors.New("module already registered")

	// ErrServiceAlreadyRegistered is returned when attempting to register a duplicate service
	ErrServiceAlreadyRegistered = errors.New("service already registered")

	// ErrCircularDependency is returned when a circular dependency is detected
	ErrCircularDependency = errors.New("circular dependency detected")

	// ErrMissingDependency is returned when a required dependency is not registered
	ErrMissingDependency = errors.New("missing dependency")

	// ErrServiceUnavailable is returned when a service is temporarily unavailable
	ErrServiceUnavailable = errors.New("service unavailable")

	// ErrInvalidConfiguration is returned when configuration validation fails
	ErrInvalidConfiguration = errors.New("invalid configuration")

	// ErrModuleStartFailed is returned when a module fails to start
	ErrModuleStartFailed = errors.New("module start failed")

	// ErrModuleStopFailed is returned when a module fails to stop
	ErrModuleStopFailed = errors.New("module stop failed")

	// ErrModulePanic is returned when a module panics during lifecycle operations
	ErrModulePanic = errors.New("module panic")

	// ErrContainerNotBound is returned when ServiceContainer is not bound to a module
	ErrContainerNotBound = errors.New("container not bound to module")

	// ErrPluginNotFound is returned when a plugin alias is not registered
	ErrPluginNotFound = errors.New("plugin not found")

	// ErrPluginAlreadyRegistered is returned when attempting to register a duplicate plugin alias
	ErrPluginAlreadyRegistered = errors.New("plugin already registered")
)

// ModuleError wraps errors with module context.
// It provides module name and operation information for debugging.
type ModuleError struct {
	ModuleName string
	Operation  string
	Err        error
}

// Error implements the error interface.
func (e *ModuleError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("module '%s': %s failed: %v", e.ModuleName, e.Operation, e.Err)
	}
	return fmt.Sprintf("module '%s': %s failed", e.ModuleName, e.Operation)
}

// Unwrap returns the wrapped error.
func (e *ModuleError) Unwrap() error {
	return e.Err
}

// ServiceError wraps errors with service context.
// It provides service name, module name, and service type information.
type ServiceError struct {
	ServiceName string
	ModuleName  string
	ServiceType types.ServiceType
	Err         error
}

// Error implements the error interface.
func (e *ServiceError) Error() string {
	serviceTypeStr := types.FormatServiceType(e.ServiceType)
	if e.Err != nil {
		return fmt.Sprintf("service '%s' (%s) in module '%s': %v", e.ServiceName, serviceTypeStr, e.ModuleName, e.Err)
	}
	return fmt.Sprintf("service '%s' (%s) in module '%s'", e.ServiceName, serviceTypeStr, e.ModuleName)
}

// Unwrap returns the wrapped error.
func (e *ServiceError) Unwrap() error {
	return e.Err
}

// DependencyError wraps errors with dependency chain context.
// It provides module, dependency, and chain information for debugging circular dependencies.
type DependencyError struct {
	Module     string
	Dependency string
	Chain      []string
	Err        error
}

// Error implements the error interface.
func (e *DependencyError) Error() string {
	chainStr := FormatDependencyChain(e.Chain)
	if e.Err != nil && len(e.Chain) > 0 {
		return fmt.Sprintf("module '%s' depends on '%s': %v (chain: %s)", e.Module, e.Dependency, e.Err, chainStr)
	} else if e.Err != nil {
		return fmt.Sprintf("module '%s' depends on '%s': %v", e.Module, e.Dependency, e.Err)
	} else if len(e.Chain) > 0 {
		return fmt.Sprintf("module '%s' depends on '%s' (chain: %s)", e.Module, e.Dependency, chainStr)
	}
	return fmt.Sprintf("module '%s' depends on '%s'", e.Module, e.Dependency)
}

// Unwrap returns the wrapped error.
func (e *DependencyError) Unwrap() error {
	return e.Err
}

// ConfigurationError wraps errors with configuration context.
// It provides option index, option name, and value information for debugging.
type ConfigurationError struct {
	OptionIndex int
	OptionName  string
	Value       any
	Err         error
}

// Error implements the error interface.
func (e *ConfigurationError) Error() string {
	if e.Err != nil && e.Value != nil {
		return fmt.Sprintf("option %d (%s) failed: %v (value: %v)", e.OptionIndex, e.OptionName, e.Err, e.Value)
	} else if e.Err != nil {
		return fmt.Sprintf("option %d (%s) failed: %v", e.OptionIndex, e.OptionName, e.Err)
	}
	return fmt.Sprintf("option %d (%s) failed", e.OptionIndex, e.OptionName)
}

// Unwrap returns the wrapped error.
func (e *ConfigurationError) Unwrap() error {
	return e.Err
}

// TimeoutError wraps errors with timeout context.
// It provides operation and duration information for debugging timeouts.
type TimeoutError struct {
	Operation string
	Duration  time.Duration
	Err       error
}

// Error implements the error interface.
func (e *TimeoutError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("operation '%s' timed out after %s: %v", e.Operation, e.Duration, e.Err)
	}
	return fmt.Sprintf("operation '%s' timed out after %s", e.Operation, e.Duration)
}

// Unwrap returns the wrapped error.
func (e *TimeoutError) Unwrap() error {
	return e.Err
}

// Timeout reports whether this error represents a timeout.
func (e *TimeoutError) Timeout() bool {
	return true
}

// EventStreamError wraps errors with event stream context.
// It provides stream name, subject, and operation information for debugging stream errors.
// This error type is specifically for JetStream operations where messages need to be
// persisted but the stream is not configured or unavailable.
type EventStreamError struct {
	StreamName string
	Subject    string
	Operation  string
	Err        error
}

// Error implements the error interface.
func (e *EventStreamError) Error() string {
	if e.StreamName != "" && e.Subject != "" {
		if e.Err != nil {
			return fmt.Sprintf("event stream error: operation '%s' failed for stream '%s' on subject '%s': %v", e.Operation, e.StreamName, e.Subject, e.Err)
		}
		return fmt.Sprintf("event stream error: operation '%s' failed for stream '%s' on subject '%s'", e.Operation, e.StreamName, e.Subject)
	} else if e.Subject != "" {
		if e.Err != nil {
			return fmt.Sprintf("event stream error: operation '%s' failed on subject '%s': %v", e.Operation, e.Subject, e.Err)
		}
		return fmt.Sprintf("event stream error: operation '%s' failed on subject '%s'", e.Operation, e.Subject)
	}
	if e.Err != nil {
		return fmt.Sprintf("event stream error: operation '%s' failed: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("event stream error: operation '%s' failed", e.Operation)
}

// Unwrap returns the wrapped error.
func (e *EventStreamError) Unwrap() error {
	return e.Err
}

// Module Error Constructors

// WrapInvalidModule wraps an error indicating an invalid module.
func WrapInvalidModule(module types.Module, reason string) error {
	moduleName := "<nil>"
	if module != nil {
		moduleName = module.Name()
	}
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  "validate",
		Err:        fmt.Errorf("module validation failed: %s", reason),
	}
}

// WrapModuleNotFound wraps ErrModuleNotFound with module context.
func WrapModuleNotFound(moduleName string) error {
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  "lookup",
		Err:        ErrModuleNotFound,
	}
}

// WrapModuleAlreadyRegistered wraps ErrModuleAlreadyRegistered with module context.
func WrapModuleAlreadyRegistered(moduleName string) error {
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  "register",
		Err:        ErrModuleAlreadyRegistered,
	}
}

// WrapModuleStartFailed wraps module start failures with context.
func WrapModuleStartFailed(moduleName string, err error) error {
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  "start",
		Err:        fmt.Errorf("%w: %v", ErrModuleStartFailed, err),
	}
}

// WrapModuleStopFailed wraps module stop failures with context.
func WrapModuleStopFailed(moduleName string, err error) error {
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  "stop",
		Err:        fmt.Errorf("%w: %v", ErrModuleStopFailed, err),
	}
}

// WrapModulePanic wraps panic recovery with module context.
func WrapModulePanic(moduleName, operation string, panicValue any) error {
	return &ModuleError{
		ModuleName: moduleName,
		Operation:  operation,
		Err:        fmt.Errorf("%w: %v", ErrModulePanic, panicValue),
	}
}

// Plugin Error Constructors

// WrapPluginNotFound wraps ErrPluginNotFound with plugin alias context.
func WrapPluginNotFound(alias string) error {
	return &ModuleError{
		ModuleName: alias,
		Operation:  "lookup",
		Err:        ErrPluginNotFound,
	}
}

// WrapPluginAlreadyRegistered wraps ErrPluginAlreadyRegistered with plugin alias context.
func WrapPluginAlreadyRegistered(alias string) error {
	return &ModuleError{
		ModuleName: alias,
		Operation:  "register",
		Err:        ErrPluginAlreadyRegistered,
	}
}

// WrapPluginStartFailed wraps plugin start failures with context.
func WrapPluginStartFailed(alias string, err error) error {
	return &ModuleError{
		ModuleName: alias,
		Operation:  "start",
		Err:        fmt.Errorf("%w: %v", ErrModuleStartFailed, err),
	}
}

// WrapPluginStopFailed wraps plugin stop failures with context.
func WrapPluginStopFailed(alias string, err error) error {
	return &ModuleError{
		ModuleName: alias,
		Operation:  "stop",
		Err:        fmt.Errorf("%w: %v", ErrModuleStopFailed, err),
	}
}

// Service Error Constructors

// WrapServiceError wraps an error with service context.
func WrapServiceError(serviceName, moduleName string, serviceType types.ServiceType, err error) error {
	return &ServiceError{
		ServiceName: serviceName,
		ModuleName:  moduleName,
		ServiceType: serviceType,
		Err:         err,
	}
}

// WrapServiceNotFound wraps ErrServiceNotFound with service context.
func WrapServiceNotFound(serviceName, moduleName string) error {
	return &ServiceError{
		ServiceName: serviceName,
		ModuleName:  moduleName,
		ServiceType: types.ServiceTypeChannel, // ServiceType unknown at this point
		Err:         ErrServiceNotFound,
	}
}

// WrapServiceAlreadyRegistered wraps ErrServiceAlreadyRegistered with service context.
func WrapServiceAlreadyRegistered(serviceName, moduleName string, serviceType types.ServiceType) error {
	return &ServiceError{
		ServiceName: serviceName,
		ModuleName:  moduleName,
		ServiceType: serviceType,
		Err:         ErrServiceAlreadyRegistered,
	}
}

// WrapServiceUnavailable wraps ErrServiceUnavailable with service context and explicit service type.
func WrapServiceUnavailable(serviceName, moduleName string, serviceType types.ServiceType, err error) error {
	return &ServiceError{
		ServiceName: serviceName,
		ModuleName:  moduleName,
		ServiceType: serviceType,
		Err:         fmt.Errorf("%w: %v", ErrServiceUnavailable, err),
	}
}

// Dependency Error Constructors

// WrapMissingDependency wraps ErrMissingDependency with dependency context.
func WrapMissingDependency(module, dependency string) error {
	return &DependencyError{
		Module:     module,
		Dependency: dependency,
		Err:        ErrMissingDependency,
	}
}

// WrapCircularDependency wraps ErrCircularDependency with dependency chain context.
func WrapCircularDependency(chain []string) error {
	if len(chain) < 2 {
		return &DependencyError{
			Err: ErrCircularDependency,
		}
	}
	return &DependencyError{
		Module:     chain[len(chain)-1],
		Dependency: chain[0],
		Chain:      chain,
		Err:        ErrCircularDependency,
	}
}

// Configuration Error Constructors

// WrapInvalidConfiguration wraps ErrInvalidConfiguration with configuration context.
func WrapInvalidConfiguration(optionIndex int, optionName string, value any, reason string) error {
	return &ConfigurationError{
		OptionIndex: optionIndex,
		OptionName:  optionName,
		Value:       value,
		Err:         fmt.Errorf("%w: %s", ErrInvalidConfiguration, reason),
	}
}

// Timeout Error Constructors

// WrapTimeout wraps an error and creates a new TimeoutError with the given context.
func WrapTimeout(operation string, duration time.Duration, err error) error {
	return &TimeoutError{
		Operation: operation,
		Duration:  duration,
		Err:       err,
	}
}

// Event Stream Error Constructors

// WrapEventStreamError wraps an error with event stream context.
// Use this when a JetStream operation fails and you have stream/subject information.
func WrapEventStreamError(streamName, subject, operation string, err error) error {
	return &EventStreamError{
		StreamName: streamName,
		Subject:    subject,
		Operation:  operation,
		Err:        err,
	}
}

// WrapEventStreamNotAvailable wraps an error indicating event stream is not available.
// This is typically used when publishing to a subject that has no configured stream,
// meaning messages cannot be persisted. Similar to "no responders" but for streams.
func WrapEventStreamNotAvailable(subject, operation string, err error) error {
	return &EventStreamError{
		Subject:   subject,
		Operation: operation,
		Err:       fmt.Errorf("stream is not available or no stream configured for subject (messages not persisted): %w", err),
	}
}

// Error Checking Utilities

// IsModuleError reports whether err is a ModuleError.
func IsModuleError(err error) bool {
	var moduleErr *ModuleError
	return errors.As(err, &moduleErr)
}

// IsServiceError reports whether err is a ServiceError.
func IsServiceError(err error) bool {
	var serviceErr *ServiceError
	return errors.As(err, &serviceErr)
}

// IsDependencyError reports whether err is a DependencyError.
func IsDependencyError(err error) bool {
	var depErr *DependencyError
	return errors.As(err, &depErr)
}

// IsConfigurationError reports whether err is a ConfigurationError.
func IsConfigurationError(err error) bool {
	var confErr *ConfigurationError
	return errors.As(err, &confErr)
}

// IsTimeoutError reports whether err is a TimeoutError.
func IsTimeoutError(err error) bool {
	var timeoutErr *TimeoutError
	return errors.As(err, &timeoutErr)
}

// IsEventStreamError reports whether err is an EventStreamError.
func IsEventStreamError(err error) bool {
	var streamErr *EventStreamError
	return errors.As(err, &streamErr)
}

// Error Extraction Utilities

// GetServiceError extracts ServiceError from err if present.
func GetServiceError(err error) (*ServiceError, bool) {
	var serviceErr *ServiceError
	ok := errors.As(err, &serviceErr)
	return serviceErr, ok
}

// GetConfigurationError extracts ConfigurationError from err if present.
func GetConfigurationError(err error) (*ConfigurationError, bool) {
	var confErr *ConfigurationError
	ok := errors.As(err, &confErr)
	return confErr, ok
}

// GetEventStreamError extracts EventStreamError from err if present.
func GetEventStreamError(err error) (*EventStreamError, bool) {
	var streamErr *EventStreamError
	ok := errors.As(err, &streamErr)
	return streamErr, ok
}

// Error Aggregation Utilities

// AggregateErrors aggregates an array of errors into a single error.
// Returns nil if the array is empty or all errors are nil.
func AggregateErrors(errs []error) error {
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

// Formatting Utilities

// FormatDependencyChain formats a dependency chain for display.
func FormatDependencyChain(chain []string) string {
	if len(chain) == 0 {
		return ""
	}
	return strings.Join(chain, " -> ")
}

// FormatFrameworkState formats a MonoFrameworkState for display.
func FormatFrameworkState(state types.MonoFrameworkState) string {
	switch state {
	case types.StateCreated:
		return "Created"
	case types.StateStarting:
		return "Starting"
	case types.StateRunning:
		return "Running"
	case types.StateStopping:
		return "Stopping"
	case types.StateStopped:
		return "Stopped"
	default:
		return fmt.Sprintf("Unknown(%d)", state)
	}
}
