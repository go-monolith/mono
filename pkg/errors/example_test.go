package errors_test

import (
	"errors"
	"fmt"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
)

// ExampleIsServiceError demonstrates checking for service errors.
func ExampleIsServiceError() {
	err := monoerrors.WrapServiceNotFound("payment-processor", "payment")

	if monoerrors.IsServiceError(err) {
		fmt.Println("This is a service error")
	}
	// Output: This is a service error
}

// ExampleIsModuleError demonstrates checking for module errors.
func ExampleIsModuleError() {
	err := monoerrors.WrapModuleNotFound("unknown-module")

	if monoerrors.IsModuleError(err) {
		fmt.Println("This is a module error")
	}
	// Output: This is a module error
}

// ExampleIsDependencyError demonstrates checking for dependency errors.
func ExampleIsDependencyError() {
	err := monoerrors.WrapMissingDependency("order-module", "payment-module")

	if monoerrors.IsDependencyError(err) {
		fmt.Println("This is a dependency error")
	}
	// Output: This is a dependency error
}

// ExampleIsConfigurationError demonstrates checking for configuration errors.
func ExampleIsConfigurationError() {
	err := monoerrors.WrapInvalidConfiguration(1, "WithNATSPort", -1, "port must be positive")

	if monoerrors.IsConfigurationError(err) {
		fmt.Println("This is a configuration error")
	}
	// Output: This is a configuration error
}

// ExampleAggregateErrors demonstrates combining multiple errors.
func ExampleAggregateErrors() {
	errs := []error{
		errors.New("first error"),
		nil, // nil errors are filtered out
		errors.New("second error"),
	}

	combined := monoerrors.AggregateErrors(errs)
	if combined != nil {
		fmt.Println("Errors were aggregated")
	}
	// Output: Errors were aggregated
}

// ExampleAggregateErrors_singleError demonstrates that single errors are returned as-is.
func ExampleAggregateErrors_singleError() {
	errs := []error{
		nil,
		errors.New("only error"),
		nil,
	}

	combined := monoerrors.AggregateErrors(errs)
	fmt.Println(combined.Error())
	// Output: only error
}

// ExampleWrapCircularDependency demonstrates wrapping circular dependency errors.
func ExampleWrapCircularDependency() {
	chain := []string{"module-a", "module-b", "module-c", "module-a"}
	err := monoerrors.WrapCircularDependency(chain)

	fmt.Println("Error detected:", monoerrors.IsDependencyError(err))
	// Output: Error detected: true
}

// ExampleFormatDependencyChain demonstrates formatting dependency chains.
func ExampleFormatDependencyChain() {
	chain := []string{"auth", "user", "notification"}
	formatted := monoerrors.FormatDependencyChain(chain)

	fmt.Println(formatted)
	// Output: auth -> user -> notification
}

// ExampleGetServiceError demonstrates extracting service error details.
func ExampleGetServiceError() {
	err := monoerrors.WrapServiceNotFound("my-service", "my-module")

	serviceErr, ok := monoerrors.GetServiceError(err)
	if ok {
		fmt.Println("Service:", serviceErr.ServiceName)
		fmt.Println("Module:", serviceErr.ModuleName)
	}
	// Output:
	// Service: my-service
	// Module: my-module
}
