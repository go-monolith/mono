package types_test

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestIsFrameworkErrorType(t *testing.T) {
	for _, errorType := range []string{types.ErrorTypeOverloaded, types.ErrorTypeShuttingDown, types.ErrorTypeHandlerPanic, types.ErrorTypeReplyFailed} {
		if !types.IsFrameworkErrorType(errorType) {
			t.Errorf("%q is a framework error type", errorType)
		}
	}
	for _, errorType := range []string{"", "validation", "overloaded-error", "errorstring"} {
		if types.IsFrameworkErrorType(errorType) {
			t.Errorf("%q is not a framework error type", errorType)
		}
	}
}
