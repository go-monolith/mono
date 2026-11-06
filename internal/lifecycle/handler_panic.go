package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// handlerPanicError is the error a recovered handler panic is converted into.
// It unwraps to errors.ErrHandlerPanic and carries the stack trace of the
// panicking goroutine, captured at the point of recovery.
type handlerPanicError struct {
	Value any
	Stack []byte
}

// Error implements the error interface.
func (e *handlerPanicError) Error() string {
	return fmt.Sprintf("handler panicked: %v", e.Value)
}

// Unwrap lets errors.Is(err, errors.ErrHandlerPanic) match.
func (e *handlerPanicError) Unwrap() error {
	return monoerrors.ErrHandlerPanic
}

// recoverHandlerPanic converts a panic in the calling frame into a
// *handlerPanicError stored in *err. It must be the deferred call itself
// (`defer recoverHandlerPanic(&err)`): recover only works when called directly
// by a deferred function.
func recoverHandlerPanic(err *error) {
	if r := recover(); r != nil {
		*err = &handlerPanicError{Value: r, Stack: debug.Stack()}
	}
}

// panicStack returns the stack recorded in err if it is a recovered handler
// panic, and whether it is one. Call sites use it to add the stack to the log
// line and, for request-reply, to pick the error type sent to the caller.
func panicStack(err error) (stack string, isPanic bool) {
	var pe *handlerPanicError
	if errors.As(err, &pe) {
		return string(pe.Stack), true
	}
	return "", false
}

// invokeRequestReplyHandler calls a request-reply handler, recovering a panic
// into an error so the subscription (and the process) survives it.
func invokeRequestReplyHandler(ctx context.Context, handler types.RequestReplyHandler, msg *types.Msg) (response []byte, err error) {
	defer recoverHandlerPanic(&err)
	return handler(ctx, msg)
}

// invokeMsgHandler calls a queue group or event consumer handler (both have the
// signature func(ctx, msg) error), recovering a panic into an error.
func invokeMsgHandler(ctx context.Context, handler func(context.Context, *types.Msg) error, msg *types.Msg) (err error) {
	defer recoverHandlerPanic(&err)
	return handler(ctx, msg)
}

// invokeBatchHandler calls a stream consumer or event stream consumer handler
// (both have the signature func(ctx, []*Msg) error), recovering a panic into
// an error. Acknowledgement stays with the handler: the framework does not
// Ack or Nak the batch after a panic, so the messages the handler did not
// acknowledge are redelivered after AckWait, exactly as when the handler
// returns an error.
func invokeBatchHandler(ctx context.Context, handler func(context.Context, []*types.Msg) error, batch []*types.Msg) (err error) {
	defer recoverHandlerPanic(&err)
	return handler(ctx, batch)
}

// handlerErrorFields returns the structured log fields for a handler error:
// the error itself and, for a recovered panic, its stack trace.
func handlerErrorFields(err error) []any {
	fields := []any{"error", err}
	if stack, isPanic := panicStack(err); isPanic {
		fields = append(fields, "panic", true, "stack", stack)
	}
	return fields
}
