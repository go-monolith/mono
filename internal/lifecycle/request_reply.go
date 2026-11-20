package lifecycle

import (
	"errors"
	"time"

	"github.com/go-monolith/mono/internal/workerpool"
	"github.com/go-monolith/mono/pkg/types"
)

// HandlerPanicText is what a caller, and the endpoint statistics, are told
// about a handler that panicked. The value it panicked with is internal (it
// can hold data of the request, or of another one) and goes to the log only.
const HandlerPanicText = "handler panicked"

// PublicErrorText returns the text of a handler error as it leaves the
// process: in the error reply to the caller and in the last_error of the
// services protocol statistics, which any client of the server can read.
func PublicErrorText(err error) string {
	if _, isPanic := panicStack(err); isPanic {
		return HandlerPanicText
	}
	return err.Error()
}

// PublicError returns err, or an error with only the public text when err
// is a recovered panic.
func PublicError(err error) error {
	if err == nil {
		return nil
	}
	if _, isPanic := panicStack(err); isPanic {
		return errors.New(HandlerPanicText)
	}
	return err
}

// HandlerErrorType returns the Mono-Error-Type of the error reply for a
// handler error: the framework's own type for a recovered panic, otherwise
// the handler error's type name (see getErrorTypeName).
//
// A type name that comes out as one of the framework's types gets the suffix
// "-error". A handler error type named OverloadedError was sent as
// "overloaded", which the client reads as the framework's "not handled,
// retry" although the handler had run. No Go type name holds a hyphen, so
// the result cannot be a framework type.
func HandlerErrorType(err error) string {
	if _, isPanic := panicStack(err); isPanic {
		return types.ErrorTypeHandlerPanic
	}
	name := getErrorTypeName(err)
	if IsFrameworkErrorType(name) {
		return name + "-error"
	}
	return name
}

// IsFrameworkErrorType reports whether errorType is one of the types the
// framework itself answers a request with.
func IsFrameworkErrorType(errorType string) bool {
	switch errorType {
	case types.ErrorTypeOverloaded, types.ErrorTypeShuttingDown, types.ErrorTypeHandlerPanic, types.ErrorTypeReplyFailed:
		return true
	}
	return false
}

// replyless records one message that arrived on a request-reply subject
// without a reply subject and was handled, and writes a warning at most once
// per overloadLogInterval, with the number since the previous line. Such a
// message is a publish to a service that is meant to be called: its response
// has nowhere to go.
func (r *overloadReporter) replyless() {
	n := r.replylessCount.Add(1)
	now := time.Now().UnixNano()
	last := r.lastReplylessLog.Load()
	if now-last < int64(overloadLogInterval) || !r.lastReplylessLog.CompareAndSwap(last, now) {
		return
	}
	r.replylessCount.Add(-n)
	r.logger.Warn("Messages without a reply subject handled on a request-reply subject; their responses were not sent",
		r.withFields("messages", n)...)
}

// SubmitRequestToPool hands a request-reply message to the pool without ever
// waiting for room: the subscription callback delivers the calls behind it,
// and a call must get its overloaded answer at once. A message without a
// reply subject cannot be answered, so when the pool is full it is dropped
// and counted with the rejected ones (submitToPool, used for queue groups
// and events, holds such a message back instead, which is their documented
// back-pressure; a request-reply subject has callers waiting).
func SubmitRequestToPool(pool *workerpool.Pool, eventBus types.EventBus, logger types.Logger, reporter *overloadReporter, msg *types.Msg, job workerpool.Job) bool {
	return SubmitToPoolWait(pool, eventBus, logger, reporter, msg, job, false)
}
