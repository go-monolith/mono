package types

import (
	"fmt"
	"runtime"

	"github.com/nats-io/nats.go"
)

// DefaultMaxPendingFactor is the multiplier applied to MaxConcurrency to
// derive the default MaxPending of a subscription: a message that has to wait
// for a free worker waits behind at most this many handler runs per worker.
//
// The default is deliberately generous. Before worker pools, a burst simply
// queued in the NATS client's buffer and every message was eventually served;
// callers never had to handle an overload error. The default therefore
// absorbs bursts first and sheds load only when a backlog has clearly built
// up. Lower MaxPending on services where failing fast matters more.
const DefaultMaxPendingFactor = 16

// DefaultMaxPendingFloor is the smallest default MaxPending. It keeps a
// low-concurrency subscription (for example MaxConcurrency 1, chosen to keep
// message order, or a small GOMAXPROCS) from rejecting an ordinary burst.
const DefaultMaxPendingFloor = 256

// ServiceOptions bounds the worker pool that runs the handlers of one NATS
// subscription (a request-reply service, one queue group of a queue group
// service, or an event consumer) on one instance.
//
// nats.go dispatches the messages of an asynchronous subscription to its
// callback one at a time, so without a pool one slow handler call delays every
// later message on the same subscription. The framework instead hands each
// message to a bounded pool: up to MaxConcurrency handlers run at once, and up
// to MaxPending messages wait for a free worker.
//
// When both are full, a message that carries a reply subject (a request-reply
// call, or a queue group send that waits for its acknowledgement) is answered
// at once with an error the caller sees as errors.ErrServiceOverloaded, so the
// caller fails fast and may retry, typically on another instance. A message
// without a reply subject (an event, or a queue group send in optimistic
// publish mode) is not rejected: the subscription waits for a slot instead,
// which applies back-pressure through the NATS client's own buffer (whose own
// pending limits, if ever reached, make the client report a slow consumer).
// Such a message is dropped only if it is still buffered when the instance
// has finished shutting down.
//
// Zero values select the defaults, which differ by kind of subscription:
//
//   - Request-reply services are pooled by default: MaxConcurrency 0 means
//     WithDefaultServiceConcurrency, or runtime.GOMAXPROCS(0)*4 when that is
//     not set. Each request is independent, so running them concurrently is
//     safe.
//   - Queue group services and event consumers stay serial by default:
//     MaxConcurrency 0 means 1, so a handler sees its messages one at a time
//     in arrival order, as before worker pools existed. Opt in to concurrency
//     per registration by setting MaxConcurrency.
//
// MaxPending 0 means DefaultMaxPending(MaxConcurrency). Negative values are
// rejected at registration.
type ServiceOptions struct {
	// MaxConcurrency is the maximum number of handlers running at once for
	// this subscription on this instance.
	//
	// 0 selects the default for the kind of subscription (see ServiceOptions).
	// 1 runs handlers one at a time in arrival order; use it for handlers that
	// rely on message order or on unsynchronized state.
	MaxConcurrency int

	// MaxPending is the maximum number of messages waiting for a free worker
	// before the subscription is considered overloaded.
	//
	// 0 selects the default: MaxConcurrency*DefaultMaxPendingFactor, but at
	// least DefaultMaxPendingFloor. A bound on the count cannot bound the
	// wait time, so a slow handler with a full queue still makes queued
	// callers wait; size MaxPending for the burst you want to absorb and
	// keep MaxConcurrency high enough to drain it.
	MaxPending int

	// PendingMsgs and PendingBytes are the nats.go pending limits of the
	// subscription: how many messages (and bytes) the NATS client buffers
	// for it before it drops messages as a slow consumer. They sit behind
	// the worker pool: the pool is the primary load-shedding mechanism, the
	// client buffer is the last resort, and a drop there surfaces as a
	// caller timeout, not ErrServiceOverloaded.
	//
	// 0 selects the default: DefaultRequestReplyPendingMsgs/Bytes for
	// request-reply services (whose callback does not wait for the pool when
	// the request carries a reply subject, so a large buffer only hides a
	// backlog), and nats.go's own defaults (500,000 messages, 64 MB) for
	// queue groups and event consumers, whose buffer is the back-pressure
	// reservoir when their pool is full. -1 means unlimited. Values below -1
	// are rejected.
	PendingMsgs  int
	PendingBytes int
}

// DefaultRequestReplyPendingMsgs is the default PendingMsgs of a request-reply
// subscription.
const DefaultRequestReplyPendingMsgs = 10_000

// DefaultRequestReplyPendingBytes is the default PendingBytes of a
// request-reply subscription: 16 MB.
const DefaultRequestReplyPendingBytes = 16 * 1024 * 1024

// ServiceOption configures ServiceOptions. Options are applied in order; the
// last one wins when two set the same field.
type ServiceOption func(*ServiceOptions)

// WithMaxConcurrency sets ServiceOptions.MaxConcurrency for one registration.
// Use 1 to process messages one at a time in arrival order.
//
// Example:
//
//	container.RegisterRequestReplyService("check-stock", handler, mono.WithMaxConcurrency(32))
func WithMaxConcurrency(n int) ServiceOption {
	return func(o *ServiceOptions) {
		o.MaxConcurrency = n
	}
}

// WithMaxPending sets ServiceOptions.MaxPending for one registration.
//
// Example:
//
//	container.RegisterRequestReplyService("check-stock", handler,
//	    mono.WithMaxConcurrency(32), mono.WithMaxPending(256))
func WithMaxPending(n int) ServiceOption {
	return func(o *ServiceOptions) {
		o.MaxPending = n
	}
}

// WithPendingLimits sets ServiceOptions.PendingMsgs and PendingBytes for one
// registration: the NATS client buffer behind the worker pool. -1 means
// unlimited for that dimension.
//
// Example:
//
//	container.RegisterRequestReplyService("check-stock", handler, mono.WithPendingLimits(1000, 4<<20))
func WithPendingLimits(msgs, bytes int) ServiceOption {
	return func(o *ServiceOptions) {
		o.PendingMsgs = msgs
		o.PendingBytes = bytes
	}
}

// ApplyServiceOptions builds a ServiceOptions from functional options. Zero
// values are kept as zero so the framework defaults are applied later.
func ApplyServiceOptions(opts ...ServiceOption) ServiceOptions {
	var o ServiceOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// Validate reports an error when a field is negative. Zero values are valid
// and mean "framework default".
func (o ServiceOptions) Validate() error {
	if o.MaxConcurrency < 0 {
		return fmt.Errorf("MaxConcurrency must be >= 0 (0 = framework default), got %d", o.MaxConcurrency)
	}
	if o.MaxPending < 0 {
		return fmt.Errorf("MaxPending must be >= 0 (0 = framework default), got %d", o.MaxPending)
	}
	if o.PendingMsgs < -1 {
		return fmt.Errorf("PendingMsgs must be >= -1 (0 = default, -1 = unlimited), got %d", o.PendingMsgs)
	}
	if o.PendingBytes < -1 {
		return fmt.Errorf("PendingBytes must be >= -1 (0 = default, -1 = unlimited), got %d", o.PendingBytes)
	}
	return nil
}

// PendingLimits returns the nats.go pending limits to apply for this
// registration and whether any should be applied at all. requestReply selects
// the request-reply defaults for zero fields; for other kinds nothing is
// applied when both fields are zero, and a single zero field takes nats.go's
// own default for that dimension (nats.go rejects a limit of 0, so the
// framework never passes one).
func (o ServiceOptions) PendingLimits(requestReply bool) (msgs, bytes int, apply bool) {
	msgs, bytes = o.PendingMsgs, o.PendingBytes
	if requestReply {
		if msgs == 0 {
			msgs = DefaultRequestReplyPendingMsgs
		}
		if bytes == 0 {
			bytes = DefaultRequestReplyPendingBytes
		}
		return msgs, bytes, true
	}
	if msgs == 0 && bytes == 0 {
		return 0, 0, false
	}
	if msgs == 0 {
		msgs = nats.DefaultSubPendingMsgsLimit
	}
	if bytes == 0 {
		bytes = nats.DefaultSubPendingBytesLimit
	}
	return msgs, bytes, true
}

// Resolve replaces zero fields with the defaults of a pooled-by-default
// subscription (request-reply). defaultConcurrency is the framework-wide
// default from WithDefaultServiceConcurrency; when it is not positive,
// DefaultMaxConcurrency is used. The result has both fields positive.
func (o ServiceOptions) Resolve(defaultConcurrency int) ServiceOptions {
	if defaultConcurrency <= 0 {
		defaultConcurrency = DefaultMaxConcurrency()
	}
	return o.ResolveWithDefault(defaultConcurrency)
}

// ResolveSerial replaces zero fields with the defaults of a serial-by-default
// subscription (queue group, event consumer): a zero MaxConcurrency becomes 1.
func (o ServiceOptions) ResolveSerial() ServiceOptions {
	return o.ResolveWithDefault(1)
}

// ResolveWithDefault replaces a zero MaxConcurrency with defaultConcurrency
// (raised to 1 if lower) and a zero MaxPending with DefaultMaxPending of the
// resulting concurrency. The result has both fields positive.
func (o ServiceOptions) ResolveWithDefault(defaultConcurrency int) ServiceOptions {
	resolved := o
	if resolved.MaxConcurrency <= 0 {
		if defaultConcurrency < 1 {
			defaultConcurrency = 1
		}
		resolved.MaxConcurrency = defaultConcurrency
	}
	if resolved.MaxPending <= 0 {
		resolved.MaxPending = DefaultMaxPending(resolved.MaxConcurrency)
	}
	return resolved
}

// DefaultMaxConcurrency returns the built-in default MaxConcurrency of a
// request-reply service: runtime.GOMAXPROCS(0)*4. Handlers mostly wait on I/O
// (a database, another service), so more workers than CPUs keeps the CPUs busy
// while some of them block.
func DefaultMaxConcurrency() int {
	return runtime.GOMAXPROCS(0) * 4
}

// DefaultMaxPending returns the default ServiceOptions.MaxPending for a
// subscription with the given MaxConcurrency:
// maxConcurrency*DefaultMaxPendingFactor, but at least DefaultMaxPendingFloor.
func DefaultMaxPending(maxConcurrency int) int {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	n := maxConcurrency * DefaultMaxPendingFactor
	if n < DefaultMaxPendingFloor {
		return DefaultMaxPendingFloor
	}
	return n
}

// EventConsumerOptions configures an event consumer registered with
// EventRegistry.RegisterEventConsumerWithOptions.
type EventConsumerOptions struct {
	// QueueGroup is the NATS queue group the consumer joins. Consumers in
	// the same group share the events between them (each event is handled
	// by one of them). Empty selects the consuming module's name, which is
	// also what RegisterEventConsumer uses.
	QueueGroup string

	// ServiceOptions bounds the worker pool of this consumer's subscription.
	// Event consumers are serial by default (MaxConcurrency 0 means 1), so
	// events are handled one at a time in publish order; set MaxConcurrency
	// above 1 to handle them concurrently.
	ServiceOptions
}
