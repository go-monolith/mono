package types

import (
	"fmt"
	"runtime"
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
}

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
	return nil
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
