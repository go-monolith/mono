package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/pkg/types"
)

// fetchErrorBackoff is how long a fetch loop waits after a failed fetch that
// returned nothing, so that a server that answers at once with an error (no
// responders while a consumer has no leader) does not make the loop spin.
const fetchErrorBackoff = time.Second

// fetchErrorLogInterval is the least time between two logged fetch failures
// of one loop; the failures in between are counted and reported with the
// next line.
const fetchErrorLogInterval = 10 * time.Second

// fetchErrorReporter logs the fetch failures of one fetch loop without
// flooding the log: the first failure at once, then at most one line (failure
// or recovery) per fetchErrorLogInterval, with the number of failures not
// logged. A failure that clears by itself (see isTransientFetchError) is a
// warning; any other is an error. It is used by a single goroutine.
type fetchErrorReporter struct {
	logger     types.Logger
	msg        string
	fields     []any
	now        func() time.Time
	lastLog    time.Time
	failures   int // since fetching last worked
	suppressed int // since the last logged line

	// lookup reads the loop's consumer from the server (see WatchConsumer);
	// nil: the loop is not checked for a consumer that no longer exists.
	lookup func(ctx context.Context) error
	// stream and consumer name the consumer in the log.
	stream, consumer string
	// gone is set while the consumer is known not to exist.
	gone bool
	// lastGoneLog is when that was last logged.
	lastGoneLog time.Time
}

const (
	// goneCheckEvery is how many failed fetches in a row without a message
	// pass between two checks whether the consumer still exists. A pull
	// request for a consumer that is gone gets "no responders", exactly as
	// one for a consumer without a leader, so only a lookup tells them apart.
	goneCheckEvery = 5

	// goneLookupTimeout bounds that lookup.
	goneLookupTimeout = 2 * time.Second

	// goneLogInterval is the time between two reminders that a fetch loop
	// has no consumer.
	goneLogInterval = 5 * time.Minute
)

// ConsumerGoneMessage is logged, at Error, when the consumer of a fetch loop
// no longer exists.
const ConsumerGoneMessage = "Consumer no longer exists: this instance consumes nothing from it until it exists again"

// ConsumerGoneHelp says what brings a fetch loop without a consumer back.
const ConsumerGoneHelp = "restart this instance: Start creates the consumer again, from its deliver policy (deliver all delivers the stream's messages again). " +
	"The consumer was deleted (nats consumer rm) or removed by the server after its inactivity threshold; a running instance does not create it again"

// WatchConsumer makes the reporter check, when fetches keep failing, whether
// the loop's consumer still exists, and say so once instead of reporting a
// failure that looks as if it would clear by itself. lookup reads the
// consumer from the server and returns the error of that.
//
// The loop is not given a new consumer: a consumer is deleted on purpose (to
// apply a setting NATS cannot change, or because the registration it belongs
// to was renamed and the stream is a work-queue), and an instance that
// recreated it at once would undo that, on every instance still running the
// old version. The loop keeps asking, so it resumes by itself when the
// consumer exists again (a restart of this instance, or the start of
// another).
func (r *fetchErrorReporter) WatchConsumer(stream, consumer string, lookup func(ctx context.Context) error) {
	r.stream, r.consumer, r.lookup = stream, consumer, lookup
}

// WatchJetStreamConsumer is WatchConsumer for a JetStream consumer handle.
func (r *fetchErrorReporter) WatchJetStreamConsumer(consumer jetstream.Consumer) {
	if consumer == nil {
		return
	}
	info := consumer.CachedInfo()
	if info == nil {
		return
	}
	r.WatchConsumer(info.Stream, info.Name, func(ctx context.Context) error {
		_, err := consumer.Info(ctx)
		return err
	})
}

// checkGone looks the consumer up after a failed fetch and reports whether
// it is known not to exist. It is asked at once when the server said the
// consumer was deleted, and otherwise every goneCheckEvery failures.
func (r *fetchErrorReporter) checkGone(err error) bool {
	if r.lookup == nil {
		return false
	}
	if !r.gone && !errors.Is(err, jetstream.ErrConsumerDeleted) && r.failures%goneCheckEvery != 0 {
		return false
	}
	// While it is gone, every goneCheckEvery failures is often enough.
	if r.gone && r.failures%goneCheckEvery != 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), goneLookupTimeout)
	defer cancel()
	lookupErr := r.lookup(ctx)
	switch {
	case errors.Is(lookupErr, jetstream.ErrConsumerNotFound), errors.Is(lookupErr, jetstream.ErrStreamNotFound):
		r.gone = true
	case lookupErr == nil:
		// It exists (again): the failure is another one.
		r.gone = false
	}
	// Any other lookup error says nothing: the state is kept.
	return r.gone
}

// logGone reports a consumer that does not exist: at once when that is
// found, then every goneLogInterval.
func (r *fetchErrorReporter) logGone(now time.Time, err error) {
	if !r.lastGoneLog.IsZero() && now.Sub(r.lastGoneLog) < goneLogInterval {
		r.suppressed++
		return
	}
	r.logger.Error(ConsumerGoneMessage, append(append([]any{}, r.fields...),
		"stream", r.stream, "consumer", r.consumer, "error", err, "failures", r.failures, "to_resume", ConsumerGoneHelp)...)
	r.lastGoneLog = now
	r.lastLog = now
	r.suppressed = 0
}

func newFetchErrorReporter(logger types.Logger, msg string, fields ...any) *fetchErrorReporter {
	return &fetchErrorReporter{logger: logger, msg: msg, fields: fields, now: time.Now}
}

// failure records a failed fetch and logs it unless a line (failure or
// recovery) was logged less than fetchErrorLogInterval ago.
func (r *fetchErrorReporter) failure(err error) {
	r.failures++
	now := r.now()
	if r.checkGone(err) {
		r.logGone(now, err)
		return
	}
	if !r.canLog(now) {
		r.suppressed++
		return
	}
	fields := append(append([]any{}, r.fields...), "error", err, "failures", r.failures, "suppressed", r.suppressed)
	if isTransientFetchError(err) {
		r.logger.Warn(r.msg, fields...)
	} else {
		r.logger.Error(r.msg, fields...)
	}
	r.lastLog = now
	r.suppressed = 0
}

// success records a fetch that worked and logs the recovery after failures,
// under the same interval as the failures, so a consumer whose fetches
// alternate between failing and working does not log twice per cycle. A
// recovery not logged yet is logged by a later success.
func (r *fetchErrorReporter) success() {
	if r.failures == 0 {
		return
	}
	now := r.now()
	if r.gone {
		// Said at once: the Error before it asked for a restart.
		r.logger.Info("Consumer exists again: consuming resumed", append(append([]any{}, r.fields...),
			"stream", r.stream, "consumer", r.consumer, "failures", r.failures)...)
		r.gone = false
		r.lastGoneLog = time.Time{}
		r.lastLog = now
		r.failures = 0
		r.suppressed = 0
		return
	}
	if !r.canLog(now) {
		return
	}
	r.logger.Info(r.msg+" cleared", append(append([]any{}, r.fields...), "failures", r.failures, "suppressed", r.suppressed)...)
	r.lastLog = now
	r.failures = 0
	r.suppressed = 0
}

// isTransientFetchError reports whether a failed fetch clears by itself: an
// error isTransientJetStreamError accepts (a consumer without a leader while a
// cluster forms or loses quorum, a timeout), or one of the two statuses the
// server answers a pull request with while the consumer's leader moves or the
// server holding it shuts down (a peer's rolling restart, a stream peer
// removal). Both are followed by a working fetch once a new leader is elected.
// They are recognized for the fetch loops only: the JetStream setup retries
// (retryJetStream) never receive them.
func isTransientFetchError(err error) bool {
	return isTransientJetStreamError(err) ||
		errors.Is(err, jetstream.ErrConsumerLeadershipChanged) ||
		errors.Is(err, jetstream.ErrServerShutdown)
}

func (r *fetchErrorReporter) canLog(now time.Time) bool {
	return r.lastLog.IsZero() || now.Sub(r.lastLog) >= fetchErrorLogInterval
}

// sleepCtx waits for d or until ctx ends, and reports whether it waited the
// full duration.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
