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
}

func newFetchErrorReporter(logger types.Logger, msg string, fields ...any) *fetchErrorReporter {
	return &fetchErrorReporter{logger: logger, msg: msg, fields: fields, now: time.Now}
}

// failure records a failed fetch and logs it unless a line (failure or
// recovery) was logged less than fetchErrorLogInterval ago.
func (r *fetchErrorReporter) failure(err error) {
	r.failures++
	now := r.now()
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
