package lifecycle

import (
	"context"
	"strconv"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// maxDeadlineDistance bounds how far from the arrival time a deadline header
// is believed. A Mono-Deadline further away than this (for example one sent in
// seconds or nanoseconds instead of milliseconds) is ignored, and a
// Mono-Timeout larger than this is clamped, which also keeps the duration
// arithmetic from overflowing.
const maxDeadlineDistance = 24 * time.Hour

// deadlineSource says which header the deadline was taken from.
type deadlineSource int

const (
	deadlineNone     deadlineSource = iota
	deadlineAbsolute                // Mono-Deadline, trusted
	deadlineRelative                // Mono-Timeout relative to arrival (fallback, or skew suspected)
)

// parseAbsoluteDeadline reads Mono-Deadline (Unix milliseconds). It returns
// false for an absent, unparseable, non-positive or implausible value (more
// than maxDeadlineDistance away from arrival, which means the wrong unit).
func parseAbsoluteDeadline(h types.Header, arrival time.Time) (time.Time, bool) {
	v := h.Get(types.HeaderDeadline)
	if v == "" {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, false
	}
	deadline := time.UnixMilli(ms)
	if d := deadline.Sub(arrival); d > maxDeadlineDistance || d < -maxDeadlineDistance {
		return time.Time{}, false
	}
	return deadline, true
}

// parseRelativeDeadline reads Mono-Timeout (milliseconds remaining when sent)
// and applies it to the arrival time. "0" means no time left and yields the
// arrival time itself. Values above maxDeadlineDistance are clamped. It
// returns false for an absent, unparseable or negative value.
func parseRelativeDeadline(h types.Header, arrival time.Time) (time.Time, bool) {
	v := h.Get(types.HeaderTimeout)
	if v == "" {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms < 0 {
		return time.Time{}, false
	}
	if ms > maxDeadlineDistance.Milliseconds() {
		ms = maxDeadlineDistance.Milliseconds()
	}
	return arrival.Add(time.Duration(ms) * time.Millisecond), true
}

// requestDeadline derives the caller's deadline from the request headers.
//
// Mono-Deadline (absolute) is exact across instances when clocks agree and,
// unlike Mono-Timeout, also counts the time the message spent before arrival.
// Mono-Timeout (relative to arrival) does not depend on either clock and can
// never end before the real deadline. When both are present and disagree by
// more than skew, the sending and handling clocks are out of step, so the
// relative value is used and skewSuspected is set so the caller can log it.
// Without that the absolute value is used; with only one header, that one.
// A request with neither header, or with values that do not parse, has no
// deadline and behaves as before the headers existed.
func requestDeadline(msg *types.Msg, arrival time.Time, skew time.Duration) (deadline time.Time, source deadlineSource, skewSuspected bool) {
	if msg == nil || msg.Header == nil {
		return time.Time{}, deadlineNone, false
	}
	absolute, hasAbsolute := parseAbsoluteDeadline(msg.Header, arrival)
	relative, hasRelative := parseRelativeDeadline(msg.Header, arrival)
	switch {
	case hasAbsolute && hasRelative:
		if diff := relative.Sub(absolute); diff > skew || diff < -skew {
			return relative, deadlineRelative, true
		}
		return absolute, deadlineAbsolute, false
	case hasAbsolute:
		return absolute, deadlineAbsolute, false
	case hasRelative:
		return relative, deadlineRelative, false
	default:
		return time.Time{}, deadlineNone, false
	}
}

// requestDecision is the outcome of requestContext.
type requestDecision struct {
	ctx           context.Context
	cancel        context.CancelFunc
	expired       bool      // the caller's deadline passed by more than the skew allowance: do not run the handler
	skewSuspected bool      // the two deadline headers disagree by more than the allowance
	deadline      time.Time // the deadline applied, zero when none
	source        deadlineSource
}

// requestContext decides how a request-reply handler runs. now is the time of
// the decision (taken once by the caller so the tests are not racing the
// clock).
//
// The handler gets a context that expires at the caller's deadline itself, so
// nested calls carry the same deadline and nothing accumulates per hop. The
// skew allowance only widens the drop decision: a request is skipped when the
// deadline passed by more than skew, which protects a handling clock that runs
// slightly ahead from dropping requests that are still wanted. Within the
// allowance the handler runs with an already expired context: its downstream
// calls fail fast, but a handler that does not depend on the context still
// answers. The returned cancel must be called when the handler is done; it
// releases the deadline timer and cancels work the handler left running.
func requestContext(parent context.Context, msg *types.Msg, arrival, now time.Time, skew time.Duration) requestDecision {
	deadline, source, skewSuspected := requestDeadline(msg, arrival, skew)
	if source == deadlineNone {
		return requestDecision{ctx: parent, cancel: func() {}}
	}
	d := requestDecision{deadline: deadline, source: source, skewSuspected: skewSuspected}
	if now.After(deadline.Add(skew)) {
		d.ctx, d.cancel, d.expired = parent, func() {}, true
		return d
	}
	d.ctx, d.cancel = context.WithDeadline(parent, deadline)
	return d
}
