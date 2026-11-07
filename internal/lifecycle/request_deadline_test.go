package lifecycle

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

func TestRequestDeadline(t *testing.T) {
	arrival := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	skew := 250 * time.Millisecond
	absolute := arrival.Add(3 * time.Second)

	tests := []struct {
		name     string
		header   types.Header
		want     time.Time
		source   deadlineSource
		skewWarn bool
	}{
		{name: "no header", header: nil, source: deadlineNone},
		{name: "empty header", header: types.Header{}, source: deadlineNone},
		{name: "absolute deadline", header: types.Header{types.HeaderDeadline: {ms(absolute)}}, want: absolute, source: deadlineAbsolute},
		{name: "timeout relative to arrival", header: types.Header{types.HeaderTimeout: {"1500"}}, want: arrival.Add(1500 * time.Millisecond), source: deadlineRelative},
		{name: "timeout zero means no time left", header: types.Header{types.HeaderTimeout: {"0"}}, want: arrival, source: deadlineRelative},
		{name: "both agree: absolute wins", header: types.Header{
			types.HeaderDeadline: {ms(absolute)},
			types.HeaderTimeout:  {"3100"}, // arrival+3.1s, within skew of absolute
		}, want: absolute, source: deadlineAbsolute},
		{name: "both disagree: relative wins and skew is suspected", header: types.Header{
			types.HeaderDeadline: {ms(absolute.Add(-2 * time.Second))}, // handling clock 2s ahead
			types.HeaderTimeout:  {"3000"},
		}, want: arrival.Add(3 * time.Second), source: deadlineRelative, skewWarn: true},
		{name: "unparseable absolute falls back to timeout", header: types.Header{
			types.HeaderDeadline: {"soon"},
			types.HeaderTimeout:  {"250"},
		}, want: arrival.Add(250 * time.Millisecond), source: deadlineRelative},
		{name: "absolute in seconds (wrong unit) is ignored", header: types.Header{
			types.HeaderDeadline: {strconv.FormatInt(absolute.Unix(), 10)},
			types.HeaderTimeout:  {"400"},
		}, want: arrival.Add(400 * time.Millisecond), source: deadlineRelative},
		{name: "absolute in nanoseconds (wrong unit) is ignored", header: types.Header{
			types.HeaderDeadline: {strconv.FormatInt(absolute.UnixNano(), 10)},
		}, source: deadlineNone},
		{name: "huge timeout is clamped instead of overflowing", header: types.Header{
			types.HeaderTimeout: {"9300000000000"},
		}, want: arrival.Add(maxDeadlineDistance), source: deadlineRelative},
		{name: "garbage only", header: types.Header{types.HeaderDeadline: {"x"}, types.HeaderTimeout: {"-5"}}, source: deadlineNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, source, skewWarn := requestDeadline(&types.Msg{Header: tt.header}, arrival, skew)
			if source != tt.source {
				t.Fatalf("source = %v, want %v", source, tt.source)
			}
			if source != deadlineNone && !got.Equal(tt.want) {
				t.Fatalf("deadline = %s, want %s", got, tt.want)
			}
			if skewWarn != tt.skewWarn {
				t.Fatalf("skewSuspected = %v, want %v", skewWarn, tt.skewWarn)
			}
		})
	}
	if _, source, _ := requestDeadline(nil, arrival, skew); source != deadlineNone {
		t.Fatal("nil message must have no deadline")
	}
}

func TestRequestContext(t *testing.T) {
	parent := context.Background()
	// context.WithDeadline compares against the wall clock, so "now" must be
	// the real time for the expiry assertions to hold; headers carry
	// milliseconds, so compare at that precision.
	now := time.Now().Truncate(time.Millisecond)
	skew := 250 * time.Millisecond

	t.Run("no header keeps the parent context", func(t *testing.T) {
		d := requestContext(parent, &types.Msg{}, now, now, skew)
		defer d.cancel()
		if d.expired || d.source != deadlineNone {
			t.Fatalf("decision = %+v, want no deadline and not expired", d)
		}
		if _, has := d.ctx.Deadline(); has {
			t.Fatal("no deadline expected on the handler context")
		}
	})

	t.Run("future deadline is applied to the handler context unchanged", func(t *testing.T) {
		want := now.Add(5 * time.Second)
		msg := &types.Msg{Header: types.Header{types.HeaderDeadline: {ms(want)}}}
		d := requestContext(parent, msg, now, now, skew)
		defer d.cancel()
		if d.expired {
			t.Fatal("a future deadline must not be reported as expired")
		}
		got, has := d.ctx.Deadline()
		if !has || !got.Equal(want) {
			t.Fatalf("handler deadline = %s, want exactly %s (no skew added)", got, want)
		}
	})

	t.Run("past deadline beyond the skew allowance is expired", func(t *testing.T) {
		past := now.Add(-time.Second)
		msg := &types.Msg{Header: types.Header{types.HeaderDeadline: {ms(past)}}}
		d := requestContext(parent, msg, now, now, skew)
		defer d.cancel()
		if !d.expired {
			t.Fatal("a deadline 1s in the past must be expired with a 250ms skew allowance")
		}
	})

	t.Run("past deadline within the skew allowance still runs, with an expired context", func(t *testing.T) {
		past := now.Add(-100 * time.Millisecond)
		msg := &types.Msg{Header: types.Header{types.HeaderDeadline: {ms(past)}}}
		d := requestContext(parent, msg, now, now, skew)
		defer d.cancel()
		if d.expired {
			t.Fatal("a deadline within the skew allowance must not be dropped")
		}
		if d.ctx.Err() == nil {
			t.Fatal("handler context should carry the (already past) caller deadline")
		}
	})

	t.Run("skew suspected is reported", func(t *testing.T) {
		msg := &types.Msg{Header: types.Header{
			types.HeaderDeadline: {ms(now.Add(-3 * time.Second))}, // this clock is ~5s ahead of the caller's
			types.HeaderTimeout:  {"2000"},
		}}
		d := requestContext(parent, msg, now, now, skew)
		defer d.cancel()
		if d.expired || !d.skewSuspected || d.source != deadlineRelative {
			t.Fatalf("decision = %+v, want relative deadline with skew suspected and not expired", d)
		}
		if got, _ := d.ctx.Deadline(); !got.Equal(now.Add(2 * time.Second)) {
			t.Fatalf("handler deadline = %s, want arrival+2s", got)
		}
	})
}

// TestOverloadReporter_ClockSkewWarningIsRateLimited checks that the clock
// skew warning is logged at most once per interval per subscription.
func TestOverloadReporter_ClockSkewWarningIsRateLimited(t *testing.T) {
	logger := &mockLogger{}
	r := newOverloadReporter(logger, "module", "orders", "service", "check")
	msg := &types.Msg{Header: types.Header{types.HeaderDeadline: []string{"1"}, types.HeaderTimeout: []string{"2000"}}}
	for i := 0; i < 5; i++ {
		r.clockSkew(time.Now(), msg)
	}
	if got := logger.countEntries("skew allowance"); got != 1 {
		t.Fatalf("skew warnings = %d, want 1 (rate limited), entries: %v", got, logger.entries)
	}
}
