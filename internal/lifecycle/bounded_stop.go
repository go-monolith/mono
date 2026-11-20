package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// StopAbandonGrace is how long after its context ended a Stop is still
// waited for. A Stop that honours its context returns at that moment, with
// the context's error or its own; the grace lets that result be reported
// instead of the Stop being taken for one that hangs.
const StopAbandonGrace = 250 * time.Millisecond

// StopMinBudget is how long a Stop is waited for when its context had
// already ended before it was called: one Stop that used up the shutdown's
// time (or a caller that passes a cancelled context) must not leave the
// Stops after it a quarter of a second each. A Stop that ignores its context
// but finishes in a moment then still runs to its end, in order, as it did
// before Stops were bounded; one that honours its context returns at once.
const StopMinBudget = 2 * time.Second

// stopWaitAfterContext returns how long a Stop is waited for once its
// context has ended: StopMinBudget when it had ended before the Stop was
// called, StopAbandonGrace when it ended while the Stop ran.
func (lm *lifecycleManager) stopWaitAfterContext(endedOnEntry bool) time.Duration {
	if !endedOnEntry {
		return StopAbandonGrace
	}
	if lm.stopMinBudget > 0 {
		return lm.stopMinBudget
	}
	return StopMinBudget
}

// RunStop calls stop, the Stop of a module, middleware module or plugin
// (kind) named name, and waits for it until ctx ends and StopAbandonGrace
// has passed (StopMinBudget when ctx had ended before the call). A Stop that
// has not returned by then is abandoned: it is left
// running in its goroutine, and RunStop returns an error wrapping
// errors.ErrStopAbandoned, so that the shutdown goes on to the modules after
// it and to the NATS server. Before, one Stop that ignored its context held
// the whole shutdown for as long as it liked, with the application in the
// stopping state and the server never stopped.
//
// A context that never ends gives no bound: the callers pass the Stop
// context, which the framework bounds by ShutdownTimeout, or the rollback
// context (see RollbackContext).
//
// A panic of stop is recovered and returned as panicValue, for the caller to
// wrap as it does for its kind. An abandoned Stop is counted until it
// returns (see AbandonedStops), which is logged.
func (lm *lifecycleManager) RunStop(ctx context.Context, kind, name string, stop func(context.Context) error) (panicValue any, err error) {
	type outcome struct {
		err        error
		panicValue any
	}
	done := make(chan outcome, 1)
	started := time.Now()
	wait := lm.stopWaitAfterContext(ctx.Err() != nil)
	go func() {
		var out outcome
		defer func() {
			if r := recover(); r != nil {
				out.panicValue = r
			}
			done <- out
		}()
		out.err = stop(ctx)
	}()

	select {
	case out := <-done:
		return out.panicValue, out.err
	case <-ctx.Done():
	}
	grace := time.NewTimer(wait)
	defer grace.Stop()
	select {
	case out := <-done:
		return out.panicValue, out.err
	case <-grace.C:
	}

	key := kind + " " + name
	lm.mu.Lock()
	if lm.abandonedStops == nil {
		lm.abandonedStops = make(map[string]int)
	}
	lm.abandonedStops[key]++
	lm.mu.Unlock()
	go func() {
		out := <-done
		lm.mu.Lock()
		if lm.abandonedStops[key]--; lm.abandonedStops[key] <= 0 {
			delete(lm.abandonedStops, key)
		}
		lm.mu.Unlock()
		lm.logger.Warn("A Stop that was abandoned has returned",
			"kind", kind, "name", name, "after", time.Since(started), "error", out.err, "panic", out.panicValue)
	}()
	return nil, fmt.Errorf("%w: Stop of %s did not return within %v after its context ended (%v) and is left running",
		monoerrors.ErrStopAbandoned, key, wait, context.Cause(ctx))
}

// AbandonedStops returns the modules, middleware modules and plugins whose
// Stop was abandoned (see RunStop) and has not returned yet, as "<kind>
// <name>", sorted.
func (lm *lifecycleManager) AbandonedStops() []string {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	names := make([]string, 0, len(lm.abandonedStops))
	for name := range lm.abandonedStops {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
