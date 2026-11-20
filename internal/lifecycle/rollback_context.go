package lifecycle

import (
	"context"
	"sync"
	"time"
)

// RollbackTimeout bounds the stopping of what a failed Start had started,
// when the Start context carries no RollbackBudget that says otherwise.
const RollbackTimeout = 30 * time.Second

// RollbackBudget says how long a Start may take to stop what it had started
// when it fails. The application puts one into the Start context
// (WithRollbackBudget), which lets a Stop that aborts the start hand over its
// own deadline (SetDeadline): the rollback then ends when that Stop stops
// waiting for it, whether it had begun already or not.
type RollbackBudget struct {
	// Timeout bounds the rollback from the moment it begins. Zero or less
	// selects RollbackTimeout.
	Timeout time.Duration

	mu sync.Mutex
	// deadline is the one handed over with SetDeadline (zero: none).
	deadline time.Time
	// live holds the rollback contexts that have not been released, by the
	// number they were given.
	live map[uint64]*liveRollback
	next uint64
}

// liveRollback is a rollback context that is in use.
type liveRollback struct {
	cancel context.CancelFunc
	// end is when the context ends by itself.
	end time.Time
	// shorten ends it earlier, once a deadline was handed over (nil: none).
	shorten *time.Timer
}

// SetDeadline hands over the deadline t of whoever waits for the rollback:
//   - a rollback that is running ends at t, at once when t has passed (its
//     context is cancelled then; it never gets more time than it had);
//   - a rollback that begins before t ends at t;
//   - a rollback that begins after t has its whole Timeout, since nobody is
//     waiting for it any more and a context that has already ended would
//     make every module skip its cleanup.
//
// It is safe to call while the Start is running, and more than once: a later
// call replaces the deadline for rollbacks that begin after it and can only
// shorten the ones that are running.
func (b *RollbackBudget) SetDeadline(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deadline = t
	for _, r := range b.live {
		if !t.Before(r.end) {
			continue
		}
		if r.shorten != nil {
			r.shorten.Stop()
		}
		r.end = t
		r.shorten = time.AfterFunc(time.Until(t), r.cancel)
	}
}

// Deadline returns the deadline handed over with SetDeadline, if any.
func (b *RollbackBudget) Deadline() (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deadline, !b.deadline.IsZero()
}

// begin returns a rollback context derived from parent that ends as the
// budget says, and the function that releases it.
func (b *RollbackBudget) begin(parent context.Context) (context.Context, context.CancelFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()

	timeout := b.Timeout
	if timeout <= 0 {
		timeout = RollbackTimeout
	}
	now := time.Now()
	end := now.Add(timeout)
	if !b.deadline.IsZero() && b.deadline.After(now) {
		end = b.deadline
	}
	ctx, cancel := context.WithDeadline(parent, end)

	if b.live == nil {
		b.live = make(map[uint64]*liveRollback)
	}
	id := b.next
	b.next++
	b.live[id] = &liveRollback{cancel: cancel, end: end}

	return ctx, func() {
		b.mu.Lock()
		if r, ok := b.live[id]; ok {
			if r.shorten != nil {
				r.shorten.Stop()
			}
			delete(b.live, id)
		}
		b.mu.Unlock()
		cancel()
	}
}

// rollbackBudgetKey is the context key of a RollbackBudget.
type rollbackBudgetKey struct{}

// WithRollbackBudget returns a context that carries budget for
// RollbackContext.
func WithRollbackBudget(ctx context.Context, budget *RollbackBudget) context.Context {
	return context.WithValue(ctx, rollbackBudgetKey{}, budget)
}

// RollbackContext returns the context a failed Start stops what it had
// started with, and the function that releases it. The Start context has
// often ended by then: its deadline is why the start failed, or it was
// cancelled to abort the start (a Stop called while starting, a signal). A
// module's Stop given that context would skip its cleanup, and the
// subscriptions would not be drained. The rollback therefore gets a context
// that is not cancelled with ctx and keeps its values. It ends as the
// RollbackBudget in ctx says (see RollbackBudget.SetDeadline), or after
// RollbackTimeout when ctx carries none.
func RollbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if budget, ok := ctx.Value(rollbackBudgetKey{}).(*RollbackBudget); ok && budget != nil {
		return budget.begin(detached)
	}
	return context.WithTimeout(detached, RollbackTimeout)
}
