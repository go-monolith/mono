package lifecycle

import (
	"context"
	"sync/atomic"
	"time"
)

// RollbackTimeout bounds the stopping of what a failed Start had started,
// when the Start context carries no RollbackBudget that says otherwise.
const RollbackTimeout = 30 * time.Second

// RollbackBudget says how long a Start may take to stop what it had started
// when it fails. The application puts one into the Start context
// (WithRollbackBudget), which lets a Stop that aborts the start hand over its
// own deadline: the rollback then ends when that Stop stops waiting for it.
type RollbackBudget struct {
	// Timeout bounds the rollback from the moment it begins. Zero or less
	// selects RollbackTimeout.
	Timeout time.Duration

	deadline atomic.Pointer[time.Time]
}

// SetDeadline makes rollbacks that begin from now on end at t instead of
// after Timeout. It is safe to call while the Start is running.
func (b *RollbackBudget) SetDeadline(t time.Time) {
	b.deadline.Store(&t)
}

// Deadline returns the deadline set with SetDeadline, if any.
func (b *RollbackBudget) Deadline() (time.Time, bool) {
	if t := b.deadline.Load(); t != nil {
		return *t, true
	}
	return time.Time{}, false
}

// rollbackBudgetKey is the context key of a RollbackBudget.
type rollbackBudgetKey struct{}

// WithRollbackBudget returns a context that carries budget for
// RollbackContext.
func WithRollbackBudget(ctx context.Context, budget *RollbackBudget) context.Context {
	return context.WithValue(ctx, rollbackBudgetKey{}, budget)
}

// RollbackContext returns the context a failed Start stops what it had
// started with. The Start context has often ended by then: its deadline is
// why the start failed, or it was cancelled to abort the start (a Stop called
// while starting, a signal). A module's Stop given that context would skip
// its cleanup, and the subscriptions would not be drained. The rollback
// therefore gets a context that is not cancelled with ctx and keeps its
// values. It ends at the deadline of the RollbackBudget in ctx when one was
// set, otherwise after the budget's Timeout, or RollbackTimeout without a
// budget.
func RollbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	timeout := RollbackTimeout
	if budget, ok := ctx.Value(rollbackBudgetKey{}).(*RollbackBudget); ok && budget != nil {
		if deadline, set := budget.Deadline(); set {
			return context.WithDeadline(detached, deadline)
		}
		if budget.Timeout > 0 {
			timeout = budget.Timeout
		}
	}
	return context.WithTimeout(detached, timeout)
}
