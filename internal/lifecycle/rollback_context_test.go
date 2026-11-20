package lifecycle

import (
	"context"
	"testing"
	"time"
)

type rollbackTestKey struct{}

func TestRollbackContext(t *testing.T) {
	near := func(got time.Time, want time.Duration) bool {
		d := time.Until(got) - want
		return d > -2*time.Second && d < 2*time.Second
	}
	tests := []struct {
		name   string
		budget *RollbackBudget
		// deadlineIn sets the budget's deadline that far from now (0 = not set).
		deadlineIn time.Duration
		want       time.Duration
	}{
		{name: "no budget", want: RollbackTimeout},
		{name: "budget without timeout", budget: &RollbackBudget{}, want: RollbackTimeout},
		{name: "budget timeout", budget: &RollbackBudget{Timeout: 5 * time.Second}, want: 5 * time.Second},
		{name: "deadline wins over the timeout", budget: &RollbackBudget{Timeout: 5 * time.Second}, deadlineIn: time.Minute, want: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), rollbackTestKey{}, "kept"))
			if tt.budget != nil {
				parent = WithRollbackBudget(parent, tt.budget)
				if tt.deadlineIn > 0 {
					tt.budget.SetDeadline(time.Now().Add(tt.deadlineIn))
				}
			}
			// The start context has ended by the time the rollback begins.
			cancelParent()

			ctx, cancel := RollbackContext(parent)
			defer cancel()
			if err := ctx.Err(); err != nil {
				t.Fatalf("rollback context ended with the start context: %v", err)
			}
			if got := ctx.Value(rollbackTestKey{}); got != "kept" {
				t.Errorf("value = %v, want the start context's", got)
			}
			deadline, ok := ctx.Deadline()
			if !ok || !near(deadline, tt.want) {
				t.Errorf("deadline in %s (set %v), want in about %s", time.Until(deadline), ok, tt.want)
			}
		})
	}
}

// A Stop hands its deadline over while the Start is running: rollbacks that
// begin before it end at it, a rollback that is running is shortened to it,
// and a rollback that begins after it has passed gets the whole timeout
// instead of a context that has already ended.
func TestRollbackBudget_SetDeadline(t *testing.T) {
	t.Run("a rollback that begins before the deadline ends at it", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: time.Hour}
		parent := WithRollbackBudget(context.Background(), budget)
		if _, set := budget.Deadline(); set {
			t.Fatal("deadline set on a new budget")
		}
		want := time.Now().Add(3 * time.Second)
		budget.SetDeadline(want)

		ctx, cancel := RollbackContext(parent)
		defer cancel()
		if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
			t.Errorf("deadline %v (set %v), want %v", got, ok, want)
		}
	})

	t.Run("a running rollback is shortened", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: time.Hour}
		ctx, cancel := RollbackContext(WithRollbackBudget(context.Background(), budget))
		defer cancel()

		budget.SetDeadline(time.Now().Add(30 * time.Millisecond))
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("running rollback did not end at the deadline handed over")
		}
	})

	t.Run("a running rollback ends at once for a deadline that has passed", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: time.Hour}
		ctx, cancel := RollbackContext(WithRollbackBudget(context.Background(), budget))
		defer cancel()

		budget.SetDeadline(time.Now().Add(-time.Second))
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("running rollback did not end for a deadline in the past")
		}
	})

	t.Run("a running rollback is never given more time", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: 50 * time.Millisecond}
		ctx, cancel := RollbackContext(WithRollbackBudget(context.Background(), budget))
		defer cancel()
		before, _ := ctx.Deadline()

		budget.SetDeadline(time.Now().Add(time.Hour))
		if after, _ := ctx.Deadline(); !after.Equal(before) {
			t.Errorf("deadline moved from %v to %v", before, after)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("rollback outlived its own timeout")
		}
	})

	t.Run("a rollback that begins after the deadline has the whole timeout", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: time.Minute}
		parent := WithRollbackBudget(context.Background(), budget)
		budget.SetDeadline(time.Now().Add(-time.Second))

		ctx, cancel := RollbackContext(parent)
		defer cancel()
		if err := ctx.Err(); err != nil {
			t.Fatalf("rollback context was born ended: %v", err)
		}
		if deadline, _ := ctx.Deadline(); time.Until(deadline) < 50*time.Second {
			t.Errorf("deadline in %s, want about the timeout (1m)", time.Until(deadline))
		}
	})

	t.Run("a released rollback is forgotten", func(t *testing.T) {
		budget := &RollbackBudget{Timeout: time.Hour}
		_, cancel := RollbackContext(WithRollbackBudget(context.Background(), budget))
		cancel()
		budget.mu.Lock()
		live := len(budget.live)
		budget.mu.Unlock()
		if live != 0 {
			t.Errorf("%d rollbacks kept after release", live)
		}
		// Handing over a deadline afterwards touches nothing.
		budget.SetDeadline(time.Now())
	})
}
