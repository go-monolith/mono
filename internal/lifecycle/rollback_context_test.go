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

// A deadline set after the budget went into the context, as a Stop does while
// the Start is running, applies to the rollbacks that begin after it.
func TestRollbackBudget_DeadlineSetWhileStarting(t *testing.T) {
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
}
