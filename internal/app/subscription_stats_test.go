package app

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestSubscriptionStats_OnlyWhileRunning checks that the framework answers
// SubscriptionStats with nil unless it is running, and delegates to the
// lifecycle manager otherwise.
func TestSubscriptionStats_OnlyWhileRunning(t *testing.T) {
	mockLM := newMockLifecycleManager()
	mockLM.stats = []types.SubscriptionStats{{Kind: "request_reply", Module: "orders", Name: "check"}}
	fw := &frameworkApplication{logger: &mockLogger{}, lifecycleManager: mockLM, state: types.StateCreated}

	if got := fw.SubscriptionStats(); got != nil {
		t.Fatalf("SubscriptionStats before Start = %v, want nil", got)
	}
	fw.state = types.StateRunning
	got := fw.SubscriptionStats()
	if len(got) != 1 || got[0].Name != "check" {
		t.Fatalf("SubscriptionStats while running = %v, want the lifecycle manager's snapshot", got)
	}
	fw.state = types.StateStopped
	if got := fw.SubscriptionStats(); got != nil {
		t.Fatalf("SubscriptionStats after Stop = %v, want nil", got)
	}
}
