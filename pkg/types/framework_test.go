package types_test

import (
	"testing"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// =============================================================================
// MonoFrameworkState.String Tests
// =============================================================================

func TestMonoFrameworkState_String(t *testing.T) {
	tests := []struct {
		name     string
		state    types.MonoFrameworkState
		expected string
	}{
		{
			name:     "StateCreated returns Created",
			state:    types.StateCreated,
			expected: "Created",
		},
		{
			name:     "StateStarting returns Starting",
			state:    types.StateStarting,
			expected: "Starting",
		},
		{
			name:     "StateRunning returns Running",
			state:    types.StateRunning,
			expected: "Running",
		},
		{
			name:     "StateStopping returns Stopping",
			state:    types.StateStopping,
			expected: "Stopping",
		},
		{
			name:     "StateStopped returns Stopped",
			state:    types.StateStopped,
			expected: "Stopped",
		},
		{
			name:     "unknown state returns Unknown with value",
			state:    types.MonoFrameworkState(99),
			expected: "Unknown(99)",
		},
		{
			name:     "negative state returns Unknown with value",
			state:    types.MonoFrameworkState(-1),
			expected: "Unknown(-1)",
		},
		{
			name:     "large state returns Unknown with value",
			state:    types.MonoFrameworkState(1000),
			expected: "Unknown(1000)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.state.String()
			if result != tt.expected {
				t.Errorf("MonoFrameworkState(%d).String() = %q, want %q", tt.state, result, tt.expected)
			}
		})
	}
}

func TestMonoFrameworkState_AllDefinedStates(t *testing.T) {
	// Verify all defined states have proper string representations
	definedStates := map[types.MonoFrameworkState]string{
		types.StateCreated:  "Created",
		types.StateStarting: "Starting",
		types.StateRunning:  "Running",
		types.StateStopping: "Stopping",
		types.StateStopped:  "Stopped",
	}

	for state, expectedName := range definedStates {
		result := state.String()
		if result != expectedName {
			t.Errorf("MonoFrameworkState(%d).String() = %q, want %q", state, result, expectedName)
		}
	}
}

func TestMonoFrameworkState_IotaOrder(t *testing.T) {
	// Verify the iota ordering is as expected
	if types.StateCreated != 0 {
		t.Errorf("StateCreated = %d, want 0", types.StateCreated)
	}
	if types.StateStarting != 1 {
		t.Errorf("StateStarting = %d, want 1", types.StateStarting)
	}
	if types.StateRunning != 2 {
		t.Errorf("StateRunning = %d, want 2", types.StateRunning)
	}
	if types.StateStopping != 3 {
		t.Errorf("StateStopping = %d, want 3", types.StateStopping)
	}
	if types.StateStopped != 4 {
		t.Errorf("StateStopped = %d, want 4", types.StateStopped)
	}
}
