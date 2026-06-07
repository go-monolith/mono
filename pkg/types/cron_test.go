package types_test

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestNormalizeCronSchedule(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		want     string
	}{
		{
			name:     "five-field gets seconds prepended",
			schedule: "0 0 * * *",
			want:     "0 0 0 * * *",
		},
		{
			name:     "five-field with names and steps",
			schedule: "30 9 * * MON-FRI",
			want:     "0 30 9 * * MON-FRI",
		},
		{
			name:     "five-field with extra whitespace",
			schedule: "  0  0  *  *  *  ",
			want:     "0 0 0 * * *",
		},
		{
			name:     "six-field unchanged",
			schedule: "15 0 0 * * *",
			want:     "15 0 0 * * *",
		},
		{
			name:     "alias unchanged",
			schedule: "@daily",
			want:     "@daily",
		},
		{
			name:     "interval unchanged",
			schedule: "@every 5m",
			want:     "@every 5m",
		},
		{
			name:     "malformed field count left for the server to reject",
			schedule: "0 0 * *",
			want:     "0 0 * *",
		},
		{
			name:     "empty unchanged",
			schedule: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := types.NormalizeCronSchedule(tt.schedule); got != tt.want {
				t.Errorf("NormalizeCronSchedule(%q) = %q, want %q", tt.schedule, got, tt.want)
			}
		})
	}
}
