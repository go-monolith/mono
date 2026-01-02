package types_test

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// =============================================================================
// FormatServiceType Tests
// =============================================================================

func TestFormatServiceType(t *testing.T) {
	tests := []struct {
		name        string
		serviceType types.ServiceType
		expected    string
	}{
		{
			name:        "ServiceTypeChannel returns channel",
			serviceType: types.ServiceTypeChannel,
			expected:    "channel",
		},
		{
			name:        "ServiceTypeRequestReply returns request_reply",
			serviceType: types.ServiceTypeRequestReply,
			expected:    "request_reply",
		},
		{
			name:        "ServiceTypeQueueGroup returns queue_group",
			serviceType: types.ServiceTypeQueueGroup,
			expected:    "queue_group",
		},
		{
			name:        "ServiceTypeStreamConsumer returns stream_consumer",
			serviceType: types.ServiceTypeStreamConsumer,
			expected:    "stream_consumer",
		},
		{
			name:        "unknown service type returns unknown with value",
			serviceType: types.ServiceType(99),
			expected:    "unknown(99)",
		},
		{
			name:        "negative service type returns unknown with value",
			serviceType: types.ServiceType(-1),
			expected:    "unknown(-1)",
		},
		{
			name:        "large service type returns unknown with value",
			serviceType: types.ServiceType(1000),
			expected:    "unknown(1000)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := types.FormatServiceType(tt.serviceType)
			if result != tt.expected {
				t.Errorf("FormatServiceType(%d) = %q, want %q", tt.serviceType, result, tt.expected)
			}
		})
	}
}

func TestFormatServiceType_AllDefinedTypes(t *testing.T) {
	// Verify all defined service types have proper mappings
	definedTypes := map[types.ServiceType]string{
		types.ServiceTypeChannel:        "channel",
		types.ServiceTypeRequestReply:   "request_reply",
		types.ServiceTypeQueueGroup:     "queue_group",
		types.ServiceTypeStreamConsumer: "stream_consumer",
	}

	for serviceType, expectedName := range definedTypes {
		result := types.FormatServiceType(serviceType)
		if result != expectedName {
			t.Errorf("FormatServiceType(%d) = %q, want %q", serviceType, result, expectedName)
		}
	}
}
