package types

import (
	"testing"
)

// =============================================================================
// FormatServiceType Tests
// =============================================================================

func TestFormatServiceType(t *testing.T) {
	tests := []struct {
		name        string
		serviceType ServiceType
		expected    string
	}{
		{
			name:        "ServiceTypeChannel returns channel",
			serviceType: ServiceTypeChannel,
			expected:    "channel",
		},
		{
			name:        "ServiceTypeRequestReply returns request_reply",
			serviceType: ServiceTypeRequestReply,
			expected:    "request_reply",
		},
		{
			name:        "ServiceTypeQueueGroup returns queue_group",
			serviceType: ServiceTypeQueueGroup,
			expected:    "queue_group",
		},
		{
			name:        "ServiceTypeStreamConsumer returns stream_consumer",
			serviceType: ServiceTypeStreamConsumer,
			expected:    "stream_consumer",
		},
		{
			name:        "unknown service type returns unknown with value",
			serviceType: ServiceType(99),
			expected:    "unknown(99)",
		},
		{
			name:        "negative service type returns unknown with value",
			serviceType: ServiceType(-1),
			expected:    "unknown(-1)",
		},
		{
			name:        "large service type returns unknown with value",
			serviceType: ServiceType(1000),
			expected:    "unknown(1000)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatServiceType(tt.serviceType)
			if result != tt.expected {
				t.Errorf("FormatServiceType(%d) = %q, want %q", tt.serviceType, result, tt.expected)
			}
		})
	}
}

func TestFormatServiceType_AllDefinedTypes(t *testing.T) {
	// Verify all defined service types have proper mappings
	definedTypes := map[ServiceType]string{
		ServiceTypeChannel:        "channel",
		ServiceTypeRequestReply:   "request_reply",
		ServiceTypeQueueGroup:     "queue_group",
		ServiceTypeStreamConsumer: "stream_consumer",
	}

	for serviceType, expectedName := range definedTypes {
		result := FormatServiceType(serviceType)
		if result != expectedName {
			t.Errorf("FormatServiceType(%d) = %q, want %q", serviceType, result, expectedName)
		}
	}
}
