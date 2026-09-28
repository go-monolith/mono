package types_test

import (
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestSanitizeConsumerName tests consumer name sanitization
func TestSanitizeConsumerName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "alphanumeric only", input: "test123", expected: "test123"},
		{name: "with spaces", input: "test consumer name", expected: "test-consumer-name"},
		{name: "with special characters", input: "test@consumer#name", expected: "testconsumername"},
		{name: "with allowed separators", input: "test-consumer_name", expected: "test-consumer_name"},
		{name: "dots become hyphens", input: "billing.v2-worker.v1", expected: "billing-v2-worker-v1"},
		{name: "wildcards and path separators removed", input: "a*b>c/d\\e", expected: "abcde"},
		{name: "empty string", input: "", expected: "consumer"},
		{name: "only invalid characters", input: "@#$%", expected: "consumer"},
		{name: "mixed valid and invalid", input: "test!@#$%consumer&*()name", expected: "testconsumername"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := types.SanitizeConsumerName(tt.input); got != tt.expected {
				t.Errorf("SanitizeConsumerName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// TestEventStreamConsumerName tests the stable durable name of event stream consumers
func TestEventStreamConsumerName(t *testing.T) {
	paymentProcessed := types.BaseEventDefinition{
		ModuleName: "billing",
		Name:       "PaymentProcessed",
		Version:    "v1",
		Subject:    "events.billing.v1.payment-processed",
	}
	paymentNotification := types.BaseEventDefinition{
		ModuleName: "billing",
		Name:       "PaymentProcessedNotification",
		Version:    "v1",
		Subject:    "events.billing.v1.payment-processed",
	}

	tests := []struct {
		name           string
		consumerModule string
		def            types.BaseEventDefinition
		expected       string
	}{
		{
			name:           "consumer module prefixes the event identity",
			consumerModule: "analytics",
			def:            paymentProcessed,
			expected:       "analytics-billing-PaymentProcessed-v1",
		},
		{
			name:           "another module consuming the same event gets its own name",
			consumerModule: "notification",
			def:            paymentProcessed,
			expected:       "notification-billing-PaymentProcessed-v1",
		},
		{
			name:           "different event on the same subject gets its own name",
			consumerModule: "notification",
			def:            paymentNotification,
			expected:       "notification-billing-PaymentProcessedNotification-v1",
		},
		{
			name:           "dotted module and version are sanitized",
			consumerModule: "billing.v2",
			def:            types.BaseEventDefinition{ModuleName: "order", Name: "Created", Version: "v1.2"},
			expected:       "billing-v2-order-Created-v1-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := types.EventStreamConsumerName(tt.consumerModule, tt.def); got != tt.expected {
				t.Errorf("EventStreamConsumerName(%q, %+v) = %q, want %q", tt.consumerModule, tt.def, got, tt.expected)
			}
		})
	}
}
