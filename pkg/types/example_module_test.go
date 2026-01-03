package types_test

import (
	"context"
	"fmt"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// ExampleModule_minimal demonstrates implementing the minimal Module interface.
func ExampleModule_minimal() {
	// Modules implement Name(), Start(), and Stop()
	// This is the minimum required for any module
	fmt.Println("Modules implement Name(), Start(), and Stop()")
	// Output: Modules implement Name(), Start(), and Stop()
}

// ExampleHealthStatus demonstrates creating health status.
func ExampleHealthStatus() {
	status := types.HealthStatus{
		Healthy: true,
		Message: "All systems operational",
		Details: map[string]any{
			"database":    "connected",
			"cache":       "connected",
			"connections": 42,
		},
	}

	fmt.Println("Healthy:", status.Healthy)
	fmt.Println("Message:", status.Message)
	fmt.Printf("Details count: %d\n", len(status.Details))
	// Output:
	// Healthy: true
	// Message: All systems operational
	// Details count: 3
}

// ExampleHealthStatus_unhealthy demonstrates reporting unhealthy status.
func ExampleHealthStatus_unhealthy() {
	status := types.HealthStatus{
		Healthy: false,
		Message: "Database connection lost",
		Details: map[string]any{
			"error":       "connection refused",
			"last_active": "2025-01-01T12:00:00Z",
		},
	}

	fmt.Println("Healthy:", status.Healthy)
	fmt.Println("Message:", status.Message)
	fmt.Printf("Details count: %d\n", len(status.Details))
	// Output:
	// Healthy: false
	// Message: Database connection lost
	// Details count: 2
}

// sampleEvent is an example event payload for demonstration.
type sampleEvent struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

// ExampleNewEventDefinition demonstrates creating a typed event definition.
func ExampleNewEventDefinition() {
	eventDef := types.NewEventDefinition[sampleEvent](
		"order",                         // moduleName
		"OrderCreated",                  // name
		"v1",                            // version
		"events.order.v1.order-created", // subject
	)

	fmt.Println("Event name:", eventDef.Name)
	fmt.Println("Event version:", eventDef.Version)
	fmt.Println("Event subject:", eventDef.Subject)
	// Output:
	// Event name: OrderCreated
	// Event version: v1
	// Event subject: events.order.v1.order-created
}

// ExampleServiceType demonstrates the service type constants.
func ExampleServiceType() {
	fmt.Println("Channel:", types.FormatServiceType(types.ServiceTypeChannel))
	fmt.Println("RequestReply:", types.FormatServiceType(types.ServiceTypeRequestReply))
	fmt.Println("QueueGroup:", types.FormatServiceType(types.ServiceTypeQueueGroup))
	fmt.Println("StreamConsumer:", types.FormatServiceType(types.ServiceTypeStreamConsumer))
	// Output:
	// Channel: channel
	// RequestReply: request_reply
	// QueueGroup: queue_group
	// StreamConsumer: stream_consumer
}

// ExampleLogLevel demonstrates the log level constants.
func ExampleLogLevel() {
	levels := []types.LogLevel{
		types.LogLevelDebug,
		types.LogLevelInfo,
		types.LogLevelWarn,
		types.LogLevelError,
	}

	for _, level := range levels {
		fmt.Println(level)
	}
	// Output:
	// 0
	// 1
	// 2
	// 3
}

// ExampleQGHP demonstrates creating queue group handler pairs.
func ExampleQGHP() {
	handler := func(_ context.Context, msg *types.Msg) error {
		fmt.Println("Processing message:", string(msg.Data))
		return nil
	}

	pair := types.QGHP{
		QueueGroup: "worker-pool",
		Handler:    handler,
	}

	fmt.Println("Queue group:", pair.QueueGroup)
	fmt.Printf("Has handler: %v\n", pair.Handler != nil)
	// Output:
	// Queue group: worker-pool
	// Has handler: true
}

// ExampleStreamConfig demonstrates configuring a JetStream stream.
func ExampleStreamConfig() {
	config := types.StreamConfig{
		Name:        "orders",
		Description: "Order events stream",
		Subjects:    []string{"events.order.>"},
		Retention:   types.WorkQueuePolicy,
		Storage:     types.FileStorage,
		MaxMsgs:     10000,
		MaxBytes:    100 * 1024 * 1024, // 100 MB
	}

	fmt.Println("Stream name:", config.Name)
	fmt.Println("Description:", config.Description)
	fmt.Printf("Subjects: %v\n", config.Subjects)
	fmt.Println("Retention:", config.Retention)
	fmt.Println("Storage:", config.Storage)
	fmt.Println("MaxMsgs:", config.MaxMsgs)
	fmt.Println("MaxBytes:", config.MaxBytes)
	// Output:
	// Stream name: orders
	// Description: Order events stream
	// Subjects: [events.order.>]
	// Retention: 2
	// Storage: 0
	// MaxMsgs: 10000
	// MaxBytes: 104857600
}

// ExampleFetchConfig demonstrates configuring JetStream fetch behavior.
func ExampleFetchConfig() {
	config := types.FetchConfig{
		BatchSize: 10,
		Timeout:   5 * time.Second,
	}

	fmt.Println("Batch size:", config.BatchSize)
	fmt.Println("Timeout:", config.Timeout)
	// Output:
	// Batch size: 10
	// Timeout: 5s
}
