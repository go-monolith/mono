// Package notification implements a notification module for sending notifications
// via queue groups and event subscriptions.
// It demonstrates the EventConsumerModule pattern for consuming events
// using RegisterTypedEventConsumer for type-safe event handling.
package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/multi-module/events"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// NAME is the unique identifier for the notification module.
const NAME = "notification"

// NotificationModule implements the mono.Module for sending notifications
type NotificationModule struct {
	logs []NotificationLog
	mu   sync.RWMutex
}

// Compile-time interface checks
var (
	_ mono.ServiceProviderModule = (*NotificationModule)(nil)
	_ mono.EventConsumerModule   = (*NotificationModule)(nil)
)

// NewModule creates a new notification module
func NewModule() *NotificationModule {
	return &NotificationModule{
		logs: make([]NotificationLog, 0),
	}
}

// Name returns the module identifier
func (m *NotificationModule) Name() string {
	return NAME
}

// Start initializes the module.
func (m *NotificationModule) Start(_ context.Context) error {
	fmt.Println("  → Notification module started")
	return nil
}

// Stop gracefully shuts down the module
func (m *NotificationModule) Stop(_ context.Context) error {
	m.mu.RLock()
	count := len(m.logs)
	m.mu.RUnlock()

	fmt.Printf("  → Notification module stopped (sent %d notifications)\n", count)
	return nil
}

// RegisterEventConsumers registers event consumers for this module.
// Called by the framework after RegisterServices but before Start.
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Use type-safe RegisterTypedEventConsumer with direct import of events.OrderCreatedV1.
	// This provides automatic unmarshaling and compile-time type safety.
	if err := helper.RegisterTypedEventConsumer(registry, events.OrderCreatedV1, m.handleOrderCreatedEvent, m); err != nil {
		return fmt.Errorf("failed to register OrderCreated consumer: %w", err)
	}

	fmt.Printf("  → Notification: registered consumer for %s.%s (%s)\n",
		events.OrderCreatedV1.ModuleName, events.OrderCreatedV1.Name, events.OrderCreatedV1.Subject)
	return nil
}

// RegisterServices registers the on_order_created queue group service using type-safe helper
func (m *NotificationModule) RegisterServices(container mono.ServiceContainer) error {
	// Register the on_order_created queue group service using typed helper
	// This automatically handles JSON unmarshaling of messages
	return helper.RegisterTypedQueueGroupService(
		container,
		"on-order-created",
		json.Unmarshal,
		types.TypedQGHP[OrderCreatedNotification]{
			QueueGroup: "notification-workers",
			Handler:    m.sendNotification,
		},
	)
}

// sendNotification handles queue group notifications with typed payload (fire-and-forget)
func (m *NotificationModule) sendNotification(_ context.Context, notif OrderCreatedNotification, _ *mono.Msg) error {
	// Log the notification - notif is already unmarshaled
	logEntry := NotificationLog{
		NotificationID: notif.OrderID,
		Channel:        "service",
		Message:        fmt.Sprintf("Order %s created for product %s (amount: $%.2f)", notif.OrderID, notif.ProductID, notif.Amount),
		Timestamp:      time.Now(),
	}

	m.mu.Lock()
	m.logs = append(m.logs, logEntry)
	m.mu.Unlock()

	fmt.Printf("  → Notification [service]: %s\n", logEntry.Message)
	return nil
}

// handleOrderCreatedEvent handles event bus notifications via EventRegistry.
// This handler uses the TypedEventConsumerHandler signature with automatic unmarshaling.
func (m *NotificationModule) handleOrderCreatedEvent(_ context.Context, event events.OrderCreatedEvent, _ *mono.Msg) error {
	// Event is already unmarshaled by RegisterTypedEventConsumer.
	// Log the notification
	logEntry := NotificationLog{
		NotificationID: event.OrderID,
		Channel:        "event",
		Message:        fmt.Sprintf("Order %s created for product %s (amount: $%.2f)", event.OrderID, event.ProductID, event.Amount),
		Timestamp:      time.Now(),
	}

	m.mu.Lock()
	m.logs = append(m.logs, logEntry)
	m.mu.Unlock()

	fmt.Printf("  → Notification [event]: %s\n", logEntry.Message)
	return nil
}

// GetLogs returns all notification logs (for testing/demo purposes)
func (m *NotificationModule) GetLogs() []NotificationLog {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]NotificationLog{}, m.logs...)
}
