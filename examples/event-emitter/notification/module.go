// Package notification provides a notification module that demonstrates EventConsumerModule.
// This module consumes order events, sends notifications, and stores them using fs-jetstream plugin.
package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/examples/event-emitter/tracking"
	"github.com/go-monolith/mono/pkg/helper"
	fsjetstream "github.com/go-monolith/mono/plugin/fs-jetstream"
)

// NAME is the unique identifier for the notification module.
const NAME = "notification"

// NotificationModule implements the notification module that consumes order events.
// It implements mono.Module, mono.EventConsumerModule, and mono.UsePluginModule.
type NotificationModule struct {
	// Storage plugin for persisting events
	storagePlugin *fsjetstream.PluginModule

	// Buckets for storing events
	orderCreatedBucket fsjetstream.FileStoragePort
	orderShippedBucket fsjetstream.FileStoragePort

	// Track notifications for demonstration
	mu                        sync.Mutex
	orderCreatedNotifications []tracking.OrderCreatedEvent
	orderShippedNotifications []tracking.OrderShippedEvent
}

// NewModule creates a new notification module instance.
func NewModule() *NotificationModule {
	return &NotificationModule{
		orderCreatedNotifications: make([]tracking.OrderCreatedEvent, 0),
		orderShippedNotifications: make([]tracking.OrderShippedEvent, 0),
	}
}

// Name returns the module name.
func (m *NotificationModule) Name() string {
	return NAME
}

// SetPlugin receives plugin instances from the framework.
// Called for all registered plugins before Start.
func (m *NotificationModule) SetPlugin(alias string, plugin mono.PluginModule) {
	if alias == "storage" {
		if storagePlugin, ok := plugin.(*fsjetstream.PluginModule); ok {
			m.storagePlugin = storagePlugin
		}
	}
}

// Start initializes the module and gets bucket references from the storage plugin.
func (m *NotificationModule) Start(_ context.Context) error {
	// Get bucket references from the storage plugin
	m.orderCreatedBucket = m.storagePlugin.Bucket("order-created-events")
	m.orderShippedBucket = m.storagePlugin.Bucket("order-shipped-events")

	if m.orderCreatedBucket == nil {
		return fmt.Errorf("order-created-events bucket not found in storage plugin")
	}
	if m.orderShippedBucket == nil {
		return fmt.Errorf("order-shipped-events bucket not found in storage plugin")
	}

	return nil
}

// Stop gracefully shuts down the module.
func (m *NotificationModule) Stop(_ context.Context) error {
	return nil
}

// RegisterEventConsumers registers event consumers for this module.
// Called by the framework after RegisterServices but before Start.
func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Pattern 1: Direct import with type-safe RegisterTypedConsumer
	// This provides automatic unmarshaling and compile-time type safety
	if err := helper.RegisterTypedEventConsumer(registry, tracking.OrderCreatedV1, m.handleOrderCreated, m); err != nil {
		return fmt.Errorf("failed to register OrderCreated consumer: %w", err)
	}

	// Pattern 2: Discover OrderShipped event via EventRegistry.GetEventByName().
	// This uses event discovery instead of direct import to avoid circular imports.
	orderShippedDef, ok := registry.GetEventByName("OrderShipped", "v1", "tracking")
	if !ok {
		return fmt.Errorf("failed to discover OrderShipped event from event registry")
	}
	// Register event consumer using the EventConsumerModule pattern.
	// Unmarshaling of event data need to match with the EventEmitter module
	if err := registry.RegisterEventConsumer(orderShippedDef, func(ctx context.Context, msg *mono.Msg) error {
		var event tracking.OrderShippedEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			return fmt.Errorf("failed to unmarshal OrderShipped event")
		}
		return m.handleOrderShipped(ctx, event, msg)
	}, m); err != nil {
		return fmt.Errorf("failed to register OrderShipped consumer: %w", err)
	}

	return nil
}

// handleOrderCreated handles OrderCreated events with type-safe signature.
// The event is automatically unmarshaled by RegisterTypedConsumer.
func (m *NotificationModule) handleOrderCreated(ctx context.Context, event tracking.OrderCreatedEvent, _ *mono.Msg) error {
	// Send notification (in a real system, this would send email, push notification, etc.)
	fmt.Printf("  [%s] Sending order confirmation notification:\n", m.Name())
	fmt.Printf("    Order ID: %s\n", event.OrderID)
	fmt.Printf("    Customer: %s\n", event.CustomerID)
	fmt.Printf("    Product: %s x %d\n", event.ProductID, event.Quantity)
	fmt.Printf("    Total: $%.2f %s\n", event.Amount, event.Currency)

	// Track for demonstration
	m.mu.Lock()
	m.orderCreatedNotifications = append(m.orderCreatedNotifications, event)
	m.mu.Unlock()

	// Save event to storage as JSON file
	if err := m.saveOrderCreatedEvent(ctx, event); err != nil {
		fmt.Printf("    [WARNING] Failed to save event to storage: %v\n", err)
	} else {
		fmt.Printf("    [STORED] Event saved to order-created-events bucket\n")
	}

	return nil
}

// handleOrderShipped handles OrderShipped events with type-safe signature.
// The event is automatically unmarshaled by RegisterTypedConsumer.
func (m *NotificationModule) handleOrderShipped(ctx context.Context, event tracking.OrderShippedEvent, _ *mono.Msg) error {
	// Send notification
	fmt.Printf("  [%s] Sending shipping notification:\n", m.Name())
	fmt.Printf("    Order ID: %s\n", event.OrderID)
	fmt.Printf("    Tracking: %s (%s)\n", event.TrackingNum, event.Carrier)

	// Track for demonstration
	m.mu.Lock()
	m.orderShippedNotifications = append(m.orderShippedNotifications, event)
	m.mu.Unlock()

	// Save event to storage as JSON file
	if err := m.saveOrderShippedEvent(ctx, event); err != nil {
		fmt.Printf("    [WARNING] Failed to save event to storage: %v\n", err)
	} else {
		fmt.Printf("    [STORED] Event saved to order-shipped-events bucket\n")
	}

	return nil
}

// saveOrderCreatedEvent saves an OrderCreatedEvent to the storage bucket as JSON.
func (m *NotificationModule) saveOrderCreatedEvent(ctx context.Context, event tracking.OrderCreatedEvent) error {
	// Create a unique filename using order ID and timestamp
	filename := fmt.Sprintf("%s_%s.json", event.OrderID, time.Now().Format("20060102_150405"))

	// Marshal event to JSON
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Store in the bucket
	_, err = m.orderCreatedBucket.Put(ctx, filename, data,
		fsjetstream.WithDescription(fmt.Sprintf("OrderCreated event for order %s", event.OrderID)),
		fsjetstream.WithHeaders(map[string]string{
			"Content-Type": "application/json",
			"Event-Type":   "OrderCreated",
			"Order-ID":     event.OrderID,
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to store event: %w", err)
	}

	return nil
}

// saveOrderShippedEvent saves an OrderShippedEvent to the storage bucket as JSON.
func (m *NotificationModule) saveOrderShippedEvent(ctx context.Context, event tracking.OrderShippedEvent) error {
	// Create a unique filename using order ID and timestamp
	filename := fmt.Sprintf("%s_%s.json", event.OrderID, time.Now().Format("20060102_150405"))

	// Marshal event to JSON
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Store in the bucket
	_, err = m.orderShippedBucket.Put(ctx, filename, data,
		fsjetstream.WithDescription(fmt.Sprintf("OrderShipped event for order %s", event.OrderID)),
		fsjetstream.WithHeaders(map[string]string{
			"Content-Type": "application/json",
			"Event-Type":   "OrderShipped",
			"Order-ID":     event.OrderID,
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to store event: %w", err)
	}

	return nil
}

// GetOrderCreatedNotifications returns all received OrderCreated notifications.
func (m *NotificationModule) GetOrderCreatedNotifications() []tracking.OrderCreatedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]tracking.OrderCreatedEvent, len(m.orderCreatedNotifications))
	copy(result, m.orderCreatedNotifications)
	return result
}

// GetOrderShippedNotifications returns all received OrderShipped notifications.
func (m *NotificationModule) GetOrderShippedNotifications() []tracking.OrderShippedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]tracking.OrderShippedEvent, len(m.orderShippedNotifications))
	copy(result, m.orderShippedNotifications)
	return result
}
