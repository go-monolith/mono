//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

// OrderCreatedEvent is the event payload for order creation
type OrderCreatedEvent struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

// OrderShippedEvent is the event payload for order shipping
type OrderShippedEvent struct {
	OrderID     string `json:"order_id"`
	TrackingNum string `json:"tracking_num"`
}

// orderEmitterModule is a test module that emits events
type orderEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *orderEmitterModule) Name() string { return m.name }

func (m *orderEmitterModule) Start(ctx context.Context) error { return nil }

func (m *orderEmitterModule) Stop(ctx context.Context) error { return nil }

func (m *orderEmitterModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// Define typed event definitions for type-safe publishing
var orderCreatedEventDef = helper.EventDefinition[OrderCreatedEvent](
	"order", "OrderCreated", "v1",
)

var orderShippedEventDef = helper.EventDefinition[OrderShippedEvent](
	"order", "OrderShipped", "v1",
)

// EmitEvents declares events this module can emit
func (m *orderEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		orderCreatedEventDef.ToBase(),
		orderShippedEventDef.ToBase(),
	}
}

// emitOrderCreated publishes an OrderCreated event using type-safe Publish
func (m *orderEmitterModule) emitOrderCreated(event OrderCreatedEvent) error {
	return orderCreatedEventDef.Publish(m.eventBus, event, nil)
}

// notificationConsumerModule is a test module that consumes events
type notificationConsumerModule struct {
	name             string
	receivedEvents   []OrderCreatedEvent
	receivedEventsMu sync.Mutex
}

func (m *notificationConsumerModule) Name() string { return m.name }

func (m *notificationConsumerModule) Start(_ context.Context) error { return nil }

func (m *notificationConsumerModule) Stop(_ context.Context) error { return nil }

func (m *notificationConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Discover the OrderCreated event
	eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
	if !ok {
		return nil // Event not found, which is fine for some tests
	}

	// Register as consumer
	return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *notificationConsumerModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
	var event OrderCreatedEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return err
	}

	m.receivedEventsMu.Lock()
	m.receivedEvents = append(m.receivedEvents, event)
	m.receivedEventsMu.Unlock()

	return nil
}

func (m *notificationConsumerModule) getReceivedEvents() []OrderCreatedEvent {
	m.receivedEventsMu.Lock()
	defer m.receivedEventsMu.Unlock()
	result := make([]OrderCreatedEvent, len(m.receivedEvents))
	copy(result, m.receivedEvents)
	return result
}

// analyticsConsumerModule is another test module that consumes the same event
type analyticsConsumerModule struct {
	name            string
	receivedCount   int
	receivedCountMu sync.Mutex
}

func (m *analyticsConsumerModule) Name() string { return m.name }

func (m *analyticsConsumerModule) Start(_ context.Context) error { return nil }

func (m *analyticsConsumerModule) Stop(_ context.Context) error { return nil }

func (m *analyticsConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Discover the OrderCreated event
	eventDef, ok := registry.GetEventByName("OrderCreated", "v1", "order")
	if !ok {
		return nil
	}

	// Register as consumer
	return registry.RegisterEventConsumer(eventDef, m.handleOrderCreated, m)
}

func (m *analyticsConsumerModule) handleOrderCreated(ctx context.Context, msg *mono.Msg) error {
	m.receivedCountMu.Lock()
	m.receivedCount++
	m.receivedCountMu.Unlock()
	return nil
}

func (m *analyticsConsumerModule) getReceivedCount() int {
	m.receivedCountMu.Lock()
	defer m.receivedCountMu.Unlock()
	return m.receivedCount
}

// TestIntegration_EventEmitterModule tests basic event emitter functionality
func TestIntegration_EventEmitterModule(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&mockLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create order module (emitter)
	orderModule := &orderEmitterModule{name: "order"}

	// Create notification module (consumer)
	notificationModule := &notificationConsumerModule{name: "notification"}

	// Register modules
	if err := fw.Register(orderModule); err != nil {
		t.Fatalf("Failed to register order module: %v", err)
	}
	if err := fw.Register(notificationModule); err != nil {
		t.Fatalf("Failed to register notification module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Emit an order created event
	event := OrderCreatedEvent{
		OrderID:    "ORD-001",
		CustomerID: "CUST-001",
		Amount:     99.99,
	}

	if err := orderModule.emitOrderCreated(event); err != nil {
		t.Fatalf("Failed to emit event: %v", err)
	}

	// Wait for event to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify notification module received the event
	receivedEvents := notificationModule.getReceivedEvents()
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 received event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].OrderID != "ORD-001" {
		t.Errorf("Expected OrderID 'ORD-001', got '%s'", receivedEvents[0].OrderID)
	}

	if receivedEvents[0].CustomerID != "CUST-001" {
		t.Errorf("Expected CustomerID 'CUST-001', got '%s'", receivedEvents[0].CustomerID)
	}

	if receivedEvents[0].Amount != 99.99 {
		t.Errorf("Expected Amount 99.99, got %f", receivedEvents[0].Amount)
	}
}

// TestIntegration_MultipleEventConsumers tests multiple consumers for the same event
func TestIntegration_MultipleEventConsumers(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&mockLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create modules
	orderModule := &orderEmitterModule{name: "order"}
	notificationModule := &notificationConsumerModule{name: "notification"}
	analyticsModule := &analyticsConsumerModule{name: "analytics"}

	// Register modules
	if err := fw.Register(orderModule); err != nil {
		t.Fatalf("Failed to register order module: %v", err)
	}
	if err := fw.Register(notificationModule); err != nil {
		t.Fatalf("Failed to register notification module: %v", err)
	}
	if err := fw.Register(analyticsModule); err != nil {
		t.Fatalf("Failed to register analytics module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Emit multiple events
	for i := 0; i < 3; i++ {
		event := OrderCreatedEvent{
			OrderID:    fmt.Sprintf("ORD-%03d", i+1),
			CustomerID: "CUST-001",
			Amount:     float64(100 + i),
		}
		if err := orderModule.emitOrderCreated(event); err != nil {
			t.Fatalf("Failed to emit event %d: %v", i, err)
		}
	}

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify both modules received all events
	notificationEvents := notificationModule.getReceivedEvents()
	if len(notificationEvents) != 3 {
		t.Errorf("Notification module expected 3 events, got %d", len(notificationEvents))
	}

	analyticsCount := analyticsModule.getReceivedCount()
	if analyticsCount != 3 {
		t.Errorf("Analytics module expected 3 events, got %d", analyticsCount)
	}
}

// TestIntegration_EventDiscovery tests event discovery through EventRegistry
func TestIntegration_EventDiscovery(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&mockLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create order module (emitter)
	orderModule := &orderEmitterModule{name: "order"}

	// Create a consumer that verifies event discovery
	discoveryModule := &eventDiscoveryModule{name: "discovery"}

	// Register modules
	if err := fw.Register(orderModule); err != nil {
		t.Fatalf("Failed to register order module: %v", err)
	}
	if err := fw.Register(discoveryModule); err != nil {
		t.Fatalf("Failed to register discovery module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify event discovery results
	if !discoveryModule.foundOrderCreated {
		t.Error("OrderCreated event should be discoverable")
	}
	if !discoveryModule.foundOrderShipped {
		t.Error("OrderShipped event should be discoverable")
	}
	if discoveryModule.foundNonExistent {
		t.Error("NonExistent event should not be found")
	}

	// Verify module events
	if discoveryModule.orderModuleEventCount != 2 {
		t.Errorf("Order module should have 2 events, got %d", discoveryModule.orderModuleEventCount)
	}

	// Verify all events count
	if discoveryModule.allEventsCount < 2 {
		t.Errorf("Should have at least 2 total events, got %d", discoveryModule.allEventsCount)
	}
}

// eventDiscoveryModule tests event discovery capabilities
type eventDiscoveryModule struct {
	name                  string
	foundOrderCreated     bool
	foundOrderShipped     bool
	foundNonExistent      bool
	orderModuleEventCount int
	allEventsCount        int
}

func (m *eventDiscoveryModule) Name() string { return m.name }

func (m *eventDiscoveryModule) Start(_ context.Context) error { return nil }

func (m *eventDiscoveryModule) Stop(_ context.Context) error { return nil }

func (m *eventDiscoveryModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Test GetEventByName
	_, m.foundOrderCreated = registry.GetEventByName("OrderCreated", "v1", "order")
	_, m.foundOrderShipped = registry.GetEventByName("OrderShipped", "v1", "order")
	_, m.foundNonExistent = registry.GetEventByName("NonExistent", "v1", "order")

	// Test GetEventsByModule
	orderEvents := registry.GetEventsByModule("order")
	m.orderModuleEventCount = len(orderEvents)

	// Test GetAllEvents
	allEvents := registry.GetAllEvents()
	m.allEventsCount = len(allEvents)

	return nil
}

// TestIntegration_EventPublishRawHelper tests the PublishRaw helper method
func TestIntegration_EventPublishRawHelper(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&mockLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create modules
	orderModule := &orderEmitterModule{name: "order"}
	notificationModule := &notificationConsumerModule{name: "notification"}

	// Register modules
	if err := fw.Register(orderModule); err != nil {
		t.Fatalf("Failed to register order module: %v", err)
	}
	if err := fw.Register(notificationModule); err != nil {
		t.Fatalf("Failed to register notification module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get event definition and use PublishRaw directly
	eventDef := orderModule.EmitEvents()[0]
	event := OrderCreatedEvent{
		OrderID:    "ORD-DIRECT",
		CustomerID: "CUST-DIRECT",
		Amount:     199.99,
	}
	data, _ := json.Marshal(event)

	// Use the helper method
	if err := eventDef.PublishRaw(orderModule.eventBus, data, nil); err != nil {
		t.Fatalf("Failed to publish using helper: %v", err)
	}

	// Wait for event to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify event was received
	receivedEvents := notificationModule.getReceivedEvents()
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 received event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].OrderID != "ORD-DIRECT" {
		t.Errorf("Expected OrderID 'ORD-DIRECT', got '%s'", receivedEvents[0].OrderID)
	}
}

// TestIntegration_EventVersioning tests multiple versions of the same event
func TestIntegration_EventVersioning(t *testing.T) {
	fw, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&mockLogger{}),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create versioned emitter module
	versionedModule := &versionedEmitterModule{name: "versioned"}

	// Create v1 consumer
	v1Consumer := &v1ConsumerModule{name: "v1-consumer"}

	// Create v2 consumer
	v2Consumer := &v2ConsumerModule{name: "v2-consumer"}

	// Register modules
	if err := fw.Register(versionedModule); err != nil {
		t.Fatalf("Failed to register versioned module: %v", err)
	}
	if err := fw.Register(v1Consumer); err != nil {
		t.Fatalf("Failed to register v1 consumer: %v", err)
	}
	if err := fw.Register(v2Consumer); err != nil {
		t.Fatalf("Failed to register v2 consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Emit v1 event
	if err := versionedModule.emitV1Event(); err != nil {
		t.Fatalf("Failed to emit v1 event: %v", err)
	}

	// Emit v2 event
	if err := versionedModule.emitV2Event(); err != nil {
		t.Fatalf("Failed to emit v2 event: %v", err)
	}

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify v1 consumer only received v1 event
	v1Count := v1Consumer.getReceivedCount()
	if v1Count != 1 {
		t.Errorf("V1 consumer expected 1 event, got %d", v1Count)
	}

	// Verify v2 consumer only received v2 event
	v2Count := v2Consumer.getReceivedCount()
	if v2Count != 1 {
		t.Errorf("V2 consumer expected 1 event, got %d", v2Count)
	}
}

// versionedEmitterModule emits events in multiple versions
type versionedEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *versionedEmitterModule) Name() string                    { return m.name }
func (m *versionedEmitterModule) Start(ctx context.Context) error { return nil }
func (m *versionedEmitterModule) Stop(ctx context.Context) error  { return nil }
func (m *versionedEmitterModule) SetEventBus(bus mono.EventBus)   { m.eventBus = bus }

func (m *versionedEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		{
			ModuleName: m.name,
			Name:       "DataUpdated",
			Subject:    "events.data.v1.updated",
			Version:    "v1",
		},
		{
			ModuleName: m.name,
			Name:       "DataUpdated",
			Subject:    "events.data.v2.updated",
			Version:    "v2",
		},
	}
}

func (m *versionedEmitterModule) emitV1Event() error {
	eventDef := m.EmitEvents()[0]
	return eventDef.PublishRaw(m.eventBus, []byte(`{"version":"v1"}`), nil)
}

func (m *versionedEmitterModule) emitV2Event() error {
	eventDef := m.EmitEvents()[1]
	return eventDef.PublishRaw(m.eventBus, []byte(`{"version":"v2"}`), nil)
}

// v1ConsumerModule consumes v1 events
type v1ConsumerModule struct {
	name          string
	receivedCount int
	mu            sync.Mutex
}

func (m *v1ConsumerModule) Name() string                  { return m.name }
func (m *v1ConsumerModule) Start(_ context.Context) error { return nil }
func (m *v1ConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *v1ConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	eventDef, ok := registry.GetEventByName("DataUpdated", "v1", "versioned")
	if !ok {
		return nil
	}
	return registry.RegisterEventConsumer(eventDef, func(_ context.Context, _ *mono.Msg) error {
		m.mu.Lock()
		m.receivedCount++
		m.mu.Unlock()
		return nil
	}, m)
}

func (m *v1ConsumerModule) getReceivedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedCount
}

// v2ConsumerModule consumes v2 events
type v2ConsumerModule struct {
	name          string
	receivedCount int
	mu            sync.Mutex
}

func (m *v2ConsumerModule) Name() string                  { return m.name }
func (m *v2ConsumerModule) Start(_ context.Context) error { return nil }
func (m *v2ConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *v2ConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	eventDef, ok := registry.GetEventByName("DataUpdated", "v2", "versioned")
	if !ok {
		return nil
	}
	return registry.RegisterEventConsumer(eventDef, func(_ context.Context, _ *mono.Msg) error {
		m.mu.Lock()
		m.receivedCount++
		m.mu.Unlock()
		return nil
	}, m)
}

func (m *v2ConsumerModule) getReceivedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedCount
}
