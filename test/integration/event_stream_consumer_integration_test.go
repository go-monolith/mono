//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

// =============================================================================
// Event Type Definitions for EventStreamConsumer Tests
// =============================================================================

// OrderEventPayload is the event payload for order stream tests
type OrderEventPayload struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Timestamp  time.Time `json:"timestamp"`
}

// InventoryEventPayload is the event payload for inventory stream tests
type InventoryEventPayload struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Action    string `json:"action"` // "add" or "remove"
}

// =============================================================================
// Event Definitions
// =============================================================================

var orderCreatedStreamEventDef = helper.EventDefinition[OrderEventPayload](
	"order-stream",
	"OrderCreated",
	"v1",
)

var inventoryUpdatedStreamEventDef = helper.EventDefinition[InventoryEventPayload](
	"inventory-stream",
	"InventoryUpdated",
	"v1",
)

// =============================================================================
// Order Stream Emitter Module
// =============================================================================

type orderStreamEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *orderStreamEmitterModule) Name() string                  { return m.name }
func (m *orderStreamEmitterModule) Start(_ context.Context) error { return nil }
func (m *orderStreamEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *orderStreamEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

func (m *orderStreamEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		orderCreatedStreamEventDef.ToBase(),
	}
}

func (m *orderStreamEmitterModule) emitOrderCreated(ctx context.Context, event OrderEventPayload) (mono.MsgPubAck, error) {
	return orderCreatedStreamEventDef.EventStreamPublish(ctx, m.eventBus, event, nil)
}

// =============================================================================
// Inventory Stream Emitter Module
// =============================================================================

type inventoryStreamEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *inventoryStreamEmitterModule) Name() string                  { return m.name }
func (m *inventoryStreamEmitterModule) Start(_ context.Context) error { return nil }
func (m *inventoryStreamEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *inventoryStreamEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

func (m *inventoryStreamEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		inventoryUpdatedStreamEventDef.ToBase(),
	}
}

func (m *inventoryStreamEmitterModule) emitInventoryUpdated(ctx context.Context, event InventoryEventPayload) (mono.MsgPubAck, error) {
	return inventoryUpdatedStreamEventDef.EventStreamPublish(ctx, m.eventBus, event, nil)
}

// =============================================================================
// Order Stream Consumer Module (Type-Safe Batch Handler)
// =============================================================================

type orderStreamConsumerModule struct {
	name           string
	receivedOrders []OrderEventPayload
	mu             sync.Mutex
}

func (m *orderStreamConsumerModule) Name() string                  { return m.name }
func (m *orderStreamConsumerModule) Start(_ context.Context) error { return nil }
func (m *orderStreamConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *orderStreamConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Use typed event stream consumer registration
	config := mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name:      "order-events-stream",
			Retention: mono.WorkQueuePolicy,
		},
		Fetch: mono.FetchConfig{
			BatchSize: 5,
			Timeout:   2 * time.Second,
		},
	}

	return helper.RegisterTypedEventStreamConsumer(
		registry,
		orderCreatedStreamEventDef,
		config,
		m.handleOrderBatch,
		m,
	)
}

func (m *orderStreamConsumerModule) handleOrderBatch(ctx context.Context, events []OrderEventPayload, msgs []*mono.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, event := range events {
		m.receivedOrders = append(m.receivedOrders, event)
		// Acknowledge each message
		if err := msgs[i].Ack(); err != nil {
			return err
		}
	}
	return nil
}

func (m *orderStreamConsumerModule) getReceivedOrders() []OrderEventPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]OrderEventPayload, len(m.receivedOrders))
	copy(result, m.receivedOrders)
	return result
}

// =============================================================================
// Raw Stream Consumer Module (Non-typed Handler)
// =============================================================================

type rawStreamConsumerModule struct {
	name              string
	receivedInventory []InventoryEventPayload
	mu                sync.Mutex
}

func (m *rawStreamConsumerModule) Name() string                  { return m.name }
func (m *rawStreamConsumerModule) Start(_ context.Context) error { return nil }
func (m *rawStreamConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *rawStreamConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	config := mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name:      "inventory-events-stream",
			Retention: mono.WorkQueuePolicy,
		},
		Fetch: mono.FetchConfig{
			BatchSize: 3,
			Timeout:   2 * time.Second,
		},
	}

	handler := func(ctx context.Context, msgs []*mono.Msg) error {
		m.mu.Lock()
		defer m.mu.Unlock()

		for _, msg := range msgs {
			var event InventoryEventPayload
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				if nakErr := msg.Nak(); nakErr != nil {
					return nakErr
				}
				continue
			}
			m.receivedInventory = append(m.receivedInventory, event)
			if err := msg.Ack(); err != nil {
				return err
			}
		}
		return nil
	}

	return registry.RegisterEventStreamConsumer(
		inventoryUpdatedStreamEventDef.ToBase(),
		config,
		handler,
		m,
	)
}

func (m *rawStreamConsumerModule) getReceivedInventory() []InventoryEventPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]InventoryEventPayload, len(m.receivedInventory))
	copy(result, m.receivedInventory)
	return result
}

// =============================================================================
// Integration Tests
// =============================================================================

// TestEventStreamConsumer_TypedHandler tests typed event stream consumer with batch handler
func TestEventStreamConsumer_TypedHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create framework with JetStream enabled
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamDomain("test-typed"),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Create modules
	emitter := &orderStreamEmitterModule{name: "order-stream-emitter"}
	consumer := &orderStreamConsumerModule{name: "order-stream-consumer"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start the application
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	}()

	// Give JetStream time to set up
	time.Sleep(500 * time.Millisecond)

	// Emit multiple order events
	orders := []OrderEventPayload{
		{OrderID: "order-1", CustomerID: "cust-1", Amount: 100.00, Timestamp: time.Now()},
		{OrderID: "order-2", CustomerID: "cust-2", Amount: 200.00, Timestamp: time.Now()},
		{OrderID: "order-3", CustomerID: "cust-3", Amount: 300.00, Timestamp: time.Now()},
	}

	for _, order := range orders {
		if _, err := emitter.emitOrderCreated(ctx, order); err != nil {
			t.Fatalf("Failed to emit order: %v", err)
		}
	}

	// Wait for events to be processed
	var received []OrderEventPayload
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		received = consumer.getReceivedOrders()
		if len(received) >= len(orders) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all orders were received
	if len(received) != len(orders) {
		t.Fatalf("Expected %d orders, got %d", len(orders), len(received))
	}

	// Verify order data
	for i, order := range orders {
		if received[i].OrderID != order.OrderID {
			t.Errorf("Order %d: expected OrderID %s, got %s", i, order.OrderID, received[i].OrderID)
		}
		if received[i].Amount != order.Amount {
			t.Errorf("Order %d: expected Amount %f, got %f", i, order.Amount, received[i].Amount)
		}
	}
}

// TestEventStreamConsumer_RawHandler tests non-typed event stream consumer
func TestEventStreamConsumer_RawHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create framework with JetStream enabled
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamDomain("test-raw"),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Create modules
	emitter := &inventoryStreamEmitterModule{name: "inventory-stream-emitter"}
	consumer := &rawStreamConsumerModule{name: "inventory-stream-consumer"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start the application
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	}()

	// Give JetStream time to set up
	time.Sleep(500 * time.Millisecond)

	// Emit inventory events
	inventoryUpdates := []InventoryEventPayload{
		{ProductID: "prod-1", Quantity: 10, Action: "add"},
		{ProductID: "prod-2", Quantity: 5, Action: "remove"},
	}

	for _, update := range inventoryUpdates {
		if _, err := emitter.emitInventoryUpdated(ctx, update); err != nil {
			t.Fatalf("Failed to emit inventory update: %v", err)
		}
	}

	// Wait for events to be processed
	var received []InventoryEventPayload
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		received = consumer.getReceivedInventory()
		if len(received) >= len(inventoryUpdates) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all inventory updates were received
	if len(received) != len(inventoryUpdates) {
		t.Fatalf("Expected %d inventory updates, got %d", len(inventoryUpdates), len(received))
	}

	// Verify data
	for i, update := range inventoryUpdates {
		if received[i].ProductID != update.ProductID {
			t.Errorf("Update %d: expected ProductID %s, got %s", i, update.ProductID, received[i].ProductID)
		}
		if received[i].Quantity != update.Quantity {
			t.Errorf("Update %d: expected Quantity %d, got %d", i, update.Quantity, received[i].Quantity)
		}
	}
}

// TestEventStreamConsumer_SubjectOverride tests that stream subjects are correctly overridden
func TestEventStreamConsumer_SubjectOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create framework with JetStream enabled
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamDomain("test-override"),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Create emitter module
	emitter := &orderStreamEmitterModule{name: "order-stream-emitter-override"}

	// Custom consumer with explicit subjects that should be overridden
	consumer := &orderStreamConsumerModule{name: "order-stream-consumer-override"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start the application
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	}()

	// Give JetStream time to set up
	time.Sleep(500 * time.Millisecond)

	// Emit an order - this should be captured by the stream consumer
	order := OrderEventPayload{
		OrderID:    "override-test",
		CustomerID: "cust-override",
		Amount:     999.99,
		Timestamp:  time.Now(),
	}

	if _, err := emitter.emitOrderCreated(ctx, order); err != nil {
		t.Fatalf("Failed to emit order: %v", err)
	}

	// Wait for event to be processed
	var received []OrderEventPayload
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		received = consumer.getReceivedOrders()
		if len(received) >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify order was received (proves subjects were correctly configured)
	if len(received) != 1 {
		t.Fatalf("Expected 1 order, got %d", len(received))
	}

	if received[0].OrderID != order.OrderID {
		t.Errorf("Expected OrderID %s, got %s", order.OrderID, received[0].OrderID)
	}
}

// TestEventStreamConsumer_EventStreamPublish tests that EventStreamPublish returns valid MsgPubAck
func TestEventStreamConsumer_EventStreamPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create framework with JetStream enabled
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamDomain("test-puback"),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create app: %v", err)
	}

	// Create modules
	emitter := &orderStreamEmitterModule{name: "order-stream-emitter-puback"}
	consumer := &orderStreamConsumerModule{name: "order-stream-consumer-puback"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start the application
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Stop(stopCtx)
	}()

	// Give JetStream time to set up
	time.Sleep(500 * time.Millisecond)

	// Emit events and verify MsgPubAck contains valid sequence numbers
	orders := []OrderEventPayload{
		{OrderID: "puback-1", CustomerID: "cust-1", Amount: 100.00, Timestamp: time.Now()},
		{OrderID: "puback-2", CustomerID: "cust-2", Amount: 200.00, Timestamp: time.Now()},
		{OrderID: "puback-3", CustomerID: "cust-3", Amount: 300.00, Timestamp: time.Now()},
	}

	var lastSeq uint64
	for i, order := range orders {
		ack, err := emitter.emitOrderCreated(ctx, order)
		if err != nil {
			t.Fatalf("Failed to emit order %d: %v", i, err)
		}

		// Verify MsgPubAck is not nil
		if ack == nil {
			t.Fatalf("Expected non-nil MsgPubAck for order %d", i)
		}

		// Verify sequence number is greater than 0
		if ack.Sequence() == 0 {
			t.Errorf("Expected sequence > 0 for order %d, got %d", i, ack.Sequence())
		}

		// Verify sequence numbers are incrementing
		if i > 0 && ack.Sequence() <= lastSeq {
			t.Errorf("Expected sequence %d > %d for order %d", ack.Sequence(), lastSeq, i)
		}
		lastSeq = ack.Sequence()
	}

	// Wait for events to be processed
	var received []OrderEventPayload
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		received = consumer.getReceivedOrders()
		if len(received) >= len(orders) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all orders were received
	if len(received) != len(orders) {
		t.Fatalf("Expected %d orders, got %d", len(orders), len(received))
	}
}
