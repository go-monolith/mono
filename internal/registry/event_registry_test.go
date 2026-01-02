package registry

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// Test event type for generic EventDefinition tests
type testOrderCreatedEvent struct {
	OrderID string
	Amount  float64
}

// TestNewEventRegistry tests the EventRegistry constructor
func TestNewEventRegistry(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})
	if registry == nil {
		t.Fatal("NewEventRegistry returned nil")
	}

	events := registry.GetAllEvents()
	if len(events) != 0 {
		t.Errorf("expected empty registry, got %d events", len(events))
	}

	consumers := registry.Entries()
	if len(consumers) != 0 {
		t.Errorf("expected no consumers, got %d", len(consumers))
	}
}

func TestNewEventRegistryPanicsOnNilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil logger")
		}
	}()

	NewEventRegistry(nil)
}

// TestRegisterEvent tests event registration
func TestRegisterEvent(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	err := registry.RegisterEvent(eventDef)
	if err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	events := registry.GetAllEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Name != "OrderCreated" {
		t.Errorf("expected 'OrderCreated', got '%s'", events[0].Name)
	}
}

func TestRegisterEventValidation(t *testing.T) {
	tests := []struct {
		name    string
		event   types.BaseEventDefinition
		wantErr string
	}{
		{
			name: "empty module name",
			event: types.BaseEventDefinition{
				ModuleName: "",
				Name:       "Test",
				Version:    "v1",
				Subject:    "events.test.v1",
			},
			wantErr: "module name cannot be empty",
		},
		{
			name: "empty event name",
			event: types.BaseEventDefinition{
				ModuleName: "test",
				Name:       "",
				Version:    "v1",
				Subject:    "events.test.v1",
			},
			wantErr: "event name cannot be empty",
		},
		{
			name: "empty version",
			event: types.BaseEventDefinition{
				ModuleName: "test",
				Name:       "Test",
				Version:    "",
				Subject:    "events.test.v1",
			},
			wantErr: "version cannot be empty",
		},
		{
			name: "empty subject",
			event: types.BaseEventDefinition{
				ModuleName: "test",
				Name:       "Test",
				Version:    "v1",
				Subject:    "",
			},
			wantErr: "subject cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewEventRegistry(&mockLogger{})
			err := registry.RegisterEvent(tt.event)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestRegisterDuplicateEvent(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	err := registry.RegisterEvent(eventDef)
	if err != nil {
		t.Fatalf("first RegisterEvent failed: %v", err)
	}

	// Try to register the same event again
	err = registry.RegisterEvent(eventDef)
	if err == nil {
		t.Fatal("expected error for duplicate event")
	}
}

func TestRegisterDifferentVersions(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventV1 := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	eventV2 := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v2.created",
		Version:    "v2",
	}

	err := registry.RegisterEvent(eventV1)
	if err != nil {
		t.Fatalf("RegisterEvent v1 failed: %v", err)
	}

	err = registry.RegisterEvent(eventV2)
	if err != nil {
		t.Fatalf("RegisterEvent v2 failed: %v", err)
	}

	events := registry.GetAllEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

// TestGetEventByName tests event discovery by name
func TestGetEventByName(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	// Find the event
	found, ok := registry.GetEventByName("OrderCreated", "v1", "order")
	if !ok {
		t.Fatal("expected to find event")
	}

	if found.Name != "OrderCreated" {
		t.Errorf("expected 'OrderCreated', got '%s'", found.Name)
	}

	if found.Version != "v1" {
		t.Errorf("expected 'v1', got '%s'", found.Version)
	}

	if found.ModuleName != "order" {
		t.Errorf("expected 'order', got '%s'", found.ModuleName)
	}

	if found.Subject != "events.orders.v1.created" {
		t.Errorf("expected 'events.orders.v1.created', got '%s'", found.Subject)
	}
}

func TestGetEventByNameNotFound(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	tests := []struct {
		name       string
		eventName  string
		version    string
		moduleName string
	}{
		{"wrong event name", "OrderUpdated", "v1", "order"},
		{"wrong version", "OrderCreated", "v2", "order"},
		{"wrong module", "OrderCreated", "v1", "payment"},
		{"all wrong", "Unknown", "v99", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := registry.GetEventByName(tt.eventName, tt.version, tt.moduleName)
			if ok {
				t.Error("expected event not to be found")
			}
		})
	}
}

// TestGetEventsByModule tests retrieving events by module name
func TestGetEventsByModule(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	// Register events from different modules
	orderEvents := []types.BaseEventDefinition{
		{ModuleName: "order", Name: "OrderCreated", Subject: "events.orders.v1.created", Version: "v1"},
		{ModuleName: "order", Name: "OrderShipped", Subject: "events.orders.v1.shipped", Version: "v1"},
	}

	paymentEvents := []types.BaseEventDefinition{
		{ModuleName: "payment", Name: "PaymentReceived", Subject: "events.payment.v1.received", Version: "v1"},
	}

	for _, e := range orderEvents {
		_ = registry.RegisterEvent(e)
	}
	for _, e := range paymentEvents {
		_ = registry.RegisterEvent(e)
	}

	// Get order module events
	orderResult := registry.GetEventsByModule("order")
	if len(orderResult) != 2 {
		t.Fatalf("expected 2 order events, got %d", len(orderResult))
	}

	// Get payment module events
	paymentResult := registry.GetEventsByModule("payment")
	if len(paymentResult) != 1 {
		t.Fatalf("expected 1 payment event, got %d", len(paymentResult))
	}

	// Get non-existent module events
	nonExistent := registry.GetEventsByModule("non-existent")
	if len(nonExistent) != 0 {
		t.Fatalf("expected 0 events for non-existent module, got %d", len(nonExistent))
	}
}

// TestGetAllEvents tests retrieving all events
func TestGetAllEvents(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	events := []types.BaseEventDefinition{
		{ModuleName: "order", Name: "OrderCreated", Subject: "events.orders.v1.created", Version: "v1"},
		{ModuleName: "order", Name: "OrderShipped", Subject: "events.orders.v1.shipped", Version: "v1"},
		{ModuleName: "payment", Name: "PaymentReceived", Subject: "events.payment.v1.received", Version: "v1"},
	}

	for _, e := range events {
		_ = registry.RegisterEvent(e)
	}

	allEvents := registry.GetAllEvents()
	if len(allEvents) != 3 {
		t.Fatalf("expected 3 events, got %d", len(allEvents))
	}

	// Verify registration order is preserved
	if allEvents[0].Name != "OrderCreated" {
		t.Errorf("expected first event 'OrderCreated', got '%s'", allEvents[0].Name)
	}
	if allEvents[1].Name != "OrderShipped" {
		t.Errorf("expected second event 'OrderShipped', got '%s'", allEvents[1].Name)
	}
	if allEvents[2].Name != "PaymentReceived" {
		t.Errorf("expected third event 'PaymentReceived', got '%s'", allEvents[2].Name)
	}
}

func TestGetAllEventsReturnsCopy(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	events1 := registry.GetAllEvents()
	events2 := registry.GetAllEvents()

	// Modify events1 and check that events2 is unaffected
	if len(events1) > 0 {
		events1[0].Name = "Modified"
	}

	if events2[0].Name == "Modified" {
		t.Error("GetAllEvents should return a copy, not a reference")
	}
}

// TestRegisterEventConsumer tests consumer registration
func TestRegisterEventConsumer(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	module := &mockModule{name: "notification"}
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	err := registry.RegisterEventConsumer(eventDef, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventConsumer failed: %v", err)
	}

	consumers := registry.Entries()
	if len(consumers) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(consumers))
	}

	if consumers[0].Module.Name() != "notification" {
		t.Errorf("expected module 'notification', got '%s'", consumers[0].Module.Name())
	}

	if consumers[0].EventDef.Name != "OrderCreated" {
		t.Errorf("expected event 'OrderCreated', got '%s'", consumers[0].EventDef.Name)
	}

	// Verify default queue group is set to module name
	if consumers[0].QueueGroup != "notification" {
		t.Errorf("expected queue group 'notification', got '%s'", consumers[0].QueueGroup)
	}
}

func TestRegisterEventConsumerValidation(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	module := &mockModule{name: "notification"}
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	// Test nil handler
	err := registry.RegisterEventConsumer(eventDef, nil, module)
	if err == nil {
		t.Error("expected error for nil handler")
	}

	// Test nil module
	err = registry.RegisterEventConsumer(eventDef, handler, nil)
	if err == nil {
		t.Error("expected error for nil module")
	}

	// Test empty subject
	emptySubjectEvent := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "",
		Version:    "v1",
	}
	err = registry.RegisterEventConsumer(emptySubjectEvent, handler, module)
	if err == nil {
		t.Error("expected error for empty subject")
	}
}

func TestEventConsumerQueueGroup(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	module := &mockModule{name: "notification"}
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	// Test default queue group (module name)
	err := registry.RegisterEventConsumer(eventDef, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventConsumer with default queue group failed: %v", err)
	}

	consumers := registry.Entries()
	if len(consumers) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(consumers))
	}

	if consumers[0].QueueGroup != "notification" {
		t.Errorf("expected default queue group 'notification', got '%s'", consumers[0].QueueGroup)
	}

	// Test custom queue group
	customHandler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = registry.RegisterEventConsumer(eventDef, customHandler, module, "custom-queue")
	if err != nil {
		t.Fatalf("RegisterEventConsumer with custom queue group failed: %v", err)
	}

	consumers = registry.Entries()
	if len(consumers) != 2 {
		t.Fatalf("expected 2 consumers, got %d", len(consumers))
	}

	if consumers[1].QueueGroup != "custom-queue" {
		t.Errorf("expected custom queue group 'custom-queue', got '%s'", consumers[1].QueueGroup)
	}

	// Test empty string queue group (should default to module name)
	emptyQueueHandler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = registry.RegisterEventConsumer(eventDef, emptyQueueHandler, module, "")
	if err != nil {
		t.Fatalf("RegisterEventConsumer with empty queue group failed: %v", err)
	}

	consumers = registry.Entries()
	if len(consumers) != 3 {
		t.Fatalf("expected 3 consumers, got %d", len(consumers))
	}

	if consumers[2].QueueGroup != "notification" {
		t.Errorf("expected empty queue group to default to 'notification', got '%s'", consumers[2].QueueGroup)
	}

	// Test multiple queue groups provided (should use first one and warn)
	multiHandler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	err = registry.RegisterEventConsumer(eventDef, multiHandler, module, "first-queue", "second-queue")
	if err != nil {
		t.Fatalf("RegisterEventConsumer with multiple queue groups failed: %v", err)
	}

	consumers = registry.Entries()
	if len(consumers) != 4 {
		t.Fatalf("expected 4 consumers, got %d", len(consumers))
	}

	if consumers[3].QueueGroup != "first-queue" {
		t.Errorf("expected multiple queue groups to use first value 'first-queue', got '%s'", consumers[3].QueueGroup)
	}
}

func TestMultipleConsumersForSameEvent(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	module1 := &mockModule{name: "notification"}
	module2 := &mockModule{name: "analytics"}

	handler1 := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	handler2 := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	err := registry.RegisterEventConsumer(eventDef, handler1, module1)
	if err != nil {
		t.Fatalf("RegisterEventConsumer 1 failed: %v", err)
	}

	err = registry.RegisterEventConsumer(eventDef, handler2, module2)
	if err != nil {
		t.Fatalf("RegisterEventConsumer 2 failed: %v", err)
	}

	consumers := registry.Entries()
	if len(consumers) != 2 {
		t.Fatalf("expected 2 consumers, got %d", len(consumers))
	}
}

// TestEntries tests retrieving all consumers
func TestEntries(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	events := []types.BaseEventDefinition{
		{ModuleName: "order", Name: "OrderCreated", Subject: "events.orders.v1.created", Version: "v1"},
		{ModuleName: "order", Name: "OrderShipped", Subject: "events.orders.v1.shipped", Version: "v1"},
	}

	for _, e := range events {
		_ = registry.RegisterEvent(e)
	}

	module := &mockModule{name: "notification"}
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	for _, e := range events {
		_ = registry.RegisterEventConsumer(e, handler, module)
	}

	entries := registry.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Verify registration order is preserved
	if entries[0].EventDef.Name != "OrderCreated" {
		t.Errorf("expected first entry 'OrderCreated', got '%s'", entries[0].EventDef.Name)
	}
	if entries[1].EventDef.Name != "OrderShipped" {
		t.Errorf("expected second entry 'OrderShipped', got '%s'", entries[1].EventDef.Name)
	}
}

func TestEntriesReturnsCopy(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	_ = registry.RegisterEvent(eventDef)

	module := &mockModule{name: "notification"}
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}

	_ = registry.RegisterEventConsumer(eventDef, handler, module)

	entries1 := registry.Entries()
	entries2 := registry.Entries()

	// Modify entries1 and check that entries2 is unaffected
	if len(entries1) > 0 {
		entries1[0].EventDef.Name = "Modified"
	}

	if entries2[0].EventDef.Name == "Modified" {
		t.Error("Entries should return a copy, not a reference")
	}
}

// TestSetMiddlewareChain tests middleware chain injection
func TestSetMiddlewareChain(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	// Create a mock middleware chain that wraps handlers
	var handlerWrapped bool
	mockChain := &mockMiddlewareChain{
		onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
			original := entry.Handler
			entry.Handler = func(ctx context.Context, msg *types.Msg) error {
				handlerWrapped = true
				return original(ctx, msg)
			}
			return entry
		},
	}

	registry.SetMiddlewareChain(mockChain)

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	var originalHandlerCalled bool
	handler := func(ctx context.Context, msg *types.Msg) error {
		originalHandlerCalled = true
		return nil
	}

	module := &mockModule{name: "notification"}
	err := registry.RegisterEventConsumer(eventDef, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventConsumer failed: %v", err)
	}

	// Get the registered consumer and call its handler
	entries := registry.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Call the wrapped handler
	err = entries[0].Handler(context.Background(), &types.Msg{Data: []byte("test")})
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	if !handlerWrapped {
		t.Error("expected handler to be wrapped by middleware")
	}
	if !originalHandlerCalled {
		t.Error("expected original handler to be called")
	}
}

// TestRegisterEventConsumerWithoutMiddlewareChain tests registration without middleware
func TestRegisterEventConsumerWithoutMiddlewareChain(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	var handlerCalled bool
	handler := func(ctx context.Context, msg *types.Msg) error {
		handlerCalled = true
		return nil
	}

	module := &mockModule{name: "notification"}
	err := registry.RegisterEventConsumer(eventDef, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventConsumer failed: %v", err)
	}

	entries := registry.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Call the handler directly (should work without middleware)
	err = entries[0].Handler(context.Background(), &types.Msg{Data: []byte("test")})
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	if !handlerCalled {
		t.Error("expected handler to be called")
	}
}

// mockMiddlewareChain is a test implementation of MiddlewareChainRunner
type mockMiddlewareChain struct {
	onEventConsumerRegistration       func(context.Context, types.EventConsumerEntry) types.EventConsumerEntry
	onEventStreamConsumerRegistration func(context.Context, types.EventStreamConsumerEntry) types.EventStreamConsumerEntry
}

func (c *mockMiddlewareChain) RunServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	return reg
}

func (c *mockMiddlewareChain) RunOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx
}

func (c *mockMiddlewareChain) RunEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	if c.onEventConsumerRegistration != nil {
		return c.onEventConsumerRegistration(ctx, entry)
	}
	return entry
}

func (c *mockMiddlewareChain) RunEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	if c.onEventStreamConsumerRegistration != nil {
		return c.onEventStreamConsumerRegistration(ctx, entry)
	}
	return entry
}

// TestConcurrentAccess tests thread safety of the registry
func TestConcurrentAccess(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	done := make(chan bool)

	// Concurrent registrations
	go func() {
		for i := 0; i < 100; i++ {
			eventDef := types.BaseEventDefinition{
				ModuleName: "order",
				Name:       fmt.Sprintf("Event%d", i),
				Subject:    fmt.Sprintf("events.order.v1.event%d", i),
				Version:    "v1",
			}
			_ = registry.RegisterEvent(eventDef)
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			_ = registry.GetAllEvents()
			_ = registry.GetEventsByModule("order")
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}

// TestRegisterEventStreamConsumer tests event stream consumer registration
func TestRegisterEventStreamConsumer(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "order-events",
		},
		Fetch: types.FetchConfig{
			BatchSize: 10,
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}
	err := registry.RegisterEventStreamConsumer(eventDef, config, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventStreamConsumer failed: %v", err)
	}

	entries := registry.StreamConsumerEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream consumer entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.EventDef.Name != "OrderCreated" {
		t.Errorf("expected event 'OrderCreated', got '%s'", entry.EventDef.Name)
	}

	// Verify module is set
	if entry.Module.Name() != "order-consumer" {
		t.Errorf("expected module 'order-consumer', got '%s'", entry.Module.Name())
	}

	// Verify that Stream.Subjects was overridden with eventDef.Subject
	if len(entry.Config.Stream.Subjects) != 1 {
		t.Fatalf("expected 1 subject, got %d", len(entry.Config.Stream.Subjects))
	}
	if entry.Config.Stream.Subjects[0] != "events.orders.v1.created" {
		t.Errorf("expected subject 'events.orders.v1.created', got '%s'", entry.Config.Stream.Subjects[0])
	}
}

func TestRegisterEventStreamConsumerValidation(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "order-events",
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}

	// Test nil handler
	err := registry.RegisterEventStreamConsumer(eventDef, config, nil, module)
	if err == nil {
		t.Error("expected error for nil handler")
	}
	if !strings.Contains(err.Error(), "handler cannot be nil") {
		t.Errorf("expected error about nil handler, got: %v", err)
	}

	// Test nil module
	err = registry.RegisterEventStreamConsumer(eventDef, config, handler, nil)
	if err == nil {
		t.Error("expected error for nil module")
	}
	if !strings.Contains(err.Error(), "module cannot be nil") {
		t.Errorf("expected error about nil module, got: %v", err)
	}

	// Test empty subject
	emptySubjectEvent := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "",
		Version:    "v1",
	}
	err = registry.RegisterEventStreamConsumer(emptySubjectEvent, config, handler, module)
	if err == nil {
		t.Error("expected error for empty subject")
	}
	if !strings.Contains(err.Error(), "subject cannot be empty") {
		t.Errorf("expected error about empty subject, got: %v", err)
	}

	// Test empty stream name
	emptyStreamConfig := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "",
		},
	}
	err = registry.RegisterEventStreamConsumer(eventDef, emptyStreamConfig, handler, module)
	if err == nil {
		t.Error("expected error for empty stream name")
	}
	if !strings.Contains(err.Error(), "stream name cannot be empty") {
		t.Errorf("expected error about empty stream name, got: %v", err)
	}
}

func TestRegisterEventStreamConsumerDefaults(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	// Config with zero values for fetch settings
	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "order-events",
		},
		Fetch: types.FetchConfig{
			BatchSize: 0, // Should default to 10
			Timeout:   0, // Should default to 5 seconds
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}
	err := registry.RegisterEventStreamConsumer(eventDef, config, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventStreamConsumer failed: %v", err)
	}

	entries := registry.StreamConsumerEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Config.Fetch.BatchSize != 10 {
		t.Errorf("expected default batch size 10, got %d", entry.Config.Fetch.BatchSize)
	}
	if entry.Config.Fetch.Timeout != 5*1e9 { // 5 seconds in nanoseconds
		t.Errorf("expected default timeout 5s, got %v", entry.Config.Fetch.Timeout)
	}
}

func TestStreamConsumerEntriesReturnsCopy(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "order-events",
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}
	_ = registry.RegisterEventStreamConsumer(eventDef, config, handler, module)

	entries1 := registry.StreamConsumerEntries()
	entries2 := registry.StreamConsumerEntries()

	// Modify entries1 and check that entries2 is unaffected
	if len(entries1) > 0 {
		entries1[0].EventDef.Name = "Modified"
	}

	if entries2[0].EventDef.Name == "Modified" {
		t.Error("StreamConsumerEntries should return a copy, not a reference")
	}
}

func TestMultipleEventStreamConsumers(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef1 := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	eventDef2 := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderShipped",
		Subject:    "events.orders.v1.shipped",
		Version:    "v1",
	}

	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name: "order-events",
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}
	err := registry.RegisterEventStreamConsumer(eventDef1, config, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventStreamConsumer 1 failed: %v", err)
	}

	err = registry.RegisterEventStreamConsumer(eventDef2, config, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventStreamConsumer 2 failed: %v", err)
	}

	entries := registry.StreamConsumerEntries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 stream consumer entries, got %d", len(entries))
	}

	// Verify registration order is preserved
	if entries[0].EventDef.Name != "OrderCreated" {
		t.Errorf("expected first entry 'OrderCreated', got '%s'", entries[0].EventDef.Name)
	}
	if entries[1].EventDef.Name != "OrderShipped" {
		t.Errorf("expected second entry 'OrderShipped', got '%s'", entries[1].EventDef.Name)
	}
}

func TestEventStreamConsumerSubjectOverride(t *testing.T) {
	registry := NewEventRegistry(&mockLogger{})

	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}

	// Config with pre-existing subjects that should be overridden
	config := types.StreamConsumerConfig{
		Stream: types.StreamConfig{
			Name:     "order-events",
			Subjects: []string{"some.other.subject", "another.subject"},
		},
	}

	handler := func(ctx context.Context, msgs []*types.Msg) error {
		return nil
	}

	module := &mockModule{name: "order-consumer"}
	err := registry.RegisterEventStreamConsumer(eventDef, config, handler, module)
	if err != nil {
		t.Fatalf("RegisterEventStreamConsumer failed: %v", err)
	}

	entries := registry.StreamConsumerEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Verify subjects were overridden
	subjects := entries[0].Config.Stream.Subjects
	if len(subjects) != 1 {
		t.Fatalf("expected 1 subject after override, got %d", len(subjects))
	}
	if subjects[0] != "events.orders.v1.created" {
		t.Errorf("expected subject 'events.orders.v1.created', got '%s'", subjects[0])
	}
}
