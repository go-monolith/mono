//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

// =============================================================================
// Event Type Definitions
// =============================================================================

// PaymentProcessedEvent is the event payload for payment processing tests
type PaymentProcessedEvent struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	Amount      float64   `json:"amount"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	ProcessedAt time.Time `json:"processed_at"`
}

// RefundRequestedEvent is the event payload for refund request tests
type RefundRequestedEvent struct {
	RefundID  string  `json:"refund_id"`
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason"`
}

// TaskCompletedEvent is the event payload for queue group tests
type TaskCompletedEvent struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Result   string `json:"result"`
	Duration int64  `json:"duration_ms"`
}

// =============================================================================
// Event Definitions
// =============================================================================

var paymentProcessedEventDef = helper.EventDefinition[PaymentProcessedEvent](
	"payment",
	"PaymentProcessed",
	"v1",
)

var refundRequestedEventDef = helper.EventDefinition[RefundRequestedEvent](
	"payment",
	"RefundRequested",
	"v1",
)

var taskCompletedEventDef = helper.EventDefinition[TaskCompletedEvent](
	"task-processor",
	"TaskCompleted",
	"v1",
)

// =============================================================================
// Payment Emitter Module
// =============================================================================

type paymentEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *paymentEmitterModule) Name() string                  { return m.name }
func (m *paymentEmitterModule) Start(_ context.Context) error { return nil }
func (m *paymentEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *paymentEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

func (m *paymentEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		paymentProcessedEventDef.ToBase(),
		refundRequestedEventDef.ToBase(),
	}
}

func (m *paymentEmitterModule) emitPaymentProcessed(event PaymentProcessedEvent) error {
	return paymentProcessedEventDef.Publish(m.eventBus, event, nil)
}

func (m *paymentEmitterModule) emitRefundRequested(event RefundRequestedEvent) error {
	return refundRequestedEventDef.Publish(m.eventBus, event, nil)
}

// =============================================================================
// Task Processor Emitter Module
// =============================================================================

type taskProcessorEmitterModule struct {
	name     string
	eventBus mono.EventBus
}

func (m *taskProcessorEmitterModule) Name() string                  { return m.name }
func (m *taskProcessorEmitterModule) Start(_ context.Context) error { return nil }
func (m *taskProcessorEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *taskProcessorEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

func (m *taskProcessorEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		taskCompletedEventDef.ToBase(),
	}
}

func (m *taskProcessorEmitterModule) emitTaskCompleted(event TaskCompletedEvent) error {
	return taskCompletedEventDef.Publish(m.eventBus, event, nil)
}

// =============================================================================
// Typed Consumer Module (Scenario 1: Type-Safe Consumers)
// =============================================================================

type typedConsumerModule struct {
	name             string
	receivedPayments []PaymentProcessedEvent
	receivedRefunds  []RefundRequestedEvent
	mu               sync.Mutex
}

func (m *typedConsumerModule) Name() string                  { return m.name }
func (m *typedConsumerModule) Start(_ context.Context) error { return nil }
func (m *typedConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *typedConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Use helper.RegisterTypedEventConsumer for type-safe consumption
	if err := helper.RegisterTypedEventConsumer(
		registry,
		paymentProcessedEventDef,
		m.handlePaymentProcessed,
		m,
	); err != nil {
		return err
	}

	return helper.RegisterTypedEventConsumer(
		registry,
		refundRequestedEventDef,
		m.handleRefundRequested,
		m,
	)
}

func (m *typedConsumerModule) handlePaymentProcessed(_ context.Context, event PaymentProcessedEvent, _ *mono.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receivedPayments = append(m.receivedPayments, event)
	return nil
}

func (m *typedConsumerModule) handleRefundRequested(_ context.Context, event RefundRequestedEvent, _ *mono.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receivedRefunds = append(m.receivedRefunds, event)
	return nil
}

func (m *typedConsumerModule) getReceivedPayments() []PaymentProcessedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]PaymentProcessedEvent, len(m.receivedPayments))
	copy(result, m.receivedPayments)
	return result
}

func (m *typedConsumerModule) getReceivedRefunds() []RefundRequestedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]RefundRequestedEvent, len(m.receivedRefunds))
	copy(result, m.receivedRefunds)
	return result
}

// =============================================================================
// Error Handling Consumer Module (Scenario 2: Error Handling)
// =============================================================================

type errorHandlingConsumerModule struct {
	name         string
	failCount    atomic.Int32
	callCount    atomic.Int32
	successCount atomic.Int32
	lastError    error
	mu           sync.Mutex
}

func (m *errorHandlingConsumerModule) Name() string                  { return m.name }
func (m *errorHandlingConsumerModule) Start(_ context.Context) error { return nil }
func (m *errorHandlingConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *errorHandlingConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return helper.RegisterTypedEventConsumer(
		registry,
		paymentProcessedEventDef,
		m.handleWithPossibleError,
		m,
	)
}

func (m *errorHandlingConsumerModule) handleWithPossibleError(_ context.Context, _ PaymentProcessedEvent, _ *mono.Msg) error {
	m.callCount.Add(1)

	// Simulate failure for first N calls
	if m.failCount.Load() > 0 {
		m.failCount.Add(-1)
		err := errors.New("simulated processing error")
		m.mu.Lock()
		m.lastError = err
		m.mu.Unlock()
		return err
	}

	m.successCount.Add(1)
	return nil
}

func (m *errorHandlingConsumerModule) setFailCount(n int32)   { m.failCount.Store(n) }
func (m *errorHandlingConsumerModule) getCallCount() int32    { return m.callCount.Load() }
func (m *errorHandlingConsumerModule) getSuccessCount() int32 { return m.successCount.Load() }
func (m *errorHandlingConsumerModule) getLastError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastError
}

// =============================================================================
// Queue Group Worker Module (Scenario 3: Queue Groups)
// =============================================================================

type queueGroupWorkerModule struct {
	name         string
	workerID     string
	processedIDs []string
	processCount atomic.Int32
	mu           sync.Mutex
}

func (m *queueGroupWorkerModule) Name() string                  { return m.name }
func (m *queueGroupWorkerModule) Start(_ context.Context) error { return nil }
func (m *queueGroupWorkerModule) Stop(_ context.Context) error  { return nil }

func (m *queueGroupWorkerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// All workers share the same queue group for load balancing
	return helper.RegisterTypedEventConsumer(
		registry,
		taskCompletedEventDef,
		m.handleTask,
		m,
		"task-workers", // Shared queue group name
	)
}

func (m *queueGroupWorkerModule) handleTask(_ context.Context, event TaskCompletedEvent, _ *mono.Msg) error {
	m.processCount.Add(1)
	m.mu.Lock()
	m.processedIDs = append(m.processedIDs, event.TaskID)
	m.mu.Unlock()
	return nil
}

func (m *queueGroupWorkerModule) getProcessCount() int32 {
	return m.processCount.Load()
}

func (m *queueGroupWorkerModule) getProcessedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.processedIDs))
	copy(result, m.processedIDs)
	return result
}

// =============================================================================
// Concurrent Consumer Module (Scenario 4: Concurrent Events)
// =============================================================================

type concurrentConsumerModule struct {
	name           string
	eventCount     atomic.Int64
	processingTime atomic.Int64
	receivedIDs    sync.Map
	expectedCount  atomic.Int64
}

func (m *concurrentConsumerModule) Name() string                  { return m.name }
func (m *concurrentConsumerModule) Start(_ context.Context) error { return nil }
func (m *concurrentConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *concurrentConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return helper.RegisterTypedEventConsumer(
		registry,
		paymentProcessedEventDef,
		m.handleConcurrent,
		m,
	)
}

func (m *concurrentConsumerModule) handleConcurrent(_ context.Context, event PaymentProcessedEvent, _ *mono.Msg) error {
	// Simulate some processing work
	processingMs := m.processingTime.Load()
	if processingMs > 0 {
		time.Sleep(time.Duration(processingMs) * time.Millisecond)
	}

	m.eventCount.Add(1)
	m.receivedIDs.Store(event.PaymentID, struct{}{})
	return nil
}

func (m *concurrentConsumerModule) waitForProcessing(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.eventCount.Load() >= m.expectedCount.Load() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (m *concurrentConsumerModule) getEventCount() int64 {
	return m.eventCount.Load()
}

func (m *concurrentConsumerModule) getUniqueIDCount() int {
	count := 0
	m.receivedIDs.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// =============================================================================
// Test Cases
// =============================================================================

// TestIntegration_TypedEventConsumer tests type-safe event consumers using RegisterTypedEventConsumer
func TestIntegration_TypedEventConsumer(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14222),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create modules
	emitter := &paymentEmitterModule{name: "payment"}
	consumer := &typedConsumerModule{name: "typed-consumer"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions
	time.Sleep(100 * time.Millisecond)

	// Emit typed events
	payment := PaymentProcessedEvent{
		PaymentID:   "PAY-001",
		OrderID:     "ORD-001",
		Amount:      99.99,
		Currency:    "USD",
		Status:      "completed",
		ProcessedAt: time.Now(),
	}
	if err := emitter.emitPaymentProcessed(payment); err != nil {
		t.Fatalf("Failed to emit payment: %v", err)
	}

	refund := RefundRequestedEvent{
		RefundID:  "REF-001",
		PaymentID: "PAY-001",
		Amount:    25.00,
		Reason:    "partial refund",
	}
	if err := emitter.emitRefundRequested(refund); err != nil {
		t.Fatalf("Failed to emit refund: %v", err)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify payment event
	payments := consumer.getReceivedPayments()
	if len(payments) != 1 {
		t.Fatalf("Expected 1 payment, got %d", len(payments))
	}
	if payments[0].PaymentID != "PAY-001" {
		t.Errorf("Expected PaymentID 'PAY-001', got '%s'", payments[0].PaymentID)
	}
	if payments[0].OrderID != "ORD-001" {
		t.Errorf("Expected OrderID 'ORD-001', got '%s'", payments[0].OrderID)
	}
	if payments[0].Amount != 99.99 {
		t.Errorf("Expected Amount 99.99, got %f", payments[0].Amount)
	}
	if payments[0].Currency != "USD" {
		t.Errorf("Expected Currency 'USD', got '%s'", payments[0].Currency)
	}
	if payments[0].Status != "completed" {
		t.Errorf("Expected Status 'completed', got '%s'", payments[0].Status)
	}

	// Verify refund event
	refunds := consumer.getReceivedRefunds()
	if len(refunds) != 1 {
		t.Fatalf("Expected 1 refund, got %d", len(refunds))
	}
	if refunds[0].RefundID != "REF-001" {
		t.Errorf("Expected RefundID 'REF-001', got '%s'", refunds[0].RefundID)
	}
	if refunds[0].PaymentID != "PAY-001" {
		t.Errorf("Expected PaymentID 'PAY-001', got '%s'", refunds[0].PaymentID)
	}
	if refunds[0].Amount != 25.00 {
		t.Errorf("Expected Amount 25.00, got %f", refunds[0].Amount)
	}
	if refunds[0].Reason != "partial refund" {
		t.Errorf("Expected Reason 'partial refund', got '%s'", refunds[0].Reason)
	}
}

// TestIntegration_EventConsumerErrorHandling tests that consumer errors don't crash the handler
func TestIntegration_EventConsumerErrorHandling(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14223),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	emitter := &paymentEmitterModule{name: "payment"}
	errorConsumer := &errorHandlingConsumerModule{name: "error-consumer"}

	// Configure to fail first 2 attempts
	errorConsumer.setFailCount(2)

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(errorConsumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Emit multiple events
	for i := 0; i < 5; i++ {
		event := PaymentProcessedEvent{
			PaymentID:   fmt.Sprintf("PAY-%03d", i+1),
			Amount:      float64(100 + i),
			Status:      "completed",
			ProcessedAt: time.Now(),
		}
		if err := emitter.emitPaymentProcessed(event); err != nil {
			t.Fatalf("Failed to emit event %d: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify handler was called for all events
	callCount := errorConsumer.getCallCount()
	if callCount != 5 {
		t.Errorf("Expected 5 handler calls, got %d", callCount)
	}

	// Verify success count (first 2 failed, rest succeeded)
	successCount := errorConsumer.getSuccessCount()
	if successCount != 3 {
		t.Errorf("Expected 3 successful, got %d", successCount)
	}

	// Verify last error was set
	lastErr := errorConsumer.getLastError()
	if lastErr == nil {
		t.Error("Expected lastError to be set")
	} else if lastErr.Error() != "simulated processing error" {
		t.Errorf("Expected error message 'simulated processing error', got '%s'", lastErr.Error())
	}
}

// TestIntegration_EventConsumerUnmarshalError tests malformed JSON handling.
// The framework's event consumer wrapper logs unmarshal errors and continues processing.
// This prevents a single malformed event from blocking the entire consumer, but malformed
// events are not retried or dead-lettered (design tradeoff to keep the framework simple).
func TestIntegration_EventConsumerUnmarshalError(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14224),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	emitter := &paymentEmitterModule{name: "payment"}
	consumer := &typedConsumerModule{name: "typed-consumer"}

	// Register modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Publish malformed data directly to the subject
	eventBus := app.EventBus("payment")
	eventDef := paymentProcessedEventDef.ToBase()

	// Send invalid JSON
	if err := eventDef.PublishRaw(eventBus, []byte("{invalid json}"), nil); err != nil {
		t.Fatalf("Failed to publish raw: %v", err)
	}

	// Send valid event after invalid
	validEvent := PaymentProcessedEvent{
		PaymentID:   "PAY-VALID",
		Amount:      50.0,
		Status:      "completed",
		ProcessedAt: time.Now(),
	}
	if err := emitter.emitPaymentProcessed(validEvent); err != nil {
		t.Fatalf("Failed to emit valid event: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify only valid event was processed
	payments := consumer.getReceivedPayments()
	if len(payments) != 1 {
		t.Errorf("Expected 1 payment (only valid one), got %d", len(payments))
	}
	if len(payments) > 0 && payments[0].PaymentID != "PAY-VALID" {
		t.Errorf("Expected PaymentID 'PAY-VALID', got '%s'", payments[0].PaymentID)
	}
}

// TestIntegration_QueueGroupEventConsumers tests load balancing with queue groups
func TestIntegration_QueueGroupEventConsumers(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14225),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create emitter
	emitter := &taskProcessorEmitterModule{name: "task-processor"}

	// Create multiple worker modules with same queue group
	worker1 := &queueGroupWorkerModule{name: "worker-1", workerID: "W1"}
	worker2 := &queueGroupWorkerModule{name: "worker-2", workerID: "W2"}
	worker3 := &queueGroupWorkerModule{name: "worker-3", workerID: "W3"}

	// Register all modules
	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(worker1); err != nil {
		t.Fatalf("Failed to register worker1: %v", err)
	}
	if err := app.Register(worker2); err != nil {
		t.Fatalf("Failed to register worker2: %v", err)
	}
	if err := app.Register(worker3); err != nil {
		t.Fatalf("Failed to register worker3: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Extra time for multiple subscriptions
	time.Sleep(200 * time.Millisecond)

	// Emit many events
	eventCount := 30
	for i := 0; i < eventCount; i++ {
		event := TaskCompletedEvent{
			TaskID:   fmt.Sprintf("TASK-%03d", i+1),
			WorkerID: "emitter",
			Result:   "success",
			Duration: 100,
		}
		if err := emitter.emitTaskCompleted(event); err != nil {
			t.Fatalf("Failed to emit event %d: %v", i, err)
		}
	}

	time.Sleep(1 * time.Second)

	// Calculate total processed
	total := worker1.getProcessCount() + worker2.getProcessCount() + worker3.getProcessCount()

	// Verify total equals event count
	if total != int32(eventCount) {
		t.Errorf("Expected total %d, got %d", eventCount, total)
	}

	// Log distribution for visibility
	t.Logf("Worker distribution: W1=%d, W2=%d, W3=%d",
		worker1.getProcessCount(), worker2.getProcessCount(), worker3.getProcessCount())

	// Verify no duplicate processing
	allIDs := make(map[string]bool)
	for _, id := range worker1.getProcessedIDs() {
		if allIDs[id] {
			t.Errorf("Duplicate processing of task %s", id)
		}
		allIDs[id] = true
	}
	for _, id := range worker2.getProcessedIDs() {
		if allIDs[id] {
			t.Errorf("Duplicate processing of task %s", id)
		}
		allIDs[id] = true
	}
	for _, id := range worker3.getProcessedIDs() {
		if allIDs[id] {
			t.Errorf("Duplicate processing of task %s", id)
		}
		allIDs[id] = true
	}

	// Verify all tasks were processed
	if len(allIDs) != eventCount {
		t.Errorf("Expected %d unique tasks, got %d", eventCount, len(allIDs))
	}
}

// TestIntegration_ConcurrentEventProcessing tests high-volume concurrent event processing
func TestIntegration_ConcurrentEventProcessing(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14226),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	emitter := &paymentEmitterModule{name: "payment"}
	consumer := &concurrentConsumerModule{
		name: "concurrent-consumer",
	}
	consumer.expectedCount.Store(100) // Expect 100 events

	// Set small processing delay to simulate work
	consumer.processingTime.Store(10) // 10ms per event

	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Emit many events concurrently
	eventCount := 100
	var emitWg sync.WaitGroup

	for i := 0; i < eventCount; i++ {
		emitWg.Add(1)
		go func(idx int) {
			defer emitWg.Done()
			event := PaymentProcessedEvent{
				PaymentID:   fmt.Sprintf("PAY-%05d", idx),
				Amount:      float64(idx * 10),
				Status:      "completed",
				ProcessedAt: time.Now(),
			}
			if err := emitter.emitPaymentProcessed(event); err != nil {
				t.Errorf("Failed to emit event %d: %v", idx, err)
			}
		}(i)
	}

	// Wait for all emissions
	emitWg.Wait()

	// Wait for processing to complete
	if !consumer.waitForProcessing(10 * time.Second) {
		t.Fatal("Timeout waiting for event processing")
	}

	// Verify all events were received
	receivedCount := consumer.getEventCount()
	if receivedCount != int64(eventCount) {
		t.Errorf("Expected %d events, received %d", eventCount, receivedCount)
	}

	// Verify all unique IDs were processed
	uniqueCount := consumer.getUniqueIDCount()
	if uniqueCount != eventCount {
		t.Errorf("Expected %d unique IDs, got %d", eventCount, uniqueCount)
	}
}

// TestIntegration_HighVolumeBurstEvents tests burst traffic handling
func TestIntegration_HighVolumeBurstEvents(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(14227),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	emitter := &paymentEmitterModule{name: "payment"}
	consumer := &concurrentConsumerModule{
		name: "burst-consumer",
	}
	consumer.expectedCount.Store(500) // Expect 500 events

	// No processing delay for maximum throughput
	consumer.processingTime.Store(0)

	if err := app.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Burst emit (no delays between events)
	eventCount := 500
	start := time.Now()

	for i := 0; i < eventCount; i++ {
		event := PaymentProcessedEvent{
			PaymentID:   fmt.Sprintf("BURST-%05d", i),
			Amount:      float64(i),
			Status:      "completed",
			ProcessedAt: time.Now(),
		}
		if err := emitter.emitPaymentProcessed(event); err != nil {
			t.Fatalf("Failed to emit burst event %d: %v", i, err)
		}
	}

	emitDuration := time.Since(start)
	t.Logf("Emitted %d events in %v", eventCount, emitDuration)

	// Wait for all to be processed
	if !consumer.waitForProcessing(30 * time.Second) {
		t.Fatalf("Timeout waiting for burst processing (received %d of %d)",
			consumer.getEventCount(), eventCount)
	}

	// Verify all received
	receivedCount := consumer.getEventCount()
	if receivedCount != int64(eventCount) {
		t.Errorf("Lost events: expected %d, got %d", eventCount, receivedCount)
	}

	// Verify all unique IDs
	uniqueCount := consumer.getUniqueIDCount()
	if uniqueCount != eventCount {
		t.Errorf("Expected %d unique IDs, got %d", eventCount, uniqueCount)
	}
}
