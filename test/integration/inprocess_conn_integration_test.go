//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

// =============================================================================
// Event Definitions
// =============================================================================

// InProcessTestEvent is the event payload for testing event emitter/consumer
type InProcessTestEvent struct {
	ID        string    `json:"id"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// inProcessTestEventDef is the typed event definition for InProcessTestEvent
var inProcessTestEventDef = helper.EventDefinition[InProcessTestEvent](
	"provider",           // ModuleName
	"InProcessTestEvent", // Name
	"v1",                 // Version
)

// =============================================================================
// Provider Module - Provides services and emits events
// =============================================================================

type inProcessProviderModule struct {
	name     string
	eventBus mono.EventBus

	// Channel service channels
	inChan  chan *mono.Msg
	outChan chan *mono.Msg

	// QueueGroup message counter
	queueGroupMsgCount atomic.Int32

	// Stream consumer message storage
	streamMessages   [][]byte
	streamMessagesMu sync.Mutex
	streamDoneCh     chan struct{}

	// Goroutine lifecycle management
	workerDone sync.WaitGroup

	mu sync.Mutex
}

func newInProcessProviderModule() *inProcessProviderModule {
	return &inProcessProviderModule{
		name:         "provider",
		inChan:       make(chan *mono.Msg, 10),
		outChan:      make(chan *mono.Msg, 10),
		streamDoneCh: make(chan struct{}),
	}
}

func (m *inProcessProviderModule) Name() string { return m.name }

func (m *inProcessProviderModule) Start(ctx context.Context) error {
	// Start channel processor goroutine with proper lifecycle management
	m.workerDone.Add(1)
	go func() {
		defer m.workerDone.Done()
		m.processChannelMessages(ctx)
	}()
	return nil
}

func (m *inProcessProviderModule) Stop(_ context.Context) error {
	close(m.inChan)
	m.workerDone.Wait() // Ensure goroutine exits before returning
	return nil
}

func (m *inProcessProviderModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

func (m *inProcessProviderModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		inProcessTestEventDef.ToBase(),
	}
}

func (m *inProcessProviderModule) RegisterServices(container mono.ServiceContainer) error {
	// Register Channel service
	if err := container.RegisterChannelService("data-channel", m.inChan, m.outChan); err != nil {
		return fmt.Errorf("failed to register channel service: %w", err)
	}

	// Register RequestReply service
	if err := container.RegisterRequestReplyService("process-data", m.handleRequestReply); err != nil {
		return fmt.Errorf("failed to register request-reply service: %w", err)
	}

	// Register QueueGroup service
	if err := container.RegisterQueueGroupService("process-task", mono.QGHP{
		QueueGroup: "workers",
		Handler:    m.handleQueueGroup,
	}); err != nil {
		return fmt.Errorf("failed to register queue-group service: %w", err)
	}

	return nil
}

func (m *inProcessProviderModule) processChannelMessages(ctx context.Context) {
	for {
		select {
		case msg, ok := <-m.inChan:
			if !ok {
				return
			}
			// Echo back with prefix
			response := &mono.Msg{
				Data: []byte("echo: " + string(msg.Data)),
			}
			select {
			case m.outChan <- response:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *inProcessProviderModule) handleRequestReply(_ context.Context, req *mono.Msg) ([]byte, error) {
	return []byte("processed: " + string(req.Data)), nil
}

func (m *inProcessProviderModule) handleQueueGroup(_ context.Context, _ *mono.Msg) error {
	m.queueGroupMsgCount.Add(1)
	return nil
}

func (m *inProcessProviderModule) getQueueGroupMsgCount() int32 {
	return m.queueGroupMsgCount.Load()
}

func (m *inProcessProviderModule) emitTestEvent(event InProcessTestEvent) error {
	return inProcessTestEventDef.Publish(m.eventBus, event, nil)
}

// =============================================================================
// Consumer Module - Depends on provider, consumes services and events
// =============================================================================

type inProcessConsumerModule struct {
	name          string
	depContainers map[string]mono.ServiceContainer

	// Event storage
	receivedEvents   []InProcessTestEvent
	receivedEventsMu sync.Mutex

	mu sync.Mutex
}

func newInProcessConsumerModule() *inProcessConsumerModule {
	return &inProcessConsumerModule{
		name:          "consumer",
		depContainers: make(map[string]mono.ServiceContainer),
	}
}

func (m *inProcessConsumerModule) Name() string { return m.name }

func (m *inProcessConsumerModule) Start(_ context.Context) error { return nil }

func (m *inProcessConsumerModule) Stop(_ context.Context) error { return nil }

func (m *inProcessConsumerModule) Dependencies() []string {
	return []string{"provider"}
}

func (m *inProcessConsumerModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.depContainers[dependency] = container
}

func (m *inProcessConsumerModule) getDepContainer(name string) mono.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depContainers[name]
}

func (m *inProcessConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Discover the InProcessTestEvent
	eventDef, ok := registry.GetEventByName("InProcessTestEvent", "v1", "provider")
	if !ok {
		return fmt.Errorf("failed to discover InProcessTestEvent")
	}

	// Register as consumer
	return registry.RegisterEventConsumer(eventDef, m.handleTestEvent, m)
}

func (m *inProcessConsumerModule) handleTestEvent(_ context.Context, msg *mono.Msg) error {
	var event InProcessTestEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return err
	}

	m.receivedEventsMu.Lock()
	m.receivedEvents = append(m.receivedEvents, event)
	m.receivedEventsMu.Unlock()

	return nil
}

func (m *inProcessConsumerModule) getReceivedEvents() []InProcessTestEvent {
	m.receivedEventsMu.Lock()
	defer m.receivedEventsMu.Unlock()
	result := make([]InProcessTestEvent, len(m.receivedEvents))
	copy(result, m.receivedEvents)
	return result
}

// =============================================================================
// Observer Module - Independent event consumer (tests multiple consumers)
// =============================================================================

type inProcessObserverModule struct {
	name          string
	receivedCount atomic.Int32
}

func newInProcessObserverModule() *inProcessObserverModule {
	return &inProcessObserverModule{
		name: "observer",
	}
}

func (m *inProcessObserverModule) Name() string { return m.name }

func (m *inProcessObserverModule) Start(_ context.Context) error { return nil }

func (m *inProcessObserverModule) Stop(_ context.Context) error { return nil }

func (m *inProcessObserverModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	// Discover the InProcessTestEvent
	eventDef, ok := registry.GetEventByName("InProcessTestEvent", "v1", "provider")
	if !ok {
		return fmt.Errorf("failed to discover InProcessTestEvent")
	}

	// Register as consumer
	return registry.RegisterEventConsumer(eventDef, m.handleTestEvent, m)
}

func (m *inProcessObserverModule) handleTestEvent(_ context.Context, _ *mono.Msg) error {
	m.receivedCount.Add(1)
	return nil
}

func (m *inProcessObserverModule) getReceivedCount() int32 {
	return m.receivedCount.Load()
}

// =============================================================================
// Stream Provider Module - Provides StreamConsumer service
// =============================================================================

type inProcessStreamProviderModule struct {
	name      string
	container mono.ServiceContainer

	// Stream consumer message storage
	streamMessages   [][]byte
	streamMessagesMu sync.Mutex
	streamDoneCh     chan struct{}
	expectedMsgCount int
}

func newInProcessStreamProviderModule(expectedMsgCount int) *inProcessStreamProviderModule {
	return &inProcessStreamProviderModule{
		name:             "stream-provider",
		streamDoneCh:     make(chan struct{}),
		expectedMsgCount: expectedMsgCount,
	}
}

func (m *inProcessStreamProviderModule) Name() string { return m.name }

func (m *inProcessStreamProviderModule) Start(_ context.Context) error { return nil }

func (m *inProcessStreamProviderModule) Stop(_ context.Context) error { return nil }

func (m *inProcessStreamProviderModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container // Store container for later use
	config := mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name:     "test-inprocess-stream",
			Subjects: []string{"test.inprocess.>"},
		},
		Fetch: mono.FetchConfig{
			BatchSize: 5,
			Timeout:   2 * time.Second,
		},
	}

	handler := func(_ context.Context, msgs []*mono.Msg) error {
		m.streamMessagesMu.Lock()
		defer m.streamMessagesMu.Unlock()

		for _, msg := range msgs {
			m.streamMessages = append(m.streamMessages, msg.Data)
			if err := msg.Ack(); err != nil {
				return err
			}
		}

		if len(m.streamMessages) >= m.expectedMsgCount {
			select {
			case <-m.streamDoneCh:
				// Already closed
			default:
				close(m.streamDoneCh)
			}
		}
		return nil
	}

	return container.RegisterStreamConsumerService("stream-processor", config, handler)
}

func (m *inProcessStreamProviderModule) getStreamMessages() [][]byte {
	m.streamMessagesMu.Lock()
	defer m.streamMessagesMu.Unlock()
	result := make([][]byte, len(m.streamMessages))
	copy(result, m.streamMessages)
	return result
}

func (m *inProcessStreamProviderModule) waitForMessages(timeout time.Duration) bool {
	select {
	case <-m.streamDoneCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// =============================================================================
// Test Functions
// =============================================================================

// TestInProcessConn_ChannelService tests Channel service via in-process connection
func TestInProcessConn_ChannelService(t *testing.T) {
	// Create framework with DontListen + InProcessConn
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create provider module
	provider := newInProcessProviderModule()

	// Create consumer module
	consumer := newInProcessConsumerModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
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

	// Get channel service from provider's container
	depContainer := consumer.getDepContainer("provider")
	if depContainer == nil {
		t.Fatal("Failed to get provider's container")
	}

	inChan, outChan, err := depContainer.GetChannelService("data-channel", "consumer-module")
	if err != nil {
		t.Fatalf("Failed to get channel service: %v", err)
	}

	// Send message
	testMsg := &mono.Msg{Data: []byte("hello")}
	select {
	case inChan <- testMsg:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout sending to channel")
	}

	// Receive response
	select {
	case response := <-outChan:
		expected := "echo: hello"
		if string(response.Data) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(response.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for response")
	}
}

// TestInProcessConn_RequestReplyService tests RequestReply service via in-process connection
func TestInProcessConn_RequestReplyService(t *testing.T) {
	// Create framework with DontListen + InProcessConn
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create provider module
	provider := newInProcessProviderModule()

	// Create consumer module
	consumer := newInProcessConsumerModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
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

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get request-reply service from provider's container
	depContainer := consumer.getDepContainer("provider")
	if depContainer == nil {
		t.Fatal("Failed to get provider's container")
	}

	client, err := depContainer.GetRequestReplyService("process-data")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	response, err := client.Call(reqCtx, []byte("test-data"))
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	expected := "processed: test-data"
	if string(response.Data) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(response.Data))
	}
}

// TestInProcessConn_QueueGroupService tests QueueGroup service via in-process connection
func TestInProcessConn_QueueGroupService(t *testing.T) {
	// Create framework with DontListen + InProcessConn
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create provider module
	provider := newInProcessProviderModule()

	// Create consumer module
	consumer := newInProcessConsumerModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
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

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get queue-group service from provider's container
	depContainer := consumer.getDepContainer("provider")
	if depContainer == nil {
		t.Fatal("Failed to get provider's container")
	}

	client, err := depContainer.GetQueueGroupService("process-task")
	if err != nil {
		t.Fatalf("Failed to get queue-group service: %v", err)
	}

	// Send multiple messages using shared context for the loop
	messageCount := 10
	sendCtx, sendCancel := context.WithTimeout(ctx, 30*time.Second)
	defer sendCancel()
	for i := 0; i < messageCount; i++ {
		if err := client.Send(sendCtx, []byte(fmt.Sprintf("task-%d", i))); err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify all messages received
	receivedCount := provider.getQueueGroupMsgCount()
	if receivedCount != int32(messageCount) {
		t.Errorf("Expected %d messages, got %d", messageCount, receivedCount)
	}
}

// TestInProcessConn_StreamConsumerService tests StreamConsumer service via in-process connection
func TestInProcessConn_StreamConsumerService(t *testing.T) {
	// Create framework with DontListen + InProcessConn + JetStream
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamStorageDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	messageCount := 5

	// Create stream provider module
	streamProvider := newInProcessStreamProviderModule(messageCount)

	// Register module
	if err := app.Register(streamProvider); err != nil {
		t.Fatalf("Failed to register stream provider: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(200 * time.Millisecond)

	// Get stream consumer service
	client, err := streamProvider.container.GetStreamConsumerService("stream-processor")
	if err != nil {
		t.Fatalf("Failed to get stream-consumer service: %v", err)
	}

	// Publish messages to stream
	for i := 0; i < messageCount; i++ {
		pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.Publish(pubCtx, []byte(fmt.Sprintf("stream-msg-%d", i)))
		pubCancel()
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for messages to be consumed
	if !streamProvider.waitForMessages(10 * time.Second) {
		t.Fatalf("Timeout waiting for stream messages")
	}

	// Verify all messages received
	messages := streamProvider.getStreamMessages()
	if len(messages) != messageCount {
		t.Errorf("Expected %d messages, got %d", messageCount, len(messages))
	}

	// Verify message content
	for i, msg := range messages {
		expected := fmt.Sprintf("stream-msg-%d", i)
		if string(msg) != expected {
			t.Errorf("Message %d: expected '%s', got '%s'", i, expected, string(msg))
		}
	}
}

// TestInProcessConn_EventEmitterConsumer tests EventEmitter/EventConsumer via in-process connection
func TestInProcessConn_EventEmitterConsumer(t *testing.T) {
	// Create framework with DontListen + InProcessConn
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create modules
	provider := newInProcessProviderModule()
	consumer := newInProcessConsumerModule()
	observer := newInProcessObserverModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}
	if err := app.Register(observer); err != nil {
		t.Fatalf("Failed to register observer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Emit events
	eventCount := 3
	for i := 0; i < eventCount; i++ {
		event := InProcessTestEvent{
			ID:        fmt.Sprintf("evt-%d", i),
			Message:   fmt.Sprintf("test message %d", i),
			Timestamp: time.Now(),
		}
		if err := provider.emitTestEvent(event); err != nil {
			t.Fatalf("Failed to emit event %d: %v", i, err)
		}
	}

	// Wait for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify consumer received all events
	consumerEvents := consumer.getReceivedEvents()
	if len(consumerEvents) != eventCount {
		t.Errorf("Consumer expected %d events, got %d", eventCount, len(consumerEvents))
	}

	// Verify observer received all events
	observerCount := observer.getReceivedCount()
	if observerCount != int32(eventCount) {
		t.Errorf("Observer expected %d events, got %d", eventCount, observerCount)
	}

	// Verify event content
	for i, evt := range consumerEvents {
		expectedID := fmt.Sprintf("evt-%d", i)
		if evt.ID != expectedID {
			t.Errorf("Event %d: expected ID '%s', got '%s'", i, expectedID, evt.ID)
		}
	}
}

// TestInProcessConn_AllServiceTypes tests all service types combined via in-process connection
func TestInProcessConn_AllServiceTypes(t *testing.T) {
	// Create framework with DontListen + InProcessConn
	app, err := mono.NewMonoApplication(
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create modules
	provider := newInProcessProviderModule()
	consumer := newInProcessConsumerModule()
	observer := newInProcessObserverModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}
	if err := app.Register(observer); err != nil {
		t.Fatalf("Failed to register observer: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get provider's container
	depContainer := consumer.getDepContainer("provider")
	if depContainer == nil {
		t.Fatal("Failed to get provider's container")
	}

	// Test 1: Channel Service
	t.Run("ChannelService", func(t *testing.T) {
		inChan, outChan, err := depContainer.GetChannelService("data-channel", "consumer-module")
		if err != nil {
			t.Fatalf("Failed to get channel service: %v", err)
		}

		inChan <- &mono.Msg{Data: []byte("channel-test")}
		select {
		case response := <-outChan:
			if string(response.Data) != "echo: channel-test" {
				t.Errorf("Unexpected response: %s", string(response.Data))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for channel response")
		}
	})

	// Test 2: RequestReply Service
	t.Run("RequestReplyService", func(t *testing.T) {
		client, err := depContainer.GetRequestReplyService("process-data")
		if err != nil {
			t.Fatalf("Failed to get request-reply service: %v", err)
		}

		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		response, err := client.Call(reqCtx, []byte("rr-test"))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		if string(response.Data) != "processed: rr-test" {
			t.Errorf("Unexpected response: %s", string(response.Data))
		}
	})

	// Test 3: QueueGroup Service
	t.Run("QueueGroupService", func(t *testing.T) {
		client, err := depContainer.GetQueueGroupService("process-task")
		if err != nil {
			t.Fatalf("Failed to get queue-group service: %v", err)
		}

		initialCount := provider.getQueueGroupMsgCount()

		sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
		defer sendCancel()
		if err := client.Send(sendCtx, []byte("qg-test")); err != nil {
			t.Fatalf("Failed to send: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		newCount := provider.getQueueGroupMsgCount()
		if newCount != initialCount+1 {
			t.Errorf("Expected count %d, got %d", initialCount+1, newCount)
		}
	})

	// Test 4: EventEmitter/EventConsumer
	t.Run("EventEmitterConsumer", func(t *testing.T) {
		initialConsumerCount := len(consumer.getReceivedEvents())
		initialObserverCount := observer.getReceivedCount()

		event := InProcessTestEvent{
			ID:        "combined-test",
			Message:   "testing all services",
			Timestamp: time.Now(),
		}
		if err := provider.emitTestEvent(event); err != nil {
			t.Fatalf("Failed to emit event: %v", err)
		}

		time.Sleep(300 * time.Millisecond)

		newConsumerCount := len(consumer.getReceivedEvents())
		newObserverCount := observer.getReceivedCount()

		if newConsumerCount != initialConsumerCount+1 {
			t.Errorf("Consumer expected %d events, got %d", initialConsumerCount+1, newConsumerCount)
		}
		if newObserverCount != initialObserverCount+1 {
			t.Errorf("Observer expected %d events, got %d", initialObserverCount+1, newObserverCount)
		}
	})
}

// TestInProcessConnOnly_WithTCPEnabled tests InProcessConn only (without DontListen)
// This verifies that in-process connection works even when TCP listener is still enabled
func TestInProcessConnOnly_WithTCPEnabled(t *testing.T) {
	// Create framework with ONLY InProcessConn (TCP still enabled)
	app, err := mono.NewMonoApplication(
		mono.WithNATSInProcessConn(), // Only InProcessConn, no DontListen
	)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer app.Stop(context.Background())

	// Create modules
	provider := newInProcessProviderModule()
	consumer := newInProcessConsumerModule()

	// Register modules
	if err := app.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
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

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get provider's container
	depContainer := consumer.getDepContainer("provider")
	if depContainer == nil {
		t.Fatal("Failed to get provider's container")
	}

	// Test RequestReply service works via in-process connection
	t.Run("RequestReplyService", func(t *testing.T) {
		client, err := depContainer.GetRequestReplyService("process-data")
		if err != nil {
			t.Fatalf("Failed to get request-reply service: %v", err)
		}

		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		defer reqCancel()

		response, err := client.Call(reqCtx, []byte("tcp-enabled-test"))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		expected := "processed: tcp-enabled-test"
		if string(response.Data) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(response.Data))
		}
	})

	// Test QueueGroup service works via in-process connection
	t.Run("QueueGroupService", func(t *testing.T) {
		client, err := depContainer.GetQueueGroupService("process-task")
		if err != nil {
			t.Fatalf("Failed to get queue-group service: %v", err)
		}

		initialCount := provider.getQueueGroupMsgCount()

		sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
		defer sendCancel()
		if err := client.Send(sendCtx, []byte("tcp-enabled-qg-test")); err != nil {
			t.Fatalf("Failed to send: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		newCount := provider.getQueueGroupMsgCount()
		if newCount != initialCount+1 {
			t.Errorf("Expected count %d, got %d", initialCount+1, newCount)
		}
	})
}
