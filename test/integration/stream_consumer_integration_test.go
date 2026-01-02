//go:build integration
// +build integration

package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
)

// streamConsumerModule is a test module that registers a stream consumer service
type streamConsumerModule struct {
	name                 string
	eventBus             mono.EventBus
	config               mono.StreamConsumerConfig
	handler              mono.StreamConsumerHandler
	receivedMessages     [][]byte
	receivedSubjects     []string
	mu                   sync.Mutex
	messagesReceived     int64
	expectedMessageCount int
	doneCh               chan struct{}
}

func newStreamConsumerModule(name string, config mono.StreamConsumerConfig, expectedCount int) *streamConsumerModule {
	m := &streamConsumerModule{
		name:                 name,
		config:               config,
		expectedMessageCount: expectedCount,
		doneCh:               make(chan struct{}),
	}

	// Set the handler that collects received messages
	m.handler = func(ctx context.Context, msgs []*mono.Msg) error {
		m.mu.Lock()
		defer m.mu.Unlock()

		for _, msg := range msgs {
			m.receivedMessages = append(m.receivedMessages, msg.Data)
			m.receivedSubjects = append(m.receivedSubjects, msg.Subject)
			msg.Ack()
		}

		count := atomic.AddInt64(&m.messagesReceived, int64(len(msgs)))
		if int(count) >= m.expectedMessageCount {
			select {
			case <-m.doneCh:
			default:
				close(m.doneCh)
			}
		}
		return nil
	}

	return m
}

func (m *streamConsumerModule) Name() string                       { return m.name }
func (m *streamConsumerModule) Dependencies() []string             { return nil }
func (m *streamConsumerModule) Start(_ context.Context) error      { return nil }
func (m *streamConsumerModule) Stop(_ context.Context) error       { return nil }
func (m *streamConsumerModule) SetEventBus(eventBus mono.EventBus) { m.eventBus = eventBus }

func (m *streamConsumerModule) RegisterServices(container mono.ServiceContainer) error {
	return container.RegisterStreamConsumerService("stream-handler", m.config, m.handler)
}

func (m *streamConsumerModule) getReceivedMessages() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]byte, len(m.receivedMessages))
	copy(result, m.receivedMessages)
	return result
}

func (m *streamConsumerModule) getReceivedSubjects() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.receivedSubjects))
	copy(result, m.receivedSubjects)
	return result
}

func (m *streamConsumerModule) waitForMessages(timeout time.Duration) bool {
	select {
	case <-m.doneCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestStreamConsumerIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("basic stream consumer receives messages", func(t *testing.T) {
		// Create test module with stream consumer config
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     "TEST_STREAM",
				Subjects: []string{"test.>"},
			},
			Consumer: mono.ConsumerConfig{
				AckWait:    30 * time.Second,
				MaxDeliver: 3,
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		expectedMessages := 10
		module := newStreamConsumerModule("test-consumer", config, expectedMessages)

		// Create framework with embedded NATS server with JetStream enabled
		// Use in-process connection to avoid port conflicts between tests
		fw, err := mono.NewMonoApplication(
			mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithJetStreamStorageDir(t.TempDir()), // Enable JetStream with temp storage
			mono.WithNATSDontListen(),                 // Disable TCP listening
			mono.WithNATSInProcessConn(),              // Use in-process connections
		)
		if err != nil {
			t.Fatalf("Failed to create framework: %v", err)
		}

		// Register module
		if err := fw.Register(module); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to register module: %v", err)
		}

		// Start framework
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := fw.Start(ctx); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to start framework: %v", err)
		}
		defer fw.Stop(context.Background())

		// Get event bus from module to publish messages
		if module.eventBus == nil {
			t.Fatal("Module event bus not set")
		}

		// Get EventStream context
		js, err := module.eventBus.EventStream()
		if err != nil {
			t.Fatalf("Failed to get EventStream context: %v", err)
		}

		// Wait a bit for stream consumer to be set up
		time.Sleep(500 * time.Millisecond)

		// Publish messages
		for i := 0; i < expectedMessages; i++ {
			data := []byte("message-" + string(rune('A'+i)))
			_, err := js.Publish(ctx, "test.events", data)
			if err != nil {
				t.Fatalf("Failed to publish message: %v", err)
			}
		}

		// Wait for messages to be received
		if !module.waitForMessages(10 * time.Second) {
			received := atomic.LoadInt64(&module.messagesReceived)
			t.Fatalf("Timeout waiting for messages. Expected %d, received %d", expectedMessages, received)
		}

		// Verify messages were received
		receivedMsgs := module.getReceivedMessages()
		if len(receivedMsgs) != expectedMessages {
			t.Errorf("Expected %d messages, got %d", expectedMessages, len(receivedMsgs))
		}

		// Verify subjects
		receivedSubjects := module.getReceivedSubjects()
		for _, subject := range receivedSubjects {
			if subject != "test.events" {
				t.Errorf("Unexpected subject: %s", subject)
			}
		}
	})

	t.Run("stream consumer with filter subject", func(t *testing.T) {
		// Create test module with filter subject
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     "FILTERED_STREAM",
				Subjects: []string{"orders.>"},
			},
			Consumer: mono.ConsumerConfig{
				FilterSubject: "orders.new", // Only receive orders.new messages
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		// Only expect 5 messages (orders.new), not orders.processed
		expectedMessages := 5
		module := newStreamConsumerModule("filtered-consumer", config, expectedMessages)

		// Create framework with embedded NATS server with JetStream enabled
		// Use in-process connection to avoid port conflicts between tests
		fw, err := mono.NewMonoApplication(
			mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithJetStreamStorageDir(t.TempDir()), // Enable JetStream with temp storage
			mono.WithNATSDontListen(),                 // Disable TCP listening
			mono.WithNATSInProcessConn(),              // Use in-process connections
		)
		if err != nil {
			t.Fatalf("Failed to create framework: %v", err)
		}

		// Register module
		if err := fw.Register(module); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to register module: %v", err)
		}

		// Start framework
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := fw.Start(ctx); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to start framework: %v", err)
		}
		defer fw.Stop(context.Background())

		// Get event bus from module to publish messages
		if module.eventBus == nil {
			t.Fatal("Module event bus not set")
		}

		// Get EventStream
		js, err := module.eventBus.EventStream()
		if err != nil {
			t.Fatalf("Failed to get EventStream: %v", err)
		}

		// Wait a bit for stream consumer to be set up
		time.Sleep(500 * time.Millisecond)

		// Publish messages to different subjects
		for i := 0; i < 5; i++ {
			_, _ = js.Publish(ctx, "orders.new", []byte("new-order"))
			_, _ = js.Publish(ctx, "orders.processed", []byte("processed-order"))
		}

		// Wait for expected messages
		if !module.waitForMessages(10 * time.Second) {
			received := atomic.LoadInt64(&module.messagesReceived)
			t.Fatalf("Timeout waiting for messages. Expected %d, received %d", expectedMessages, received)
		}

		// Give a bit of extra time to ensure no extra messages arrive
		time.Sleep(500 * time.Millisecond)

		// Verify only filtered messages were received
		receivedMsgs := module.getReceivedMessages()
		if len(receivedMsgs) != expectedMessages {
			t.Errorf("Expected %d messages (only orders.new), got %d", expectedMessages, len(receivedMsgs))
		}

		// Verify all received subjects are orders.new
		receivedSubjects := module.getReceivedSubjects()
		for _, subject := range receivedSubjects {
			if subject != "orders.new" {
				t.Errorf("Expected only 'orders.new' subjects, got: %s", subject)
			}
		}
	})

	t.Run("stream consumer receives messages published before consumer starts", func(t *testing.T) {
		// Create test module
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     "DURABLE_STREAM",
				Subjects: []string{"durable.>"},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		expectedMessages := 5
		module := newStreamConsumerModule("durable-consumer", config, expectedMessages)

		// Create framework with JetStream
		// Use in-process connection to avoid port conflicts between tests
		fw, err := mono.NewMonoApplication(
			mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithJetStreamStorageDir(t.TempDir()), // Enable JetStream with temp storage
			mono.WithNATSDontListen(),                 // Disable TCP listening
			mono.WithNATSInProcessConn(),              // Use in-process connections
		)
		if err != nil {
			t.Fatalf("Failed to create framework: %v", err)
		}

		// Register the module but don't start yet
		if err := fw.Register(module); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to register module: %v", err)
		}

		// Start framework (this creates the stream but consumer loop hasn't started fetching)
		if err := fw.Start(ctx); err != nil {
			fw.Stop(context.Background())
			t.Fatalf("Failed to start framework: %v", err)
		}
		defer fw.Stop(context.Background())

		// Get EventStream and publish messages
		js, err := module.eventBus.EventStream()
		if err != nil {
			t.Fatalf("Failed to get EventStream: %v", err)
		}

		// Publish messages - these will be stored in JetStream
		for i := 0; i < expectedMessages; i++ {
			_, err := js.Publish(ctx, "durable.events", []byte("pre-message"))
			if err != nil {
				t.Fatalf("Failed to publish pre-message: %v", err)
			}
		}

		// Wait for pre-published messages to be received by consumer
		if !module.waitForMessages(10 * time.Second) {
			received := atomic.LoadInt64(&module.messagesReceived)
			t.Fatalf("Timeout waiting for pre-published messages. Expected %d, received %d", expectedMessages, received)
		}

		// Verify messages were received
		receivedMsgs := module.getReceivedMessages()
		if len(receivedMsgs) != expectedMessages {
			t.Errorf("Expected %d pre-published messages, got %d", expectedMessages, len(receivedMsgs))
		}
	})
}
