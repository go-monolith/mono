// Package bench provides performance benchmarks for mono-framework service types.
//
// These benchmarks measure the throughput and latency of different service patterns
// using TCP socket connections (not in-process). This is useful for comparing
// the overhead of network I/O versus in-process communication.
//
// Run benchmarks with: go test -bench='BenchmarkSocket' -benchmem ./bench/
package bench

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	mono "github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// newSocketApp creates a MonoApplication using TCP socket connections.
func newSocketApp() (mono.MonoApplication, error) {
	return NewBenchAppWithOptions(BenchAppOptions{InProcess: false})
}

// newSocketAppWithJetStream creates a MonoApplication with JetStream using TCP socket connections.
func newSocketAppWithJetStream(storageDir string) (mono.MonoApplication, error) {
	return NewBenchAppWithOptions(BenchAppOptions{InProcess: false, JetStreamDir: storageDir})
}

// BenchmarkSocketChannelService benchmarks the Channel service round-trip performance
// using TCP socket connections.
func BenchmarkSocketChannelService(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketChannelServiceWithPayload(b, size)
		})
	}
}

func benchmarkSocketChannelServiceWithPayload(b *testing.B, payloadSize int) {
	// Setup: Create channels and modules
	inChan := make(chan *mono.Msg, 1)
	outChan := make(chan *mono.Msg, 1)
	ctx := context.Background()

	provider := NewBenchProviderModule("provider")
	provider.SetupFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("bench-channel", inChan, outChan)
	}

	consumer := NewBenchConsumerModule("consumer", []string{"provider"})

	app, err := newSocketApp()
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(provider); err != nil {
		b.Fatalf("Failed to register provider: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Get channel service
	depContainer := consumer.DepContainer()
	if depContainer == nil {
		b.Fatal("Failed to get dependency container")
	}
	benchInChan, benchOutChan, err := depContainer.GetChannelService("bench-channel", "bench-consumer")
	if err != nil {
		b.Fatalf("Failed to get channel service: %v", err)
	}

	// Start a goroutine to echo messages back
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range inChan {
			select {
			case outChan <- &mono.Msg{Data: msg.Data}:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		close(inChan)
		<-done
	}()

	payload := GeneratePayload(payloadSize)

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		benchInChan <- &mono.Msg{Data: payload}
		result = <-benchOutChan
	}
}

// BenchmarkSocketRequestReplyService benchmarks the RequestReply service performance
// using TCP socket connections.
func BenchmarkSocketRequestReplyService(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketRequestReplyServiceWithPayload(b, size)
		})
	}
}

func benchmarkSocketRequestReplyServiceWithPayload(b *testing.B, payloadSize int) {
	ctx := context.Background()

	provider := NewBenchProviderModule("provider")
	provider.SetupFunc = func(container mono.ServiceContainer) error {
		handler := func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return req.Data, nil // Echo back the data
		}
		return container.RegisterRequestReplyService("bench-rr", handler)
	}

	consumer := NewBenchConsumerModule("consumer", []string{"provider"})

	app, err := newSocketApp()
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(provider); err != nil {
		b.Fatalf("Failed to register provider: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get request-reply service
	depContainer := consumer.DepContainer()
	if depContainer == nil {
		b.Fatal("Failed to get dependency container")
	}
	client, err := depContainer.GetRequestReplyService("bench-rr")
	if err != nil {
		b.Fatalf("Failed to get request-reply service: %v", err)
	}

	payload := GeneratePayload(payloadSize)

	// Create a single context with longer timeout to avoid allocation overhead in loop
	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		resp, err := client.Call(benchCtx, payload)
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		result = resp
	}
}

// BenchmarkSocketQueueGroupService benchmarks the QueueGroup service performance
// using TCP socket connections.
func BenchmarkSocketQueueGroupService(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketQueueGroupServiceWithPayload(b, size)
		})
	}
}

func benchmarkSocketQueueGroupServiceWithPayload(b *testing.B, payloadSize int) {
	ctx := context.Background()
	var msgCount atomic.Int64

	provider := NewBenchProviderModule("provider")
	provider.SetupFunc = func(container mono.ServiceContainer) error {
		handler := func(_ context.Context, _ *mono.Msg) error {
			msgCount.Add(1)
			return nil
		}
		return container.RegisterQueueGroupService("bench-qg", mono.QGHP{
			QueueGroup: "bench-workers",
			Handler:    handler,
		})
	}

	consumer := NewBenchConsumerModule("consumer", []string{"provider"})

	app, err := newSocketApp()
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(provider); err != nil {
		b.Fatalf("Failed to register provider: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	// Get queue-group service
	depContainer := consumer.DepContainer()
	if depContainer == nil {
		b.Fatal("Failed to get dependency container")
	}
	client, err := depContainer.GetQueueGroupService("bench-qg")
	if err != nil {
		b.Fatalf("Failed to get queue-group service: %v", err)
	}

	payload := GeneratePayload(payloadSize)

	// Create a single context with longer timeout to avoid allocation overhead in loop
	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		err := client.Send(benchCtx, payload)
		if err != nil {
			b.Fatalf("Send failed: %v", err)
		}
	}

	b.StopTimer()

	// Wait for all messages to be processed
	deadline := time.Now().Add(10 * time.Second)
	for msgCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// BenchmarkSocketStreamConsumerService benchmarks the StreamConsumer service performance
// using TCP socket connections.
func BenchmarkSocketStreamConsumerService(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketStreamConsumerServiceWithPayload(b, size)
		})
	}
}

func benchmarkSocketStreamConsumerServiceWithPayload(b *testing.B, payloadSize int) {
	ctx := context.Background()
	var msgCount atomic.Int64

	provider := NewBenchProviderModule("provider")
	provider.SetupFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     "bench-stream",
				Subjects: []string{"bench.stream.>"},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 10,
				Timeout:   2 * time.Second,
			},
		}
		handler := func(_ context.Context, msgs []*mono.Msg) error {
			for _, msg := range msgs {
				msgCount.Add(1)
				if err := msg.Ack(); err != nil {
					return err
				}
			}
			return nil
		}
		return container.RegisterStreamConsumerService("bench-stream-consumer", config, handler)
	}

	// Use TempDir for JetStream storage
	storageDir := b.TempDir()
	app, err := newSocketAppWithJetStream(storageDir)
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(provider); err != nil {
		b.Fatalf("Failed to register provider: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Wait for stream and consumer to be ready
	time.Sleep(200 * time.Millisecond)

	// Get stream consumer service
	container := provider.Container()
	if container == nil {
		b.Fatal("Failed to get provider container")
	}
	client, err := container.GetStreamConsumerService("bench-stream-consumer")
	if err != nil {
		b.Fatalf("Failed to get stream-consumer service: %v", err)
	}

	payload := GeneratePayload(payloadSize)

	// Create a single context with longer timeout to avoid allocation overhead in loop
	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		_, err := client.Publish(benchCtx, payload)
		if err != nil {
			b.Fatalf("Publish failed: %v", err)
		}
	}

	b.StopTimer()

	// Wait for all messages to be consumed
	deadline := time.Now().Add(30 * time.Second)
	for msgCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
}

// =============================================================================
// Event Consumer Benchmarks
// =============================================================================

// BenchmarkSocketEventConsumer benchmarks the EventConsumer pub/sub performance
// using TCP socket connections.
func BenchmarkSocketEventConsumer(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketEventConsumerWithPayload(b, size)
		})
	}
}

func benchmarkSocketEventConsumerWithPayload(b *testing.B, payloadSize int) {
	ctx := context.Background()
	var msgCount atomic.Int64

	// Create event definition (used only for registration, not marshaling)
	eventDef := types.NewEventDefinition[struct{}]("bench-emitter", "BenchEvent", "v1", "events.bench.v1.test")

	// Create emitter module
	emitter := NewBenchEventEmitterModule("bench-emitter", eventDef.ToBase())

	// Create consumer module with raw handler (no unmarshal)
	handler := func(_ context.Context, msg *mono.Msg) error {
		msgCount.Add(1)
		result = msg.Data // sink to prevent optimization
		return nil
	}
	consumer := NewBenchEventConsumerModule("bench-consumer", eventDef.ToBase(), handler)

	app, err := newSocketApp()
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(emitter); err != nil {
		b.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	payload := GeneratePayload(payloadSize)
	eventBus := emitter.EventBus()

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		if err := eventDef.PublishRaw(eventBus, payload, nil); err != nil {
			b.Fatalf("PublishRaw failed: %v", err)
		}
	}

	b.StopTimer()

	// Wait for all events to be consumed
	deadline := time.Now().Add(10 * time.Second)
	for msgCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := msgCount.Load(); got < int64(b.N) {
		b.Logf("Warning: received %d/%d events (fire-and-forget may lose events)", got, b.N)
	}
}

// BenchmarkSocketEventStreamConsumer benchmarks the EventStreamConsumer performance
// using TCP socket connections.
func BenchmarkSocketEventStreamConsumer(b *testing.B) {
	for _, size := range PayloadSizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			benchmarkSocketEventStreamConsumerWithPayload(b, size)
		})
	}
}

func benchmarkSocketEventStreamConsumerWithPayload(b *testing.B, payloadSize int) {
	ctx := context.Background()
	var msgCount atomic.Int64

	// Create event definition (used only for registration, not marshaling)
	eventDef := types.NewEventDefinition[struct{}]("bench-emitter", "BenchEvent", "v1", "events.bench.v1.test")

	// Create emitter module
	emitter := NewBenchEventEmitterModule("bench-stream-emitter", eventDef.ToBase())

	// Create stream consumer config
	config := mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name: "bench-events-stream",
		},
		Fetch: mono.FetchConfig{
			BatchSize: 10,
			Timeout:   2 * time.Second,
		},
	}

	// Create stream consumer with raw batch handler (no unmarshal)
	handler := func(_ context.Context, msgs []*mono.Msg) error {
		for _, msg := range msgs {
			msgCount.Add(1)
			result = msg.Data // sink to prevent optimization
			if err := msg.Ack(); err != nil {
				return err
			}
		}
		return nil
	}
	consumer := NewBenchEventStreamConsumerModule("bench-stream-consumer", eventDef.ToBase(), config, handler)

	// Use TempDir for JetStream storage
	storageDir := b.TempDir()
	app, err := newSocketAppWithJetStream(storageDir)
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(emitter); err != nil {
		b.Fatalf("Failed to register emitter: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Wait for stream and consumer to be ready
	time.Sleep(200 * time.Millisecond)

	payload := GeneratePayload(payloadSize)
	eventBus := emitter.EventBus()

	// Create a single context with longer timeout to avoid allocation overhead in loop
	benchCtx, benchCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer benchCancel()

	b.ResetTimer()
	b.SetBytes(int64(payloadSize))

	for i := 0; i < b.N; i++ {
		// Use EventStreamPublishRaw for guaranteed JetStream persistence
		if _, err := eventDef.EventStreamPublishRaw(benchCtx, eventBus, payload, nil); err != nil {
			b.Fatalf("EventStreamPublishRaw failed: %v", err)
		}
	}

	b.StopTimer()

	// Wait for all events to be consumed
	deadline := time.Now().Add(30 * time.Second)
	for msgCount.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if got := msgCount.Load(); got < int64(b.N) {
		b.Fatalf("Message loss detected: received %d/%d events", got, b.N)
	}
}
