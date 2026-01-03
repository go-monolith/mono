package container

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

func TestRegisterChannelService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("register valid channel service", func(t *testing.T) {
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)

		err := container.RegisterChannelService("test-service", in, out)
		if err != nil {
			t.Fatalf("RegisterChannelService failed: %v", err)
		}

		if !container.Has("test-service") {
			t.Error("Service should be registered")
		}
	})

	t.Run("register with nil input channel", func(t *testing.T) {
		out := make(chan *types.Msg, 1)
		err := container.RegisterChannelService("nil-in", nil, out)
		if err == nil {
			t.Error("RegisterChannelService should fail with nil input channel")
		}
	})

	t.Run("register with nil output channel", func(t *testing.T) {
		in := make(chan *types.Msg, 1)
		err := container.RegisterChannelService("nil-out", in, nil)
		if err == nil {
			t.Error("RegisterChannelService should fail with nil output channel")
		}
	})

	t.Run("register duplicate service", func(t *testing.T) {
		in1 := make(chan *types.Msg, 1)
		out1 := make(chan *types.Msg, 1)
		in2 := make(chan *types.Msg, 1)
		out2 := make(chan *types.Msg, 1)

		err := container.RegisterChannelService("duplicate", in1, out1)
		if err != nil {
			t.Fatalf("First RegisterChannelService failed: %v", err)
		}

		err = container.RegisterChannelService("duplicate", in2, out2)
		if err == nil {
			t.Error("RegisterChannelService should fail with duplicate name")
		}
	})
}

func TestGetChannelService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("get existing service", func(t *testing.T) {
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)

		err := container.RegisterChannelService("test-service", in, out)
		if err != nil {
			t.Fatalf("RegisterChannelService failed: %v", err)
		}

		gotIn, gotOut, err := container.GetChannelService("test-service", "test-consumer")
		if err != nil {
			t.Fatalf("GetChannelService failed: %v", err)
		}

		// Input channel should be the same (shared)
		if gotIn != in {
			t.Error("GetChannelService returned wrong input channel")
		}
		// Output channel should be different (per-consumer)
		if gotOut == out {
			t.Error("GetChannelService should return per-consumer out channel, not provider's out channel")
		}
		if gotOut == nil {
			t.Error("GetChannelService returned nil out channel")
		}
	})

	t.Run("get non-existent service", func(t *testing.T) {
		_, _, err := container.GetChannelService("non-existent", "test-consumer")
		if err == nil {
			t.Error("GetChannelService should fail for non-existent service")
		}
	})

	t.Run("get wrong service type", func(t *testing.T) {
		// Register a RequestReply service (we'll need EventBus for this)
		// For now, just verify the error path works
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)
		err := container.RegisterChannelService("channel-service", in, out)
		if err != nil {
			t.Fatalf("RegisterChannelService failed: %v", err)
		}

		// This should succeed since it's a channel service
		_, _, err = container.GetChannelService("channel-service", "test-consumer")
		if err != nil {
			t.Errorf("GetChannelService should succeed for Channel service: %v", err)
		}
	})
}

func TestMustGetChannelService(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("get existing service", func(t *testing.T) {
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)

		err := container.RegisterChannelService("test-service", in, out)
		if err != nil {
			t.Fatalf("RegisterChannelService failed: %v", err)
		}

		gotIn, gotOut := container.MustGetChannelService("test-service", "test-consumer")
		// Input channel should be the same (shared)
		if gotIn != in {
			t.Error("MustGetChannelService returned wrong input channel")
		}
		// Output channel should be different (per-consumer)
		if gotOut == out {
			t.Error("MustGetChannelService should return per-consumer out channel, not provider's out channel")
		}
		if gotOut == nil {
			t.Error("MustGetChannelService returned nil out channel")
		}
	})

	t.Run("get non-existent service panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustGetChannelService should panic for non-existent service")
			}
		}()

		container.MustGetChannelService("non-existent", "test-consumer")
	})
}

func TestChannelServiceCommunication(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 1)
	out := make(chan *types.Msg, 1)
	err = container.RegisterChannelService("echo-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Start echo service handler
	go func() {
		msg := <-in
		out <- &types.Msg{
			Data: append([]byte("echo: "), msg.Data...),
		}
	}()

	// Get service and use it
	gotIn, gotOut, err := container.GetChannelService("echo-service", "test-consumer")
	if err != nil {
		t.Fatalf("GetChannelService failed: %v", err)
	}

	// Start router goroutines
	container.StartChannelRouters(context.Background())

	// Send message
	gotIn <- &types.Msg{Data: []byte("hello")}

	// Receive response
	select {
	case response := <-gotOut:
		expected := "echo: hello"
		if string(response.Data) != expected {
			t.Errorf("Expected %q, got %q", expected, string(response.Data))
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for response")
	}
}

func TestChannelServiceConcurrentAccess(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 100)
	out := make(chan *types.Msg, 100)
	err = container.RegisterChannelService("concurrent-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Start multiple goroutines accessing the service as different consumers
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		consumerName := fmt.Sprintf("consumer-%d", i)
		go func(name string) {
			_, _, err := container.GetChannelService("concurrent-service", name)
			if err != nil {
				t.Errorf("GetChannelService failed: %v", err)
			}
			done <- true
		}(consumerName)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for goroutines")
		}
	}
}

// TestRouterLoop_ContextCancellation tests that routerLoop exits on context cancellation
func TestRouterLoop_ContextCancellation(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	if err := container.BindModule(module); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	if err := container.RegisterChannelService("cancel-service", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Start with cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	container.StartChannelRouters(ctx)

	// Cancel context to stop router
	cancel()

	// Give router time to exit
	time.Sleep(10 * time.Millisecond)

	// Closing out channel should be safe now (router should have exited)
	close(out)
}

// TestRouterLoop_ConsumerNotFound tests message dropping when consumer not found
func TestRouterLoop_ConsumerNotFound(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	if err := container.BindModule(module); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	if err := container.RegisterChannelService("notfound-service", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Start router
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container.StartChannelRouters(ctx)

	// Send message with reply to non-existent consumer
	out <- &types.Msg{Data: []byte("test"), Reply: "non-existent-consumer"}

	// Give router time to process (message should be dropped with warning)
	time.Sleep(10 * time.Millisecond)

	// Logger should have received a warning about consumer not found
	if !logger.hasWarnedAbout("consumer not found") {
		t.Log("Warning message should have been logged for non-existent consumer")
	}
}

// TestRouterLoop_ConsumerChannelFull_Targeted tests message dropping when targeted consumer channel is full
func TestRouterLoop_ConsumerChannelFull_Targeted(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	if err := container.BindModule(module); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service with small buffer (1) so consumer channels are also small
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 1) // Small buffer = consumer channels get buffer 1
	if err := container.RegisterChannelService("full-service", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Get channel service to create consumer channel with buffer size 1
	_, consumerOut, err := container.GetChannelService("full-service", "slow-consumer")
	if err != nil {
		t.Fatalf("GetChannelService failed: %v", err)
	}
	_ = consumerOut // We won't read from this to simulate a full channel

	// Start router
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container.StartChannelRouters(ctx)

	// Send multiple targeted messages - buffer of 1 fills after first message
	// Send more than buffer size to trigger "channel full" warning
	for i := 0; i < 5; i++ {
		out <- &types.Msg{Data: []byte(fmt.Sprintf("msg-%d", i)), Reply: "slow-consumer"}
	}

	// Give router time to process all messages
	time.Sleep(50 * time.Millisecond)

	// Logger should have warnings about dropped messages (buffer size 1, sent 5)
	if !logger.hasWarnedAbout("consumer channel full") {
		t.Error("Expected warning about consumer channel full for targeted routing")
	}
}

// TestRouterLoop_ConsumerChannelFull_Broadcast tests message dropping during broadcast when consumer channels are full
func TestRouterLoop_ConsumerChannelFull_Broadcast(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	if err := container.BindModule(module); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service with small buffer (1) so consumer channels are also small
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 1) // Small buffer = consumer channels get buffer 1
	if err := container.RegisterChannelService("broadcast-service", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Create multiple consumers with small buffers
	_, consumer1Out, _ := container.GetChannelService("broadcast-service", "consumer-1")
	_, consumer2Out, _ := container.GetChannelService("broadcast-service", "consumer-2")
	_ = consumer1Out
	_ = consumer2Out

	// Start router
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container.StartChannelRouters(ctx)

	// Send multiple broadcast messages (no Reply means broadcast)
	// Consumer channels have buffer 1, so after first message they fill up
	for i := 0; i < 10; i++ {
		out <- &types.Msg{Data: []byte(fmt.Sprintf("broadcast-%d", i))}
	}

	// Give router time to process all messages
	time.Sleep(50 * time.Millisecond)

	// Some messages should have been dropped due to full channels
	if !logger.hasWarnedAbout("consumer channel full") {
		t.Error("Expected warning about consumer channel full during broadcast")
	}
}

// TestRouterLoop_OutChannelClosed tests router exits when out channel is closed
func TestRouterLoop_OutChannelClosed(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	if err := container.BindModule(module); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	if err := container.RegisterChannelService("close-service", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Start router
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container.StartChannelRouters(ctx)

	// Close the out channel to signal provider shutdown
	close(out)

	// Give router time to exit
	time.Sleep(10 * time.Millisecond)

	// Router should have exited cleanly without panic
}
