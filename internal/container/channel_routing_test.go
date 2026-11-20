package container

import (
	"context"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// TestChannelServiceFanOut tests that messages with empty Reply field
// are broadcast to all consumers (fan-out pattern).
func TestChannelServiceFanOut(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "provider"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	err = container.RegisterChannelService("fanout-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Get service for 3 different consumers
	_, consumerA, err := container.GetChannelService("fanout-service", "consumer-A")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-A failed: %v", err)
	}

	_, consumerB, err := container.GetChannelService("fanout-service", "consumer-B")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-B failed: %v", err)
	}

	_, consumerC, err := container.GetChannelService("fanout-service", "consumer-C")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-C failed: %v", err)
	}

	// Start router goroutines
	container.StartChannelRouters(context.Background())

	// Send message with empty Reply field (fan-out)
	out <- &types.Msg{
		Data:  []byte("broadcast message"),
		Reply: "", // Empty - should fan out
	}

	// All three consumers should receive the message
	select {
	case msg := <-consumerA:
		if string(msg.Data) != "broadcast message" {
			t.Errorf("Consumer A received wrong message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer A did not receive message")
	}

	select {
	case msg := <-consumerB:
		if string(msg.Data) != "broadcast message" {
			t.Errorf("Consumer B received wrong message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer B did not receive message")
	}

	select {
	case msg := <-consumerC:
		if string(msg.Data) != "broadcast message" {
			t.Errorf("Consumer C received wrong message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer C did not receive message")
	}
}

// TestChannelServiceTargetedRouting tests that messages with non-empty Reply field
// are sent only to the matching consumer (targeted routing).
func TestChannelServiceTargetedRouting(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "provider"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	err = container.RegisterChannelService("targeted-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Get service for 3 different consumers
	_, consumerA, err := container.GetChannelService("targeted-service", "consumer-A")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-A failed: %v", err)
	}

	_, consumerB, err := container.GetChannelService("targeted-service", "consumer-B")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-B failed: %v", err)
	}

	_, consumerC, err := container.GetChannelService("targeted-service", "consumer-C")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-C failed: %v", err)
	}

	// Start router goroutines
	container.StartChannelRouters(context.Background())

	// Send message targeted to consumer-B
	out <- &types.Msg{
		Data:  []byte("targeted message for B"),
		Reply: "consumer-B", // Targeted to consumer-B
	}

	// Only consumer-B should receive the message
	select {
	case msg := <-consumerB:
		if string(msg.Data) != "targeted message for B" {
			t.Errorf("Consumer B received wrong message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer B did not receive targeted message")
	}

	// Consumer A and C should NOT receive the message
	select {
	case <-consumerA:
		t.Error("Consumer A should not receive targeted message for consumer-B")
	case <-time.After(100 * time.Millisecond):
		// Expected - no message
	}

	select {
	case <-consumerC:
		t.Error("Consumer C should not receive targeted message for consumer-B")
	case <-time.After(100 * time.Millisecond):
		// Expected - no message
	}
}

// TestChannelServiceUnknownConsumer tests that messages targeted to unknown consumers
// are dropped with a warning (no fallback to fan-out).
func TestChannelServiceUnknownConsumer(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "provider"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	err = container.RegisterChannelService("unknown-consumer-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Get service for consumer-A only
	_, consumerA, err := container.GetChannelService("unknown-consumer-service", "consumer-A")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-A failed: %v", err)
	}

	// Start router goroutines
	container.StartChannelRouters(context.Background())

	// Send message targeted to non-existent consumer-B
	out <- &types.Msg{
		Data:  []byte("message for unknown consumer"),
		Reply: "consumer-B", // Unknown consumer
	}

	// Consumer-A should NOT receive the message (no fallback to fan-out)
	select {
	case <-consumerA:
		t.Error("Consumer A should not receive message targeted to unknown consumer-B")
	case <-time.After(100 * time.Millisecond):
		// Expected - message dropped
	}

	// Check that logger received a warning
	if !logger.hasWarnedAbout("consumer not found for reply") {
		t.Error("Logger should have warned about unknown consumer")
	}
}

// TestChannelServiceMixedRouting tests both fan-out and targeted routing in sequence.
func TestChannelServiceMixedRouting(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "provider"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Register channel service
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	err = container.RegisterChannelService("mixed-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Get service for 2 consumers
	_, consumerA, err := container.GetChannelService("mixed-service", "consumer-A")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-A failed: %v", err)
	}

	_, consumerB, err := container.GetChannelService("mixed-service", "consumer-B")
	if err != nil {
		t.Fatalf("GetChannelService for consumer-B failed: %v", err)
	}

	// Start router goroutines
	container.StartChannelRouters(context.Background())

	// Test 1: Fan-out (empty Reply)
	out <- &types.Msg{Data: []byte("broadcast"), Reply: ""}

	// Both should receive
	select {
	case msg := <-consumerA:
		if string(msg.Data) != "broadcast" {
			t.Errorf("Consumer A received wrong broadcast: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer A did not receive broadcast")
	}

	select {
	case msg := <-consumerB:
		if string(msg.Data) != "broadcast" {
			t.Errorf("Consumer B received wrong broadcast: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer B did not receive broadcast")
	}

	// Test 2: Targeted to A
	out <- &types.Msg{Data: []byte("for A"), Reply: "consumer-A"}

	select {
	case msg := <-consumerA:
		if string(msg.Data) != "for A" {
			t.Errorf("Consumer A received wrong targeted message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer A did not receive targeted message")
	}

	// Consumer B should not receive
	select {
	case <-consumerB:
		t.Error("Consumer B should not receive message targeted to A")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	// Test 3: Targeted to B
	out <- &types.Msg{Data: []byte("for B"), Reply: "consumer-B"}

	select {
	case msg := <-consumerB:
		if string(msg.Data) != "for B" {
			t.Errorf("Consumer B received wrong targeted message: %s", string(msg.Data))
		}
	case <-time.After(time.Second):
		t.Error("Consumer B did not receive targeted message")
	}

	// Consumer A should not receive
	select {
	case <-consumerA:
		t.Error("Consumer A should not receive message targeted to B")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

// TestChannelRouter_NilMessage: a provider that sends nil on its out channel
// used to panic the router goroutine, which nothing recovers, and with it
// the process. The nil is dropped and the router goes on.
func TestChannelRouter_NilMessage(t *testing.T) {
	container := NewServiceContainer(&mockLogger{})
	if err := container.BindModule(&mockModule{name: "provider"}); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}
	in := make(chan *types.Msg, 10)
	out := make(chan *types.Msg, 10)
	if err := container.RegisterChannelService("jobs", in, out); err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}
	_, consumer, err := container.GetChannelService("jobs", "consumer")
	if err != nil {
		t.Fatalf("GetChannelService failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	container.StartChannelRouters(ctx)

	out <- nil
	out <- &types.Msg{Reply: "consumer", Data: []byte("after the nil")}
	select {
	case msg := <-consumer:
		if string(msg.Data) != "after the nil" {
			t.Fatalf("message = %q", msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the router did not go on after a nil message")
	}
}

// ContainerAskingChain is a middleware chain whose service registration hook
// asks the container it is called for.
type ContainerAskingChain struct {
	mockMiddlewareChainRunner
	Container types.ServiceContainer
	SawItself bool
}

// RunServiceRegistration asks the container whether the service exists.
func (c *ContainerAskingChain) RunServiceRegistration(_ context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	c.SawItself = c.Container.Has(reg.Name)
	_ = c.Container.Entries()
	return reg
}

// TestRegisterService_MiddlewareMayAskTheContainer: the middleware chain ran
// with the container's lock held, so a hook that asked the container
// anything waited for a lock its own caller held, for good.
func TestRegisterService_MiddlewareMayAskTheContainer(t *testing.T) {
	c := NewServiceContainer(&mockLogger{})
	if err := c.BindModule(&mockModule{name: "provider"}); err != nil {
		t.Fatal(err)
	}
	chain := &ContainerAskingChain{Container: c}
	c.SetMiddlewareChain(chain)

	done := make(chan error, 1)
	go func() {
		done <- c.RegisterChannelService("jobs", make(chan *types.Msg, 1), make(chan *types.Msg, 1))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("registering a service deadlocked on a middleware that asked the container")
	}
	if chain.SawItself {
		t.Error("the service was visible in the container before its registration finished")
	}
	if !c.Has("jobs") {
		t.Error("the service is not registered")
	}
	// A second registration of the name is still refused.
	if err := c.RegisterChannelService("jobs", make(chan *types.Msg, 1), make(chan *types.Msg, 1)); err == nil {
		t.Error("a duplicate registration was accepted")
	}
}
