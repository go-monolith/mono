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
