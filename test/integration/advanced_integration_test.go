//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/middleware/audit"
)

// TestIntegration_ServiceContainer tests service container functionality
func TestIntegration_ServiceContainer(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create channels for service
	inChan := make(chan *mono.Msg, 10)
	outChan := make(chan *mono.Msg, 10)

	// Create module that registers services
	module := &testModule{name: "service-module"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		// Register a channel service
		return container.RegisterChannelService("test-channel", inChan, outChan)
	}

	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify service was registered via module's container
	if module.container == nil {
		t.Fatal("Module container is nil")
	}

	// Retrieve the channel service through the container API
	retrievedIn, retrievedOut, err := module.container.GetChannelService("test-channel", "test-consumer")
	if err != nil {
		t.Fatalf("Failed to get channel service: %v", err)
	}

	// Verify we got the correct in channel (shared) and a per-consumer out channel
	if retrievedIn != inChan {
		t.Error("Retrieved inChan does not match registered channel")
	}
	// outChan is now per-consumer, so it should be different from provider's out channel
	if retrievedOut == outChan {
		t.Error("Retrieved outChan should be per-consumer, not the provider's out channel")
	}
	if retrievedOut == nil {
		t.Error("Retrieved outChan should not be nil")
	}

	// Test channel communication: send on in, receive on in (loopback test)
	// Note: Actual bidirectional communication would require a service that
	// reads from inChan and writes to outChan. Here we just verify the
	// channels are usable.
	testMsg := &mono.Msg{Data: []byte("test message")}

	// Send to input channel
	select {
	case retrievedIn <- testMsg:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to send to channel")
	}

	// Receive from input channel (verifies channel is functional)
	select {
	case msg := <-retrievedIn:
		if string(msg.Data) != "test message" {
			t.Errorf("expected 'test message', got '%s'", string(msg.Data))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to receive from channel")
	}
}

// TestIntegration_RequestReplyService tests request-reply service
func TestIntegration_RequestReplyService(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that registers the request-reply service
	responder := &testModule{name: "responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("test-request", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return []byte("response: " + string(req.Data)), nil
		})
	}

	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register responder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify container is available
	if responder.container == nil {
		t.Fatal("Responder container is nil")
	}

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("test-request")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request and verify response
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	response, err := client.Call(reqCtx, []byte("hello"))
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}

	expected := "response: hello"
	if string(response.Data) != expected {
		t.Errorf("expected '%s', got '%s'", expected, string(response.Data))
	}
}

// TestIntegration_QueueGroupService tests queue group service
func TestIntegration_QueueGroupService(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	var receivedCount atomic.Int32

	// Create worker module with queue group service
	worker := &testModule{name: "worker"}
	worker.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("process-task",
			mono.QGHP{
				QueueGroup: "workers",
				Handler: func(ctx context.Context, msg *mono.Msg) error {
					receivedCount.Add(1)
					return nil
				},
			},
		)
	}

	if err := fw.Register(worker); err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give worker time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish messages to the queue group service subject
	eventBus := fw.EventBus("worker")
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		// Publish to the service subject: services.<module>.<service>
		if err := eventBus.Publish("services.worker.process-task", []byte("work")); err != nil {
			t.Errorf("Publish %d failed: %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify messages were received
	received := receivedCount.Load()
	if received != int32(messageCount) {
		t.Errorf("expected %d messages, got %d", messageCount, received)
	}
}

// TestIntegration_QueueGroupServiceMultipleGroups tests a service registered with multiple queue groups
func TestIntegration_QueueGroupServiceMultipleGroups(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	var highPriorityCount atomic.Int32
	var lowPriorityCount atomic.Int32
	var mediumPriorityCount atomic.Int32

	// Create worker module with multiple queue groups for the same service
	worker := &testModule{name: "worker"}
	worker.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("process-task",
			mono.QGHP{
				QueueGroup: "high-priority-workers",
				Handler: func(ctx context.Context, msg *mono.Msg) error {
					highPriorityCount.Add(1)
					return nil
				},
			},
			mono.QGHP{
				QueueGroup: "low-priority-workers",
				Handler: func(ctx context.Context, msg *mono.Msg) error {
					lowPriorityCount.Add(1)
					return nil
				},
			},
			mono.QGHP{
				QueueGroup: "medium-priority-workers",
				Handler: func(ctx context.Context, msg *mono.Msg) error {
					mediumPriorityCount.Add(1)
					return nil
				},
			},
		)
	}

	if err := fw.Register(worker); err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give workers time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish messages to the queue group service subject
	// All queue groups should receive ALL messages (broadcast behavior)
	eventBus := fw.EventBus("worker")
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		// Publish to the service subject: services.<module>.<service>
		if err := eventBus.Publish("services.worker.process-task", []byte("work")); err != nil {
			t.Errorf("Publish %d failed: %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify ALL queue groups received ALL messages (NATS queue group broadcast behavior)
	highReceived := highPriorityCount.Load()
	lowReceived := lowPriorityCount.Load()
	mediumReceived := mediumPriorityCount.Load()

	if highReceived != int32(messageCount) {
		t.Errorf("high-priority queue: expected %d messages, got %d", messageCount, highReceived)
	}
	if lowReceived != int32(messageCount) {
		t.Errorf("low-priority queue: expected %d messages, got %d", messageCount, lowReceived)
	}
	if mediumReceived != int32(messageCount) {
		t.Errorf("medium-priority queue: expected %d messages, got %d", messageCount, mediumReceived)
	}

	// Verify total count: each queue group gets all messages
	totalReceived := highReceived + lowReceived + mediumReceived
	expectedTotal := int32(messageCount * 3) // 3 queue groups
	if totalReceived != expectedTotal {
		t.Errorf("total messages: expected %d, got %d", expectedTotal, totalReceived)
	}
}

// TestIntegration_AuditLogging tests audit logging during framework lifecycle using middleware
func TestIntegration_AuditLogging(t *testing.T) {
	var auditBuf bytes.Buffer

	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Register audit middleware FIRST to observe all events
	// Hash chaining disabled by default
	auditModule, err := audit.New(
		audit.WithOutput(&auditBuf),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module
	module := &testModule{name: "audit-test", dependencies: []string{}}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Verify audit log entries (check before consuming buffer)
	if auditBuf.Len() == 0 {
		t.Fatal("audit log is empty")
	}

	// Parse audit entries
	var entries []audit.Entry
	decoder := json.NewDecoder(&auditBuf)
	for decoder.More() {
		var entry audit.Entry
		if err := decoder.Decode(&entry); err != nil {
			t.Fatalf("failed to decode audit entry: %v", err)
		}
		entries = append(entries, entry)
	}

	// Should have at least module started and stopped events
	// Note: Module registration events cannot be captured because they occur
	// before the middleware chain is built
	if len(entries) < 2 {
		t.Errorf("expected at least 2 audit entries, got %d", len(entries))
	}

	// Verify event types
	var hasStarted, hasStopped bool
	for _, entry := range entries {
		switch entry.EventType {
		case audit.EventModuleStarted:
			hasStarted = true
		case audit.EventModuleStopped:
			hasStopped = true
		}
	}

	if !hasStarted {
		t.Error("missing module started event")
	}
	if !hasStopped {
		t.Error("missing module stopped event")
	}

	// Note: Module registered events are no longer tested because middleware
	// cannot capture them (they occur before the middleware chain is built)
}

// TestIntegration_ConcurrentModules tests concurrent module operations
func TestIntegration_ConcurrentModules(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create multiple modules
	moduleCount := 10
	for i := 0; i < moduleCount; i++ {
		module := &testModule{name: fmt.Sprintf("module-%d", i)}
		if err := fw.Register(module); err != nil {
			t.Fatalf("Failed to register module %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify all modules have event bus
	for i := 0; i < moduleCount; i++ {
		moduleName := fmt.Sprintf("module-%d", i)
		eventBus := fw.EventBus(moduleName)
		if eventBus == nil {
			t.Errorf("Module %s does not have event bus", moduleName)
		}
	}
}

// TestIntegration_ErrorRecovery tests error handling and recovery
func TestIntegration_ErrorRecovery(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create module that fails on start
	failingModule := &testModule{name: "failing-module"}
	failingModule.startFunc = func(ctx context.Context) error {
		return context.Canceled
	}

	if err := fw.Register(failingModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start should fail
	err = fw.Start(ctx)
	if err == nil {
		t.Fatal("expected error when starting failing module")
	}

	// Framework should still be stoppable
	if err := fw.Stop(ctx); err != nil {
		t.Errorf("Stop failed after start error: %v", err)
	}
}

// TestIntegration_ComplexDependencyGraph tests complex module dependencies
func TestIntegration_ComplexDependencyGraph(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create diamond dependency:
	//     D
	//    / \
	//   B   C
	//    \ /
	//     A

	moduleA := &testModule{name: "A"}
	moduleB := &testModule{name: "B", dependencies: []string{"A"}}
	moduleC := &testModule{name: "C", dependencies: []string{"A"}}
	moduleD := &testModule{name: "D", dependencies: []string{"B", "C"}}

	for _, m := range []*testModule{moduleA, moduleB, moduleC, moduleD} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register module %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify all modules started
	for _, m := range []*testModule{moduleA, moduleB, moduleC, moduleD} {
		if !m.wasStarted() {
			t.Errorf("Module %s was not started", m.name)
		}
	}

	// Stop and verify reverse order
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	for _, m := range []*testModule{moduleA, moduleB, moduleC, moduleD} {
		if !m.wasStopped() {
			t.Errorf("Module %s was not stopped", m.name)
		}
	}
}
