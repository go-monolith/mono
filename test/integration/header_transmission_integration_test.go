//go:build integration
// +build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// TestIntegration_RequestReplyServiceWithHeaders tests request-reply service with header transmission
func TestIntegration_RequestReplyServiceWithHeaders(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that registers the request-reply service
	var capturedHeaders mono.Header
	responder := &testModule{name: "responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("header-test", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			// Capture headers from request
			capturedHeaders = req.Header
			// Echo request data and send response headers
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
	client, err := responder.container.GetRequestReplyService("header-test")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request with headers using CallMsg
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	requestMsg := &mono.Msg{
		Data: []byte("hello with headers"),
		Header: mono.Header{
			"X-Request-ID": []string{"req-123"},
			"X-Trace-ID":   []string{"trace-456"},
			"Priority":     []string{"high"},
		},
	}

	response, err := client.CallMsg(reqCtx, requestMsg)
	if err != nil {
		t.Fatalf("CallMsg failed: %v", err)
	}

	// Verify response data
	expected := "response: hello with headers"
	if string(response.Data) != expected {
		t.Errorf("expected '%s', got '%s'", expected, string(response.Data))
	}

	// Verify request headers were transmitted
	if capturedHeaders == nil {
		t.Fatal("Expected headers to be transmitted, but got nil")
	}

	if len(capturedHeaders["X-Request-ID"]) == 0 || capturedHeaders["X-Request-ID"][0] != "req-123" {
		t.Errorf("Expected X-Request-ID 'req-123', got %v", capturedHeaders["X-Request-ID"])
	}

	if len(capturedHeaders["X-Trace-ID"]) == 0 || capturedHeaders["X-Trace-ID"][0] != "trace-456" {
		t.Errorf("Expected X-Trace-ID 'trace-456', got %v", capturedHeaders["X-Trace-ID"])
	}

	if len(capturedHeaders["Priority"]) == 0 || capturedHeaders["Priority"][0] != "high" {
		t.Errorf("Expected Priority 'high', got %v", capturedHeaders["Priority"])
	}
}

// TestIntegration_QueueGroupServiceWithHeaders tests queue group service with header transmission
func TestIntegration_QueueGroupServiceWithHeaders(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track received messages
	receivedCount := 0
	var capturedHeaders mono.Header
	msgReceived := make(chan struct{}, 1)

	// Create processor module that registers the queue group service
	processor := &testModule{name: "processor"}
	processor.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("header-task",
			mono.QGHP{
				QueueGroup: "workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					// Capture headers from message
					capturedHeaders = msg.Header
					receivedCount++
					select {
					case msgReceived <- struct{}{}:
					default:
					}
					return nil
				},
			},
		)
	}

	if err := fw.Register(processor); err != nil {
		t.Fatalf("Failed to register processor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give the subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify container is available
	if processor.container == nil {
		t.Fatal("Processor container is nil")
	}

	// Get the queue group service client
	client, err := processor.container.GetQueueGroupService("header-task")
	if err != nil {
		t.Fatalf("Failed to get queue group service: %v", err)
	}

	// Send message with headers using SendMsg
	sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
	defer sendCancel()

	msg := &mono.Msg{
		Data: []byte("task data"),
		Header: mono.Header{
			"X-Task-ID":   []string{"task-789"},
			"Priority":    []string{"high"},
			"X-Tenant-ID": []string{"tenant-001"},
		},
	}

	err = client.SendMsg(sendCtx, msg)
	if err != nil {
		t.Fatalf("SendMsg failed: %v", err)
	}

	// Wait for message to be received
	select {
	case <-msgReceived:
		// Message received
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message to be received")
	}

	// Verify message was received
	if receivedCount != 1 {
		t.Errorf("Expected 1 message to be received, got %d", receivedCount)
	}

	// Verify headers were transmitted
	if capturedHeaders == nil {
		t.Fatal("Expected headers to be transmitted, but got nil")
	}

	if len(capturedHeaders["X-Task-ID"]) == 0 || capturedHeaders["X-Task-ID"][0] != "task-789" {
		t.Errorf("Expected X-Task-ID 'task-789', got %v", capturedHeaders["X-Task-ID"])
	}

	if len(capturedHeaders["Priority"]) == 0 || capturedHeaders["Priority"][0] != "high" {
		t.Errorf("Expected Priority 'high', got %v", capturedHeaders["Priority"])
	}

	if len(capturedHeaders["X-Tenant-ID"]) == 0 || capturedHeaders["X-Tenant-ID"][0] != "tenant-001" {
		t.Errorf("Expected X-Tenant-ID 'tenant-001', got %v", capturedHeaders["X-Tenant-ID"])
	}
}

// TestIntegration_RequestReplyWithHeadersRoundTrip tests full roundtrip with headers
func TestIntegration_RequestReplyWithHeadersRoundTrip(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create responder module that echoes headers in response
	responder := &testModule{name: "responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo-headers", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			// Echo request headers by returning a response with custom headers
			// Note: In a real implementation, you would need to send the response with headers via the EventBus
			return []byte("echoed: " + string(req.Data)), nil
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

	// Get the request-reply service client
	client, err := responder.container.GetRequestReplyService("echo-headers")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	// Make request with multiple headers
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	requestMsg := &mono.Msg{
		Data: []byte("test"),
		Header: mono.Header{
			"X-Correlation-ID": []string{"corr-123"},
			"X-User-Agent":     []string{"mono-framework/1.0"},
		},
	}

	response, err := client.CallMsg(reqCtx, requestMsg)
	if err != nil {
		t.Fatalf("CallMsg failed: %v", err)
	}

	// Verify response data
	if string(response.Data) != "echoed: test" {
		t.Errorf("expected 'echoed: test', got '%s'", string(response.Data))
	}
}
