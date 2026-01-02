//go:build integration
// +build integration

package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/middleware/requestid"
)

// TestMiddlewareChainOrdering verifies that middleware executes in registration order.
func TestMiddlewareChainOrdering(t *testing.T) {
	var executionOrder []string
	var mu sync.Mutex

	// Create test middleware that records execution order
	middleware1 := &testMiddleware{
		name: "middleware1",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			mu.Lock()
			executionOrder = append(executionOrder, "middleware1")
			mu.Unlock()
			return octx
		},
	}

	middleware2 := &testMiddleware{
		name: "middleware2",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			mu.Lock()
			executionOrder = append(executionOrder, "middleware2")
			mu.Unlock()
			return octx
		},
	}

	middleware3 := &testMiddleware{
		name: "middleware3",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			mu.Lock()
			executionOrder = append(executionOrder, "middleware3")
			mu.Unlock()
			return octx
		},
	}

	// Create chain
	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{middleware1, middleware2, middleware3},
	}

	// Execute chain
	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         &mono.Msg{Data: []byte("test")},
		Ctx:         context.Background(),
		Metadata:    make(map[string]any),
	}

	chain.RunOutgoingMessage(octx)

	// Verify execution order
	mu.Lock()
	defer mu.Unlock()

	if len(executionOrder) != 3 {
		t.Fatalf("expected 3 middleware executions, got %d", len(executionOrder))
	}

	expected := []string{"middleware1", "middleware2", "middleware3"}
	for i, name := range expected {
		if executionOrder[i] != name {
			t.Errorf("execution order[%d]: expected %s, got %s", i, name, executionOrder[i])
		}
	}
}

// TestMiddlewareMessageModification verifies that middleware can modify messages in the chain.
func TestMiddlewareMessageModification(t *testing.T) {
	// Create middleware that adds headers
	middleware1 := &testMiddleware{
		name: "header-adder-1",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			if octx.Msg.Header == nil {
				octx.Msg.Header = make(mono.Header)
			}
			octx.Msg.Header["X-Custom-1"] = []string{"value1"}
			return octx
		},
	}

	middleware2 := &testMiddleware{
		name: "header-adder-2",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			if octx.Msg.Header == nil {
				octx.Msg.Header = make(mono.Header)
			}
			octx.Msg.Header["X-Custom-2"] = []string{"value2"}
			return octx
		},
	}

	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{middleware1, middleware2},
	}

	msg := &mono.Msg{
		Data:   []byte("test"),
		Header: make(mono.Header),
	}

	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         msg,
		Ctx:         context.Background(),
		Metadata:    make(map[string]any),
	}

	result := chain.RunOutgoingMessage(octx)

	// Verify both headers were added
	if len(result.Msg.Header["X-Custom-1"]) == 0 {
		t.Error("expected X-Custom-1 header to be added")
	}
	if result.Msg.Header["X-Custom-1"][0] != "value1" {
		t.Errorf("expected X-Custom-1 value 'value1', got %q", result.Msg.Header["X-Custom-1"][0])
	}

	if len(result.Msg.Header["X-Custom-2"]) == 0 {
		t.Error("expected X-Custom-2 header to be added")
	}
	if result.Msg.Header["X-Custom-2"][0] != "value2" {
		t.Errorf("expected X-Custom-2 value 'value2', got %q", result.Msg.Header["X-Custom-2"][0])
	}
}

// TestRequestIDPropagationWithMultipleMiddleware verifies request ID works with other middleware.
func TestRequestIDPropagationWithMultipleMiddleware(t *testing.T) {
	// Create requestid middleware
	reqIDMiddleware, err := requestid.New()
	if err != nil {
		t.Fatalf("failed to create requestid middleware: %v", err)
	}

	// Create another middleware that adds custom headers
	customMiddleware := &testMiddleware{
		name: "custom-header",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			if octx.Msg.Header == nil {
				octx.Msg.Header = make(mono.Header)
			}
			octx.Msg.Header["X-Custom"] = []string{"custom-value"}
			return octx
		},
	}

	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{reqIDMiddleware, customMiddleware},
	}

	// First wrap a handler to inject request ID into context
	var capturedCtx context.Context
	handler := func(ctx context.Context, req *mono.Msg) ([]byte, error) {
		capturedCtx = ctx
		return []byte("response"), nil
	}

	// Register the handler through service registration to get wrapped with request ID
	reg := mono.ServiceRegistration{
		Type:           mono.ServiceTypeRequestReply,
		RequestHandler: handler,
	}

	wrappedReg := reqIDMiddleware.OnServiceRegistration(context.Background(), reg)

	// Call the wrapped handler with a message that has request ID header
	incomingMsg := &mono.Msg{
		Data:   []byte("incoming"),
		Header: mono.Header{"X-Request-ID": []string{"test-request-id-123"}},
	}

	wrappedReg.RequestHandler(context.Background(), incomingMsg)

	// Now use the captured context (which has request ID) for outgoing message
	outgoingMsg := &mono.Msg{
		Data:   []byte("test"),
		Header: make(mono.Header),
	}

	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         outgoingMsg,
		Ctx:         capturedCtx,
		Metadata:    make(map[string]any),
	}

	result := chain.RunOutgoingMessage(octx)

	// Verify request ID was added
	if len(result.Msg.Header["X-Request-ID"]) == 0 {
		t.Error("expected X-Request-ID header to be added")
	}
	if result.Msg.Header["X-Request-ID"][0] != "test-request-id-123" {
		t.Errorf("expected request ID 'test-request-id-123', got %q", result.Msg.Header["X-Request-ID"][0])
	}

	// Verify custom header was also added
	if len(result.Msg.Header["X-Custom"]) == 0 {
		t.Error("expected X-Custom header to be added")
	}
	if result.Msg.Header["X-Custom"][0] != "custom-value" {
		t.Errorf("expected X-Custom value 'custom-value', got %q", result.Msg.Header["X-Custom"][0])
	}
}

// TestMiddlewareContextMetadata verifies middleware can use context metadata.
func TestMiddlewareContextMetadata(t *testing.T) {
	middleware := &testMiddleware{
		name: "metadata-user",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			// Store something in metadata
			octx.Metadata["processed"] = true
			octx.Metadata["count"] = 42

			// Add header based on metadata
			if octx.Msg.Header == nil {
				octx.Msg.Header = make(mono.Header)
			}
			octx.Msg.Header["X-Processed"] = []string{"true"}

			return octx
		},
	}

	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{middleware},
	}

	msg := &mono.Msg{
		Data:   []byte("test"),
		Header: make(mono.Header),
	}

	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         msg,
		Ctx:         context.Background(),
		Metadata:    make(map[string]any),
	}

	result := chain.RunOutgoingMessage(octx)

	// Verify metadata was set
	if processed, ok := result.Metadata["processed"].(bool); !ok || !processed {
		t.Error("expected metadata['processed'] to be true")
	}

	if count, ok := result.Metadata["count"].(int); !ok || count != 42 {
		t.Errorf("expected metadata['count'] to be 42, got %v", count)
	}

	// Verify header was added
	if len(result.Msg.Header["X-Processed"]) == 0 {
		t.Error("expected X-Processed header to be added")
	}
}

// TestMiddlewareServiceTypeFiltering verifies middleware can filter by service type.
func TestMiddlewareServiceTypeFiltering(t *testing.T) {
	callCount := 0

	middleware := &testMiddleware{
		name: "type-filter",
		onOutgoing: func(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
			// Only process RequestReply
			if octx.ServiceType == mono.ServiceTypeRequestReply {
				callCount++
				if octx.Msg.Header == nil {
					octx.Msg.Header = make(mono.Header)
				}
				octx.Msg.Header["X-Request-Reply"] = []string{"true"}
			}
			return octx
		},
	}

	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{middleware},
	}

	testCases := []struct {
		name         string
		serviceType  mono.ServiceType
		expectCall   bool
		expectHeader bool
	}{
		{
			name:         "RequestReply",
			serviceType:  mono.ServiceTypeRequestReply,
			expectCall:   true,
			expectHeader: true,
		},
		{
			name:         "QueueGroup",
			serviceType:  mono.ServiceTypeQueueGroup,
			expectCall:   false,
			expectHeader: false,
		},
		{
			name:         "StreamConsumer",
			serviceType:  mono.ServiceTypeStreamConsumer,
			expectCall:   false,
			expectHeader: false,
		},
		{
			name:         "Channel",
			serviceType:  mono.ServiceTypeChannel,
			expectCall:   false,
			expectHeader: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			callCount = 0

			msg := &mono.Msg{
				Data:   []byte("test"),
				Header: make(mono.Header),
			}

			octx := mono.OutgoingMessageContext{
				ServiceType: tc.serviceType,
				ServiceName: "test-service",
				ModuleName:  "test-module",
				Subject:     "test.subject",
				Msg:         msg,
				Ctx:         context.Background(),
				Metadata:    make(map[string]any),
			}

			result := chain.RunOutgoingMessage(octx)

			if tc.expectCall && callCount != 1 {
				t.Errorf("expected middleware to be called once, got %d calls", callCount)
			}

			if !tc.expectCall && callCount != 0 {
				t.Errorf("expected middleware not to be called, got %d calls", callCount)
			}

			hasHeader := len(result.Msg.Header["X-Request-Reply"]) > 0
			if tc.expectHeader && !hasHeader {
				t.Error("expected X-Request-Reply header to be added")
			}

			if !tc.expectHeader && hasHeader {
				t.Error("expected X-Request-Reply header not to be added")
			}
		})
	}
}

// TestMiddlewareNilHeaderHandling verifies middleware handles nil headers correctly.
func TestMiddlewareNilHeaderHandling(t *testing.T) {
	reqIDMiddleware, err := requestid.New()
	if err != nil {
		t.Fatalf("failed to create requestid middleware: %v", err)
	}

	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{reqIDMiddleware},
	}

	// First wrap a handler to inject request ID into context
	var capturedCtx context.Context
	handler := func(ctx context.Context, req *mono.Msg) ([]byte, error) {
		capturedCtx = ctx
		return []byte("response"), nil
	}

	// Register the handler through service registration
	reg := mono.ServiceRegistration{
		Type:           mono.ServiceTypeRequestReply,
		RequestHandler: handler,
	}

	wrappedReg := reqIDMiddleware.OnServiceRegistration(context.Background(), reg)

	// Call the wrapped handler with a message that has request ID header
	incomingMsg := &mono.Msg{
		Data:   []byte("incoming"),
		Header: mono.Header{"X-Request-ID": []string{"test-id"}},
	}

	wrappedReg.RequestHandler(context.Background(), incomingMsg)

	// Now test outgoing message with nil header
	msg := &mono.Msg{
		Data:   []byte("test"),
		Header: nil, // Explicitly nil
	}

	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         msg,
		Ctx:         capturedCtx,
		Metadata:    make(map[string]any),
	}

	result := chain.RunOutgoingMessage(octx)

	// Verify header was initialized and request ID added
	if result.Msg.Header == nil {
		t.Fatal("expected header to be initialized")
	}

	if len(result.Msg.Header["X-Request-ID"]) == 0 {
		t.Error("expected X-Request-ID header to be added")
	}

	if result.Msg.Header["X-Request-ID"][0] != "test-id" {
		t.Errorf("expected request ID 'test-id', got %q", result.Msg.Header["X-Request-ID"][0])
	}
}

// TestMiddlewareEmptyChain verifies empty middleware chain works correctly.
func TestMiddlewareEmptyChain(t *testing.T) {
	chain := &testMiddlewareChain{
		middlewares: []mono.MiddlewareModule{},
	}

	msg := &mono.Msg{
		Data:   []byte("test"),
		Header: mono.Header{"X-Original": []string{"value"}},
	}

	octx := mono.OutgoingMessageContext{
		ServiceType: mono.ServiceTypeRequestReply,
		ServiceName: "test-service",
		ModuleName:  "test-module",
		Subject:     "test.subject",
		Msg:         msg,
		Ctx:         context.Background(),
		Metadata:    make(map[string]any),
	}

	result := chain.RunOutgoingMessage(octx)

	// Verify message is unchanged
	if result.Msg.Header["X-Original"][0] != "value" {
		t.Errorf("expected original header to be preserved, got %q", result.Msg.Header["X-Original"][0])
	}

	if len(result.Msg.Header) != 1 {
		t.Errorf("expected only original header, got %d headers", len(result.Msg.Header))
	}
}

// Helper types for testing

type testMiddleware struct {
	name       string
	onOutgoing func(mono.OutgoingMessageContext) mono.OutgoingMessageContext
}

func (m *testMiddleware) Name() string {
	return m.name
}

func (m *testMiddleware) Start(ctx context.Context) error {
	return nil
}

func (m *testMiddleware) Stop(ctx context.Context) error {
	return nil
}

func (m *testMiddleware) OnModuleLifecycle(ctx context.Context, event mono.ModuleLifecycleEvent) mono.ModuleLifecycleEvent {
	return event
}

func (m *testMiddleware) OnServiceRegistration(ctx context.Context, reg mono.ServiceRegistration) mono.ServiceRegistration {
	return reg
}

func (m *testMiddleware) OnConfigurationChange(ctx context.Context, event mono.ConfigurationEvent) mono.ConfigurationEvent {
	return event
}

func (m *testMiddleware) OnOutgoingMessage(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
	if m.onOutgoing != nil {
		return m.onOutgoing(octx)
	}
	return octx
}

func (m *testMiddleware) OnEventConsumerRegistration(_ context.Context, entry mono.EventConsumerEntry) mono.EventConsumerEntry {
	return entry
}

func (m *testMiddleware) OnEventStreamConsumerRegistration(_ context.Context, entry mono.EventStreamConsumerEntry) mono.EventStreamConsumerEntry {
	return entry
}

type testMiddlewareChain struct {
	middlewares []mono.MiddlewareModule
}

func (c *testMiddlewareChain) RunOutgoingMessage(octx mono.OutgoingMessageContext) mono.OutgoingMessageContext {
	for _, mw := range c.middlewares {
		octx = mw.OnOutgoingMessage(octx)
	}
	return octx
}

func (c *testMiddlewareChain) RunServiceRegistration(ctx context.Context, reg mono.ServiceRegistration) mono.ServiceRegistration {
	for _, mw := range c.middlewares {
		reg = mw.OnServiceRegistration(ctx, reg)
	}
	return reg
}

func (c *testMiddlewareChain) RunEventConsumerRegistration(ctx context.Context, entry mono.EventConsumerEntry) mono.EventConsumerEntry {
	for _, mw := range c.middlewares {
		entry = mw.OnEventConsumerRegistration(ctx, entry)
	}
	return entry
}

func (c *testMiddlewareChain) RunEventStreamConsumerRegistration(ctx context.Context, entry mono.EventStreamConsumerEntry) mono.EventStreamConsumerEntry {
	for _, mw := range c.middlewares {
		entry = mw.OnEventStreamConsumerRegistration(ctx, entry)
	}
	return entry
}
