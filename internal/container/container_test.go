package container

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// mockModule implements types.Module for testing
type mockModule struct {
	name string
}

func (m *mockModule) Name() string                    { return m.name }
func (m *mockModule) Start(ctx context.Context) error { return nil }
func (m *mockModule) Stop(ctx context.Context) error  { return nil }

// mockLogger implements types.Logger for testing
type mockLogger struct {
	mu       sync.Mutex
	warnings []string
}

func (m *mockLogger) Debug(msg string, args ...any) {}
func (m *mockLogger) Info(msg string, args ...any)  {}
func (m *mockLogger) Warn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.warnings == nil {
		m.warnings = make([]string, 0)
	}
	m.warnings = append(m.warnings, msg)
}
func (m *mockLogger) Error(msg string, args ...any)       {}
func (m *mockLogger) With(args ...any) types.Logger       { return m }
func (m *mockLogger) WithModule(name string) types.Logger { return m }
func (m *mockLogger) WithError(err error) types.Logger    { return m }

func (m *mockLogger) hasWarnedAbout(substring string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, warn := range m.warnings {
		if strings.Contains(warn, substring) {
			return true
		}
	}
	return false
}

func TestNewServiceContainer(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)

	if container == nil {
		t.Fatal("NewServiceContainer returned nil")
	}

	// Verify container is in unbound state
	if container.Has("any-service") {
		t.Error("New container should not have any services")
	}
}

func TestBindModule(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)

	t.Run("bind valid module", func(t *testing.T) {
		module := &mockModule{name: "test-module"}
		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}
	})

	t.Run("bind nil module", func(t *testing.T) {
		container := NewServiceContainer(logger)
		err := container.BindModule(nil)
		if err == nil {
			t.Error("BindModule should fail with nil module")
		}
	})

	t.Run("bind module with empty name", func(t *testing.T) {
		container := NewServiceContainer(logger)
		module := &mockModule{name: ""}
		err := container.BindModule(module)
		if err == nil {
			t.Error("BindModule should fail with empty module name")
		}
	})

	t.Run("bind module twice", func(t *testing.T) {
		container := NewServiceContainer(logger)
		module1 := &mockModule{name: "module-1"}
		module2 := &mockModule{name: "module-2"}

		err := container.BindModule(module1)
		if err != nil {
			t.Fatalf("First BindModule failed: %v", err)
		}

		err = container.BindModule(module2)
		if err == nil {
			t.Error("BindModule should fail when already bound")
		}
	})
}

func TestHas(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Initially no services
	if container.Has("test-service") {
		t.Error("Has should return false for non-existent service")
	}

	// Register a channel service
	in := make(chan *types.Msg, 1)
	out := make(chan *types.Msg, 1)
	err = container.RegisterChannelService("test-service", in, out)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Now it should exist
	if !container.Has("test-service") {
		t.Error("Has should return true for registered service")
	}
}

func TestUnregister(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	t.Run("unregister existing service", func(t *testing.T) {
		in := make(chan *types.Msg, 1)
		out := make(chan *types.Msg, 1)
		err := container.RegisterChannelService("test-service", in, out)
		if err != nil {
			t.Fatalf("RegisterChannelService failed: %v", err)
		}

		err = container.Unregister("test-service")
		if err != nil {
			t.Errorf("Unregister failed: %v", err)
		}

		if container.Has("test-service") {
			t.Error("Service should not exist after Unregister")
		}
	})

	t.Run("unregister non-existent service", func(t *testing.T) {
		err := container.Unregister("non-existent")
		if err == nil {
			t.Error("Unregister should fail for non-existent service")
		}
	})

	t.Run("unregister non-existent service without bound module", func(t *testing.T) {
		unboundContainer := NewServiceContainer(logger)
		err := unboundContainer.Unregister("non-existent")
		if err == nil {
			t.Error("Unregister should fail for non-existent service")
		}
		// Verify error message contains "<unbound>"
		if err != nil && !strings.Contains(err.Error(), "<unbound>") {
			t.Errorf("Expected error to contain '<unbound>', got: %v", err)
		}
	})
}

func TestEntries(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)
	module := &mockModule{name: "test-module"}

	err := container.BindModule(module)
	if err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}

	// Initially empty
	entries := container.Entries()
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}

	// Register services
	in1 := make(chan *types.Msg, 1)
	out1 := make(chan *types.Msg, 1)
	err = container.RegisterChannelService("service-1", in1, out1)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	in2 := make(chan *types.Msg, 1)
	out2 := make(chan *types.Msg, 1)
	err = container.RegisterChannelService("service-2", in2, out2)
	if err != nil {
		t.Fatalf("RegisterChannelService failed: %v", err)
	}

	// Check entries
	entries = container.Entries()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	// Verify entries contain correct data
	foundService1 := false
	foundService2 := false
	for _, entry := range entries {
		if entry.Name == "service-1" {
			foundService1 = true
			if entry.Type != types.ServiceTypeChannel {
				t.Error("service-1 should be Channel type")
			}
			if entry.ModuleName != "test-module" {
				t.Error("service-1 should have correct module name")
			}
		}
		if entry.Name == "service-2" {
			foundService2 = true
		}
	}

	if !foundService1 || !foundService2 {
		t.Error("Entries should contain all registered services")
	}
}

func TestValidateServiceName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{"valid lowercase", "test", false},
		{"valid kebab-case", "test-service", false},
		{"valid with numbers", "service-123", false},
		{"empty string", "", true},
		{"uppercase", "TestService", true},
		{"camelCase", "testService", true},
		{"underscore", "test_service", true},
		{"leading hyphen", "-test", true},
		{"trailing hyphen", "test-", true},
		{"double hyphen", "test--service", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServiceName(tt.input)
			if (err != nil) != tt.wantError {
				t.Errorf("validateServiceName(%q) error = %v, wantError %v", tt.input, err, tt.wantError)
			}
		})
	}
}

func TestComputeServiceSubject(t *testing.T) {
	logger := &mockLogger{}

	t.Run("unbound container", func(t *testing.T) {
		container := NewServiceContainer(logger).(*serviceContainer)
		_, err := container.computeServiceSubject("test-service")
		if err != monoerrors.ErrContainerNotBound {
			t.Errorf("Expected ErrContainerNotBound, got %v", err)
		}
	})

	t.Run("valid subject computation", func(t *testing.T) {
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "inventory"}
		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		subject, err := container.computeServiceSubject("check-stock")
		if err != nil {
			t.Fatalf("computeServiceSubject failed: %v", err)
		}

		expected := "services.inventory.check-stock"
		if subject != expected {
			t.Errorf("Expected subject %q, got %q", expected, subject)
		}
	})

	t.Run("invalid service name", func(t *testing.T) {
		container := NewServiceContainer(logger).(*serviceContainer)
		module := &mockModule{name: "inventory"}
		err := container.BindModule(module)
		if err != nil {
			t.Fatalf("BindModule failed: %v", err)
		}

		_, err = container.computeServiceSubject("Invalid-Service")
		if err == nil {
			t.Error("Expected error for invalid service name")
		}
	})
}

func TestRegisterServiceUnbound(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger)

	// Try to register without binding
	in := make(chan *types.Msg, 1)
	out := make(chan *types.Msg, 1)
	err := container.RegisterChannelService("test", in, out)
	if err != monoerrors.ErrContainerNotBound {
		t.Errorf("Expected ErrContainerNotBound, got %v", err)
	}
}

// mockMiddlewareChain implements types.MiddlewareChainRunner for testing
type mockMiddlewareChainRunner struct {
	serviceRegistrationCalled bool
	outgoingMessageCalled     bool
	capturedServiceReg        types.ServiceRegistration
	injectHeaders             map[string][]string
	modifyMsg                 func(*types.Msg) *types.Msg
}

func (m *mockMiddlewareChainRunner) RunServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	m.serviceRegistrationCalled = true
	m.capturedServiceReg = reg
	// Middleware can wrap handlers
	if reg.RequestHandler != nil {
		originalHandler := reg.RequestHandler
		reg.RequestHandler = func(ctx context.Context, msg *types.Msg) ([]byte, error) {
			// Add header before calling original
			if msg.Header == nil {
				msg.Header = make(types.Header)
			}
			msg.Header["X-Middleware"] = []string{"wrapped"}
			return originalHandler(ctx, msg)
		}
	}
	return reg
}

func (m *mockMiddlewareChainRunner) RunOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	m.outgoingMessageCalled = true
	// Inject headers
	if m.injectHeaders != nil {
		if octx.Msg.Header == nil {
			octx.Msg.Header = make(types.Header)
		}
		for key, values := range m.injectHeaders {
			octx.Msg.Header[key] = append(octx.Msg.Header[key], values...)
		}
	}
	// Allow custom msg modification
	if m.modifyMsg != nil {
		octx.Msg = m.modifyMsg(octx.Msg)
	}
	return octx
}

func (m *mockMiddlewareChainRunner) RunEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	return entry
}

func (m *mockMiddlewareChainRunner) RunEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	return entry
}

// TestNew tests the New() factory function
func TestNew(t *testing.T) {
	logger := &mockLogger{}
	container := New(logger)

	if container == nil {
		t.Fatal("New returned nil")
	}

	// Verify it returns a proper ServiceContainer
	if container.Has("nonexistent") {
		t.Error("New container should not have any services")
	}
}

// TestSetMiddlewareChain tests the SetMiddlewareChain function
func TestSetMiddlewareChain(t *testing.T) {
	t.Run("set middleware chain", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)

		mockChain := &mockMiddlewareChainRunner{}
		container.SetMiddlewareChain(mockChain)

		if container.middlewareChain == nil {
			t.Error("middleware chain should be set")
		}
	})

	t.Run("set nil middleware chain", func(t *testing.T) {
		logger := &mockLogger{}
		container := NewServiceContainer(logger).(*serviceContainer)

		container.SetMiddlewareChain(nil)

		if container.middlewareChain != nil {
			t.Error("middleware chain should be nil")
		}
	})
}

// TestBuildServiceRegistration tests the buildServiceRegistration function
func TestBuildServiceRegistration(t *testing.T) {
	t.Run("build from RequestReply entry", func(t *testing.T) {
		handler := func(ctx context.Context, msg *types.Msg) ([]byte, error) {
			return []byte("response"), nil
		}

		entry := &types.ServiceEntry{
			Type:           types.ServiceTypeRequestReply,
			Name:           "test-service",
			ModuleName:     "test-module",
			Subject:        "services.test.test-service",
			RequestHandler: handler,
		}

		reg := buildServiceRegistration(entry)

		if reg.Type != entry.Type {
			t.Error("type should match")
		}
		if reg.Name != entry.Name {
			t.Error("name should match")
		}
		if reg.ModuleName != entry.ModuleName {
			t.Error("module name should match")
		}
		if reg.Subject != entry.Subject {
			t.Error("subject should match")
		}
		if reg.RequestHandler == nil {
			t.Error("handler should be copied")
		}
		if reg.Metadata == nil {
			t.Error("metadata should be initialized")
		}
	})

	t.Run("build from QueueGroup entry", func(t *testing.T) {
		handler1 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		entry := &types.ServiceEntry{
			Type:       types.ServiceTypeQueueGroup,
			Name:       "test-queue",
			ModuleName: "test-module",
			Subject:    "services.test.test-queue",
			QueueHandlers: []types.QGHP{
				{QueueGroup: "workers", Handler: handler1},
			},
		}

		reg := buildServiceRegistration(entry)

		if len(reg.QueueHandlers) != 1 {
			t.Error("queue handlers should be copied")
		}
	})

	t.Run("build from StreamConsumer entry", func(t *testing.T) {
		handler := func(ctx context.Context, msgs []*types.Msg) error {
			return nil
		}

		entry := &types.ServiceEntry{
			Type:                  types.ServiceTypeStreamConsumer,
			Name:                  "test-stream",
			ModuleName:            "test-module",
			Subject:               "services.test.test-stream",
			StreamConsumerHandler: handler,
			StreamConsumerConfig:  &types.StreamConsumerConfig{},
		}

		reg := buildServiceRegistration(entry)

		if reg.StreamHandler == nil {
			t.Error("stream handler should be copied")
		}
	})
}

// TestApplyServiceRegistration tests the applyServiceRegistration function
func TestApplyServiceRegistration(t *testing.T) {
	t.Run("apply handler modifications", func(t *testing.T) {
		originalHandler := func(ctx context.Context, msg *types.Msg) ([]byte, error) {
			return []byte("original"), nil
		}

		wrappedHandler := func(ctx context.Context, msg *types.Msg) ([]byte, error) {
			return []byte("wrapped"), nil
		}

		entry := &types.ServiceEntry{
			Type:           types.ServiceTypeRequestReply,
			Name:           "test-service",
			RequestHandler: originalHandler,
		}

		reg := types.ServiceRegistration{
			Type:           types.ServiceTypeRequestReply,
			Name:           "test-service",
			RequestHandler: wrappedHandler,
		}

		applyServiceRegistration(entry, reg)

		// Handler should be replaced
		if entry.RequestHandler == nil {
			t.Error("handler should be applied")
		}

		// Verify it's the wrapped handler by calling it
		result, _ := entry.RequestHandler(context.Background(), &types.Msg{})
		if string(result) != "wrapped" {
			t.Error("wrapped handler should be applied")
		}
	})

	t.Run("apply queue handler modifications", func(t *testing.T) {
		handler1 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}
		handler2 := func(ctx context.Context, msg *types.Msg) error {
			return nil
		}

		entry := &types.ServiceEntry{
			Type: types.ServiceTypeQueueGroup,
			QueueHandlers: []types.QGHP{
				{QueueGroup: "workers", Handler: handler1},
			},
		}

		reg := types.ServiceRegistration{
			Type: types.ServiceTypeQueueGroup,
			QueueHandlers: []types.QGHP{
				{QueueGroup: "workers", Handler: handler2},
			},
		}

		applyServiceRegistration(entry, reg)

		if len(entry.QueueHandlers) != 1 {
			t.Error("queue handlers should be applied")
		}
	})

	t.Run("apply stream consumer config modifications", func(t *testing.T) {
		entry := &types.ServiceEntry{
			Type: types.ServiceTypeStreamConsumer,
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{Name: "ORIGINAL"},
			},
		}

		reg := types.ServiceRegistration{
			Type: types.ServiceTypeStreamConsumer,
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{Name: "MODIFIED"},
			},
		}

		applyServiceRegistration(entry, reg)

		if entry.StreamConsumerConfig.Stream.Name != "MODIFIED" {
			t.Error("stream config should be applied")
		}
	})

	t.Run("apply channel modifications", func(t *testing.T) {
		inChan1 := make(chan *types.Msg, 1)
		outChan1 := make(chan *types.Msg, 1)
		inChan2 := make(chan *types.Msg, 1)
		outChan2 := make(chan *types.Msg, 1)

		entry := &types.ServiceEntry{
			Type:       types.ServiceTypeChannel,
			InChannel:  inChan1,
			OutChannel: outChan1,
		}

		reg := types.ServiceRegistration{
			Type:       types.ServiceTypeChannel,
			InChannel:  inChan2,
			OutChannel: outChan2,
		}

		applyServiceRegistration(entry, reg)

		if entry.InChannel != inChan2 {
			t.Error("in channel should be applied")
		}
		if entry.OutChannel != outChan2 {
			t.Error("out channel should be applied")
		}

		close(inChan1)
		close(outChan1)
		close(inChan2)
		close(outChan2)
	})
}

// Integration tests that exercise middleware through actual service calls

// TestRequestReplyWithMiddlewareIntegration tests middleware integration through actual Call
func TestRequestReplyWithMiddlewareIntegration(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test"}
	container.BindModule(module)

	// Set up middleware that injects headers
	mockChain := &mockMiddlewareChainRunner{
		injectHeaders: map[string][]string{
			"X-Middleware-Test": {"integration"},
		},
	}
	container.SetMiddlewareChain(mockChain)

	// Mock EventBus that captures the message with headers
	var capturedMsg *types.Msg
	eventBus := &mockEventBus{
		requestMsgWithCtxHandler: func(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
			capturedMsg = msg
			return &types.Msg{Data: []byte("response")}, nil
		},
	}
	container.SetEventBus(eventBus)

	// Register service
	handler := func(ctx context.Context, req *types.Msg) ([]byte, error) {
		return []byte("ok"), nil
	}
	container.RegisterRequestReplyService("test-svc", handler)

	// Get client and make call
	client, err := container.GetRequestReplyService("test-svc")
	if err != nil {
		t.Fatalf("failed to get service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Call(ctx, []byte("request"))
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	// Verify middleware was called and injected header
	if !mockChain.outgoingMessageCalled {
		t.Error("middleware should have been called")
	}

	if capturedMsg == nil {
		t.Fatal("message was not captured")
	}

	if len(capturedMsg.Header["X-Middleware-Test"]) == 0 {
		t.Error("middleware should have injected header")
	}
}

// TestQueueGroupWithMiddlewareIntegration tests middleware integration through actual Send
func TestQueueGroupWithMiddlewareIntegration(t *testing.T) {
	logger := &mockLogger{}
	container := NewServiceContainer(logger).(*serviceContainer)
	module := &mockModule{name: "test"}
	container.BindModule(module)

	// Set up middleware
	mockChain := &mockMiddlewareChainRunner{
		injectHeaders: map[string][]string{
			"X-Queue-Header": {"test"},
		},
	}
	container.SetMiddlewareChain(mockChain)

	// Mock EventBus
	var capturedMsg *types.Msg
	eventBus := &mockEventBus{
		publishMsgFunc: func(msg *types.Msg) error {
			capturedMsg = msg
			return nil
		},
	}
	container.SetEventBus(eventBus)

	// Register service
	handler := func(ctx context.Context, msg *types.Msg) error {
		return nil
	}
	container.RegisterQueueGroupService("queue-svc", types.QGHP{
		QueueGroup: "workers",
		Handler:    handler,
	})

	// Get client and send message
	client, err := container.GetQueueGroupService("queue-svc")
	if err != nil {
		t.Fatalf("failed to get service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Send(ctx, []byte("message"))
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// Verify middleware was called
	if !mockChain.outgoingMessageCalled {
		t.Error("middleware should have been called")
	}

	if capturedMsg != nil && len(capturedMsg.Header["X-Queue-Header"]) == 0 {
		t.Error("middleware should have injected header")
	}
}
