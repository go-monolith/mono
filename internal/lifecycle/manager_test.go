package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// mockModule implements types.Module
type mockModule struct {
	name          string
	startCalled   bool
	stopCalled    bool
	startErr      error
	stopErr       error
	startPanic    any
	stopPanic     any
	dependencies  []string
	depContainers map[string]types.ServiceContainer
	startOrder    *[]string // For tracking start order in tests
	stopOrder     *[]string // For tracking stop order in tests
	mu            sync.Mutex
}

func (m *mockModule) Name() string {
	return m.name
}

func (m *mockModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	if m.startPanic != nil {
		panic(m.startPanic)
	}
	return m.startErr
}

func (m *mockModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	if m.stopOrder != nil {
		*m.stopOrder = append(*m.stopOrder, m.name)
	}
	if m.stopPanic != nil {
		panic(m.stopPanic)
	}
	return m.stopErr
}

func (m *mockModule) Dependencies() []string {
	return m.dependencies
}

func (m *mockModule) SetDependencyServiceContainer(name string, container types.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.depContainers == nil {
		m.depContainers = make(map[string]types.ServiceContainer)
	}
	m.depContainers[name] = container
}

func (m *mockModule) wasStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalled
}

func (m *mockModule) wasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

// mockEventBusAwareModule implements types.EventBusAwareModule
type mockEventBusAwareModule struct {
	mockModule
	eventBus types.EventBus
}

func (m *mockEventBusAwareModule) SetEventBus(eventBus types.EventBus) {
	m.eventBus = eventBus
}

// mockModuleWithServices implements types.Module, types.EventBusAwareModule, and types.ServiceProviderModule
type mockModuleWithServices struct {
	mockModule
	serviceEntries []*types.ServiceEntry
	eventBus       types.EventBus
}

func (m *mockModuleWithServices) SetEventBus(eventBus types.EventBus) {
	m.eventBus = eventBus
}

func (m *mockModuleWithServices) RegisterServices(container types.ServiceContainer) error {
	for _, entry := range m.serviceEntries {
		switch entry.Type {
		case types.ServiceTypeRequestReply:
			if err := container.RegisterRequestReplyService(entry.Name, entry.RequestHandler); err != nil {
				return err
			}
		case types.ServiceTypeQueueGroup:
			if err := container.RegisterQueueGroupService(entry.Name, entry.QueueHandlers...); err != nil {
				return err
			}
		}
	}
	return nil
}

// mockLogger implements types.Logger
type mockLogger struct {
	mu      sync.Mutex
	entries []string
}

func (m *mockLogger) Debug(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, fmt.Sprintf("DEBUG: %s %v", msg, args))
}

func (m *mockLogger) Info(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, fmt.Sprintf("INFO: %s %v", msg, args))
}

func (m *mockLogger) Warn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, fmt.Sprintf("WARN: %s %v", msg, args))
}

func (m *mockLogger) Error(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, fmt.Sprintf("ERROR: %s %v", msg, args))
}

func (m *mockLogger) With(args ...any) types.Logger       { return m }
func (m *mockLogger) WithModule(name string) types.Logger { return m }
func (m *mockLogger) WithError(err error) types.Logger    { return m }

func (m *mockLogger) hasErrorContaining(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.entries {
		if len(entry) >= 5 && entry[:5] == "ERROR" {
			if len(entry) > len(substr) && contains(entry, substr) {
				return true
			}
		}
	}
	return false
}

func (m *mockLogger) hasWarnContaining(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.entries {
		if len(entry) >= 4 && entry[:4] == "WARN" {
			if contains(entry, substr) {
				return true
			}
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// mockEventBus implements types.EventBus
type mockEventBus struct {
	mu                   sync.Mutex
	subscriptions        map[string][]types.MsgHandler
	queueSubscriptions   map[string]map[string][]types.MsgHandler // subject -> queue -> handlers
	publishedMessages    []publishedMessage
	publishErr           error
	publishNotify        chan struct{}
	eventStream          types.EventStream // For testing stream consumers
	eventStreamErr       error             // Error to return from EventStream()
	queueSubscribeErr    error             // Error to return from QueueSubscribe
	queueSubscribeErrFor string            // Only fail QueueSubscribe for this subject (empty = all)
}

type publishedMessage struct {
	subject string
	data    []byte
}

func newMockEventBus() *mockEventBus {
	return &mockEventBus{
		subscriptions:      make(map[string][]types.MsgHandler),
		queueSubscriptions: make(map[string]map[string][]types.MsgHandler),
		publishedMessages:  make([]publishedMessage, 0),
	}
}

func (m *mockEventBus) Publish(subject string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.publishedMessages = append(m.publishedMessages, publishedMessage{subject: subject, data: data})

	// Notify waiting tests that publish completed
	if m.publishNotify != nil {
		select {
		case m.publishNotify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *mockEventBus) getPublishedMessages(subject string) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var messages [][]byte
	for _, msg := range m.publishedMessages {
		if msg.subject == subject {
			messages = append(messages, msg.data)
		}
	}
	return messages
}

func (m *mockEventBus) setPublishError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishErr = err
}

func (m *mockEventBus) getHandlers(subject string) []types.MsgHandler {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.subscriptions[subject]
}

func (m *mockEventBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscriptions[subject] = append(m.subscriptions[subject], handler)
	return &mockSubscription{subject: subject}, nil
}

func (m *mockEventBus) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we should return an error for this subscription
	if m.queueSubscribeErr != nil {
		if m.queueSubscribeErrFor == "" || m.queueSubscribeErrFor == subject {
			return nil, m.queueSubscribeErr
		}
	}

	m.subscriptions[subject] = append(m.subscriptions[subject], handler)
	// Track queue subscriptions
	if m.queueSubscriptions[subject] == nil {
		m.queueSubscriptions[subject] = make(map[string][]types.MsgHandler)
	}
	m.queueSubscriptions[subject][queue] = append(m.queueSubscriptions[subject][queue], handler)
	return &mockSubscription{subject: subject}, nil
}

func (m *mockEventBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	return nil, nil
}

func (m *mockEventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	return nil, nil
}

func (m *mockEventBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	return nil, nil
}

// getQueueSubscriptions returns the handlers for a specific queue on a subject
func (m *mockEventBus) getQueueSubscriptions(subject, queue string) []types.MsgHandler {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queueSubscriptions[subject] == nil {
		return nil
	}
	return m.queueSubscriptions[subject][queue]
}

func (m *mockEventBus) SubscribeSync(subject string) (types.Subscription, error) {
	return &mockSubscription{subject: subject}, nil
}

func (m *mockEventBus) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	return &mockSubscription{subject: subject}, nil
}

func (m *mockEventBus) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	return &mockSubscription{subject: subject}, nil
}

func (m *mockEventBus) EventStream() (types.EventStream, error) {
	if m.eventStreamErr != nil {
		return nil, m.eventStreamErr
	}
	if m.eventStream != nil {
		return m.eventStream, nil
	}
	return nil, errors.New("not implemented")
}

func (m *mockEventBus) PublishMsg(msg *types.Msg) error {
	return nil
}

func (m *mockEventBus) SetRuntimeContext(ctx context.Context) {
	// Mock implementation - no-op for tests
}

// mockSubscription implements types.Subscription
type mockSubscription struct {
	subject string
	drained bool
}

func (m *mockSubscription) Unsubscribe() error {
	return nil
}

func (m *mockSubscription) Drain() error {
	m.drained = true
	return nil
}

func (m *mockSubscription) IsValid() bool {
	return true
}

func (m *mockSubscription) Subject() string {
	return m.subject
}

func (m *mockSubscription) Queue() string {
	return ""
}

func (m *mockSubscription) NextMsg(timeout time.Duration) (*types.Msg, error) {
	return nil, errors.New("not implemented")
}

func (m *mockSubscription) NextMsgWithContext(ctx context.Context) (*types.Msg, error) {
	return nil, errors.New("not implemented")
}

// mockAuditLogger implements types.AuditLogger
func TestLifecycleManager_StartSingleModule(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !module.wasStarted() {
		t.Error("Module was not started")
	}

}

func TestLifecycleManager_StartMultipleModulesInOrder(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	// Create modules with dependencies
	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &mockModule{name: "module-c", dependencies: []string{"module-b"}}

	if err := reg.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := reg.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := reg.Register(moduleC); err != nil {
		t.Fatalf("Failed to register module C: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// All modules should be started
	if !moduleA.wasStarted() {
		t.Error("Module A was not started")
	}
	if !moduleB.wasStarted() {
		t.Error("Module B was not started")
	}
	if !moduleC.wasStarted() {
		t.Error("Module C was not started")
	}
}

func TestLifecycleManager_StartFailureTriggersRollback(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{name: "module-b", dependencies: []string{"module-a"}, startErr: errors.New("start failed")}
	moduleC := &mockModule{name: "module-c", dependencies: []string{"module-b"}}

	if err := reg.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := reg.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := reg.Register(moduleC); err != nil {
		t.Fatalf("Failed to register module C: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	err := lm.Start(ctx)
	if err == nil {
		t.Fatal("Expected Start to fail")
	}

	// Module A should be started and then stopped (rollback)
	if !moduleA.wasStarted() {
		t.Error("Module A was not started")
	}
	if !moduleA.wasStopped() {
		t.Error("Module A was not stopped during rollback")
	}

	// Module B should be started but not stopped (it failed to start)
	if !moduleB.wasStarted() {
		t.Error("Module B was not attempted to start")
	}

	// Module C should not be started (comes after B in dependency order)
	if moduleC.wasStarted() {
		t.Error("Module C should not have been started")
	}
}

func TestLifecycleManager_StartPanicRecovery(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	module := &mockModule{name: "panic-module", startPanic: "test panic"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	err := lm.Start(ctx)
	if err == nil {
		t.Fatal("Expected Start to fail due to panic")
	}

	if !errors.Is(err, monoerrors.ErrModulePanic) {
		t.Errorf("Expected ErrModulePanic, got: %v", err)
	}
}

func TestLifecycleManager_StopModulesInReverseOrder(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := reg.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := reg.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := lm.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Both modules should be stopped
	if !moduleA.wasStopped() {
		t.Error("Module A was not stopped")
	}
	if !moduleB.wasStopped() {
		t.Error("Module B was not stopped")
	}
}

func TestLifecycleManager_StopContinuesOnError(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{name: "module-b", stopErr: errors.New("stop failed")}

	if err := reg.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := reg.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err := lm.Stop(ctx)
	if err == nil {
		t.Fatal("Expected Stop to return error")
	}

	// Both modules should be stopped despite error
	if !moduleA.wasStopped() {
		t.Error("Module A was not stopped")
	}
	if !moduleB.wasStopped() {
		t.Error("Module B was not stopped")
	}
}

func TestLifecycleManager_StopPanicRecovery(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	module := &mockModule{name: "panic-module", stopPanic: "test panic"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err := lm.Stop(ctx)
	if err == nil {
		t.Fatal("Expected Stop to fail due to panic")
	}

	if !errors.Is(err, monoerrors.ErrModulePanic) {
		t.Errorf("Expected ErrModulePanic, got: %v", err)
	}
}

func TestLifecycleManager_EventBusAwareModuleGetsEventBus(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	module := &mockEventBusAwareModule{
		mockModule: mockModule{name: "nats-module"},
	}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if module.eventBus == nil {
		t.Error("EventBus was not set on NATS-aware module")
	}
}

func TestLifecycleManager_DependencyAwareModuleGetsDependencyContainers(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	moduleA := &mockModule{name: "module-a"}
	moduleB := &mockModule{
		name:         "module-b",
		dependencies: []string{"module-a"},
	}

	if err := reg.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := reg.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if moduleB.depContainers == nil || moduleB.depContainers["module-a"] == nil {
		t.Error("Dependency container was not set on module B")
	}
}

func TestLifecycleManager_WaitForShutdown(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	// Start and stop to close shutdown channel
	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := lm.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// WaitForShutdown should return immediately
	done := make(chan error, 1)
	go func() {
		done <- lm.WaitForShutdown(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("WaitForShutdown returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("WaitForShutdown blocked unexpectedly")
	}
}

func TestLifecycleManager_WaitForShutdownContextCancellation(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := lm.WaitForShutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}
}

// TestRequestReplyResponsePublished verifies that when a RequestReply service
// handler returns a successful response, it is published to the reply subject
// specified in the incoming message.
func TestRequestReplyResponsePublished(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()
	eventBus.publishNotify = make(chan struct{}, 1)

	// Create a module with a RequestReply service
	responseData := []byte("test response")
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "test-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Subject: "test.subject",
				Type:    types.ServiceTypeRequestReply,
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return responseData, nil
				},
			},
		},
	}

	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger the handler by simulating a request
	replySubject := "test.reply"
	requestSubject := "services.test-module.test-service"
	msg := &types.Msg{
		Subject: requestSubject,
		Reply:   replySubject,
		Data:    []byte("request"),
	}

	// Get the handler and invoke it
	handlers := eventBus.getHandlers(requestSubject)
	if len(handlers) == 0 {
		t.Fatalf("No handler registered for %s", requestSubject)
	}
	handlers[0](ctx, msg)

	// Wait for publish to complete
	select {
	case <-eventBus.publishNotify:
		// Handler completed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Handler didn't publish within timeout")
	}

	// Verify the response was published to the reply subject
	publishedResponses := eventBus.getPublishedMessages(replySubject)
	if len(publishedResponses) != 1 {
		t.Fatalf("Expected 1 published response, got %d", len(publishedResponses))
	}

	if string(publishedResponses[0]) != string(responseData) {
		t.Errorf("Expected response %q, got %q", string(responseData), string(publishedResponses[0]))
	}
}

// TestRequestReplyNoResponseWhenEmpty verifies that when a RequestReply handler
// returns nil response, no message is published to prevent sending empty responses.
func TestRequestReplyNoResponseWhenEmpty(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()
	eventBus.publishNotify = make(chan struct{}, 1)

	// Create a module with a RequestReply service that returns nil response
	handlerDone := make(chan struct{})
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "test-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Subject: "test.subject",
				Type:    types.ServiceTypeRequestReply,
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					defer close(handlerDone)
					return nil, nil
				},
			},
		},
	}

	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger the handler
	replySubject := "test.reply"
	requestSubject := "services.test-module.test-service"
	msg := &types.Msg{
		Subject: requestSubject,
		Reply:   replySubject,
		Data:    []byte("request"),
	}

	handlers := eventBus.getHandlers(requestSubject)
	if len(handlers) == 0 {
		t.Fatalf("No handler registered for %s", requestSubject)
	}
	handlers[0](ctx, msg)

	// Wait for handler to complete
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(1 * time.Second):
		t.Fatal("Handler didn't complete within timeout")
	}

	// Verify no response was published
	publishedResponses := eventBus.getPublishedMessages(replySubject)
	if len(publishedResponses) != 0 {
		t.Fatalf("Expected no published response, got %d", len(publishedResponses))
	}
}

// TestRequestReplyNoResponseWhenNoReplySubject verifies that responses are not
// published when the incoming message lacks a reply subject.
func TestRequestReplyNoResponseWhenNoReplySubject(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()
	eventBus.publishNotify = make(chan struct{}, 1)

	// Create a module with a RequestReply service
	handlerDone := make(chan struct{})
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "test-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Subject: "test.subject",
				Type:    types.ServiceTypeRequestReply,
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					defer close(handlerDone)
					return []byte("response"), nil
				},
			},
		},
	}

	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger the handler without a reply subject
	requestSubject := "services.test-module.test-service"
	msg := &types.Msg{
		Subject: requestSubject,
		Reply:   "", // Empty reply subject
		Data:    []byte("request"),
	}

	handlers := eventBus.getHandlers(requestSubject)
	if len(handlers) == 0 {
		t.Fatalf("No handler registered for %s", requestSubject)
	}
	handlers[0](ctx, msg)

	// Wait for handler to complete
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(1 * time.Second):
		t.Fatal("Handler didn't complete within timeout")
	}

	// Verify no messages were published at all
	if len(eventBus.publishedMessages) != 0 {
		t.Fatalf("Expected no published messages, got %d", len(eventBus.publishedMessages))
	}
}

// TestRequestReplyPublishError verifies that publish errors are logged and
// handled gracefully without causing the handler to panic.
func TestRequestReplyPublishError(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()
	eventBus.publishNotify = make(chan struct{}, 1)

	// Set up the event bus to return an error on Publish
	publishError := errors.New("publish failed")
	eventBus.setPublishError(publishError)

	// Create a module with a RequestReply service
	handlerDone := make(chan struct{})
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "test-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Subject: "test.subject",
				Type:    types.ServiceTypeRequestReply,
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					defer close(handlerDone)
					return []byte("response"), nil
				},
			},
		},
	}

	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger the handler
	replySubject := "test.reply"
	requestSubject := "services.test-module.test-service"
	msg := &types.Msg{
		Subject: requestSubject,
		Reply:   replySubject,
		Data:    []byte("request"),
	}

	handlers := eventBus.getHandlers(requestSubject)
	if len(handlers) == 0 {
		t.Fatalf("No handler registered for %s", requestSubject)
	}
	handlers[0](ctx, msg)

	// Wait for handler to complete
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(1 * time.Second):
		t.Fatal("Handler didn't complete within timeout")
	}

	// Verify error was logged
	if !logger.hasErrorContaining("Failed to publish response") {
		t.Errorf("Expected error log for publish failure")
	}
}

// TestRequestReplyUsesQueueSubscribe verifies that RequestReply services
// use QueueSubscribe with the service name as the queue group.
func TestRequestReplyUsesQueueSubscribe(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	logger := &mockLogger{}
	eventBus := newMockEventBus()

	// Create a module with a RequestReply service
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "inventory"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:       "check-stock",
				Subject:    "services.inventory.check-stock",
				Type:       types.ServiceTypeRequestReply,
				QueueGroup: "check-stock",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return []byte("in-stock"), nil
				},
			},
		},
	}

	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)
	ctx := context.Background()

	if err := lm.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify QueueSubscribe was called with service name as queue group
	requestSubject := "services.inventory.check-stock"
	queueHandlers := eventBus.getQueueSubscriptions(requestSubject, "check-stock")
	if len(queueHandlers) == 0 {
		t.Errorf("Expected QueueSubscribe to be called with queue group 'check-stock'")
	}
}

// TestGetRuntimeContext tests that GetRuntimeContext returns a non-nil context
func TestGetRuntimeContext(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	eventBus := newMockEventBus()
	logger := &mockLogger{}

	manager := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)

	ctx := manager.GetRuntimeContext()
	if ctx == nil {
		t.Fatal("GetRuntimeContext() returned nil")
	}

	// Verify the context is not yet cancelled
	select {
	case <-ctx.Done():
		t.Error("Runtime context is already cancelled")
	default:
		// Context is not cancelled, as expected
	}
}

// TestRuntimeContextCancelledOnStop tests that the runtime context is cancelled when Stop() is called
func TestRuntimeContextCancelledOnStop(t *testing.T) {
	reg := registry.NewModuleRegistry(&mockLogger{})
	eventBus := newMockEventBus()
	logger := &mockLogger{}

	manager := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, nil, logger, 0)

	// Get the runtime context before stopping
	runtimeCtx := manager.GetRuntimeContext()
	if runtimeCtx == nil {
		t.Fatal("GetRuntimeContext() returned nil")
	}

	// Verify context is not cancelled initially
	select {
	case <-runtimeCtx.Done():
		t.Fatal("Runtime context is already cancelled before Stop()")
	default:
		// Expected
	}

	// Call Stop()
	ctx := context.Background()
	err := manager.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}

	// Verify the runtime context is now cancelled
	select {
	case <-runtimeCtx.Done():
		// Expected - context should be cancelled
		if runtimeCtx.Err() != context.Canceled {
			t.Errorf("Expected context.Canceled error, got: %v", runtimeCtx.Err())
		}
	case <-time.After(1 * time.Second):
		t.Error("Runtime context was not cancelled after Stop()")
	}
}

// mockEventRegistry implements types.EventRegistry for testing
type mockEventRegistry struct {
	consumerEntries       []types.EventConsumerEntry
	streamConsumerEntries []types.EventStreamConsumerEntry
}

func newMockEventRegistry() *mockEventRegistry {
	return &mockEventRegistry{
		consumerEntries:       make([]types.EventConsumerEntry, 0),
		streamConsumerEntries: make([]types.EventStreamConsumerEntry, 0),
	}
}

func (m *mockEventRegistry) RegisterEvent(def types.BaseEventDefinition) error {
	return nil
}

func (m *mockEventRegistry) GetEventsByModule(moduleName string) []types.BaseEventDefinition {
	return nil
}

func (m *mockEventRegistry) GetEventByName(name string, version string, moduleName string) (types.BaseEventDefinition, bool) {
	return types.BaseEventDefinition{}, false
}

func (m *mockEventRegistry) GetAllEvents() []types.BaseEventDefinition {
	return nil
}

func (m *mockEventRegistry) RegisterEventConsumer(eventDef types.BaseEventDefinition, handler types.EventConsumerHandler, module types.Module, queueGroup ...string) error {
	return nil
}

func (m *mockEventRegistry) Entries() []types.EventConsumerEntry {
	return m.consumerEntries
}

func (m *mockEventRegistry) RegisterEventStreamConsumer(eventDef types.BaseEventDefinition, config types.StreamConsumerConfig, handler types.EventStreamConsumerHandler, module types.Module) error {
	return nil
}

func (m *mockEventRegistry) StreamConsumerEntries() []types.EventStreamConsumerEntry {
	return m.streamConsumerEntries
}

func (m *mockEventRegistry) SetMiddlewareChain(chain types.MiddlewareChainRunner) {
}

// TestGetServiceContainer tests retrieving service container for a module
func TestGetServiceContainer(t *testing.T) {
	t.Run("returns container for existing module", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

		module := &mockModule{name: "test-module"}
		if err := reg.Register(module); err != nil {
			t.Fatal(err)
		}

		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		container := lm.GetServiceContainer("test-module")
		if container == nil {
			t.Error("expected container, got nil")
		}
	})

	t.Run("returns nil for non-existent module", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

		container := lm.GetServiceContainer("non-existent")
		if container != nil {
			t.Error("expected nil for non-existent module")
		}
	})
}

// TestGetMiddlewareHook tests the middleware hook getter
func TestGetMiddlewareHook(t *testing.T) {
	t.Run("returns hook function", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)
		hook := lm.GetMiddlewareHook()

		if hook == nil {
			t.Fatal("expected hook function, got nil")
		}

		// Call hook to ensure it doesn't panic
		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test",
			Metadata:   make(map[string]any),
		}
		hook(context.Background(), event)
	})
}

// TestSanitizeConsumerName tests consumer name sanitization
func TestSanitizeConsumerName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "alphanumeric only",
			input:    "test123",
			expected: "test123",
		},
		{
			name:     "with spaces",
			input:    "test consumer name",
			expected: "test-consumer-name",
		},
		{
			name:     "with special characters",
			input:    "test@consumer#name",
			expected: "testconsumername",
		},
		{
			name:     "with allowed separators",
			input:    "test-consumer_name.v1",
			expected: "test-consumer_name.v1",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "consumer",
		},
		{
			name:     "only invalid characters",
			input:    "@#$%",
			expected: "consumer",
		},
		{
			name:     "mixed valid and invalid",
			input:    "test!@#$%consumer&*()name",
			expected: "testconsumername",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeConsumerName(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeConsumerName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestQueueGroupServiceSetup tests queue group service NATS subscription setup
func TestQueueGroupServiceSetup(t *testing.T) {
	t.Run("queue group service subscriptions are created", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		// Module with queue group service
		module := &mockModuleWithServices{
			mockModule: mockModule{name: "test"},
			serviceEntries: []*types.ServiceEntry{
				{
					Name:    "test-queue-service",
					Type:    types.ServiceTypeQueueGroup,
					Subject: "test.queue.service",
					QueueHandlers: []types.QGHP{
						{
							QueueGroup: "workers",
							Handler:    func(ctx context.Context, msg *types.Msg) error { return nil },
						},
					},
				},
			},
		}

		if err := reg.Register(module); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify subscription was created
		eventBus.mu.Lock()
		subCount := len(eventBus.subscriptions)
		eventBus.mu.Unlock()

		if subCount == 0 {
			t.Error("expected subscriptions to be created")
		}

		// Cleanup
		if err := lm.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
}

// TestSetupNATSSubscriptionsWithEventConsumers tests event consumer subscription setup
func TestSetupNATSSubscriptionsWithEventConsumers(t *testing.T) {
	t.Run("event consumer subscriptions are created", func(t *testing.T) {
		reg := registry.NewModuleRegistry(&mockLogger{})
		pluginReg := registry.NewPluginRegistry(&mockLogger{})
		logger := &mockLogger{}
		eventReg := newMockEventRegistry()
		eventBus := newMockEventBus()

		// Add mock event consumer entry
		eventReg.consumerEntries = []types.EventConsumerEntry{
			{
				EventDef: types.BaseEventDefinition{
					Name:       "TestEvent",
					Version:    "v1",
					ModuleName: "test",
					Subject:    "events.test.v1.test-event",
				},
				Handler:    func(ctx context.Context, msg *types.Msg) error { return nil },
				Module:     &mockModule{name: "consumer"},
				QueueGroup: "consumers",
			},
		}

		module := &mockModule{name: "test"}
		if err := reg.Register(module); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify event consumer subscription was created
		eventBus.mu.Lock()
		subCount := len(eventBus.subscriptions)
		eventBus.mu.Unlock()

		if subCount == 0 {
			t.Error("expected event consumer subscriptions to be created")
		}

		// Cleanup
		if err := lm.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
}

// ============================================================================
// Mocks for Stream Consumer Tests
// ============================================================================

// mockEventStream implements types.EventStream for testing
type mockEventStream struct {
	createStreamErr   error
	createConsumerErr error
	createdStreams    []string
	createdConsumers  []string
	mu                sync.Mutex
}

func (m *mockEventStream) Publish(ctx context.Context, subject string, data []byte) (types.MsgPubAck, error) {
	return nil, nil
}

func (m *mockEventStream) PublishMsg(ctx context.Context, msg *types.Msg) (types.MsgPubAck, error) {
	return nil, nil
}

func (m *mockEventStream) CreateOrUpdateStream(ctx context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	if m.createStreamErr != nil {
		return nil, m.createStreamErr
	}
	m.mu.Lock()
	m.createdStreams = append(m.createdStreams, cfg.Name)
	m.mu.Unlock()
	return nil, nil // Return nil stream for now - not needed for these tests
}

func (m *mockEventStream) CreateOrUpdateConsumer(ctx context.Context, streamName string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	if m.createConsumerErr != nil {
		return nil, m.createConsumerErr
	}
	m.mu.Lock()
	m.createdConsumers = append(m.createdConsumers, streamName+"/"+cfg.Name)
	m.mu.Unlock()
	return &mockJetStreamConsumer{}, nil
}

func (m *mockEventStream) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	return nil, nil
}

func (m *mockEventStream) DeleteStream(ctx context.Context, name string) error {
	return nil
}

// mockJetStreamConsumer implements jetstream.Consumer for testing
type mockJetStreamConsumer struct {
	fetchErr      error
	fetchMessages []jetstream.Msg
	fetchTimeout  bool
	batchErr      error // Error to return from MessageBatch.Error()
	mu            sync.Mutex
}

func (m *mockJetStreamConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	if m.fetchTimeout {
		return nil, nats.ErrTimeout
	}
	return &mockMessageBatch{messages: m.fetchMessages, err: m.batchErr}, nil
}

func (m *mockJetStreamConsumer) FetchBytes(maxBytes int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return m.Fetch(0, opts...)
}

func (m *mockJetStreamConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	return m.Fetch(batch)
}

func (m *mockJetStreamConsumer) Next(opts ...jetstream.FetchOpt) (jetstream.Msg, error) {
	return nil, nil
}

func (m *mockJetStreamConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return nil, nil
}

func (m *mockJetStreamConsumer) Messages(opts ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return nil, nil
}

func (m *mockJetStreamConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return nil, nil
}

func (m *mockJetStreamConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return nil
}

// mockMessageBatch implements jetstream.MessageBatch for testing
type mockMessageBatch struct {
	messages []jetstream.Msg
	err      error
}

func (m *mockMessageBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(m.messages))
	for _, msg := range m.messages {
		if msg != nil {
			ch <- msg
		}
	}
	close(ch)
	return ch
}

func (m *mockMessageBatch) Error() error {
	return m.err
}

// Test setupStreamConsumer function
func TestSetupStreamConsumer(t *testing.T) {
	t.Run("successful setup with defaults", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream-consumer",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "TEST_STREAM",
					Subjects: []string{"test.>"},
				},
				Consumer: types.ConsumerConfig{},
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   5 * time.Second,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := lm.setupStreamConsumer(ctx, entry)
		if err != nil {
			t.Fatalf("setupStreamConsumer failed: %v", err)
		}

		// Verify stream consumer was registered
		lm.mu.RLock()
		cancelFunc, exists := lm.streamConsumers["test-stream-consumer"]
		lm.mu.RUnlock()
		if !exists {
			t.Error("stream consumer should be registered")
		}

		// Verify stream and consumer were created
		es := eventBus.eventStream.(*mockEventStream)
		es.mu.Lock()
		if len(es.createdStreams) != 1 || es.createdStreams[0] != "TEST_STREAM" {
			t.Errorf("expected stream TEST_STREAM to be created, got %v", es.createdStreams)
		}
		if len(es.createdConsumers) == 0 {
			t.Error("expected consumer to be created")
		}
		es.mu.Unlock()

		// Clean up: cancel goroutine and wait for cleanup
		cancelFunc()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("nil config error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := &types.ServiceEntry{
			Name:                 "test-stream-consumer",
			StreamConsumerConfig: nil, // Nil config
		}

		err := lm.setupStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupStreamConsumer should fail with nil config")
		}
		if !strings.Contains(err.Error(), "stream consumer config is nil") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("EventStream error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStreamErr: errors.New("JetStream not available"),
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream-consumer",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "TEST_STREAM",
					Subjects: []string{"test.>"},
				},
			},
		}

		err := lm.setupStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupStreamConsumer should fail when EventStream() fails")
		}
		if !strings.Contains(err.Error(), "failed to get JetStream") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("CreateOrUpdateStream error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{
				createStreamErr: errors.New("stream creation failed"),
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream-consumer",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "TEST_STREAM",
					Subjects: []string{"test.>"},
				},
			},
		}

		err := lm.setupStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupStreamConsumer should fail when CreateOrUpdateStream fails")
		}
		if !strings.Contains(err.Error(), "failed to create stream") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("CreateOrUpdateConsumer error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{
				createConsumerErr: errors.New("consumer creation failed"),
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream-consumer",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "TEST_STREAM",
					Subjects: []string{"test.>"},
				},
			},
		}

		err := lm.setupStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupStreamConsumer should fail when CreateOrUpdateConsumer fails")
		}
		if !strings.Contains(err.Error(), "failed to create consumer") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// Test setupEventStreamConsumer function
func TestSetupEventStreamConsumer(t *testing.T) {
	t.Run("successful setup", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
				Subject:    "events.test.v1",
			},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "EVENTS",
					Subjects: []string{"events.test.v1"},
				},
				Consumer: types.ConsumerConfig{},
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   5 * time.Second,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			SequenceID: 1,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := lm.setupEventStreamConsumer(ctx, entry)
		if err != nil {
			t.Fatalf("setupEventStreamConsumer failed: %v", err)
		}

		// Verify stream consumer was registered with sequence-based key
		lm.mu.RLock()
		cancelFunc, exists := lm.streamConsumers["event-stream-1"]
		lm.mu.RUnlock()
		if !exists {
			t.Error("event stream consumer should be registered")
		}

		// Clean up: cancel goroutine and wait for cleanup
		cancelFunc()
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("EventStream error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStreamErr: errors.New("JetStream not available"),
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "EVENTS",
					Subjects: []string{"events.test.v1"},
				},
			},
			SequenceID: 1,
		}

		err := lm.setupEventStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupEventStreamConsumer should fail when EventStream() fails")
		}
	})

	t.Run("CreateOrUpdateStream error", func(t *testing.T) {
		logger := &mockLogger{}
		eventBus := &mockEventBus{
			eventStream: &mockEventStream{
				createStreamErr: errors.New("stream creation failed"),
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			eventBus:        eventBus,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "EVENTS",
					Subjects: []string{"events.test.v1"},
				},
			},
			SequenceID: 1,
		}

		err := lm.setupEventStreamConsumer(context.Background(), entry)
		if err == nil {
			t.Error("setupEventStreamConsumer should fail when CreateOrUpdateStream fails")
		}
	})
}

// Test runStreamConsumerLoop function
func TestRunStreamConsumerLoop(t *testing.T) {
	t.Run("context cancellation stops loop", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give loop time to start
		time.Sleep(50 * time.Millisecond)

		// Cancel context to stop loop
		cancel()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop exited
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after context cancellation")
		}
	})

	t.Run("fetch timeout is handled gracefully", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchTimeout: true, // Will return nats.ErrTimeout
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Wait for context timeout
		<-ctx.Done()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop handled timeout gracefully
		case <-time.After(1 * time.Second):
			t.Fatal("loop did not exit gracefully")
		}
	})

	t.Run("fetch error triggers backoff", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchErr: errors.New("fetch failed"),
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Wait for context timeout
		<-ctx.Done()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop handled errors and backoff
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after context cancellation")
		}
	})

	t.Run("handler error is logged", func(t *testing.T) {
		logger := &mockLogger{}

		handlerErr := errors.New("handler failed")
		handlerCalled := false

		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("test")}},
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				handlerCalled = true
				return handlerErr
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give handler time to be called
		time.Sleep(50 * time.Millisecond)

		// Cancel context
		cancel()

		// Wait for loop to exit
		<-done

		if !handlerCalled {
			t.Error("handler should have been called")
		}
	})
}

// Test runEventStreamConsumerLoop function
func TestRunEventStreamConsumerLoop(t *testing.T) {
	t.Run("context cancellation stops loop", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give loop time to start
		time.Sleep(50 * time.Millisecond)

		// Cancel context to stop loop
		cancel()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop exited
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after context cancellation")
		}
	})

	t.Run("fetch timeout is handled gracefully", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchTimeout: true,
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Wait for context timeout
		<-ctx.Done()

		// Wait for loop to exit
		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("loop did not exit gracefully")
		}
	})

	t.Run("handler error is logged", func(t *testing.T) {
		logger := &mockLogger{}

		handlerErr := errors.New("handler failed")
		handlerCalled := false

		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("test")}},
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				handlerCalled = true
				return handlerErr
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give handler time to be called
		time.Sleep(50 * time.Millisecond)

		// Cancel context
		cancel()

		// Wait for loop to exit
		<-done

		if !handlerCalled {
			t.Error("handler should have been called")
		}
	})
}

// mockJetStreamMsg implements jetstream.Msg for testing
type mockJetStreamMsg struct {
	data []byte
}

func (m *mockJetStreamMsg) Data() []byte {
	return m.data
}

func (m *mockJetStreamMsg) Headers() nats.Header {
	return nil
}

func (m *mockJetStreamMsg) Subject() string {
	return "test.subject"
}

func (m *mockJetStreamMsg) Reply() string {
	return ""
}

func (m *mockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, nil
}

func (m *mockJetStreamMsg) Ack() error {
	return nil
}

func (m *mockJetStreamMsg) DoubleAck(ctx context.Context) error {
	return nil
}

func (m *mockJetStreamMsg) Nak() error {
	return nil
}

func (m *mockJetStreamMsg) NakWithDelay(delay time.Duration) error {
	return nil
}

func (m *mockJetStreamMsg) InProgress() error {
	return nil
}

func (m *mockJetStreamMsg) Term() error {
	return nil
}

func (m *mockJetStreamMsg) TermWithReason(reason string) error {
	return nil
}

// Additional tests for better coverage of loop functions
func TestRunStreamConsumerLoopAdditionalCoverage(t *testing.T) {
	t.Run("empty batch is skipped", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{}, // Empty batch
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				t.Error("handler should not be called for empty batch")
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Wait for context timeout
		<-ctx.Done()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop handled empty batch
		case <-time.After(1 * time.Second):
			t.Fatal("loop did not exit")
		}
	})

	t.Run("fetch batch error is logged", func(t *testing.T) {
		logger := &mockLogger{}

		batchErr := errors.New("batch error")
		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("test")}},
			batchErr:      batchErr,
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give time for batch error to be logged
		time.Sleep(50 * time.Millisecond)

		// Cancel context
		cancel()

		// Wait for loop to exit
		<-done
	})

	t.Run("context cancelled during backoff", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchErr: errors.New("fetch failed"),
		}

		entry := &types.ServiceEntry{
			Name:       "test-stream",
			ModuleName: "testmodule",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give time for error and backoff to start
		time.Sleep(50 * time.Millisecond)

		// Cancel during backoff
		cancel()

		// Wait for loop to exit
		select {
		case <-done:
			// Success - loop handled cancellation during backoff
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after cancellation during backoff")
		}
	})
}

func TestRunEventStreamConsumerLoopAdditionalCoverage(t *testing.T) {
	t.Run("empty batch is skipped", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{}, // Empty batch
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				t.Error("handler should not be called for empty batch")
				return nil
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Wait for context timeout
		<-ctx.Done()

		// Wait for loop to exit
		select {
		case <-done:
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("loop did not exit")
		}
	})

	t.Run("fetch batch error is logged", func(t *testing.T) {
		logger := &mockLogger{}

		batchErr := errors.New("batch error")
		consumer := &mockJetStreamConsumer{
			fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("test")}},
			batchErr:      batchErr,
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give time for batch error to be logged
		time.Sleep(50 * time.Millisecond)

		// Cancel context
		cancel()

		// Wait for loop to exit
		<-done
	})

	t.Run("context cancelled during backoff", func(t *testing.T) {
		logger := &mockLogger{}
		consumer := &mockJetStreamConsumer{
			fetchErr: errors.New("fetch failed"),
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "testmodule",
				Name:       "TestEvent",
				Version:    "v1",
			},
			Config: types.StreamConsumerConfig{
				Fetch: types.FetchConfig{
					BatchSize: 10,
					Timeout:   100 * time.Millisecond,
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			SequenceID: 1,
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
			mu:              sync.RWMutex{},
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Run loop in goroutine
		done := make(chan struct{})
		go func() {
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
			close(done)
		}()

		// Give time for error and backoff to start
		time.Sleep(50 * time.Millisecond)

		// Cancel during backoff
		cancel()

		// Wait for loop to exit
		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after cancellation during backoff")
		}
	})
}

// ============================================================================
// Tests for Task 13: Stop and Rollback Functions
// ============================================================================

// mockPluginWithServices implements types.PluginModule and types.ServiceProviderModule
type mockPluginWithServices struct {
	name                string
	container           types.ServiceContainer
	startErr            error
	stopErr             error
	startPanic          any
	stopPanic           any
	registerServicesErr error
	servicesCalled      bool
	startCalled         bool
	stopCalled          bool
	mu                  sync.Mutex
}

func (m *mockPluginWithServices) Name() string {
	return m.name
}

func (m *mockPluginWithServices) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	if m.startPanic != nil {
		panic(m.startPanic)
	}
	return m.startErr
}

func (m *mockPluginWithServices) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	if m.stopPanic != nil {
		panic(m.stopPanic)
	}
	return m.stopErr
}

func (m *mockPluginWithServices) SetContainer(container types.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.container = container
}

func (m *mockPluginWithServices) Container() types.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.container
}

func (m *mockPluginWithServices) RegisterServices(container types.ServiceContainer) error {
	m.servicesCalled = true
	return m.registerServicesErr
}

func (m *mockPluginWithServices) wasStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalled
}

func (m *mockPluginWithServices) wasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

// Test stopPlugin function
func TestStopPlugin(t *testing.T) {
	t.Run("successful plugin stop", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)

		plugin := &mockPluginWithServices{
			name: "test-plugin",
		}

		if err := pluginReg.Register(plugin, "test-alias"); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopPlugin(context.Background(), plugin, "test-alias")
		if err != nil {
			t.Errorf("stopPlugin() should succeed, got error: %v", err)
		}

		if !plugin.wasStopped() {
			t.Error("plugin should be stopped")
		}
	})

	t.Run("plugin stop error is wrapped and logged", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)

		stopErr := errors.New("stop failed")
		plugin := &mockPluginWithServices{
			name:    "failing-plugin",
			stopErr: stopErr,
		}

		if err := pluginReg.Register(plugin, "fail-alias"); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopPlugin(context.Background(), plugin, "fail-alias")
		if err == nil {
			t.Error("stopPlugin() should return error")
		}

		if !strings.Contains(err.Error(), "stop failed") {
			t.Errorf("error should contain original error message, got: %v", err)
		}

		// Verify error was logged
		if !logger.hasErrorContaining("Plugin stop failed") {
			t.Error("expected error log for plugin stop failure")
		}
	})

	t.Run("plugin panic during stop is recovered and logged", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)

		plugin := &mockPluginWithServices{
			name:      "panic-plugin",
			stopPanic: "test panic",
		}

		if err := pluginReg.Register(plugin, "panic-alias"); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopPlugin(context.Background(), plugin, "panic-alias")
		if err == nil {
			t.Error("stopPlugin() should return error on panic")
		}

		if !strings.Contains(err.Error(), "panic") {
			t.Errorf("error should indicate panic, got: %v", err)
		}

		// Verify panic was logged
		if !logger.hasErrorContaining("Plugin panic during stop") {
			t.Error("expected error log for plugin panic")
		}
	})
}

// Test stopMiddlewareModule function
func TestStopMiddlewareModule(t *testing.T) {
	t.Run("successful middleware stop", func(t *testing.T) {
		logger := &mockLogger{}

		middleware := &mockMiddlewareModule{
			mockModule: mockModule{name: "test-middleware"},
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopMiddlewareModule(context.Background(), middleware)
		if err != nil {
			t.Errorf("stopMiddlewareModule() should succeed, got error: %v", err)
		}

		if !middleware.wasStopped() {
			t.Error("middleware should be stopped")
		}
	})

	t.Run("middleware stop error is wrapped and logged", func(t *testing.T) {
		logger := &mockLogger{}

		stopErr := errors.New("middleware stop failed")
		middleware := &mockMiddlewareModule{
			mockModule: mockModule{
				name:    "failing-middleware",
				stopErr: stopErr,
			},
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopMiddlewareModule(context.Background(), middleware)
		if err == nil {
			t.Error("stopMiddlewareModule() should return error")
		}

		if !strings.Contains(err.Error(), "middleware stop failed") {
			t.Errorf("error should contain original error message, got: %v", err)
		}

		// Verify error was logged
		if !logger.hasErrorContaining("Middleware module stop failed") {
			t.Error("expected error log for middleware stop failure")
		}
	})

	t.Run("middleware panic during stop is recovered and logged", func(t *testing.T) {
		logger := &mockLogger{}

		middleware := &mockMiddlewareModule{
			mockModule: mockModule{
				name:      "panic-middleware",
				stopPanic: "middleware panic",
			},
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).stopMiddlewareModule(context.Background(), middleware)
		if err == nil {
			t.Error("stopMiddlewareModule() should return error on panic")
		}

		if !errors.Is(err, monoerrors.ErrModulePanic) {
			t.Errorf("error should be ErrModulePanic, got: %v", err)
		}

		// Verify panic was logged
		if !logger.hasErrorContaining("Middleware module panic during stop") {
			t.Error("expected error log for middleware panic")
		}
	})
}

// Test rollback function
func TestRollback(t *testing.T) {
	t.Run("rollback stops all started modules in reverse order", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)

		var stopOrder []string
		moduleA := &mockModule{name: "module-a", stopOrder: &stopOrder}
		moduleB := &mockModule{name: "module-b", stopOrder: &stopOrder}
		moduleC := &mockModule{name: "module-c", stopOrder: &stopOrder}

		if err := reg.Register(moduleA); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(moduleB); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(moduleC); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			reg,
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		started := []string{"module-a", "module-b", "module-c"}
		originalErr := errors.New("module start failed")

		err := lm.(*lifecycleManager).rollback(context.Background(), started, originalErr)

		// Verify error contains original error
		if err == nil {
			t.Fatal("rollback should return error")
		}
		if !strings.Contains(err.Error(), "module start failed") {
			t.Errorf("rollback error should wrap original error, got: %v", err)
		}

		// Verify modules were stopped in reverse order (c, b, a)
		if len(stopOrder) != 3 {
			t.Fatalf("expected 3 modules to be stopped, got %d", len(stopOrder))
		}
		if stopOrder[0] != "module-c" || stopOrder[1] != "module-b" || stopOrder[2] != "module-a" {
			t.Errorf("expected reverse order [c, b, a], got %v", stopOrder)
		}
	})

	t.Run("rollback continues on module stop error", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)

		var stopOrder []string
		moduleA := &mockModule{name: "module-a", stopOrder: &stopOrder}
		moduleB := &mockModule{
			name:      "module-b",
			stopErr:   errors.New("stop failed"),
			stopOrder: &stopOrder,
		}
		moduleC := &mockModule{name: "module-c", stopOrder: &stopOrder}

		if err := reg.Register(moduleA); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(moduleB); err != nil {
			t.Fatal(err)
		}
		if err := reg.Register(moduleC); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			reg,
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		started := []string{"module-a", "module-b", "module-c"}
		originalErr := errors.New("original failure")

		err := lm.(*lifecycleManager).rollback(context.Background(), started, originalErr)

		// Verify rollback completed despite module-b stop error
		if err == nil {
			t.Fatal("rollback should return error")
		}

		// All three modules should be stopped even though module-b failed
		if len(stopOrder) != 3 {
			t.Errorf("expected all 3 modules to be stopped, got %d", len(stopOrder))
		}

		// Verify error was logged for module-b
		if !logger.hasErrorContaining("Failed to stop module during rollback") {
			t.Error("expected error log for module stop failure during rollback")
		}
	})

	t.Run("rollback handles module not found error", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)

		// Only register module-a, not module-b
		moduleA := &mockModule{name: "module-a"}
		if err := reg.Register(moduleA); err != nil {
			t.Fatal(err)
		}

		lm := NewLifecycleManager(
			reg,
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		// Try to rollback with non-existent module-b
		started := []string{"module-a", "module-b"}
		originalErr := errors.New("original failure")

		err := lm.(*lifecycleManager).rollback(context.Background(), started, originalErr)

		// Verify rollback completed
		if err == nil {
			t.Fatal("rollback should return error")
		}

		// Verify error was logged for module not found
		if !logger.hasErrorContaining("Failed to get module during rollback") {
			t.Error("expected error log for module not found during rollback")
		}
	})

	t.Run("rollback with empty started list", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)

		lm := NewLifecycleManager(
			reg,
			registry.NewPluginRegistry(logger),
			newMockEventBus(),
			nil,
			logger,
			0,
		)

		originalErr := errors.New("original failure")
		err := lm.(*lifecycleManager).rollback(context.Background(), []string{}, originalErr)

		// Should return wrapped error even with empty list
		if err == nil {
			t.Fatal("rollback should return error")
		}
		if !strings.Contains(err.Error(), "original failure") {
			t.Errorf("should wrap original error, got: %v", err)
		}
	})
}

// Test startPlugin function
func TestStartPlugin(t *testing.T) {
	t.Run("successful plugin start", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		plugin := &mockPluginWithServices{
			name: "test-plugin",
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "test-alias")
		if err != nil {
			t.Errorf("startPlugin() should succeed, got error: %v", err)
		}

		if plugin.Container() == nil {
			t.Error("plugin container should be set")
		}

		if !plugin.wasStarted() {
			t.Error("plugin should be started")
		}
	})

	t.Run("plugin start error is wrapped", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		startErr := errors.New("plugin start failed")
		plugin := &mockPluginWithServices{
			name:     "failing-plugin",
			startErr: startErr,
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "fail-alias")
		if err == nil {
			t.Error("startPlugin() should return error")
		}

		if !strings.Contains(err.Error(), "plugin start failed") {
			t.Errorf("error should contain original error message, got: %v", err)
		}
	})

	t.Run("plugin panic during start is recovered", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		plugin := &mockPluginWithServices{
			name:       "panic-plugin",
			startPanic: "start panic",
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "panic-alias")
		if err == nil {
			t.Error("startPlugin() should return error on panic")
		}

		if !strings.Contains(err.Error(), "panic") {
			t.Errorf("error should indicate panic, got: %v", err)
		}

		// Verify panic was logged
		if !logger.hasErrorContaining("Plugin panic during start") {
			t.Error("expected error log for plugin panic")
		}
	})

	t.Run("plugin with service registration", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		plugin := &mockPluginWithServices{
			name: "service-plugin",
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "service-alias")
		if err != nil {
			t.Errorf("startPlugin() should succeed, got error: %v", err)
		}

		if !plugin.servicesCalled {
			t.Error("RegisterServices() should be called")
		}

		if !plugin.wasStarted() {
			t.Error("plugin should be started")
		}
	})

	t.Run("service registration error stops plugin start", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		regErr := errors.New("service registration failed")
		plugin := &mockPluginWithServices{
			name:                "reg-fail-plugin",
			registerServicesErr: regErr,
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "reg-fail-alias")
		if err == nil {
			t.Error("startPlugin() should return error on service registration failure")
		}

		if !strings.Contains(err.Error(), "service registration failed") {
			t.Errorf("error should contain registration error, got: %v", err)
		}

		if plugin.wasStarted() {
			t.Error("plugin should not be started after registration failure")
		}
	})

	t.Run("plugin implements EventBusAwareModule", func(t *testing.T) {
		logger := &mockLogger{}
		pluginReg := registry.NewPluginRegistry(logger)
		eventBus := newMockEventBus()

		// Create plugin that implements EventBusAwareModule
		plugin := &mockEventBusAwarePluginModule{
			name: "eventbus-plugin",
		}

		lm := NewLifecycleManager(
			registry.NewModuleRegistry(logger),
			pluginReg,
			eventBus,
			nil,
			logger,
			0,
		)

		err := lm.(*lifecycleManager).startPlugin(context.Background(), plugin, "eventbus-alias")
		if err != nil {
			t.Errorf("startPlugin() should succeed, got error: %v", err)
		}

		if plugin.eventBus == nil {
			t.Error("EventBus should be set on EventBusAwareModule plugin")
		}

		if !plugin.wasStarted() {
			t.Error("plugin should be started")
		}
	})
}

// mockEventBusAwarePluginModule implements both PluginModule and EventBusAwareModule
type mockEventBusAwarePluginModule struct {
	name       string
	container  types.ServiceContainer
	startErr   error
	stopErr    error
	startPanic any
	stopPanic  any
	eventBus   types.EventBus
	mu         sync.Mutex
}

func (m *mockEventBusAwarePluginModule) Name() string {
	return m.name
}

func (m *mockEventBusAwarePluginModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startPanic != nil {
		panic(m.startPanic)
	}
	return m.startErr
}

func (m *mockEventBusAwarePluginModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopPanic != nil {
		panic(m.stopPanic)
	}
	return m.stopErr
}

func (m *mockEventBusAwarePluginModule) SetContainer(container types.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.container = container
}

func (m *mockEventBusAwarePluginModule) Container() types.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.container
}

func (m *mockEventBusAwarePluginModule) SetEventBus(eventBus types.EventBus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventBus = eventBus
}

func (m *mockEventBusAwarePluginModule) wasStarted() bool {
	return m.container != nil
}

// mockEventBusWithSubscriptionError is an event bus that can fail on QueueSubscribe
type mockEventBusWithSubscriptionError struct {
	mockEventBus
	queueSubscribeErr error
}

func (m *mockEventBusWithSubscriptionError) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	if m.queueSubscribeErr != nil {
		return nil, m.queueSubscribeErr
	}
	return m.mockEventBus.QueueSubscribe(subject, queue, handler)
}

// TestLifecycleManager_Start_SubscriptionError tests Start() when subscription fails
func TestLifecycleManager_Start_SubscriptionError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := &mockEventBusWithSubscriptionError{
		mockEventBus:      *newMockEventBus(),
		queueSubscribeErr: errors.New("subscription failed"),
	}
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	// Create a module with request-reply service that will trigger subscription
	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Type:    types.ServiceTypeRequestReply,
				Subject: "services.service-module.test-service",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return []byte("response"), nil
				},
				QueueGroup: "service-module",
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start should fail because subscription fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when subscription fails")
	}

	if !strings.Contains(err.Error(), "subscription failed") {
		t.Errorf("Expected error to contain 'subscription failed', got: %v", err)
	}
}

// TestLifecycleManager_Stop_NoModules tests Stop() when no modules registered
func TestLifecycleManager_Stop_NoModules(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Start the lifecycle manager
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop should succeed even with no modules
	err := lm.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop should succeed with no modules, got: %v", err)
	}
}

// TestLifecycleManager_Stop_MultipleModulesWithErrors tests Stop() with multiple module errors
func TestLifecycleManager_Stop_MultipleModulesWithErrors(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Create modules that fail to stop
	failingModule1 := &mockModule{
		name:    "failing-module-1",
		stopErr: errors.New("module 1 stop failed"),
	}
	failingModule2 := &mockModule{
		name:    "failing-module-2",
		stopErr: errors.New("module 2 stop failed"),
	}

	if err := reg.Register(failingModule1); err != nil {
		t.Fatalf("Failed to register module 1: %v", err)
	}
	if err := reg.Register(failingModule2); err != nil {
		t.Fatalf("Failed to register module 2: %v", err)
	}

	// Start the lifecycle manager
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop should return combined errors
	err := lm.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when modules stop fail")
	}

	// Error should contain at least one of the module errors
	errMsg := err.Error()
	if !strings.Contains(errMsg, "module 1 stop failed") && !strings.Contains(errMsg, "module 2 stop failed") {
		t.Errorf("Expected error to contain module stop error, got: %v", err)
	}
}

// TestLifecycleManager_Start_PluginError tests Start() when plugin fails
func TestLifecycleManager_Start_PluginError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Create a plugin that fails to start
	failingPlugin := &mockPluginModule{
		name:     "failing-plugin",
		startErr: errors.New("plugin start failed"),
	}

	if err := pluginReg.Register(failingPlugin, "failing-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Start should fail because plugin fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when plugin start fails")
	}

	if !strings.Contains(err.Error(), "plugin start failed") {
		t.Errorf("Expected error to contain 'plugin start failed', got: %v", err)
	}
}

// TestLifecycleManager_Stop_ModuleError tests Stop() when module stop fails
func TestLifecycleManager_Stop_ModuleError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Create a module that fails to stop
	failingModule := &mockModule{
		name:    "failing-module",
		stopErr: errors.New("module stop failed"),
	}

	if err := reg.Register(failingModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start the lifecycle manager
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop should return error from failing module
	err := lm.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when module stop fails")
	}

	if !strings.Contains(err.Error(), "module stop failed") {
		t.Errorf("Expected error to contain 'module stop failed', got: %v", err)
	}
}

// TestLifecycleManager_Stop_PluginError tests Stop() when plugin stop fails
func TestLifecycleManager_Stop_PluginError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Create a plugin that fails to stop
	failingPlugin := &mockPluginModule{
		name:    "failing-plugin",
		stopErr: errors.New("plugin stop failed"),
	}

	if err := pluginReg.Register(failingPlugin, "failing-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Start the lifecycle manager
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop should return error from failing plugin
	err := lm.Stop(context.Background())
	if err == nil {
		t.Fatal("Expected error when plugin stop fails")
	}

	if !strings.Contains(err.Error(), "plugin stop failed") {
		t.Errorf("Expected error to contain 'plugin stop failed', got: %v", err)
	}
}

// TestLifecycleManager_GetPlugin_NotFound tests GetPlugin() when plugin doesn't exist
func TestLifecycleManager_GetPlugin_NotFound(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Get a plugin that doesn't exist
	plugin := lm.GetPlugin("nonexistent-plugin")
	if plugin != nil {
		t.Error("GetPlugin should return nil for nonexistent plugin")
	}
}

// TestLifecycleManager_GetPlugin_Exists tests GetPlugin() when plugin exists
func TestLifecycleManager_GetPlugin_Exists(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Register a plugin
	plugin := &mockPluginModule{name: "test-plugin"}
	if err := pluginReg.Register(plugin, "test-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Get the plugin
	retrievedPlugin := lm.GetPlugin("test-plugin")
	if retrievedPlugin == nil {
		t.Fatal("GetPlugin should return the plugin")
	}
	if retrievedPlugin.Name() != "test-plugin" {
		t.Errorf("Expected plugin name 'test-plugin', got %s", retrievedPlugin.Name())
	}
}

// ============================================================================
// Task 21: Lifecycle Manager Startup Edge Cases Tests
// ============================================================================

// mockMiddlewareModuleWithStartError is a middleware module that fails during Start()
type mockMiddlewareModuleWithStartError struct {
	mockModule
}

func (m *mockMiddlewareModuleWithStartError) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
	return m.startErr
}

func (m *mockMiddlewareModuleWithStartError) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	return m.stopErr
}

func (m *mockMiddlewareModuleWithStartError) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	return event
}

func (m *mockMiddlewareModuleWithStartError) OnServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	return reg
}

func (m *mockMiddlewareModuleWithStartError) OnConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	return event
}

func (m *mockMiddlewareModuleWithStartError) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx
}

func (m *mockMiddlewareModuleWithStartError) OnEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	return entry
}

func (m *mockMiddlewareModuleWithStartError) OnEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	return entry
}

// TestLifecycleManager_Start_MiddlewareStartFailure tests that middleware start failure returns error
func TestLifecycleManager_Start_MiddlewareStartFailure(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a middleware module that will fail during Start
	middleware := &mockMiddlewareModuleWithStartError{
		mockModule: mockModule{
			name:     "failing-middleware",
			startErr: errors.New("middleware start failed"),
		},
	}
	if err := reg.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Start should fail when middleware fails to start")
	}

	if !strings.Contains(err.Error(), "failed to start middleware module") {
		t.Errorf("Expected error to contain 'failed to start middleware module', got: %v", err)
	}
}

// mockEventEmitterModuleWithError implements EventEmitterModule with failing event registration
type mockEventEmitterModuleWithError struct {
	mockModule
	events []types.BaseEventDefinition
}

func (m *mockEventEmitterModuleWithError) SetEventBus(eventBus types.EventBus) {}

func (m *mockEventEmitterModuleWithError) EmitEvents() []types.BaseEventDefinition {
	return m.events
}

// TestLifecycleManager_Start_EventEmitterRegistrationError tests event registration failure
func TestLifecycleManager_Start_EventEmitterRegistrationError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	// First, register an event with the same ModuleName.Name.Version as the emitter will try to emit
	// The duplicate check is based on the key: ModuleName.Name.Version
	eventRegistry.RegisterEvent(types.BaseEventDefinition{
		Subject:    "events.test.first",
		ModuleName: "event-emitter", // Same as the emitter module name
		Name:       "TestEvent",
		Version:    "v1",
	})

	// Create an emitter that emits a duplicate event (will cause registration error)
	// Same ModuleName.Name.Version as above will trigger the duplicate error
	emitter := &mockEventEmitterModuleWithError{
		mockModule: mockModule{name: "event-emitter"},
		events: []types.BaseEventDefinition{
			{
				Subject:    "events.test.second",
				ModuleName: "event-emitter", // Same key as first event
				Name:       "TestEvent",
				Version:    "v1",
			},
		},
	}
	if err := reg.Register(emitter); err != nil {
		t.Fatalf("Failed to register emitter: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Start should fail when event registration fails")
	}

	if !strings.Contains(err.Error(), "failed to register event") {
		t.Errorf("Expected error to contain 'failed to register event', got: %v", err)
	}
}

// mockServiceProviderWithError implements ServiceProviderModule with failing RegisterServices
type mockServiceProviderWithError struct {
	mockModule
	registerError error
}

func (m *mockServiceProviderWithError) RegisterServices(container types.ServiceContainer) error {
	return m.registerError
}

// TestLifecycleManager_Start_RegisterServicesError tests RegisterServices failure
func TestLifecycleManager_Start_RegisterServicesError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Create a service provider that fails during registration
	provider := &mockServiceProviderWithError{
		mockModule:    mockModule{name: "failing-provider"},
		registerError: errors.New("service registration failed"),
	}
	if err := reg.Register(provider); err != nil {
		t.Fatalf("Failed to register provider: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Start should fail when RegisterServices fails")
	}

	if !strings.Contains(err.Error(), "service registration failed") {
		t.Errorf("Expected error to contain 'service registration failed', got: %v", err)
	}
}

// mockEventConsumerModuleWithError implements EventConsumerModule with failing RegisterEventConsumers
type mockEventConsumerModuleWithError struct {
	mockModule
	registerError error
}

func (m *mockEventConsumerModuleWithError) RegisterEventConsumers(registry types.EventRegistry) error {
	return m.registerError
}

// TestLifecycleManager_Start_EventConsumerRegistrationError tests EventConsumerModule registration failure
func TestLifecycleManager_Start_EventConsumerRegistrationError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	// Create an event consumer that fails during registration
	consumer := &mockEventConsumerModuleWithError{
		mockModule:    mockModule{name: "failing-consumer"},
		registerError: errors.New("event consumer registration failed"),
	}
	if err := reg.Register(consumer); err != nil {
		t.Fatalf("Failed to register consumer: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Start should fail when RegisterEventConsumers fails")
	}

	if !strings.Contains(err.Error(), "event consumer registration failed") {
		t.Errorf("Expected error to contain 'event consumer registration failed', got: %v", err)
	}
}

// TestLifecycleManager_Start_WithQueueGroupOptimisticWindow tests queueGroupOptimisticWindow configuration
func TestLifecycleManager_Start_WithQueueGroupOptimisticWindow(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a simple module
	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Create lifecycle manager with queueGroupOptimisticWindow > 0
	queueGroupWindow := 10 * time.Millisecond
	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, queueGroupWindow)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Verify module was started
	if !module.wasStarted() {
		t.Error("Module should be started")
	}

	// The queueGroupOptimisticWindow path should have been executed
	// We can't directly verify the container setting, but we can verify the start succeeded
}

// TestLifecycleManager_Start_SetMiddlewareChainOnEventRegistry tests middleware chain injection into event registry
func TestLifecycleManager_Start_SetMiddlewareChainOnEventRegistry(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	// Register a middleware module to trigger middleware chain creation
	middleware := &mockMiddlewareModuleWithStartError{
		mockModule: mockModule{name: "test-middleware"},
	}
	if err := reg.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	// Register a regular module
	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Both middleware and module should be started
	if !middleware.wasStarted() {
		t.Error("Middleware should be started")
	}
	if !module.wasStarted() {
		t.Error("Module should be started")
	}
}

// TestLifecycleManager_Start_UsePluginModuleInjection tests plugin injection into UsePluginModule
func TestLifecycleManager_Start_UsePluginModuleInjection(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a plugin
	plugin := &mockPluginModule{name: "test-plugin"}
	if err := pluginReg.Register(plugin, "plugin-alias"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Register a module that uses plugins (uses mockUsePluginModule from plugin_test.go)
	module := &mockUsePluginModule{
		mockModule: mockModule{name: "plugin-user"},
	}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Start plugins first
	if err := lm.(*lifecycleManager).startPlugins(context.Background()); err != nil {
		t.Fatalf("Failed to start plugins: %v", err)
	}

	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Verify plugin was injected (uses setPlugins field from mockUsePluginModule in plugin_test.go)
	if module.setPlugins == nil {
		t.Fatal("Plugins map should be initialized")
	}
	if module.setPlugins["plugin-alias"] == nil {
		t.Error("Plugin should be injected with alias 'plugin-alias'")
	}
}

// ============================================================================
// Task 25: Lifecycle Teardown and Error Path Tests
// ============================================================================

// TestLifecycleManager_Stop_ModulePanic tests stopModule panic recovery
func TestLifecycleManager_Stop_ModulePanic(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a module that panics on stop
	module := &mockModule{
		name:      "panic-module",
		stopPanic: "test panic during stop",
	}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Start the module
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Stop should recover from panic and return error
	err = lm.Stop(context.Background())
	if err == nil {
		t.Error("Stop should return error when module panics")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("Error should mention panic, got: %v", err)
	}
}

// TestLifecycleManager_Stop_ModuleStopError tests stopModule error handling
func TestLifecycleManager_Stop_ModuleStopError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a module that returns error on stop
	module := &mockModule{
		name:    "error-module",
		stopErr: errors.New("stop failed"),
	}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Start the module
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Stop should return aggregated error
	err = lm.Stop(context.Background())
	if err == nil {
		t.Error("Stop should return error when module stop fails")
	}
}

// TestLifecycleManager_Stop_MiddlewareModuleError tests middleware module stop error
func TestLifecycleManager_Stop_MiddlewareModuleError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	// Register a middleware module that fails on stop
	middleware := &mockMiddlewareModuleWithStopError{
		mockModule: mockModule{name: "failing-middleware"},
	}
	if err := reg.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	// Start
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Stop should return error from middleware
	err = lm.Stop(context.Background())
	if err == nil {
		t.Error("Stop should return error when middleware stop fails")
	}
}

// mockMiddlewareModuleWithStopError is a middleware that fails on stop
type mockMiddlewareModuleWithStopError struct {
	mockModule
}

func (m *mockMiddlewareModuleWithStopError) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	return event
}

func (m *mockMiddlewareModuleWithStopError) OnServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	return reg
}

func (m *mockMiddlewareModuleWithStopError) OnConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	return event
}

func (m *mockMiddlewareModuleWithStopError) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx
}

func (m *mockMiddlewareModuleWithStopError) OnEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	return entry
}

func (m *mockMiddlewareModuleWithStopError) OnEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	return entry
}

func (m *mockMiddlewareModuleWithStopError) Stop(ctx context.Context) error {
	return errors.New("middleware stop failed")
}

// TestLifecycleManager_TeardownNATSSubscriptions_DrainError tests drain error handling
func TestLifecycleManager_TeardownNATSSubscriptions_DrainError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Create a mock subscription that fails on drain
	mockSub := &mockSubscriptionWithDrainError{}

	// Register a simple module
	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)
	impl := lm.(*lifecycleManager)

	// Start the manager
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Manually add a subscription that will fail to drain
	impl.mu.Lock()
	impl.subscriptions["test-module"] = []types.Subscription{mockSub}
	impl.mu.Unlock()

	// Stop should still succeed (drain errors are logged but not returned)
	err = lm.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop should succeed despite drain errors: %v", err)
	}
}

// mockSubscriptionWithDrainError is a subscription that fails on Drain
type mockSubscriptionWithDrainError struct{}

func (s *mockSubscriptionWithDrainError) Drain() error {
	return errors.New("drain failed")
}

func (s *mockSubscriptionWithDrainError) Unsubscribe() error {
	return nil
}

func (s *mockSubscriptionWithDrainError) IsValid() bool {
	return true
}

func (s *mockSubscriptionWithDrainError) Subject() string {
	return "test.subject"
}

func (s *mockSubscriptionWithDrainError) Queue() string {
	return "test-queue"
}

func (s *mockSubscriptionWithDrainError) NextMsg(timeout time.Duration) (*types.Msg, error) {
	return nil, nats.ErrTimeout
}

func (s *mockSubscriptionWithDrainError) NextMsgWithContext(ctx context.Context) (*types.Msg, error) {
	return nil, ctx.Err()
}

// TestLifecycleManager_GetMiddlewareHook_NilChain tests GetMiddlewareHook with nil chain
func TestLifecycleManager_GetMiddlewareHook_NilChain(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// No middleware modules registered - chain will be nil
	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)

	// Get the hook
	hook := lm.GetMiddlewareHook()
	if hook == nil {
		t.Fatal("Hook should not be nil")
	}

	// Call the hook with nil chain - should not panic
	event := types.ModuleLifecycleEvent{
		Type:       types.ModuleStartedEvent,
		ModuleName: "test",
	}
	hook(context.Background(), event) // Should not panic
}

// TestLifecycleManager_TeardownNATSSubscriptions_ContextCancellation tests context cancellation during teardown
func TestLifecycleManager_TeardownNATSSubscriptions_ContextCancellation(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a simple module
	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)
	impl := lm.(*lifecycleManager)

	// Start the manager
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Add a slow draining subscription
	slowSub := &slowDrainingSubscription{drainTime: 10 * time.Second}
	impl.mu.Lock()
	impl.subscriptions["test-module"] = []types.Subscription{slowSub}
	impl.mu.Unlock()

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Stop should handle context cancellation gracefully
	_ = impl.Stop(ctx)
	// Error might be nil or context-related, either is acceptable
	// Main thing is it doesn't hang
}

// slowDrainingSubscription is a subscription that takes a long time to drain
type slowDrainingSubscription struct {
	drainTime time.Duration
}

func (s *slowDrainingSubscription) Drain() error {
	time.Sleep(s.drainTime)
	return nil
}

func (s *slowDrainingSubscription) Unsubscribe() error {
	return nil
}

func (s *slowDrainingSubscription) IsValid() bool {
	return true
}

func (s *slowDrainingSubscription) Subject() string {
	return "test.subject"
}

func (s *slowDrainingSubscription) Queue() string {
	return "test-queue"
}

func (s *slowDrainingSubscription) NextMsg(timeout time.Duration) (*types.Msg, error) {
	return nil, nats.ErrTimeout
}

func (s *slowDrainingSubscription) NextMsgWithContext(ctx context.Context) (*types.Msg, error) {
	return nil, ctx.Err()
}

// TestLifecycleManager_Stop_WithMiddlewareChain tests stop with middleware chain notifications
func TestLifecycleManager_Stop_WithMiddlewareChain(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	// Register a middleware module
	middleware := &mockMiddlewareModule{
		mockModule: mockModule{name: "test-middleware"},
	}
	if err := reg.Register(middleware); err != nil {
		t.Fatalf("Failed to register middleware: %v", err)
	}

	// Register a regular module
	module := &mockModule{name: "test-module"}
	if err := reg.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	// Start
	err := lm.Start(context.Background())
	if err != nil {
		t.Fatalf("Start should succeed: %v", err)
	}

	// Stop should notify middleware chain
	err = lm.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop should succeed: %v", err)
	}

	// Verify module was stopped
	if !module.wasStopped() {
		t.Error("Module should be stopped")
	}
}

// TestLifecycleManager_RollbackPlugins_Error tests rollbackPlugins error handling
func TestLifecycleManager_RollbackPlugins_Error(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()

	// Register a plugin that fails on stop
	failingPlugin := &mockPluginModuleWithStopError{
		mockPluginModule: mockPluginModule{name: "failing-plugin"},
	}
	if err := pluginReg.Register(failingPlugin, "failing-plugin"); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, nil, logger, 0)
	impl := lm.(*lifecycleManager)

	// Start plugins
	err := impl.startPlugins(context.Background())
	if err != nil {
		t.Fatalf("startPlugins should succeed: %v", err)
	}

	// Rollback plugins - should log error but not panic
	// The function returns void, so we just verify it doesn't panic
	impl.rollbackPlugins(context.Background(), []string{"failing-plugin"})
}

// mockPluginModuleWithStopError is a plugin that fails on stop
type mockPluginModuleWithStopError struct {
	mockPluginModule
}

func (m *mockPluginModuleWithStopError) Stop(ctx context.Context) error {
	return errors.New("plugin stop failed")
}

// ============================================================================
// Tests for setupNATSSubscriptions handler callbacks
// ============================================================================

// TestLifecycleManager_RequestReplyHandler_Error tests RequestReply handler error callback
func TestLifecycleManager_RequestReplyHandler_Error(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	handlerError := errors.New("handler failed")
	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Type:    types.ServiceTypeRequestReply,
				Subject: "services.service-module.test-service",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return nil, handlerError
				},
				QueueGroup: "service-module",
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.service-module.test-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler with a message that has a reply subject
	msg := &types.Msg{
		Subject: "services.service-module.test-service",
		Reply:   "reply-subject",
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify error was logged
	if !logger.hasErrorContaining("handler") {
		t.Error("Expected error to be logged for handler error")
	}
}

// TestLifecycleManager_RequestReplyHandler_PublishError tests RequestReply publish response error
func TestLifecycleManager_RequestReplyHandler_PublishError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Type:    types.ServiceTypeRequestReply,
				Subject: "services.service-module.test-service",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return []byte("response"), nil
				},
				QueueGroup: "service-module",
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Set publish error
	eventBus.setPublishError(errors.New("publish failed"))

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.service-module.test-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler with a message that has a reply subject
	msg := &types.Msg{
		Subject: "services.service-module.test-service",
		Reply:   "reply-subject",
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify error was logged
	if !logger.hasErrorContaining("publish") {
		t.Error("Expected error to be logged for publish failure")
	}
}

// TestLifecycleManager_RequestReplyHandler_NoReplySubject tests RequestReply with no reply subject
func TestLifecycleManager_RequestReplyHandler_NoReplySubject(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Type:    types.ServiceTypeRequestReply,
				Subject: "services.service-module.test-service",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					return []byte("response"), nil
				},
				QueueGroup: "service-module",
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.service-module.test-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler WITHOUT a reply subject
	msg := &types.Msg{
		Subject: "services.service-module.test-service",
		Reply:   "", // No reply subject
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify warning was logged
	if !logger.hasWarnContaining("reply") {
		t.Error("Expected warning to be logged for missing reply subject")
	}
}

// mockModuleWithQueueGroupServices creates a module with queue group services
type mockModuleWithQueueGroupServices struct {
	mockModule
	serviceEntries []*types.ServiceEntry
	eventBus       types.EventBus
}

func (m *mockModuleWithQueueGroupServices) SetEventBus(eventBus types.EventBus) {
	m.eventBus = eventBus
}

func (m *mockModuleWithQueueGroupServices) RegisterServices(container types.ServiceContainer) error {
	for _, entry := range m.serviceEntries {
		if entry.Type == types.ServiceTypeQueueGroup {
			if err := container.RegisterQueueGroupService(entry.Name, entry.QueueHandlers...); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestLifecycleManager_QueueGroupHandler_ACKPublishError tests QueueGroup ACK publish error
func TestLifecycleManager_QueueGroupHandler_ACKPublishError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	handlerCalled := false
	serviceModule := &mockModuleWithQueueGroupServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "qg-service",
				Type:    types.ServiceTypeQueueGroup,
				Subject: "services.qg-module.qg-service",
				QueueHandlers: []types.QGHP{
					{
						QueueGroup: "qg-module",
						Handler: func(ctx context.Context, msg *types.Msg) error {
							handlerCalled = true
							return nil
						},
					},
				},
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Set publish error to fail ACK
	eventBus.setPublishError(errors.New("ACK publish failed"))

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.qg-module.qg-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler with a message that has a reply subject (for ACK)
	msg := &types.Msg{
		Subject: "services.qg-module.qg-service",
		Reply:   "ack-reply-subject",
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify error was logged and handler was NOT called (early return on ACK failure)
	if !logger.hasErrorContaining("ACK") {
		t.Error("Expected error to be logged for ACK publish failure")
	}
	if handlerCalled {
		t.Error("Handler should not be called when ACK fails")
	}
}

// TestLifecycleManager_QueueGroupHandler_HandlerError tests QueueGroup handler error
func TestLifecycleManager_QueueGroupHandler_HandlerError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	handlerError := errors.New("qg handler failed")
	serviceModule := &mockModuleWithQueueGroupServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "qg-service",
				Type:    types.ServiceTypeQueueGroup,
				Subject: "services.qg-module.qg-service",
				QueueHandlers: []types.QGHP{
					{
						QueueGroup: "qg-module",
						Handler: func(ctx context.Context, msg *types.Msg) error {
							return handlerError
						},
					},
				},
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.qg-module.qg-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler WITHOUT reply subject (skip ACK path)
	msg := &types.Msg{
		Subject: "services.qg-module.qg-service",
		Reply:   "", // No reply subject means no ACK
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify error was logged
	if !logger.hasErrorContaining("QueueGroup handler error") {
		t.Error("Expected error to be logged for QueueGroup handler error")
	}
}

// TestLifecycleManager_QueueGroupHandler_SuccessWithACK tests QueueGroup successful flow with ACK
func TestLifecycleManager_QueueGroupHandler_SuccessWithACK(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	handlerCalled := false
	serviceModule := &mockModuleWithQueueGroupServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "qg-service",
				Type:    types.ServiceTypeQueueGroup,
				Subject: "services.qg-module.qg-service",
				QueueHandlers: []types.QGHP{
					{
						QueueGroup: "qg-module",
						Handler: func(ctx context.Context, msg *types.Msg) error {
							handlerCalled = true
							return nil
						},
					},
				},
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("services.qg-module.qg-service")
	if len(handlers) == 0 {
		t.Fatal("Expected handler to be registered")
	}

	// Invoke handler with reply subject (for ACK)
	msg := &types.Msg{
		Subject: "services.qg-module.qg-service",
		Reply:   "ack-reply-subject",
		Data:    []byte("request"),
	}
	handlers[0](context.Background(), msg)

	// Verify handler was called and ACK was sent
	if !handlerCalled {
		t.Error("Handler should have been called")
	}

	// Verify ACK was published
	ackMsgs := eventBus.getPublishedMessages("ack-reply-subject")
	if len(ackMsgs) == 0 {
		t.Error("Expected ACK to be published")
	}
}

// TestLifecycleManager_QueueGroupSubscriptionError tests QueueGroup subscription error
func TestLifecycleManager_QueueGroupSubscriptionError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventBus.queueSubscribeErr = errors.New("queue subscription failed")
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	serviceModule := &mockModuleWithQueueGroupServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "qg-service",
				Type:    types.ServiceTypeQueueGroup,
				Subject: "services.qg-module.qg-service",
				QueueHandlers: []types.QGHP{
					{
						QueueGroup: "qg-module",
						Handler: func(ctx context.Context, msg *types.Msg) error {
							return nil
						},
					},
				},
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start should fail because subscription fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when QueueGroup subscription fails")
	}

	if !strings.Contains(err.Error(), "queue subscription failed") {
		t.Errorf("Expected error to contain 'queue subscription failed', got: %v", err)
	}
}

// TestLifecycleManager_EventConsumerHandler_Error tests event consumer handler error callback
func TestLifecycleManager_EventConsumerHandler_Error(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventReg := newMockEventRegistry()

	handlerError := errors.New("event handler failed")
	eventReg.consumerEntries = []types.EventConsumerEntry{
		{
			EventDef: types.BaseEventDefinition{
				Name:       "TestEvent",
				Version:    "v1",
				ModuleName: "test-producer",
				Subject:    "events.test-producer.v1.test-event",
			},
			Handler: func(ctx context.Context, msg *types.Msg) error {
				return handlerError
			},
			Module:     &mockModule{name: "consumer-module"},
			QueueGroup: "consumers",
		},
	}

	module := &mockModule{name: "test"}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lm.Stop(context.Background())
	}()

	// Get the registered handler and invoke it
	handlers := eventBus.getHandlers("events.test-producer.v1.test-event")
	if len(handlers) == 0 {
		t.Fatal("Expected event consumer handler to be registered")
	}

	// Invoke the handler
	msg := &types.Msg{
		Subject: "events.test-producer.v1.test-event",
		Data:    []byte("event data"),
	}
	handlers[0](context.Background(), msg)

	// Verify error was logged
	if !logger.hasErrorContaining("Event consumer handler error") {
		t.Error("Expected error to be logged for event consumer handler error")
	}
}

// TestLifecycleManager_EventConsumerSubscriptionError tests event consumer subscription error
func TestLifecycleManager_EventConsumerSubscriptionError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventBus.queueSubscribeErr = errors.New("event subscription failed")
	eventBus.queueSubscribeErrFor = "events.test-producer.v1.test-event"
	eventReg := newMockEventRegistry()

	eventReg.consumerEntries = []types.EventConsumerEntry{
		{
			EventDef: types.BaseEventDefinition{
				Name:       "TestEvent",
				Version:    "v1",
				ModuleName: "test-producer",
				Subject:    "events.test-producer.v1.test-event",
			},
			Handler: func(ctx context.Context, msg *types.Msg) error {
				return nil
			},
			Module:     &mockModule{name: "consumer-module"},
			QueueGroup: "consumers",
		},
	}

	module := &mockModule{name: "test"}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

	// Start should fail because event subscription fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when event consumer subscription fails")
	}

	if !strings.Contains(err.Error(), "event subscription failed") {
		t.Errorf("Expected error to contain 'event subscription failed', got: %v", err)
	}
}

// mockModuleWithStreamConsumer creates a module with stream consumer services
type mockModuleWithStreamConsumer struct {
	mockModule
	serviceEntries []*types.ServiceEntry
	eventBus       types.EventBus
}

func (m *mockModuleWithStreamConsumer) SetEventBus(eventBus types.EventBus) {
	m.eventBus = eventBus
}

func (m *mockModuleWithStreamConsumer) RegisterServices(container types.ServiceContainer) error {
	for _, entry := range m.serviceEntries {
		if entry.Type == types.ServiceTypeStreamConsumer {
			if err := container.RegisterStreamConsumerService(entry.Name, *entry.StreamConsumerConfig, entry.StreamConsumerHandler); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestLifecycleManager_StreamConsumerSetupError tests stream consumer setup error
func TestLifecycleManager_StreamConsumerSetupError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventStream := &mockEventStream{
		createStreamErr: errors.New("stream creation failed"),
	}
	eventBus.eventStream = eventStream
	eventBus.eventStreamErr = nil
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	serviceModule := &mockModuleWithStreamConsumer{
		mockModule: mockModule{name: "stream-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "stream-service",
				Type:    types.ServiceTypeStreamConsumer,
				Subject: "services.stream-module.stream-service",
				StreamConsumerConfig: &types.StreamConsumerConfig{
					Stream: types.StreamConfig{
						Name:     "test-stream",
						Subjects: []string{"test.>"},
					},
					Consumer: types.ConsumerConfig{
						Name:        "test-consumer",
						Durable:     "test-consumer",
						Description: "test consumer",
					},
				},
				StreamConsumerHandler: func(ctx context.Context, msgs []*types.Msg) error {
					return nil
				},
			},
		},
	}

	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	// Start should fail because stream consumer setup fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when stream consumer setup fails")
	}

	if !strings.Contains(err.Error(), "stream creation failed") {
		t.Errorf("Expected error to contain 'stream creation failed', got: %v", err)
	}
}

// TestLifecycleManager_EventStreamConsumerSetupError tests event stream consumer setup error
func TestLifecycleManager_EventStreamConsumerSetupError(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventStream := &mockEventStream{
		createStreamErr: errors.New("event stream creation failed"),
	}
	eventBus.eventStream = eventStream
	eventBus.eventStreamErr = nil
	eventReg := newMockEventRegistry()

	eventReg.streamConsumerEntries = []types.EventStreamConsumerEntry{
		{
			EventDef: types.BaseEventDefinition{
				Name:       "StreamEvent",
				Version:    "v1",
				ModuleName: "test-producer",
				Subject:    "events.test-producer.v1.stream-event",
			},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name:     "event-stream",
					Subjects: []string{"events.test-producer.v1.stream-event"},
				},
				Consumer: types.ConsumerConfig{
					Name:        "event-consumer",
					Description: "test event stream consumer",
				},
			},
			Handler: func(ctx context.Context, msgs []*types.Msg) error {
				return nil
			},
			Module: &mockModule{name: "stream-consumer-module"},
		},
	}

	module := &mockModule{name: "test"}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)

	// Start should fail because event stream consumer setup fails
	err := lm.Start(context.Background())
	if err == nil {
		t.Fatal("Expected error when event stream consumer setup fails")
	}

	if !strings.Contains(err.Error(), "event stream creation failed") {
		t.Errorf("Expected error to contain 'event stream creation failed', got: %v", err)
	}
}

// TestGetErrorTypeName tests the getErrorTypeName helper function
func TestGetErrorTypeName(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
		{
			name:     "ServiceError",
			err:      &monoerrors.ServiceError{ServiceName: "test", ModuleName: "mod"},
			expected: "service",
		},
		{
			name:     "TimeoutError",
			err:      &monoerrors.TimeoutError{Operation: "test"},
			expected: "timeout",
		},
		{
			name:     "ModuleError",
			err:      &monoerrors.ModuleError{ModuleName: "test"},
			expected: "module",
		},
		{
			name:     "DependencyError",
			err:      &monoerrors.DependencyError{Module: "test"},
			expected: "dependency",
		},
		{
			name:     "ConfigurationError",
			err:      &monoerrors.ConfigurationError{OptionName: "test"},
			expected: "configuration",
		},
		{
			name:     "EventStreamError",
			err:      &monoerrors.EventStreamError{Operation: "test"},
			expected: "eventstream",
		},
		{
			name:     "RemoteError",
			err:      &monoerrors.RemoteError{Message: "test"},
			expected: "remote",
		},
		{
			name:     "standard error",
			err:      errors.New("standard error"),
			expected: "errorstring", // errors.errorString type name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getErrorTypeName(tt.err)
			if result != tt.expected {
				t.Errorf("getErrorTypeName(%T) = %q, want %q", tt.err, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Cron delivery tests (runCronConsumerLoop, dispatchCronTick, invokeCronHandler)
// =============================================================================

// recordingAcker records Ack/Nak calls. It is used as types.Msg.NatsMsg so the
// reflection-based Msg.Ack()/Nak() observe it.
type recordingAcker struct {
	mu   sync.Mutex
	acks int
	naks int
}

func (r *recordingAcker) Ack() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks++
	return nil
}

func (r *recordingAcker) Nak() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.naks++
	return nil
}

func (r *recordingAcker) counts() (ack int, nak int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acks, r.naks
}

// recordingJetStreamMsg wraps mockJetStreamMsg to record Nak calls (used to
// verify in-flight Nak on context cancellation inside the loop).
type recordingJetStreamMsg struct {
	*mockJetStreamMsg
	nakCount int32
}

func (m *recordingJetStreamMsg) Nak() error {
	atomic.AddInt32(&m.nakCount, 1)
	return nil
}

func newCronTestManager() *lifecycleManager {
	return &lifecycleManager{
		logger:          &mockLogger{},
		streamConsumers: make(map[string]context.CancelFunc),
		runtimeCtx:      context.Background(),
		mu:              sync.RWMutex{},
	}
}

func TestInvokeCronHandler(t *testing.T) {
	lm := newCronTestManager()

	t.Run("success returns nil", func(t *testing.T) {
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			return nil
		}}
		if err := lm.invokeCronHandler(context.Background(), entry, &types.Msg{}); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("handler error propagates", func(t *testing.T) {
		want := errors.New("boom")
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			return want
		}}
		if err := lm.invokeCronHandler(context.Background(), entry, &types.Msg{}); !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	})

	t.Run("panic converted to error", func(t *testing.T) {
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			panic("kaboom")
		}}
		err := lm.invokeCronHandler(context.Background(), entry, &types.Msg{})
		if err == nil {
			t.Fatal("expected error from recovered panic, got nil")
		}
		if !strings.Contains(err.Error(), "panic") {
			t.Fatalf("expected panic error, got %v", err)
		}
	})
}

func TestDispatchCronTick(t *testing.T) {
	lm := newCronTestManager()

	t.Run("ack on success", func(t *testing.T) {
		rec := &recordingAcker{}
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			return nil
		}}
		lm.dispatchCronTick(context.Background(), entry, &types.Msg{NatsMsg: rec})
		if ack, nak := rec.counts(); ack != 1 || nak != 0 {
			t.Fatalf("expected 1 ack 0 nak, got %d ack %d nak", ack, nak)
		}
	})

	t.Run("nak on handler error", func(t *testing.T) {
		rec := &recordingAcker{}
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			return errors.New("fail")
		}}
		lm.dispatchCronTick(context.Background(), entry, &types.Msg{NatsMsg: rec})
		if ack, nak := rec.counts(); ack != 0 || nak != 1 {
			t.Fatalf("expected 0 ack 1 nak, got %d ack %d nak", ack, nak)
		}
	})

	t.Run("nak on panic", func(t *testing.T) {
		rec := &recordingAcker{}
		entry := &types.ServiceEntry{Name: "job", CronHandler: func(_ context.Context, _ *types.Msg) error {
			panic("p")
		}}
		lm.dispatchCronTick(context.Background(), entry, &types.Msg{NatsMsg: rec})
		if ack, nak := rec.counts(); ack != 0 || nak != 1 {
			t.Fatalf("expected 0 ack 1 nak on panic, got %d ack %d nak", ack, nak)
		}
	})
}

func TestRunCronConsumerLoop_CtxCancel(t *testing.T) {
	lm := newCronTestManager()
	consumer := &mockJetStreamConsumer{}
	entry := &types.ServiceEntry{Name: "cron-job", ModuleName: "m", CronHandler: func(_ context.Context, _ *types.Msg) error {
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		lm.runCronConsumerLoop(ctx, consumer, entry)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cron loop did not exit on context cancellation")
	}
}

func TestRunCronConsumerLoop_FetchTimeout(t *testing.T) {
	lm := newCronTestManager()
	consumer := &mockJetStreamConsumer{fetchTimeout: true} // returns nats.ErrTimeout
	entry := &types.ServiceEntry{Name: "cron-job", ModuleName: "m", CronHandler: func(_ context.Context, _ *types.Msg) error {
		return nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		lm.runCronConsumerLoop(ctx, consumer, entry)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cron loop did not exit after fetch timeouts and ctx cancel")
	}
}

func TestRunCronConsumerLoop_DeliversTick(t *testing.T) {
	lm := newCronTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled int32
	entry := &types.ServiceEntry{Name: "cron-job", ModuleName: "m", CronHandler: func(_ context.Context, msg *types.Msg) error {
		atomic.AddInt32(&handled, 1)
		cancel() // stop after the first delivery
		return nil
	}}
	consumer := &mockJetStreamConsumer{fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("tick")}}}

	done := make(chan struct{})
	go func() {
		lm.runCronConsumerLoop(ctx, consumer, entry)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cron loop did not exit")
	}
	if atomic.LoadInt32(&handled) == 0 {
		t.Fatal("cron handler was never invoked")
	}
}

func TestRunCronConsumerLoop_NaksInFlightOnCancel(t *testing.T) {
	lm := newCronTestManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The handler cancels the context during the first message, so the second
	// message in the batch hits the cancellation check and must be Nak'd.
	entry := &types.ServiceEntry{Name: "cron-job", ModuleName: "m", CronHandler: func(_ context.Context, _ *types.Msg) error {
		cancel()
		return nil
	}}
	inFlight := &recordingJetStreamMsg{mockJetStreamMsg: &mockJetStreamMsg{data: []byte("second")}}
	consumer := &mockJetStreamConsumer{fetchMessages: []jetstream.Msg{
		&mockJetStreamMsg{data: []byte("first")},
		inFlight,
	}}

	done := make(chan struct{})
	go func() {
		lm.runCronConsumerLoop(ctx, consumer, entry)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cron loop did not exit")
	}
	if got := atomic.LoadInt32(&inFlight.nakCount); got != 1 {
		t.Fatalf("expected in-flight message Nak'd once on cancel, got %d", got)
	}
}
