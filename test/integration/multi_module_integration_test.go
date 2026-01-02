//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/v1"
)

// multiTestModule is an enhanced test module with dependency-aware features
// for comprehensive multi-module integration testing.
type multiTestModule struct {
	name                 string
	dependencies         []string
	startTime            time.Time
	stopTime             time.Time
	startCalled          bool
	stopCalled           bool
	eventBus             mono.EventBus
	container            mono.ServiceContainer
	depContainers        map[string]mono.ServiceContainer
	registerServicesFunc func(mono.ServiceContainer) error
	startFunc            func(context.Context) error
	stopFunc             func(context.Context) error
	mu                   sync.Mutex
}

func (m *multiTestModule) Name() string {
	return m.name
}

func (m *multiTestModule) Dependencies() []string {
	return m.dependencies
}

func (m *multiTestModule) Start(ctx context.Context) error {
	m.mu.Lock()
	m.startTime = time.Now()
	m.startCalled = true
	m.mu.Unlock()

	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *multiTestModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	m.stopTime = time.Now()
	m.stopCalled = true
	m.mu.Unlock()

	if m.stopFunc != nil {
		return m.stopFunc(ctx)
	}
	return nil
}

func (m *multiTestModule) SetEventBus(eventBus mono.EventBus) {
	m.eventBus = eventBus
}

func (m *multiTestModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	if m.registerServicesFunc != nil {
		return m.registerServicesFunc(container)
	}
	return nil
}

func (m *multiTestModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.depContainers == nil {
		m.depContainers = make(map[string]mono.ServiceContainer)
	}
	m.depContainers[dependency] = container
}

func (m *multiTestModule) getDepContainer(name string) mono.ServiceContainer {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.depContainers == nil {
		return nil
	}
	return m.depContainers[name]
}

func (m *multiTestModule) wasStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCalled
}

func (m *multiTestModule) wasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

func (m *multiTestModule) getStartTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startTime
}

func (m *multiTestModule) getStopTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopTime
}

// verifyStartOrder checks that modules started in the correct dependency order.
// Each module in the list should have started after all modules listed before it.
func verifyStartOrder(t *testing.T, modules ...*multiTestModule) {
	t.Helper()
	for i := 1; i < len(modules); i++ {
		prev := modules[i-1]
		curr := modules[i]
		if !prev.wasStarted() {
			t.Errorf("Module %s was not started", prev.name)
			continue
		}
		if !curr.wasStarted() {
			t.Errorf("Module %s was not started", curr.name)
			continue
		}
		if curr.getStartTime().Before(prev.getStartTime()) {
			t.Errorf("Module %s started before %s (expected reverse order)", curr.name, prev.name)
		}
	}
}

// verifyStopOrder checks that modules stopped in the correct reverse dependency order.
func verifyStopOrder(t *testing.T, modules ...*multiTestModule) {
	t.Helper()
	for i := 1; i < len(modules); i++ {
		prev := modules[i-1]
		curr := modules[i]
		if !prev.wasStopped() {
			t.Errorf("Module %s was not stopped", prev.name)
			continue
		}
		if !curr.wasStopped() {
			t.Errorf("Module %s was not stopped", curr.name)
			continue
		}
		if curr.getStopTime().Before(prev.getStopTime()) {
			t.Errorf("Module %s stopped before %s (expected reverse order)", curr.name, prev.name)
		}
	}
}

// =============================================================================
// Category 1: Inter-Module Service Communication Tests
// =============================================================================

// TestMultiModule_DependencyServiceAccess tests that a dependent module can access
// its dependency's service container through SetDependencyServiceContainer.
func TestMultiModule_DependencyServiceAccess(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A registers a channel service
	inChan := make(chan *mono.Msg, 10)
	outChan := make(chan *mono.Msg, 10)

	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("data-channel", inChan, outChan)
	}

	// Module B depends on A and will access A's service
	var receivedContainer mono.ServiceContainer
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(_ context.Context) error {
		receivedContainer = moduleB.getDepContainer("module-a")
		return nil
	}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify B received A's container
	if receivedContainer == nil {
		t.Fatal("Module B did not receive dependency container for module A")
	}

	// Verify B can access A's channel service
	retrievedIn, retrievedOut, err := receivedContainer.GetChannelService("data-channel", "module-B")
	if err != nil {
		t.Fatalf("Failed to get channel service from dependency: %v", err)
	}

	// in channel should match (shared), but out channel is per-consumer
	if retrievedIn != inChan {
		t.Error("Retrieved in channel does not match registered channel")
	}
	if retrievedOut == outChan {
		t.Error("Retrieved out channel should be per-consumer, not provider's channel")
	}
	if retrievedOut == nil {
		t.Error("Retrieved out channel should not be nil")
	}
}

// TestMultiModule_RequestReplyBetweenModules tests request-reply communication
// between a service provider module and its dependent module.
func TestMultiModule_RequestReplyBetweenModules(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A provides a request-reply service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("process-data", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return []byte("processed: " + string(req.Data)), nil
		})
	}

	// Module B depends on A and will call A's service
	var callResult string
	var callErr error
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// B calls A's service through the dependency container
	depContainer := moduleB.getDepContainer("module-a")
	if depContainer == nil {
		t.Fatal("Module B did not receive dependency container")
	}

	client, err := depContainer.GetRequestReplyService("process-data")
	if err != nil {
		t.Fatalf("Failed to get request-reply service: %v", err)
	}

	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()

	response, callErr := client.Call(reqCtx, []byte("hello"))
	if callErr != nil {
		t.Fatalf("Request failed: %v", callErr)
	}
	callResult = string(response.Data)

	expected := "processed: hello"
	if callResult != expected {
		t.Errorf("Expected '%s', got '%s'", expected, callResult)
	}
}

// TestMultiModule_ChannelServiceBetweenModules tests bidirectional channel
// communication between modules.
func TestMultiModule_ChannelServiceBetweenModules(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A creates channels and processes messages
	inChan := make(chan *mono.Msg, 10)
	outChan := make(chan *mono.Msg, 10)

	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("processor", inChan, outChan)
	}

	// Start a goroutine to process messages (simulating A's worker)
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case msg := <-inChan:
				outChan <- &mono.Msg{Data: []byte("reply: " + string(msg.Data))}
			case <-done:
				return
			}
		}
	}()

	// Module B depends on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// B gets A's channels and communicates
	depContainer := moduleB.getDepContainer("module-a")
	if depContainer == nil {
		t.Fatal("Module B did not receive dependency container")
	}

	bInChan, bOutChan, err := depContainer.GetChannelService("processor", "module-B")
	if err != nil {
		t.Fatalf("Failed to get channel service: %v", err)
	}

	// B sends message to A's input channel
	bInChan <- &mono.Msg{Data: []byte("test message")}

	// B receives response from A's output channel
	select {
	case response := <-bOutChan:
		expected := "reply: test message"
		if string(response.Data) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(response.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for channel response")
	}
}

// TestMultiModule_QueueGroupServiceBetweenModules tests queue group service
// where a dependent module publishes tasks to the provider's queue.
func TestMultiModule_QueueGroupServiceBetweenModules(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	var receivedCount atomic.Int32

	// Module A provides queue group service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("tasks",
			mono.QGHP{
				QueueGroup: "workers",
				Handler: func(_ context.Context, msg *mono.Msg) error {
					receivedCount.Add(1)
					return nil
				},
			},
		)
	}

	// Module B depends on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// B publishes tasks to A's queue through event bus
	eventBus := fw.EventBus("module-b")
	if eventBus == nil {
		t.Fatal("Module B event bus is nil")
	}

	messageCount := 5
	for i := 0; i < messageCount; i++ {
		if err := eventBus.Publish("services.module-a.tasks", []byte("task")); err != nil {
			t.Errorf("Publish failed: %v", err)
		}
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	received := receivedCount.Load()
	if received != int32(messageCount) {
		t.Errorf("Expected %d messages, got %d", messageCount, received)
	}
}

// =============================================================================
// Category 2: Complex Dependency Graph Tests
// =============================================================================

// TestMultiModule_DiamondDependencyWithSharedState tests diamond dependency pattern
// where B and C both depend on A, and D depends on both B and C.
func TestMultiModule_DiamondDependencyWithSharedState(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Shared counter that B and C will increment
	var counter atomic.Int32

	// Module A (base) - no dependencies
	moduleA := &multiTestModule{name: "module-a"}

	// Module B depends on A, increments counter
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(_ context.Context) error {
		counter.Add(1)
		return nil
	}

	// Module C depends on A, increments counter
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-a"}}
	moduleC.startFunc = func(_ context.Context) error {
		counter.Add(1)
		return nil
	}

	// Module D depends on B and C
	var counterValueAtD int32
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-b", "module-c"}}
	moduleD.startFunc = func(_ context.Context) error {
		counterValueAtD = counter.Load()
		return nil
	}

	// Register in arbitrary order
	for _, m := range []*multiTestModule{moduleD, moduleA, moduleC, moduleB} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify all modules started
	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if !m.wasStarted() {
			t.Errorf("Module %s was not started", m.name)
		}
	}

	// Verify counter was 2 when D started (both B and C had run)
	if counterValueAtD != 2 {
		t.Errorf("Expected counter to be 2 when D started, got %d", counterValueAtD)
	}

	// Verify A started before B and C
	if moduleB.getStartTime().Before(moduleA.getStartTime()) {
		t.Error("Module B started before A")
	}
	if moduleC.getStartTime().Before(moduleA.getStartTime()) {
		t.Error("Module C started before A")
	}

	// Verify D started after B and C
	if moduleD.getStartTime().Before(moduleB.getStartTime()) {
		t.Error("Module D started before B")
	}
	if moduleD.getStartTime().Before(moduleC.getStartTime()) {
		t.Error("Module D started before C")
	}
}

// TestMultiModule_FanOutDependency tests one module with multiple dependents.
func TestMultiModule_FanOutDependency(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A provides request-reply service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return []byte("echo: " + string(req.Data)), nil
		})
	}

	// B, C, D all depend on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-a"}}
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-a"}}

	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Each dependent calls A's service
	dependents := []*multiTestModule{moduleB, moduleC, moduleD}
	for _, dep := range dependents {
		depContainer := dep.getDepContainer("module-a")
		if depContainer == nil {
			t.Errorf("Module %s did not receive dependency container", dep.name)
			continue
		}

		client, err := depContainer.GetRequestReplyService("echo")
		if err != nil {
			t.Errorf("Module %s failed to get service: %v", dep.name, err)
			continue
		}

		reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.Call(reqCtx, []byte(dep.name))
		reqCancel()

		if err != nil {
			t.Errorf("Module %s request failed: %v", dep.name, err)
			continue
		}

		expected := "echo: " + dep.name
		if string(response.Data) != expected {
			t.Errorf("Module %s: expected '%s', got '%s'", dep.name, expected, string(response.Data))
		}
	}
}

// TestMultiModule_FanInDependency tests multiple modules feeding into one.
func TestMultiModule_FanInDependency(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Channels for each source module
	chanA := make(chan *mono.Msg, 10)
	chanB := make(chan *mono.Msg, 10)
	chanC := make(chan *mono.Msg, 10)

	// Modules A, B, C each register a channel service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("data", chanA, make(chan *mono.Msg, 1))
	}

	moduleB := &multiTestModule{name: "module-b"}
	moduleB.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("data", chanB, make(chan *mono.Msg, 1))
	}

	moduleC := &multiTestModule{name: "module-c"}
	moduleC.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterChannelService("data", chanC, make(chan *mono.Msg, 1))
	}

	// Module D depends on A, B, C and aggregates
	var aggregatedData []string
	var aggregateMu sync.Mutex

	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-a", "module-b", "module-c"}}
	moduleD.startFunc = func(_ context.Context) error {
		// Read from each dependency's channel
		sources := []struct {
			name string
			ch   chan *mono.Msg
		}{
			{"module-a", chanA},
			{"module-b", chanB},
			{"module-c", chanC},
		}

		for _, src := range sources {
			depContainer := moduleD.getDepContainer(src.name)
			if depContainer == nil {
				continue
			}
			inCh, _, err := depContainer.GetChannelService("data", "aggregator")
			if err != nil {
				continue
			}
			// Read available data
			select {
			case msg := <-inCh:
				aggregateMu.Lock()
				aggregatedData = append(aggregatedData, string(msg.Data))
				aggregateMu.Unlock()
			default:
				// No data yet
			}
		}
		return nil
	}

	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	// Pre-populate channels before start
	chanA <- &mono.Msg{Data: []byte("from-a")}
	chanB <- &mono.Msg{Data: []byte("from-b")}
	chanC <- &mono.Msg{Data: []byte("from-c")}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify D aggregated data from all sources
	aggregateMu.Lock()
	defer aggregateMu.Unlock()

	if len(aggregatedData) != 3 {
		t.Errorf("Expected 3 aggregated items, got %d", len(aggregatedData))
	}
}

// TestMultiModule_DeepChainWithServicePropagation tests a 5-module chain where
// data propagates through services.
func TestMultiModule_DeepChainWithServicePropagation(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track the chain of values
	var chainValues []string
	var chainMu sync.Mutex

	addToChain := func(value string) {
		chainMu.Lock()
		chainValues = append(chainValues, value)
		chainMu.Unlock()
	}

	// Module A - root
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.startFunc = func(_ context.Context) error {
		addToChain("A")
		return nil
	}

	// Module B depends on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(_ context.Context) error {
		addToChain("B")
		return nil
	}

	// Module C depends on B
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-b"}}
	moduleC.startFunc = func(_ context.Context) error {
		addToChain("C")
		return nil
	}

	// Module D depends on C
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-c"}}
	moduleD.startFunc = func(_ context.Context) error {
		addToChain("D")
		return nil
	}

	// Module E depends on D
	moduleE := &multiTestModule{name: "module-e", dependencies: []string{"module-d"}}
	moduleE.startFunc = func(_ context.Context) error {
		addToChain("E")
		return nil
	}

	// Register in reverse order to test dependency resolution
	for _, m := range []*multiTestModule{moduleE, moduleD, moduleC, moduleB, moduleA} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify chain order
	chainMu.Lock()
	defer chainMu.Unlock()

	expected := []string{"A", "B", "C", "D", "E"}
	if len(chainValues) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(chainValues))
	}

	for i, exp := range expected {
		if chainValues[i] != exp {
			t.Errorf("Position %d: expected %s, got %s", i, exp, chainValues[i])
		}
	}
}

// =============================================================================
// Category 3: Lifecycle Ordering Verification Tests
// =============================================================================

// TestMultiModule_StartOrderVerification verifies modules start in dependency order.
func TestMultiModule_StartOrderVerification(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Create dependency graph: A <- B <- C, B <- D
	moduleA := &multiTestModule{name: "module-a"}
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-b"}}
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-b"}}

	// Register in random order
	for _, m := range []*multiTestModule{moduleC, moduleA, moduleD, moduleB} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify order: A before B, B before C, B before D
	if moduleB.getStartTime().Before(moduleA.getStartTime()) {
		t.Error("B started before A")
	}
	if moduleC.getStartTime().Before(moduleB.getStartTime()) {
		t.Error("C started before B")
	}
	if moduleD.getStartTime().Before(moduleB.getStartTime()) {
		t.Error("D started before B")
	}
}

// TestMultiModule_StopOrderVerification verifies modules stop in reverse dependency order.
func TestMultiModule_StopOrderVerification(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	// Create chain: A <- B <- C
	moduleA := &multiTestModule{name: "module-a"}
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-b"}}

	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Verify stop order: C before B, B before A
	if moduleB.getStopTime().Before(moduleC.getStopTime()) {
		t.Error("B stopped before C")
	}
	if moduleA.getStopTime().Before(moduleB.getStopTime()) {
		t.Error("A stopped before B")
	}
}

// TestMultiModule_ServiceAvailabilityAtStart verifies that dependency services
// are available when a module starts.
func TestMultiModule_ServiceAvailabilityAtStart(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A registers a service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		inChan := make(chan *mono.Msg, 10)
		outChan := make(chan *mono.Msg, 10)
		return container.RegisterChannelService("available-service", inChan, outChan)
	}

	// Module B tries to access A's service during Start
	var serviceAvailable bool
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(_ context.Context) error {
		depContainer := moduleB.getDepContainer("module-a")
		if depContainer == nil {
			return errors.New("dependency container not available")
		}
		_, _, err := depContainer.GetChannelService("available-service", "test-module")
		serviceAvailable = (err == nil)
		return nil
	}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	if !serviceAvailable {
		t.Error("Dependency service was not available when module B started")
	}
}

// =============================================================================
// Category 4: Error Handling with Dependencies Tests
// =============================================================================

// TestMultiModule_DependencyStartFailure tests that when a dependency fails to start,
// its dependents are not started.
func TestMultiModule_DependencyStartFailure(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A fails to start
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.startFunc = func(_ context.Context) error {
		return errors.New("module A failed")
	}

	// Module B depends on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when starting framework with failing module")
	}

	// Verify B was never started
	if moduleB.wasStarted() {
		t.Error("Module B should not have been started when dependency A failed")
	}
}

// TestMultiModule_DependentStartFailure tests that when a dependent fails,
// its dependencies are rolled back.
func TestMultiModule_DependentStartFailure(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Module A starts successfully
	moduleA := &multiTestModule{name: "module-a"}

	// Module B depends on A but fails to start
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(_ context.Context) error {
		return errors.New("module B failed")
	}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when starting framework with failing module")
	}

	// Verify A was started
	if !moduleA.wasStarted() {
		t.Error("Module A should have been started")
	}

	// Verify A was stopped (rollback)
	if !moduleA.wasStopped() {
		t.Error("Module A should have been stopped during rollback")
	}
}

// TestMultiModule_PartialChainFailure tests partial failure in a dependency chain.
func TestMultiModule_PartialChainFailure(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Chain: A -> B -> C -> D, where C fails
	moduleA := &multiTestModule{name: "module-a"}
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-b"}}
	moduleC.startFunc = func(_ context.Context) error {
		return errors.New("module C failed")
	}
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-c"}}

	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when starting framework with failing module")
	}

	// Verify A and B were started
	if !moduleA.wasStarted() {
		t.Error("Module A should have been started")
	}
	if !moduleB.wasStarted() {
		t.Error("Module B should have been started")
	}

	// Verify D was never started
	if moduleD.wasStarted() {
		t.Error("Module D should not have been started")
	}

	// Verify A and B were stopped (rollback)
	if !moduleA.wasStopped() {
		t.Error("Module A should have been stopped during rollback")
	}
	if !moduleB.wasStopped() {
		t.Error("Module B should have been stopped during rollback")
	}
}

// =============================================================================
// Category 5: Concurrent Operations Tests
// =============================================================================

// TestMultiModule_ConcurrentEventPublishing tests multiple modules publishing
// and receiving events concurrently.
func TestMultiModule_ConcurrentEventPublishing(t *testing.T) {
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	const moduleCount = 3
	const messagesPerModule = 10
	const topic = "shared.events"

	// Track received messages per module
	type moduleReceived struct {
		count atomic.Int32
	}
	received := make([]*moduleReceived, moduleCount)
	for i := range received {
		received[i] = &moduleReceived{}
	}

	modules := make([]*multiTestModule, moduleCount)
	for i := 0; i < moduleCount; i++ {
		idx := i
		modules[i] = &multiTestModule{name: fmt.Sprintf("module-%c", 'a'+i)}
		modules[i].startFunc = func(_ context.Context) error {
			// Subscribe to shared topic
			if modules[idx].eventBus != nil {
				_, err := modules[idx].eventBus.Subscribe(topic, func(_ context.Context, _ *mono.Msg) {
					received[idx].count.Add(1)
				})
				if err != nil {
					return err
				}
			}
			return nil
		}
	}

	for _, m := range modules {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give subscriptions time to be ready
	time.Sleep(100 * time.Millisecond)

	// Each module publishes messages concurrently
	var wg sync.WaitGroup
	for i := 0; i < moduleCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			eventBus := fw.EventBus(modules[idx].name)
			if eventBus == nil {
				t.Errorf("Module %s has no event bus", modules[idx].name)
				return
			}
			for j := 0; j < messagesPerModule; j++ {
				if err := eventBus.Publish(topic, []byte("msg")); err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	// Wait for message delivery
	time.Sleep(500 * time.Millisecond)

	// Each module should have received all messages (3 modules * 10 messages = 30)
	expectedTotal := int32(moduleCount * messagesPerModule)
	for i, r := range received {
		count := r.count.Load()
		if count != expectedTotal {
			t.Errorf("Module %d: expected %d messages, got %d", i, expectedTotal, count)
		}
	}
}

// =============================================================================
// Category 6: StreamConsumer Service Tests
// =============================================================================

// TestMultiModule_StreamConsumerServiceBetweenModules tests that a dependent module
// can access its dependency's StreamConsumer service and publish messages.
func TestMultiModule_StreamConsumerServiceBetweenModules(t *testing.T) {
	fw, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track received messages
	var receivedMessages [][]byte
	var receivedMu sync.Mutex
	expectedMessages := 5
	doneCh := make(chan struct{})

	// Module A provides a StreamConsumer service
	streamName := fmt.Sprintf("STREAM_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	subjectPrefix := strings.ToLower(streamName)

	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", subjectPrefix)},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			receivedMu.Lock()
			defer receivedMu.Unlock()

			for _, msg := range msgs {
				receivedMessages = append(receivedMessages, msg.Data)
				msg.Ack()
			}

			if len(receivedMessages) >= expectedMessages {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			return nil
		}

		return container.RegisterStreamConsumerService("stream-processor", config, handler)
	}

	// Module B depends on A and will publish to A's stream
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give stream consumer time to be ready
	time.Sleep(100 * time.Millisecond)

	// B gets A's StreamConsumer service and publishes messages
	depContainer := moduleB.getDepContainer("module-a")
	if depContainer == nil {
		t.Fatal("Module B did not receive dependency container")
	}

	client, err := depContainer.GetStreamConsumerService("stream-processor")
	if err != nil {
		t.Fatalf("Failed to get stream consumer service: %v", err)
	}

	// Publish messages
	for i := 0; i < expectedMessages; i++ {
		data := []byte(fmt.Sprintf("message-%d", i))
		_, err := client.Publish(ctx, data)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Wait for messages to be received
	select {
	case <-doneCh:
		// Success
	case <-time.After(10 * time.Second):
		receivedMu.Lock()
		count := len(receivedMessages)
		receivedMu.Unlock()
		t.Fatalf("Timeout waiting for messages. Expected %d, received %d", expectedMessages, count)
	}

	// Verify all messages received
	receivedMu.Lock()
	if len(receivedMessages) != expectedMessages {
		t.Errorf("Expected %d messages, got %d", expectedMessages, len(receivedMessages))
	}
	receivedMu.Unlock()
}

// TestMultiModule_StreamConsumerPublishFromMultipleModules tests multiple dependent
// modules publishing to the same StreamConsumer service.
func TestMultiModule_StreamConsumerPublishFromMultipleModules(t *testing.T) {
	fw, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track received messages
	var receivedCount atomic.Int32
	messagesPerModule := 3
	moduleCount := 3
	expectedTotal := messagesPerModule * moduleCount
	doneCh := make(chan struct{})

	// Module A provides StreamConsumer service
	streamName := fmt.Sprintf("STREAM_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	subjectPrefix := strings.ToLower(streamName)

	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", subjectPrefix)},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 10,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			for _, msg := range msgs {
				msg.Ack()
				count := receivedCount.Add(1)
				if int(count) >= expectedTotal {
					select {
					case <-doneCh:
					default:
						close(doneCh)
					}
				}
			}
			return nil
		}

		return container.RegisterStreamConsumerService("shared-stream", config, handler)
	}

	// Modules B, C, D all depend on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-a"}}
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-a"}}

	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give stream consumer time to be ready
	time.Sleep(100 * time.Millisecond)

	// Each dependent module publishes messages
	dependents := []*multiTestModule{moduleB, moduleC, moduleD}
	for _, dep := range dependents {
		depContainer := dep.getDepContainer("module-a")
		if depContainer == nil {
			t.Fatalf("Module %s did not receive dependency container", dep.name)
		}

		client, err := depContainer.GetStreamConsumerService("shared-stream")
		if err != nil {
			t.Fatalf("Module %s failed to get service: %v", dep.name, err)
		}

		for i := 0; i < messagesPerModule; i++ {
			data := []byte(fmt.Sprintf("%s-msg-%d", dep.name, i))
			if _, err := client.Publish(ctx, data); err != nil {
				t.Fatalf("Module %s failed to publish: %v", dep.name, err)
			}
		}
	}

	// Wait for all messages
	select {
	case <-doneCh:
		// Success
	case <-time.After(10 * time.Second):
		count := receivedCount.Load()
		t.Fatalf("Timeout waiting for messages. Expected %d, received %d", expectedTotal, count)
	}

	// Verify count
	finalCount := receivedCount.Load()
	if int(finalCount) != expectedTotal {
		t.Errorf("Expected %d total messages, got %d", expectedTotal, finalCount)
	}
}

// TestMultiModule_StreamConsumerDiamondDependency tests StreamConsumer in
// diamond dependency pattern.
func TestMultiModule_StreamConsumerDiamondDependency(t *testing.T) {
	fw, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track received messages
	var receivedCount atomic.Int32
	expectedTotal := 3 // 1 from B, 1 from C, 1 from D
	doneCh := make(chan struct{})

	// Module A provides StreamConsumer service (base of diamond)
	streamName := fmt.Sprintf("STREAM_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	subjectPrefix := strings.ToLower(streamName)

	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", subjectPrefix)},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			for _, msg := range msgs {
				msg.Ack()
				count := receivedCount.Add(1)
				if int(count) >= expectedTotal {
					select {
					case <-doneCh:
					default:
						close(doneCh)
					}
				}
			}
			return nil
		}

		return container.RegisterStreamConsumerService("diamond-stream", config, handler)
	}

	// Module B and C both depend on A
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &multiTestModule{name: "module-c", dependencies: []string{"module-a"}}

	// Module D depends on both B and C, and also A (top of diamond)
	moduleD := &multiTestModule{name: "module-d", dependencies: []string{"module-a", "module-b", "module-c"}}

	for _, m := range []*multiTestModule{moduleD, moduleA, moduleC, moduleB} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.name, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify all modules started
	for _, m := range []*multiTestModule{moduleA, moduleB, moduleC, moduleD} {
		if !m.wasStarted() {
			t.Errorf("Module %s was not started", m.name)
		}
	}

	// Give stream consumer time to be ready
	time.Sleep(100 * time.Millisecond)

	// B and C each publish one message
	for _, module := range []*multiTestModule{moduleB, moduleC, moduleD} {
		depContainer := module.getDepContainer("module-a")
		if depContainer == nil {
			t.Fatalf("Module %s did not receive module-a dependency container", module.name)
		}

		client, err := depContainer.GetStreamConsumerService("diamond-stream")
		if err != nil {
			t.Fatalf("Module %s failed to get service: %v", module.name, err)
		}

		data := []byte(fmt.Sprintf("from-%s", module.name))
		if _, err := client.Publish(ctx, data); err != nil {
			t.Fatalf("Module %s failed to publish: %v", module.name, err)
		}
	}

	// Wait for all messages
	select {
	case <-doneCh:
		// Success
	case <-time.After(10 * time.Second):
		count := receivedCount.Load()
		t.Fatalf("Timeout waiting for messages. Expected %d, received %d", expectedTotal, count)
	}

	// Verify count
	finalCount := receivedCount.Load()
	if int(finalCount) != expectedTotal {
		t.Errorf("Expected %d messages, got %d", expectedTotal, finalCount)
	}
}

// TestMultiModule_StreamConsumerChainProcessing tests data flowing through
// a chain of modules via StreamConsumer.
func TestMultiModule_StreamConsumerChainProcessing(t *testing.T) {
	fw, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Track final output
	var finalMessages []string
	var finalMu sync.Mutex
	expectedCount := 3
	doneCh := make(chan struct{})

	// Stream names for chain
	streamA := fmt.Sprintf("STREAM_A_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	streamB := fmt.Sprintf("STREAM_B_%s", strings.ReplaceAll(t.Name(), "/", "_"))

	// Module A: receives input, forwards to B with "A-" prefix
	// Must depend on module-b since it forwards messages to B's stream
	moduleA := &multiTestModule{name: "module-a", dependencies: []string{"module-b"}}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamA,
				Subjects: []string{strings.ToLower(streamA) + ".>"},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(ctx context.Context, msgs []*mono.Msg) error {
			// Get module B's container to forward messages
			depContainerB := moduleA.getDepContainer("module-b")
			if depContainerB == nil {
				return fmt.Errorf("module A cannot access module B container")
			}

			client, err := depContainerB.GetStreamConsumerService("process-b")
			if err != nil {
				return err
			}

			for _, msg := range msgs {
				msg.Ack()
				// Forward with prefix
				processedData := []byte("A-" + string(msg.Data))
				if _, err := client.Publish(ctx, processedData); err != nil {
					return err
				}
			}
			return nil
		}

		return container.RegisterStreamConsumerService("process-a", config, handler)
	}

	// Module B: receives from A, adds "B-" prefix, stores final result
	moduleB := &multiTestModule{name: "module-b"}
	moduleB.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamB,
				Subjects: []string{strings.ToLower(streamB) + ".>"},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			finalMu.Lock()
			defer finalMu.Unlock()

			for _, msg := range msgs {
				msg.Ack()
				// Add final prefix
				processedData := "B-" + string(msg.Data)
				finalMessages = append(finalMessages, processedData)
			}

			if len(finalMessages) >= expectedCount {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			return nil
		}

		return container.RegisterStreamConsumerService("process-b", config, handler)
	}

	// Register B first since A depends on B
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Give stream consumers time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish initial messages directly to module A's stream using JetStream
	// (simulating external message source rather than inter-module communication)
	js, err := moduleA.eventBus.EventStream()
	if err != nil {
		t.Fatalf("Failed to get EventStream: %v", err)
	}

	for i := 0; i < expectedCount; i++ {
		data := []byte(fmt.Sprintf("msg%d", i))
		if _, err := js.Publish(ctx, strings.ToLower(streamA)+".input", data); err != nil {
			t.Fatalf("Failed to publish initial message: %v", err)
		}
	}

	// Wait for chain processing to complete
	select {
	case <-doneCh:
		// Success
	case <-time.After(10 * time.Second):
		finalMu.Lock()
		count := len(finalMessages)
		finalMu.Unlock()
		t.Fatalf("Timeout waiting for chain processing. Expected %d, received %d", expectedCount, count)
	}

	// Verify final messages have both prefixes
	finalMu.Lock()
	defer finalMu.Unlock()

	if len(finalMessages) != expectedCount {
		t.Errorf("Expected %d final messages, got %d", expectedCount, len(finalMessages))
	}

	for i, msg := range finalMessages {
		if !strings.HasPrefix(msg, "B-A-") {
			t.Errorf("Message %d should have 'B-A-' prefix, got: %s", i, msg)
		}
	}
}

// TestMultiModule_StreamConsumerAvailabilityAtStart verifies that dependency's
// StreamConsumer service is available when a dependent module starts.
func TestMultiModule_StreamConsumerAvailabilityAtStart(t *testing.T) {
	fw, err := mono.NewMonoApplication(mono.WithJetStreamStorageDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	streamName := fmt.Sprintf("STREAM_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	subjectPrefix := strings.ToLower(streamName)

	// Module A registers StreamConsumer service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", subjectPrefix)},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			for _, msg := range msgs {
				msg.Ack()
			}
			return nil
		}

		return container.RegisterStreamConsumerService("lifecycle-service", config, handler)
	}

	// Module B tries to access A's service during Start
	var serviceAvailable bool
	var clientRetrieved mono.StreamConsumerServiceClient
	moduleB := &multiTestModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleB.startFunc = func(ctx context.Context) error {
		depContainer := moduleB.getDepContainer("module-a")
		if depContainer == nil {
			return errors.New("dependency container not available")
		}

		var err error
		clientRetrieved, err = depContainer.GetStreamConsumerService("lifecycle-service")
		serviceAvailable = (err == nil)

		return nil
	}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	if !serviceAvailable {
		t.Error("StreamConsumer service was not available when module B started")
	}

	if clientRetrieved == nil {
		t.Error("StreamConsumer service client was not retrieved successfully")
	}

	// Now test that the client can publish after framework is fully started
	_, err = clientRetrieved.Publish(ctx, []byte("test-after-start"))
	if err != nil {
		t.Errorf("Failed to publish to StreamConsumer service after framework started: %v", err)
	}
}

// TestMultiModule_StreamConsumerWithoutJetStream tests error handling when
// StreamConsumer is used without JetStream enabled.
func TestMultiModule_StreamConsumerWithoutJetStream(t *testing.T) {
	// Create framework WITHOUT JetStream
	fw, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	streamName := fmt.Sprintf("STREAM_%s", strings.ReplaceAll(t.Name(), "/", "_"))
	subjectPrefix := strings.ToLower(streamName)

	// Module A tries to register StreamConsumer service
	moduleA := &multiTestModule{name: "module-a"}
	moduleA.registerServicesFunc = func(container mono.ServiceContainer) error {
		config := mono.StreamConsumerConfig{
			Stream: mono.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", subjectPrefix)},
			},
			Fetch: mono.FetchConfig{
				BatchSize: 5,
				Timeout:   2 * time.Second,
			},
		}

		handler := func(_ context.Context, msgs []*mono.Msg) error {
			for _, msg := range msgs {
				msg.Ack()
			}
			return nil
		}

		// This should succeed during registration
		return container.RegisterStreamConsumerService("no-js-service", config, handler)
	}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start should fail because JetStream is not enabled
	err = fw.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when starting framework with StreamConsumer but no JetStream")
	}

	// Verify error message mentions JetStream
	errLower := strings.ToLower(err.Error())
	if !strings.Contains(errLower, "jetstream") {
		t.Errorf("Expected error to mention JetStream, got: %v", err)
	}
}
