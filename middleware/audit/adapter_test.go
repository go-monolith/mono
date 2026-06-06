package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// mockContainer implements mono.ServiceContainer for adapter testing.
type mockContainer struct {
	channelServices map[string]struct {
		in  chan *types.Msg
		out chan *types.Msg
	}
	mu sync.Mutex
}

func newMockContainer() *mockContainer {
	return &mockContainer{
		channelServices: make(map[string]struct {
			in  chan *types.Msg
			out chan *types.Msg
		}),
	}
}

func (m *mockContainer) BindModule(_ types.Module) error { return nil }
func (m *mockContainer) SetEventBus(_ types.EventBus)    {}
func (m *mockContainer) SetQueueGroupOptimisticWindow(_ time.Duration) {
}
func (m *mockContainer) SetMiddlewareChain(_ types.MiddlewareChainRunner) {}

func (m *mockContainer) RegisterChannelService(name string, in chan *types.Msg, out chan *types.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channelServices[name] = struct {
		in  chan *types.Msg
		out chan *types.Msg
	}{in: in, out: out}
	return nil
}

func (m *mockContainer) GetChannelService(name string, consumerModule string) (chan *types.Msg, chan *types.Msg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.channelServices[name]; ok {
		return svc.in, svc.out, nil
	}
	return nil, nil, errors.ErrServiceNotFound
}

func (m *mockContainer) MustGetChannelService(name string, consumerModule string) (chan *types.Msg, chan *types.Msg) {
	in, out, err := m.GetChannelService(name, consumerModule)
	if err != nil {
		panic(err)
	}
	return in, out
}

func (m *mockContainer) RegisterRequestReplyService(_ string, _ types.RequestReplyHandler) error {
	return nil
}
func (m *mockContainer) GetRequestReplyService(_ string) (types.RequestReplyServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockContainer) MustGetRequestReplyService(_ string) types.RequestReplyServiceClient {
	return nil
}
func (m *mockContainer) RegisterQueueGroupService(_ string, _ ...types.QGHP) error {
	return nil
}
func (m *mockContainer) GetQueueGroupService(_ string) (types.QueueGroupServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockContainer) MustGetQueueGroupService(_ string) types.QueueGroupServiceClient { return nil }
func (m *mockContainer) RegisterStreamConsumerService(_ string, _ types.StreamConsumerConfig, _ types.StreamConsumerHandler) error {
	return nil
}
func (m *mockContainer) RegisterCronService(_ string, _ types.CronServiceConfig, _ types.CronHandler) error {
	return nil
}
func (m *mockContainer) GetStreamConsumerService(_ string) (types.StreamConsumerServiceClient, error) {
	return nil, errors.ErrServiceNotFound
}
func (m *mockContainer) Has(_ string) bool                     { return false }
func (m *mockContainer) Unregister(_ string) error             { return nil }
func (m *mockContainer) Entries() []*types.ServiceEntry        { return nil }
func (m *mockContainer) StartChannelRouters(_ context.Context) {}

// mockAuditHandler simulates the audit module's channel handling for testing.
//
// It processes messages from inChan and sends responses to outChan.
// Entries are stored and can be retrieved via getEntries().
//
// Failure simulation:
//   - Set failOnEntry to a 0-based index to simulate a failure on that specific entry.
//   - When failOnEntry matches the current entry index, the handler returns an error
//     response instead of saving the entry.
//   - Set failOnEntry to -1 (default) to disable failure simulation.
//
// Thread safety:
//   - The handler runs in a background goroutine started by newMockAuditHandler().
//   - All entry access is protected by mutex.
//   - Call stop() to cleanly shut down the handler goroutine.
type mockAuditHandler struct {
	inChan      chan *types.Msg
	outChan     chan *types.Msg
	entries     []Entry
	mu          sync.Mutex
	failOnEntry int // Set to entry index (0-based) to simulate failure, -1 to disable
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func newMockAuditHandler() *mockAuditHandler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &mockAuditHandler{
		inChan:      make(chan *types.Msg, 100),
		outChan:     make(chan *types.Msg, 100),
		entries:     make([]Entry, 0),
		failOnEntry: -1,
		ctx:         ctx,
		cancel:      cancel,
	}
	h.wg.Add(1)
	go h.handle()
	return h
}

func (h *mockAuditHandler) handle() {
	defer h.wg.Done()
	entryIdx := 0
	for {
		select {
		case <-h.ctx.Done():
			return
		case msg, ok := <-h.inChan:
			if !ok {
				return
			}

			var entry Entry
			if err := json.Unmarshal(msg.Data, &entry); err != nil {
				if msg.Reply != "" {
					response := SaveEntryResponse{Success: false, Error: err.Error()}
					data, _ := json.Marshal(response)
					h.outChan <- &types.Msg{Subject: msg.Reply, Data: data}
				}
				continue
			}

			h.mu.Lock()
			shouldFail := h.failOnEntry == entryIdx
			if !shouldFail {
				h.entries = append(h.entries, entry)
			}
			h.mu.Unlock()

			if msg.Reply != "" {
				var response SaveEntryResponse
				if shouldFail {
					response = SaveEntryResponse{Success: false, Error: "simulated failure"}
				} else {
					response = SaveEntryResponse{Success: true}
				}
				data, _ := json.Marshal(response)
				h.outChan <- &types.Msg{Subject: msg.Reply, Data: data}
			}
			entryIdx++
		}
	}
}

func (h *mockAuditHandler) stop() {
	h.cancel()
	close(h.inChan)
	h.wg.Wait()
	close(h.outChan)
}

func (h *mockAuditHandler) getEntries() []Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]Entry, len(h.entries))
	copy(result, h.entries)
	return result
}

func TestNewAuditAdapter(t *testing.T) {
	t.Run("nil container returns error", func(t *testing.T) {
		_, err := NewAuditAdapter(nil, "test-module")
		if err == nil {
			t.Error("expected error for nil container")
		}
	})

	t.Run("missing audit-trail service returns error", func(t *testing.T) {
		container := newMockContainer()
		_, err := NewAuditAdapter(container, "test-module")
		if err == nil {
			t.Error("expected error when audit-trail service is not registered")
		}
	})

	t.Run("success with registered service", func(t *testing.T) {
		container := newMockContainer()
		inChan := make(chan *types.Msg, 10)
		outChan := make(chan *types.Msg, 10)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)

		adapter, err := NewAuditAdapter(container, "test-module")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if adapter == nil {
			t.Error("expected non-nil adapter")
		}
	})
}

func TestAuditAdapter_SaveAuditTrail(t *testing.T) {
	t.Run("empty entries returns zero", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		count, err := adapter.SaveAuditTrail(context.Background(), []Entry{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if count != 0 {
			t.Errorf("expected count 0, got %d", count)
		}
	})

	t.Run("saves single entry", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := []Entry{
			{
				EventType:   EventCustomAuditTrail,
				ModuleName:  "test-module",
				ServiceName: "test-service",
				Details: map[string]any{
					"action": "test-action",
				},
			},
		}

		count, err := adapter.SaveAuditTrail(context.Background(), entries)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %d", count)
		}

		savedEntries := handler.getEntries()
		if len(savedEntries) != 1 {
			t.Errorf("expected 1 saved entry, got %d", len(savedEntries))
		}
	})

	t.Run("saves multiple entries", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := make([]Entry, 5)
		for i := range entries {
			entries[i] = Entry{
				EventType:   EventCustomAuditTrail,
				ModuleName:  fmt.Sprintf("module-%d", i),
				ServiceName: fmt.Sprintf("service-%d", i),
			}
		}

		count, err := adapter.SaveAuditTrail(context.Background(), entries)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 5 {
			t.Errorf("expected count 5, got %d", count)
		}

		savedEntries := handler.getEntries()
		if len(savedEntries) != 5 {
			t.Errorf("expected 5 saved entries, got %d", len(savedEntries))
		}
	})

	t.Run("handles entry failure", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		handler.failOnEntry = 1 // Fail on second entry
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := []Entry{
			{ModuleName: "module-0"},
			{ModuleName: "module-1"}, // This one will fail
			{ModuleName: "module-2"},
		}

		count, err := adapter.SaveAuditTrail(context.Background(), entries)
		if err == nil {
			t.Error("expected error when entry fails")
		}
		// First and third entries should succeed
		if count != 2 {
			t.Errorf("expected count 2 (skipping failed entry), got %d", count)
		}
	})

	t.Run("context cancellation during send", func(t *testing.T) {
		container := newMockContainer()
		// Use unbuffered channels to block send
		inChan := make(chan *types.Msg)
		outChan := make(chan *types.Msg, 10)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		entries := []Entry{{ModuleName: "test"}}
		_, err := adapter.SaveAuditTrail(ctx, entries)
		if err == nil {
			t.Error("expected error on cancelled context")
		}
	})

	t.Run("context cancellation during response wait", func(t *testing.T) {
		container := newMockContainer()
		inChan := make(chan *types.Msg, 10)
		// Use unbuffered outChan so we never receive response
		outChan := make(chan *types.Msg)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		entries := []Entry{{ModuleName: "test"}}
		_, err := adapter.SaveAuditTrail(ctx, entries)
		if err == nil {
			t.Error("expected error on timeout")
		}
	})

	t.Run("invalid response format", func(t *testing.T) {
		container := newMockContainer()
		inChan := make(chan *types.Msg, 10)
		outChan := make(chan *types.Msg, 10)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		// Start a goroutine to respond with invalid JSON
		go func() {
			<-inChan
			outChan <- &types.Msg{Data: []byte("invalid json")}
		}()

		entries := []Entry{{ModuleName: "test"}}
		count, err := adapter.SaveAuditTrail(context.Background(), entries)
		if err == nil {
			t.Error("expected error for invalid response format")
		}
		if count != 0 {
			t.Errorf("expected count 0, got %d", count)
		}
	})
}

func TestAuditAdapter_AsyncSaveAuditTrail(t *testing.T) {
	t.Run("empty entries returns nil", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		err := adapter.AsyncSaveAuditTrail(context.Background(), []Entry{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("sends single entry async", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := []Entry{
			{
				EventType:  EventCustomAuditTrail,
				ModuleName: "async-module",
			},
		}

		err := adapter.AsyncSaveAuditTrail(context.Background(), entries)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Wait for processing
		time.Sleep(50 * time.Millisecond)

		savedEntries := handler.getEntries()
		if len(savedEntries) != 1 {
			t.Errorf("expected 1 saved entry, got %d", len(savedEntries))
		}
	})

	t.Run("sends multiple entries async", func(t *testing.T) {
		container := newMockContainer()
		handler := newMockAuditHandler()
		defer handler.stop()

		_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := make([]Entry, 10)
		for i := range entries {
			entries[i] = Entry{
				EventType:  EventCustomAuditTrail,
				ModuleName: fmt.Sprintf("async-module-%d", i),
			}
		}

		err := adapter.AsyncSaveAuditTrail(context.Background(), entries)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		savedEntries := handler.getEntries()
		if len(savedEntries) != 10 {
			t.Errorf("expected 10 saved entries, got %d", len(savedEntries))
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		container := newMockContainer()
		// Use unbuffered channel to block
		inChan := make(chan *types.Msg)
		outChan := make(chan *types.Msg, 10)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		entries := []Entry{{ModuleName: "test"}}
		err := adapter.AsyncSaveAuditTrail(ctx, entries)
		if err == nil {
			t.Error("expected error on cancelled context")
		}
	})

	t.Run("does not wait for response", func(t *testing.T) {
		container := newMockContainer()
		inChan := make(chan *types.Msg, 10)
		// Never send response on outChan
		outChan := make(chan *types.Msg)
		_ = container.RegisterChannelService("audit-trail", inChan, outChan)
		adapter, _ := NewAuditAdapter(container, "test-module")

		entries := []Entry{{ModuleName: "test"}}

		start := time.Now()
		err := adapter.AsyncSaveAuditTrail(context.Background(), entries)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		// Should return quickly without waiting for response
		if elapsed > 100*time.Millisecond {
			t.Errorf("async save should not wait for response, took %v", elapsed)
		}
	})
}

func TestAuditAdapter_ConcurrentSave(t *testing.T) {
	container := newMockContainer()
	handler := newMockAuditHandler()
	defer handler.stop()

	_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
	adapter, _ := NewAuditAdapter(container, "test-module")

	const numGoroutines = 20
	const entriesPerGoroutine = 5
	var wg sync.WaitGroup

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			entries := make([]Entry, entriesPerGoroutine)
			for j := range entries {
				entries[j] = Entry{
					EventType:  EventCustomAuditTrail,
					ModuleName: fmt.Sprintf("module-%d-%d", idx, j),
				}
			}
			_, _ = adapter.SaveAuditTrail(context.Background(), entries)
		}(i)
	}

	wg.Wait()

	// Wait for all processing to complete
	time.Sleep(100 * time.Millisecond)

	savedEntries := handler.getEntries()
	expectedCount := numGoroutines * entriesPerGoroutine
	if len(savedEntries) != expectedCount {
		t.Errorf("expected %d saved entries, got %d", expectedCount, len(savedEntries))
	}
}

func TestAuditAdapter_MarshalError(t *testing.T) {
	container := newMockContainer()
	handler := newMockAuditHandler()
	defer handler.stop()

	_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
	adapter, _ := NewAuditAdapter(container, "test-module")

	// Create an entry with an unmarshalable value
	entries := []Entry{
		{
			ModuleName: "test",
			Details: map[string]any{
				"bad_value": make(chan int), // Channels cannot be marshaled to JSON
			},
		},
	}

	t.Run("SaveAuditTrail marshal error", func(t *testing.T) {
		_, err := adapter.SaveAuditTrail(context.Background(), entries)
		if err == nil {
			t.Error("expected marshal error")
		}
	})

	t.Run("AsyncSaveAuditTrail marshal error", func(t *testing.T) {
		err := adapter.AsyncSaveAuditTrail(context.Background(), entries)
		if err == nil {
			t.Error("expected marshal error")
		}
	})
}

// TestWaitForResponses_Timeout tests waitForResponses timeout behavior.
func TestWaitForResponses_Timeout(t *testing.T) {
	container := newMockContainer()
	handler := newMockAuditHandler()
	defer handler.stop()

	_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
	adapter, err := NewAuditAdapter(container, "test-module")
	if err != nil {
		t.Fatalf("NewAuditAdapter() failed: %v", err)
	}

	// Cast to concrete type to access waitForResponses
	auditAdapter := adapter.(*auditAdapter)

	// Call waitForResponses with count > 0 but no responses sent
	// Should timeout after 20ms per response
	start := time.Now()
	auditAdapter.waitForResponses(context.Background(), 2)
	elapsed := time.Since(start)

	// Should have timed out on first response (20ms), not waited for second
	if elapsed > 50*time.Millisecond {
		t.Errorf("waitForResponses took too long: %v (expected ~20ms timeout)", elapsed)
	}
}

// TestWaitForResponses_MultipleResponses tests draining multiple responses.
func TestWaitForResponses_MultipleResponses(t *testing.T) {
	container := newMockContainer()
	handler := newMockAuditHandler()
	defer handler.stop()

	_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
	adapter, err := NewAuditAdapter(container, "test-module")
	if err != nil {
		t.Fatalf("NewAuditAdapter() failed: %v", err)
	}

	// Cast to concrete type
	auditAdapter := adapter.(*auditAdapter)

	// Send responses to the output channel
	go func() {
		for range 3 {
			handler.outChan <- &types.Msg{Data: []byte(`{"status":"ok"}`)}
		}
	}()

	// Wait for 3 responses - should complete quickly without timeout
	start := time.Now()
	auditAdapter.waitForResponses(context.Background(), 3)
	elapsed := time.Since(start)

	// Should complete in < 20ms since responses are available
	if elapsed > 20*time.Millisecond {
		t.Errorf("waitForResponses took too long: %v (expected < 20ms)", elapsed)
	}
}

// TestWaitForResponses_ErrorResponses tests that waitForResponses discards error responses.
func TestWaitForResponses_ErrorResponses(t *testing.T) {
	container := newMockContainer()
	handler := newMockAuditHandler()
	defer handler.stop()

	_ = container.RegisterChannelService("audit-trail", handler.inChan, handler.outChan)
	adapter, err := NewAuditAdapter(container, "test-module")
	if err != nil {
		t.Fatalf("NewAuditAdapter() failed: %v", err)
	}

	// Cast to concrete type
	auditAdapter := adapter.(*auditAdapter)

	// Send error responses (waitForResponses should just discard them)
	go func() {
		handler.outChan <- &types.Msg{Data: []byte(`{"error":"something went wrong"}`)}
		handler.outChan <- &types.Msg{Data: []byte(`{"error":"another error"}`)}
	}()

	// Wait for 2 responses - should discard them without error
	start := time.Now()
	auditAdapter.waitForResponses(context.Background(), 2)
	elapsed := time.Since(start)

	// Should complete quickly
	if elapsed > 20*time.Millisecond {
		t.Errorf("waitForResponses took too long: %v", elapsed)
	}
}
