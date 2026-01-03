// Package analytics implements an analytics module for event tracking and retrieval
// using channel-based services and request-reply patterns.
// Events are persisted using the kv-jetstream plugin for durable storage.
package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono"
	kvjetstream "github.com/go-monolith/mono/plugin/kv-jetstream"
)

// NAME is the unique identifier for the analytics module.
const NAME = "analytics"

// BucketName is the name of the KV bucket used for storing events.
const BucketName = "analytics-events"

// AnalyticModule implements the mono.Module using channel-based services
// and persists events to a KV store via the kv-jetstream plugin.
type AnalyticModule struct {
	// Channel-based service channels (bidirectional)
	trackEventIn  chan *mono.Msg
	trackEventOut chan *mono.Msg

	// KV storage (from kv-jetstream plugin)
	kvPlugin *kvjetstream.PluginModule
	kvStore  kvjetstream.KVStoragePort

	// Event counter for this session (atomic counter for O(1) performance)
	// This tracks events created by this instance only. For a fully accurate
	// count across restarts, use getTotalEventCount() which queries the KV store.
	eventCount int
	mu         sync.RWMutex

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger
}

// Compile-time interface checks
var (
	_ mono.ServiceProviderModule = (*AnalyticModule)(nil)
	_ mono.UsePluginModule       = (*AnalyticModule)(nil)
)

// NewModule creates a new analytics module
func NewModule() *AnalyticModule {
	return &AnalyticModule{
		// Create buffered channels for better performance
		trackEventIn:  make(chan *mono.Msg, 100),
		trackEventOut: make(chan *mono.Msg, 100),
		logger:        slog.New(slog.NewTextHandler(os.Stdout, nil)).With("module", NAME),
	}
}

// Name returns the module identifier
func (m *AnalyticModule) Name() string {
	return NAME
}

// SetPlugin receives a plugin instance by its alias.
func (m *AnalyticModule) SetPlugin(alias string, plugin mono.PluginModule) {
	if alias == "kv" {
		kvPlugin, ok := plugin.(*kvjetstream.PluginModule)
		if ok {
			m.kvPlugin = kvPlugin
		}
	}
}

// Start initializes the module and starts the channel handler goroutine
func (m *AnalyticModule) Start(_ context.Context) error {
	// Get the KV bucket from the plugin
	if m.kvPlugin == nil {
		return fmt.Errorf("kv plugin not set")
	}

	m.kvStore = m.kvPlugin.Bucket(BucketName)
	if m.kvStore == nil {
		return fmt.Errorf("bucket %q not found in kv plugin", BucketName)
	}

	// Create cancellable context for graceful shutdown
	m.ctx, m.cancel = context.WithCancel(context.Background())

	// Start handler goroutine with WaitGroup tracking
	m.wg.Add(1)
	go m.handleTrackEventChannel()

	fmt.Println("  → Analytics module started (using kv-jetstream for storage)")
	return nil
}

// Stop gracefully shuts down the module
func (m *AnalyticModule) Stop(ctx context.Context) error {
	fmt.Println("  → Analytics module stopping...")

	// Signal shutdown to handler goroutine
	m.cancel()

	// Close input channel to stop receiving new requests
	close(m.trackEventIn)

	// Wait for handler to complete with timeout
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("  → Analytics: handler goroutine exited cleanly")
	case <-time.After(5 * time.Second):
		fmt.Println("  → Analytics: handler goroutine shutdown timeout")
	}

	// Close output channel after handler completes
	close(m.trackEventOut)

	// Get event count from KV store for final reporting
	// This query is acceptable in Stop() since performance is not critical during shutdown
	eventCount, err := m.getTotalEventCount(ctx)
	if err != nil {
		m.logger.Warn("failed to get event count from KV store", "error", err)
		// Fall back to session counter if KV store query fails
		m.mu.RLock()
		sessionCount := m.eventCount
		m.mu.RUnlock()
		fmt.Printf("  → Analytics module stopped (session tracked %d events, total count unavailable)\n", sessionCount)
	} else {
		fmt.Printf("  → Analytics module stopped (total %d events in store)\n", eventCount)
	}
	return nil
}

// RegisterServices registers the channel-based track-event service and request-reply get-event service
func (m *AnalyticModule) RegisterServices(container mono.ServiceContainer) error {
	// Register bidirectional channel service
	// The framework will handle the service registration and subject mapping
	if err := container.RegisterChannelService("track-event", m.trackEventIn, m.trackEventOut); err != nil {
		return err
	}

	// Register the get-event request-reply service
	return container.RegisterRequestReplyService("get-event", m.getEvent)
}

// handleTrackEventChannel is the goroutine that processes channel requests
func (m *AnalyticModule) handleTrackEventChannel() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			// Graceful shutdown requested
			return

		case msg, ok := <-m.trackEventIn:
			if !ok {
				// Channel closed, exit gracefully
				return
			}

			// Process the request
			response := m.processTrackEvent(msg)

			// Send response with timeout to avoid blocking on shutdown
			select {
			case m.trackEventOut <- response:
				// Response sent successfully
			case <-time.After(1 * time.Second):
				m.logger.Warn("timeout sending response", "event_id", response.Subject)
			case <-m.ctx.Done():
				// Shutdown requested, exit
				return
			}
		}
	}
}

// processTrackEvent handles individual event tracking requests
func (m *AnalyticModule) processTrackEvent(msg *mono.Msg) *mono.Msg {
	// Parse request
	var request TrackEventRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		return m.createErrorResponse(msg, fmt.Sprintf("invalid request: %v", err))
	}

	// Validate request
	if request.EventType == "" {
		return m.createErrorResponse(msg, "event_type is required")
	}
	if request.UserID == "" {
		return m.createErrorResponse(msg, "user_id is required")
	}

	// Generate event ID
	eventID := fmt.Sprintf("evt_%s_%d", request.UserID, time.Now().UnixNano())

	// Create event record
	record := eventRecord{
		ID:         eventID,
		Request:    request,
		RecordedAt: time.Now(),
	}

	// Serialize and store in KV store
	recordData, err := json.Marshal(record)
	if err != nil {
		return m.createErrorResponse(msg, fmt.Sprintf("failed to serialize event: %v", err))
	}

	_, err = m.kvStore.PutWithRevisionWithContext(m.ctx, eventID, recordData, 0)
	if err != nil {
		return m.createErrorResponse(msg, fmt.Sprintf("failed to store event: %v", err))
	}

	// Increment session counter (O(1) operation, doesn't query KV store)
	// Note: This counter tracks events created by this instance only.
	// For a fully accurate count, query the KV store directly (expensive for high-throughput).
	m.mu.Lock()
	m.eventCount++
	totalCount := m.eventCount
	m.mu.Unlock()

	// Create success response
	response := TrackEventResponse{
		Success:    true,
		EventID:    eventID,
		TotalCount: totalCount,
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return m.createErrorResponse(msg, fmt.Sprintf("failed to marshal response: %v", err))
	}

	fmt.Printf("  → Analytics: tracked event %s (type=%s, user=%s)\n",
		eventID, request.EventType, request.UserID)

	return &mono.Msg{
		Subject: msg.Reply, // Use reply subject from request
		Data:    responseData,
	}
}

// createErrorResponse creates an error response message and logs the error
func (m *AnalyticModule) createErrorResponse(msg *mono.Msg, errorMsg string) *mono.Msg {
	// Log the error for debugging/monitoring purposes
	m.logger.Warn("track event failed", "error", errorMsg, "subject", msg.Subject)

	response := TrackEventResponse{
		Success: false,
		EventID: "",
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		// Fallback to empty response if marshaling fails
		responseData = []byte(`{"success":false}`)
	}

	return &mono.Msg{
		Subject: msg.Reply,
		Data:    responseData,
	}
}

// getEvent handles get-event request-reply service requests.
// It validates the request, retrieves the event from KV storage, and returns
// a GetEventResponse indicating whether the event was found.
// Returns an error for invalid requests or marshaling failures.
func (m *AnalyticModule) getEvent(ctx context.Context, req *mono.Msg) ([]byte, error) {
	// Validate request size
	if len(req.Data) > 1024*1024 { // 1MB limit
		return nil, fmt.Errorf("request too large")
	}

	// Parse request
	var request GetEventRequest
	if err := json.Unmarshal(req.Data, &request); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	// Validate request
	if request.EventID == "" {
		return nil, fmt.Errorf("event_id is required")
	}
	if len(request.EventID) > 256 {
		return nil, fmt.Errorf("event_id too long")
	}

	// Retrieve event from KV store
	entry, err := m.kvStore.GetEntryWithContext(ctx, request.EventID)

	// Create response
	response := GetEventResponse{
		Found: false,
	}

	if err == nil && entry != nil {
		// Deserialize the event record
		var record eventRecord
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			return nil, fmt.Errorf("failed to deserialize event: %w", err)
		}

		response.Found = true
		response.EventID = record.ID
		response.EventType = record.Request.EventType
		response.UserID = record.Request.UserID
		response.Properties = record.Request.Properties
		response.Timestamp = record.Request.Timestamp
		response.RecordedAt = record.RecordedAt

		fmt.Printf("  → Analytics: retrieved event %s (type=%s, user=%s)\n",
			record.ID, record.Request.EventType, record.Request.UserID)
	} else {
		fmt.Printf("  → Analytics: event %s not found\n", request.EventID)
	}

	data, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return data, nil
}

// getTotalEventCount returns the total number of events stored in the KV store.
// This provides an accurate count that persists across module restarts and
// correctly reflects any deleted events.
func (m *AnalyticModule) getTotalEventCount(ctx context.Context) (int, error) {
	keys, err := m.kvStore.KeysWithContext(ctx)
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}
