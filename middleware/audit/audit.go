// Package audit provides a middleware module for tamper-evident audit logging.
//
// The audit module implements types.MiddlewareModule to intercept framework events
// and log them with cryptographic hash chaining for tamper detection.
//
// Example usage:
//
//	auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
//	auditModule, _ := audit.New(
//	    audit.WithOutput(auditFile),
//	    audit.WithHashChaining(""),  // Start new chain
//	)
//
//	framework, _ := mono.NewMonoApplication()
//	framework.Register(auditModule)  // Register as first module
//	framework.Start(ctx)
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// AuditModule implements types.MiddlewareModule to provide tamper-evident audit logging.
//
// The module:
//   - Logs all module lifecycle events (registered, started, stopped)
//   - Logs all service registration events
//   - Logs configuration change events
//   - Uses SHA-256 hash chaining for tamper detection
//   - Writes JSON-formatted entries to configured output
//   - Provides a channel service for custom audit trail entries
//
// The module is an observer - it doesn't modify events, just logs them.
type AuditModule struct {
	name        string
	writer      io.Writer
	hashChain   *HashChain
	userCtxFunc func(context.Context) string
	mu          sync.Mutex

	// Channel service for custom audit entries
	auditTrailIn  chan *types.Msg
	auditTrailOut chan *types.Msg

	// Lifecycle management
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	started  bool
	stopOnce sync.Once
}

// Ensure Module implements MiddlewareModule.
var _ types.MiddlewareModule = (*AuditModule)(nil)

// New creates a new audit module with the given options.
//
// The module requires at least WithOutput to be specified.
// Hash chaining must be explicitly enabled using WithHashChaining.
//
// Example:
//
//	auditModule, err := audit.New(
//	    audit.WithOutput(auditFile),
//	    audit.WithHashChaining(""),  // Start new chain, or pass lastHash to resume
//	    audit.WithUserContext(extractUserFromContext),
//	)
func New(opts ...Option) (*AuditModule, error) {
	options := defaultOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	if options.Output == nil {
		return nil, fmt.Errorf("audit module requires WithOutput option")
	}

	m := &AuditModule{
		name:        "audit",
		writer:      options.Output,
		userCtxFunc: options.UserContextFunc,
		// Create buffered channels for the audit trail service
		auditTrailIn:  make(chan *types.Msg, 100),
		auditTrailOut: make(chan *types.Msg, 100),
	}

	if options.EnableChaining {
		m.hashChain = NewHashChain(options.LastSavedHash)
	}

	return m, nil
}

// Name returns the module name.
func (m *AuditModule) Name() string {
	return m.name
}

// Start initializes the audit module and starts the channel handler goroutine.
func (m *AuditModule) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Prevent multiple starts
	if m.started {
		return fmt.Errorf("audit module already started")
	}

	// Create cancellable context for graceful shutdown
	m.ctx, m.cancel = context.WithCancel(context.Background())

	// Start handler goroutine for channel service
	m.wg.Add(1)
	go m.handleAuditTrailChannel()

	m.started = true
	return nil
}

// Stop flushes and closes the audit log.
// If the writer implements io.Closer, it will be closed.
// Stop is idempotent - multiple calls will have no effect.
func (m *AuditModule) Stop(_ context.Context) error {
	var stopErr error

	m.stopOnce.Do(func() {
		// Signal shutdown to handler goroutine
		m.mu.Lock()
		if m.cancel != nil {
			m.cancel()
		}
		m.mu.Unlock()

		// Close input channel to stop receiving new requests
		close(m.auditTrailIn)

		// Wait for handler to complete with timeout
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Handler exited cleanly
		case <-time.After(5 * time.Second):
			// Handler shutdown timeout - continue with cleanup
		}

		// Close output channel after handler completes
		close(m.auditTrailOut)

		m.mu.Lock()
		defer m.mu.Unlock()

		// Flush if writer supports Sync
		if syncer, ok := m.writer.(interface{ Sync() error }); ok {
			if err := syncer.Sync(); err != nil {
				stopErr = fmt.Errorf("failed to sync audit log: %w", err)
				return
			}
		}

		// Close if writer supports Close
		if closer, ok := m.writer.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				stopErr = fmt.Errorf("failed to close audit log: %w", err)
				return
			}
		}
	})

	return stopErr
}

// OnModuleLifecycle intercepts module lifecycle events and logs them.
// The event is passed through unchanged (observer pattern).
//
// Note: ModuleRegisteredEvent cannot be captured because it occurs before
// the middleware chain is built (during framework.Register(), which happens
// before framework.Start()). Only ModuleStartedEvent and ModuleStoppedEvent
// are captured.
func (m *AuditModule) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	var entry Entry

	switch event.Type {
	case types.ModuleStartedEvent:
		entry = Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStarted,
			ModuleName: event.ModuleName,
			Details: map[string]any{
				"duration_ms": float64(event.Duration.Milliseconds()),
			},
		}

	case types.ModuleStoppedEvent:
		details := map[string]any{}
		if event.Error != nil {
			details["error"] = event.Error.Error()
		}
		entry = Entry{
			Timestamp:  time.Now().UTC(),
			EventType:  EventModuleStopped,
			ModuleName: event.ModuleName,
			Details:    details,
		}

	default:
		// Ignore ModuleRegisteredEvent and other unknown events
		return event
	}

	m.writeEntry(ctx, entry)
	return event // Pass through unchanged
}

// OnServiceRegistration intercepts service registration events and logs them.
// The registration is passed through unchanged (observer pattern).
func (m *AuditModule) OnServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	entry := Entry{
		Timestamp:   time.Now().UTC(),
		EventType:   EventServiceRegistered,
		ServiceName: reg.Name,
		ModuleName:  reg.ModuleName,
		Details: map[string]any{
			"service_type": types.FormatServiceType(reg.Type),
		},
	}

	m.writeEntry(ctx, entry)
	return reg // Pass through unchanged
}

// OnConfigurationChange intercepts configuration change events and logs them.
// The event is passed through unchanged (observer pattern).
func (m *AuditModule) OnConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	entry := Entry{
		Timestamp: time.Now().UTC(),
		EventType: EventConfigurationUpdate,
		Details: map[string]any{
			"option_name": event.OptionName,
			"old_value":   logger.RedactSensitiveValue(event.OptionName, event.OldValue),
			"new_value":   logger.RedactSensitiveValue(event.OptionName, event.NewValue),
		},
	}

	m.writeEntry(ctx, entry)
	return event // Pass through unchanged
}

// OnOutgoingMessage passes through outgoing messages without modification.
// Audit module does not intercept outgoing messages.
func (m *AuditModule) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx // Pass through unchanged
}

// OnEventConsumerRegistration logs event consumer registration (observer pattern).
// The entry is passed through unchanged.
func (m *AuditModule) OnEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	auditEntry := Entry{
		Timestamp:   time.Now().UTC(),
		EventType:   EventServiceRegistered,
		ServiceName: entry.EventDef.Name,
		ModuleName:  entry.Module.Name(),
		Details: map[string]any{
			"service_type": "event_consumer",
			"event_module": entry.EventDef.ModuleName,
			"event_name":   entry.EventDef.Name,
			"version":      entry.EventDef.Version,
			"queue_group":  entry.QueueGroup,
		},
	}
	m.writeEntry(ctx, auditEntry)
	return entry // Pass through unchanged
}

// OnEventStreamConsumerRegistration logs event stream consumer registration (observer pattern).
// The entry is passed through unchanged.
func (m *AuditModule) OnEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	// Defensive nil check for Module
	moduleName := "<unknown>"
	if entry.Module != nil {
		moduleName = entry.Module.Name()
	}

	auditEntry := Entry{
		Timestamp:   time.Now().UTC(),
		EventType:   EventServiceRegistered,
		ServiceName: entry.EventDef.Name,
		ModuleName:  moduleName,
		Details: map[string]any{
			"service_type":    "event_stream_consumer",
			"consumer_module": moduleName,                // Consumer module
			"event_module":    entry.EventDef.ModuleName, // Event source module (emitter)
			"event_name":      entry.EventDef.Name,
			"version":         entry.EventDef.Version,
			"stream_name":     entry.Config.Stream.Name,
			"sequence_id":     entry.SequenceID,
		},
	}
	m.writeEntry(ctx, auditEntry)
	return entry // Pass through unchanged
}

// writeEntry writes an audit entry with optional hash chaining.
func (m *AuditModule) writeEntry(ctx context.Context, entry Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add user context if available
	entry.UserContext = m.userCtxFunc(ctx)

	// Add hash chaining if enabled
	if m.hashChain != nil {
		entry = m.hashChain.AddEntry(entry)
	}

	// Write to audit log as JSON
	data, err := json.Marshal(entry)
	if err != nil {
		// This should not happen in practice, but log to stderr if it does
		if _, writeErr := fmt.Fprintf(m.writer, "{\"error\":\"failed to marshal audit entry: %v\"}\n", err); writeErr != nil {
			// Critical: Cannot write to audit log
			fmt.Fprintf(os.Stderr, "CRITICAL: Audit log write failure: marshal_err=%v, write_err=%v\n", err, writeErr)
			return
		}
		return
	}

	// Write JSON line
	if _, err := m.writer.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to write audit entry: %v\n", err)
		return
	}
	if _, err := m.writer.Write([]byte("\n")); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to write audit entry newline: %v\n", err)
		return
	}
}

// RegisterServices registers the channel-based audit-trail service.
//
// Other modules can use this service to save custom audit entries via the adapter.
// The service name is "audit-trail".
func (m *AuditModule) RegisterServices(container types.ServiceContainer) error {
	return container.RegisterChannelService("audit-trail", m.auditTrailIn, m.auditTrailOut)
}

// handleAuditTrailChannel is the goroutine that processes channel requests
// for custom audit trail entries.
func (m *AuditModule) handleAuditTrailChannel() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			// Graceful shutdown requested
			return

		case msg, ok := <-m.auditTrailIn:
			if !ok {
				// Channel closed, exit gracefully
				return
			}

			// Process the single entry
			response := m.processAuditTrailEntry(msg)

			// Only send response if this is a sync request (has Reply subject)
			if response != nil && msg.Reply != "" {
				// Send response with timeout to avoid blocking on shutdown
				select {
				case m.auditTrailOut <- response:
					// Response sent successfully
				case <-time.After(1 * time.Second):
					// Timeout sending response - continue
				case <-m.ctx.Done():
					// Shutdown requested, exit
					return
				}
			}
		}
	}
}

// SaveEntryResponse represents the response after saving a single audit entry.
type SaveEntryResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// processAuditTrailEntry handles a single audit entry from the channel.
func (m *AuditModule) processAuditTrailEntry(msg *types.Msg) *types.Msg {
	// Parse entry
	var entry Entry
	if err := json.Unmarshal(msg.Data, &entry); err != nil {
		return m.createEntryErrorResponse(msg, fmt.Sprintf("invalid entry: %v", err))
	}

	// Set timestamp to current time (as per requirement)
	entry.Timestamp = time.Now().UTC()

	// Set event type if not specified
	if entry.EventType == "" {
		entry.EventType = EventCustomAuditTrail
	}

	// Write the entry using the standard method
	m.writeEntry(context.Background(), entry)

	// Create success response
	response := SaveEntryResponse{
		Success: true,
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return m.createEntryErrorResponse(msg, fmt.Sprintf("failed to marshal response: %v", err))
	}

	return &types.Msg{
		Subject: msg.Reply,
		Data:    responseData,
	}
}

// createEntryErrorResponse creates an error response message.
func (m *AuditModule) createEntryErrorResponse(msg *types.Msg, errorMsg string) *types.Msg {
	response := SaveEntryResponse{
		Success: false,
		Error:   errorMsg,
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		// Fallback to basic error response if marshaling fails
		responseData = []byte(`{"success":false,"error":"internal error"}`)
	}

	return &types.Msg{
		Subject: msg.Reply,
		Data:    responseData,
	}
}
