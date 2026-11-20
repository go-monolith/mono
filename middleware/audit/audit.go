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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/internal/output"
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

	// Lifecycle management. lifecycleMu serialises Start, Stop and
	// RollbackStart and guards cancel, handlerDone and started. mu guards
	// the writer, the chain and the two flags below; a write holds it, so
	// a handler stuck in a write holds it too. Start and stop take it,
	// with lifecycleMu held, only once the handler has exited (or was
	// never started), so they do not wait behind such a write.
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	// handlerDone is closed when the handler goroutine of the current
	// start has exited. One channel per start: a WaitGroup would be waited
	// on by a Stop that gave up while the next Start adds to it.
	handlerDone chan struct{}
	started     bool
	// outputClosed is set once Stop closed the writer: nothing can be
	// written after that, so the module cannot be started again.
	outputClosed bool
	// tornLine is set when a failed write left part of a line in the log.
	tornLine bool
	// closeOwed is set (under lifecycleMu) when a final Stop gave up on a
	// handler that was still in a write: the log is closed once it exits.
	closeOwed bool
}

// ErrOutputClosed is returned by Start, and for an entry that arrives, after
// Stop closed the module's output. A stopped module whose output was closed
// cannot be started again: build a new one with New.
var ErrOutputClosed = errors.New("audit log output was closed by Stop")

// handlerStopTimeout bounds how long Stop waits for the audit-trail handler
// to write the entries it accepted.
const handlerStopTimeout = 5 * time.Second

// Ensure Module implements MiddlewareModule.
var _ types.MiddlewareModule = (*AuditModule)(nil)

// Ensure Module can be stopped by the rollback of a failed Start and started
// again.
var _ types.RollbackAwareMiddleware = (*AuditModule)(nil)

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
//
// It fails when the module is already started (also while the handler a
// RollbackStart gave up on is still in a write), and with ErrOutputClosed
// when an earlier Stop closed the output. A module stopped by RollbackStart, or
// whose output Stop does not close (os.Stdout, os.Stderr, a writer that is
// not an io.Closer), can be started again.
func (m *AuditModule) Start(_ context.Context) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	// Prevent multiple starts
	if m.started {
		if !m.handlerStopped() {
			return fmt.Errorf("audit module already started")
		}
		// A stop gave up on a handler that was still in a write, and the
		// handler has finished since: what that stop left undone is done
		// now. Nobody would call it again, and without this every later
		// Start would fail.
		m.started = false
		if m.closeOwed {
			// It was the final Stop: the log is closed, as it would have
			// been, and the module cannot be started again.
			m.closeOwed = false
			if err := m.closeOutput(); err != nil {
				return err
			}
			return fmt.Errorf("audit module cannot be started again: %w", ErrOutputClosed)
		}
		m.mu.Lock()
		err := output.Flush(m.writer, "audit log")
		m.mu.Unlock()
		if err != nil {
			return err
		}
	}
	m.mu.Lock()
	closed := m.outputClosed
	m.mu.Unlock()
	if closed {
		return fmt.Errorf("audit module cannot be started again: %w", ErrOutputClosed)
	}

	// Create cancellable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	// Start handler goroutine for channel service
	m.handlerDone = make(chan struct{})
	go m.handleAuditTrailChannel(ctx, m.handlerDone)

	m.started = true
	return nil
}

// Stop writes the entries the audit-trail service has accepted, then flushes
// and closes the audit log (see output.FlushAndClose: os.Stdout and os.Stderr
// are never closed). Stop is idempotent - multiple calls will have no effect.
//
// The channels of the audit-trail service are not closed: other modules send
// on them, and a send on a closed channel panics. An entry sent after Stop is
// not written: SaveAuditTrail waits for a confirmation until its context ends
// and returns an error, AsyncSaveAuditTrail cannot tell.
func (m *AuditModule) Stop(ctx context.Context) error {
	return m.stop(ctx, true)
}

// RollbackStart stops the module like Stop but only flushes the audit log and
// leaves it open, so the module can be started again. The framework calls it
// instead of Stop when the application's Start failed after this module had
// started (types.RollbackAwareMiddleware): Stop would close the log file, and
// the application's next Start would have nothing to write to.
func (m *AuditModule) RollbackStart(ctx context.Context) error {
	return m.stop(ctx, false)
}

// stop stops the handler goroutine after it wrote what it had accepted, then
// flushes the writer and, with closeOutput, closes it.
//
// The wait for the handler is not given up the moment ctx ends: by the time
// a middleware is stopped the Stop context has often ended already (a module
// before it used it up), and the handler only has to write what is in its
// channel, which takes milliseconds. Closing the log under it would lose
// exactly the entries Stop is there to write.
func (m *AuditModule) stop(ctx context.Context, closeOutput bool) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()

	if !m.started {
		if !closeOutput {
			return nil
		}
		// Never started, or stopped by RollbackStart: the hooks may have
		// written entries all the same, and the final Stop owns the writer.
		return m.closeOutput()
	}

	// Tell the handler to stop; it first writes what is in the channel.
	m.cancel()
	if err := WaitForHandler(ctx, m.handlerDone); err != nil {
		// The handler is still in a write and holds the writer: it is
		// neither flushed nor closed under it, and the module stays
		// started, so that no second handler is started beside this one.
		// Nobody calls Stop a second time, so what a final Stop owes, the
		// close of the log, is done when the handler has exited.
		if closeOutput && !m.closeOwed {
			m.closeOwed = true
			go m.closeWhenHandlerStops(m.handlerDone)
		}
		return err
	}
	m.closeOwed = false
	m.started = false

	if !closeOutput {
		m.mu.Lock()
		defer m.mu.Unlock()
		return output.Flush(m.writer, "audit log")
	}
	return m.closeOutput()
}

// closeOutput flushes and closes the writer, except os.Stdout and os.Stderr,
// which the rest of the process keeps using (see output.FlushAndClose), once.
func (m *AuditModule) closeOutput() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.outputClosed {
		return nil
	}
	m.outputClosed = output.Closes(m.writer)
	return output.FlushAndClose(m.writer, "audit log")
}

// closeWhenHandlerStops closes the log once the handler a final Stop gave up
// on has exited, unless a Start or another Stop did so first. Without it the
// log of a module whose Stop gave up would never be closed: the application
// calls Stop once.
func (m *AuditModule) closeWhenHandlerStops(done <-chan struct{}) {
	<-done
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if !m.closeOwed || !m.started {
		return
	}
	m.closeOwed = false
	m.started = false
	if err := m.closeOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to close audit log after its handler stopped: %v\n", err)
	}
}

// handlerStopped reports whether the handler of the current start was told
// to stop and has exited. Called with lifecycleMu held.
func (m *AuditModule) handlerStopped() bool {
	if m.handlerDone == nil {
		return false
	}
	select {
	case <-m.handlerDone:
		return true
	default:
		return false
	}
}

// handlerDrainGrace is how long the handler is still waited for after the
// Stop context ended. It is shorter than the time the framework waits for a
// Stop whose context has ended, so the wait fits into it.
const handlerDrainGrace = 100 * time.Millisecond

// WaitForHandler waits until done is closed, for at most handlerStopTimeout
// and, once ctx has ended, handlerDrainGrace more. It returns an error when
// the handler has not finished by then.
func WaitForHandler(ctx context.Context, done <-chan struct{}) error {
	timer := time.NewTimer(handlerStopTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("audit-trail handler still writing after %v; entries it accepted may not be in the log", handlerStopTimeout)
	case <-ctx.Done():
	}
	grace := time.NewTimer(handlerDrainGrace)
	defer grace.Stop()
	select {
	case <-done:
		return nil
	case <-grace.C:
		return fmt.Errorf("audit-trail handler still writing %v after the Stop context ended; entries it accepted may not be in the log: %w",
			handlerDrainGrace, ctx.Err())
	}
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

	m.recordEntry(ctx, entry)
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

	m.recordEntry(ctx, entry)
	return reg // Pass through unchanged
}

// OnConfigurationChange records a configuration change event and passes it
// through unchanged (observer pattern). The framework never calls it (see
// types.MiddlewareModule.OnConfigurationChange), so a running application
// writes no configuration.updated entries.
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

	m.recordEntry(ctx, entry)
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
	m.recordEntry(ctx, auditEntry)
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
	m.recordEntry(ctx, auditEntry)
	return entry // Pass through unchanged
}

// recordEntry writes an entry of the framework's own events. A hook has
// nobody to return an error to, so a failed write is reported on stderr.
func (m *AuditModule) recordEntry(ctx context.Context, entry Entry) {
	if err := m.writeEntry(ctx, entry); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to write audit entry: %v\n", err)
	}
}

// writeEntry writes an audit entry with optional hash chaining and returns
// the error of a write that failed.
//
// The chain advances only once the entry is in the log: an entry that was
// never written must not be the predecessor of the next one, or every later
// entry fails verification. The line and its newline go out in one Write; a
// write that fails after part of the line was written leaves a torn line,
// which the next write ends first, so the next entry never shares it.
func (m *AuditModule) writeEntry(ctx context.Context, entry Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.outputClosed {
		return ErrOutputClosed
	}

	// Add user context if available
	entry.UserContext = m.userCtxFunc(ctx)

	// Add hash chaining if enabled
	if m.hashChain != nil {
		entry = m.hashChain.Next(entry)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		// Details that cannot be encoded: the log gets a line saying that an
		// entry is missing here, and the chain stays where it was.
		marshalErr := fmt.Errorf("failed to marshal audit entry: %w", err)
		if _, writeErr := fmt.Fprintf(m.writer, "{\"error\":%q}\n", marshalErr.Error()); writeErr != nil {
			return errors.Join(marshalErr, fmt.Errorf("failed to write audit entry: %w", writeErr))
		}
		return marshalErr
	}
	line := append(data, '\n')
	if m.tornLine {
		// The write before this one failed after part of its line was
		// written: end that line, so this entry starts on its own.
		line = append([]byte{'\n'}, line...)
	}
	n, err := m.writer.Write(line)
	if err != nil {
		// A torn line that is still open (nothing of this write got out),
		// or one this write left, has to be ended by the next write.
		m.tornLine = (m.tornLine && n == 0) || (n > 0 && n < len(line))
		return fmt.Errorf("failed to write audit entry: %w", err)
	}
	m.tornLine = false
	if m.hashChain != nil {
		m.hashChain.Commit(entry)
	}
	return nil
}

// RegisterServices registers the channel-based audit-trail service.
//
// Other modules can use this service to save custom audit entries via the adapter.
// The service name is "audit-trail".
func (m *AuditModule) RegisterServices(container types.ServiceContainer) error {
	return container.RegisterChannelService("audit-trail", m.auditTrailIn, m.auditTrailOut)
}

// responseSendTimeout bounds how long the handler waits to hand a
// confirmation to the channel router.
const responseSendTimeout = time.Second

// handleAuditTrailChannel is the goroutine that processes channel requests
// for custom audit trail entries, until ctx ends. It then writes the entries
// still in the channel, so that what the module accepted before Stop is in
// the log when Stop returns.
func (m *AuditModule) handleAuditTrailChannel(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			m.drainAuditTrail()
			return

		case msg := <-m.auditTrailIn:
			if msg == nil {
				continue
			}
			response := m.processAuditTrailEntry(msg)

			// Only send response if this is a sync request (has Reply subject)
			if response == nil || msg.Reply == "" {
				continue
			}
			timer := time.NewTimer(responseSendTimeout)
			select {
			case m.auditTrailOut <- response:
				timer.Stop()
			case <-timer.C:
				// Nobody takes the confirmation - continue
			case <-ctx.Done():
				timer.Stop()
				m.drainAuditTrail()
				return
			}
		}
	}
}

// drainAuditTrail writes the entries waiting in the channel, without
// blocking. A confirmation is handed over only when there is room for it:
// the modules that asked have stopped by then.
func (m *AuditModule) drainAuditTrail() {
	for {
		select {
		case msg := <-m.auditTrailIn:
			if msg == nil {
				continue
			}
			response := m.processAuditTrailEntry(msg)
			if response == nil || msg.Reply == "" {
				continue
			}
			select {
			case m.auditTrailOut <- response:
			default:
			}
		default:
			return
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

	// Write the entry using the standard method. An entry that is not in
	// the log is not confirmed as saved.
	if err := m.writeEntry(context.Background(), entry); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to write audit entry: %v\n", err)
		return m.createEntryErrorResponse(msg, err.Error())
	}

	// Create success response
	response := SaveEntryResponse{
		Success: true,
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return m.createEntryErrorResponse(msg, fmt.Sprintf("failed to marshal response: %v", err))
	}

	// Reply carries the consumer module name the request came from, so the
	// channel router delivers the response to that consumer only.
	return &types.Msg{
		Subject: msg.Reply,
		Reply:   msg.Reply,
		Data:    responseData,
		Header:  ConfirmationHeader(msg),
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

	// Reply carries the consumer module name the request came from, so the
	// channel router delivers the response to that consumer only.
	return &types.Msg{
		Subject: msg.Reply,
		Reply:   msg.Reply,
		Data:    responseData,
		Header:  ConfirmationHeader(msg),
	}
}

// CallHeader is the header a synchronous save puts on each entry it sends
// and the audit module copies into the confirmation, so that the adapter can
// tell the confirmations of its current call from those of an earlier one
// that it gave up waiting for.
const CallHeader = "Mono-Audit-Call"

// ConfirmationHeader returns the header of the confirmation of request: its
// CallHeader, when it has one.
func ConfirmationHeader(request *types.Msg) types.Header {
	if request == nil || len(request.Header[CallHeader]) == 0 {
		return nil
	}
	return types.Header{CallHeader: request.Header[CallHeader]}
}
