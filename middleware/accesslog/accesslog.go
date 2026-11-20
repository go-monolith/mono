// Package accesslog provides a middleware module for access logging.
//
// The accesslog module implements types.MiddlewareModule to intercept service
// handlers and log request/response details including timing, sizes, and status.
//
// Example usage:
//
//	accessFile, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//	accessModule, _ := accesslog.New(
//	    accesslog.WithOutput(accessFile),
//	    accesslog.WithFormat(accesslog.FormatJSON),
//	)
//
//	framework, _ := mono.NewMonoApplication()
//	framework.Register(accessModule)
//	framework.Start(ctx)
package accesslog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono/internal/output"
	"github.com/go-monolith/mono/internal/reqid"
	"github.com/go-monolith/mono/middleware/requestid"
	"github.com/go-monolith/mono/pkg/types"
)

// channelProxy manages the proxy channels and goroutines for a channel service.
type channelProxy struct {
	serviceName string
	moduleName  string

	// Original channels (from the module)
	originalIn  chan *types.Msg
	originalOut chan *types.Msg

	// Proxy channels (exposed to consumers)
	proxyIn  chan *types.Msg
	proxyOut chan *types.Msg

	// Lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	closeInOnce  sync.Once
	closeOutOnce sync.Once
}

// AccessLogModule implements types.MiddlewareModule to provide access logging.
//
// The module:
//   - Wraps all NATS-based service handlers (RequestReply, QueueGroup, StreamConsumer)
//   - Wraps channel services with proxy channels for access logging
//   - Captures request timing, sizes, and status
//   - Outputs in configurable formats (text or JSON)
//   - Allows field selection
//
// Unlike audit middleware (observer pattern), this middleware actively wraps
// handlers to capture before/after timing information.
type AccessLogModule struct {
	name            string
	writer          io.Writer
	format          Format
	fields          []Field
	requestIDHeader string
	formatter       Formatter
	mu              sync.Mutex
	shutdown        atomic.Bool
	// writes is held shared by every write for as long as it runs, and
	// taken exclusively by stop once the shutdown flag is set: when stop
	// has had it, no write is in progress and none begins. A WaitGroup
	// cannot do this: a write that read the flag just before it was set
	// would add itself beside the Wait, and stop would close the log
	// under it.
	writes sync.RWMutex
	// outputClosed is set (under mu) once Stop closed the writer: nothing
	// can be written after that, so the module cannot be started again.
	outputClosed bool

	// Channel proxy management
	channelProxies   map[string]*channelProxy
	channelProxiesMu sync.RWMutex
}

// Ensure Module implements MiddlewareModule.
var _ types.MiddlewareModule = (*AccessLogModule)(nil)

// Ensure Module can be stopped by the rollback of a failed Start and started
// again.
var _ types.RollbackAwareMiddleware = (*AccessLogModule)(nil)

// ErrOutputClosed is returned by Start after Stop closed the module's output.
// A stopped module whose output was closed cannot be started again: build a
// new one with New.
var ErrOutputClosed = errors.New("access log output was closed by Stop")

// New creates a new access log module with the given options.
//
// The module requires at least WithOutput to be specified.
//
// Example:
//
//	accessModule, err := accesslog.New(
//	    accesslog.WithOutput(accessFile),
//	    accesslog.WithFormat(accesslog.FormatText),
//	    accesslog.WithFields([]accesslog.Field{
//	        accesslog.FieldTimestamp,
//	        accesslog.FieldService,
//	        accesslog.FieldDurationMS,
//	    }),
//	)
func New(opts ...Option) (*AccessLogModule, error) {
	options := defaultOptions()
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	if options.Output == nil {
		return nil, fmt.Errorf("access log module requires WithOutput option")
	}

	m := &AccessLogModule{
		name:            "accesslog",
		writer:          options.Output,
		format:          options.Format,
		fields:          options.Fields,
		requestIDHeader: options.RequestIDHeader,
		formatter:       newFormatter(options.Format),
		channelProxies:  make(map[string]*channelProxy),
	}

	return m, nil
}

// Name returns the module name.
func (m *AccessLogModule) Name() string {
	return m.name
}

// Start initializes the access log module.
//
// It fails with ErrOutputClosed when an earlier Stop closed the output. A
// module stopped by RollbackStart, or whose output Stop does not close
// (os.Stdout, os.Stderr, a writer that is not an io.Closer), can be started
// again: Start clears the shutdown state Stop left, which used to make a
// restarted module log nothing.
func (m *AccessLogModule) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.outputClosed {
		return fmt.Errorf("access log module cannot be started again: %w", ErrOutputClosed)
	}
	m.shutdown.Store(false)
	return nil
}

// Stop stops the channel proxies, waits for the writes in flight, then
// flushes and closes the access log (see output.FlushAndClose: os.Stdout and
// os.Stderr are never closed).
func (m *AccessLogModule) Stop(ctx context.Context) error {
	return m.stop(ctx, true)
}

// RollbackStart stops the module like Stop but only flushes the access log
// and leaves it open, so the module can be started again. The framework calls
// it instead of Stop when the application's Start failed after this module
// had started (types.RollbackAwareMiddleware): Stop would close the log file,
// and the application's next Start would have nothing to write to.
func (m *AccessLogModule) RollbackStart(ctx context.Context) error {
	return m.stop(ctx, false)
}

// stopGrace is how long the proxies and the writes in flight are still
// waited for after the Stop context ended. It is shorter than the time the
// framework waits for a Stop whose context has ended.
const stopGrace = 100 * time.Millisecond

// WaitWithGrace waits until done is closed and reports whether it was. Once
// ctx has ended it waits stopGrace more: by the time a middleware is stopped
// the Stop context has often ended already (a module before it used it up),
// and giving up at once would leave the log neither flushed nor closed
// although the proxies and writes finish in a moment.
func WaitWithGrace(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
	}
	grace := time.NewTimer(stopGrace)
	defer grace.Stop()
	select {
	case <-done:
		return true
	case <-grace.C:
		return false
	}
}

// stop stops the proxies and writes, flushes the writer and, with
// closeOutput, closes it.
func (m *AccessLogModule) stop(ctx context.Context, closeOutput bool) error {
	// Signal shutdown to prevent new writes
	m.shutdown.Store(true)

	// Stop all channel proxies - copy to local slice to avoid holding lock during Wait
	m.channelProxiesMu.Lock()
	proxies := make([]*channelProxy, 0, len(m.channelProxies))
	for _, proxy := range m.channelProxies {
		proxy.cancel()
		proxies = append(proxies, proxy)
	}
	// Clear the map to allow GC and prevent reuse
	m.channelProxies = make(map[string]*channelProxy)
	m.channelProxiesMu.Unlock()

	// Wait for channel proxy goroutines to complete with timeout
	if len(proxies) > 0 {
		done := make(chan struct{})
		go func() {
			for _, proxy := range proxies {
				proxy.wg.Wait()
			}
			close(done)
		}()

		if !WaitWithGrace(ctx, done) {
			return fmt.Errorf("timeout waiting for channel proxies to stop: %w", ctx.Err())
		}
	}

	// Wait for all in-flight writes to complete with timeout. The flag is
	// set, so a write that gets the lock after this returns at once.
	done := make(chan struct{})
	go func() {
		m.writes.Lock()
		m.writes.Unlock() //nolint:staticcheck // SA2001: taken only to wait for the writes in progress
		close(done)
	}()

	if !WaitWithGrace(ctx, done) {
		return fmt.Errorf("timeout waiting for in-flight writes to complete: %w", ctx.Err())
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.outputClosed {
		return nil // an earlier Stop closed it
	}
	if !closeOutput {
		return output.Flush(m.writer, "access log")
	}
	// Flush and close the writer, except os.Stdout and os.Stderr, which the
	// rest of the process keeps using (see output.FlushAndClose).
	m.outputClosed = output.Closes(m.writer)
	return output.FlushAndClose(m.writer, "access log")
}

// OnModuleLifecycle passes through lifecycle events without modification.
// Access log does not intercept lifecycle events.
func (m *AccessLogModule) OnModuleLifecycle(_ context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	return event // Pass through unchanged
}

// OnServiceRegistration wraps NATS-based service handlers for access logging.
//
// This method intercepts service registration and wraps handlers to capture:
//   - Request start time (before handler call)
//   - Request end time (after handler call)
//   - Response status (success/error)
//   - Request/response size
//
// Handler types wrapped:
//   - RequestReply: wraps RequestHandler
//   - QueueGroup: wraps each handler in QueueHandlers
//   - StreamConsumer: wraps StreamHandler
//   - Channel: passed through unchanged (in-process Go channels, not NATS)
func (m *AccessLogModule) OnServiceRegistration(_ context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	switch reg.Type {
	case types.ServiceTypeRequestReply:
		if reg.RequestHandler != nil {
			reg.RequestHandler = m.wrapRequestReplyHandler(
				reg.RequestHandler,
				reg.ModuleName,
				reg.Name,
			)
		}

	case types.ServiceTypeQueueGroup:
		if len(reg.QueueHandlers) > 0 {
			wrappedPairs := make([]types.QGHP, len(reg.QueueHandlers))
			for i, pair := range reg.QueueHandlers {
				wrappedPairs[i] = types.QGHP{
					QueueGroup: pair.QueueGroup,
					Handler:    m.wrapQueueGroupHandler(pair.Handler, reg.ModuleName, reg.Name),
					Options:    pair.Options, // worker pool bounds must survive wrapping
				}
			}
			reg.QueueHandlers = wrappedPairs
		}

	case types.ServiceTypeStreamConsumer:
		if reg.StreamHandler != nil {
			reg.StreamHandler = m.wrapStreamConsumerHandler(
				reg.StreamHandler,
				reg.ModuleName,
				reg.Name,
			)
		}

	case types.ServiceTypeChannel:
		// Wrap channel services with proxy for access logging
		if reg.InChannel != nil && reg.OutChannel != nil {
			proxyIn, proxyOut := m.wrapChannelService(
				reg.InChannel,
				reg.OutChannel,
				reg.ModuleName,
				reg.Name,
			)
			reg.InChannel = proxyIn
			reg.OutChannel = proxyOut
		}

	default:
		// Unknown service type - pass through unchanged
	}

	return reg
}

// OnConfigurationChange passes through configuration events without modification.
// Access log does not intercept configuration events.
func (m *AccessLogModule) OnConfigurationChange(_ context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	return event // Pass through unchanged
}

// OnOutgoingMessage passes through outgoing messages without modification.
// Access log does not intercept outgoing messages.
func (m *AccessLogModule) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx // Pass through unchanged
}

// OnEventConsumerRegistration wraps event consumer handlers for access logging.
//
// This method intercepts event consumer registration and wraps handlers to capture:
//   - Request start time (before handler call)
//   - Request end time (after handler call)
//   - Response status (success/error)
//   - Request size
func (m *AccessLogModule) OnEventConsumerRegistration(_ context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	if entry.Handler != nil {
		entry.Handler = m.wrapEventConsumerHandler(
			entry.Handler,
			entry.Module.Name(),
			entry.EventDef.Name,
		)
	}
	return entry
}

// OnEventStreamConsumerRegistration wraps event stream consumer handlers for access logging.
//
// This method intercepts event stream consumer registration and wraps handlers to capture:
//   - Request start time (before handler call)
//   - Request end time (after handler call)
//   - Response status (success/error)
//   - Total request size (sum of all messages in batch)
func (m *AccessLogModule) OnEventStreamConsumerRegistration(_ context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	if entry.Handler != nil {
		// Defensive nil check for Module
		moduleName := "<unknown>"
		if entry.Module != nil {
			moduleName = entry.Module.Name()
		}
		entry.Handler = m.wrapEventStreamConsumerHandler(
			entry.Handler,
			moduleName,
			entry.EventDef.Name,
		)
	}
	return entry
}

// wrapEventConsumerHandler wraps an EventConsumerHandler to capture timing and status.
func (m *AccessLogModule) wrapEventConsumerHandler(
	original types.EventConsumerHandler,
	moduleName, eventName string,
) types.EventConsumerHandler {
	return func(ctx context.Context, msg *types.Msg) error {
		start := time.Now()
		requestSize := len(msg.Data)
		reqID := m.extractRequestID(msg)
		if reqID == "" {
			reqID = requestid.GetRequestID(ctx)
		}

		// The line is written when the handler returns and also when it
		// panics: a deferred function runs in both cases. The panic is not
		// recovered here, so it reaches the framework with its own stack.
		completed := false
		var err error
		defer func() {
			duration := time.Since(start)
			status := StatusSuccess
			if err != nil || !completed {
				status = StatusError
			}

			entry := Entry{
				Timestamp:   start.UTC(),
				RequestID:   reqID,
				Module:      moduleName,
				Service:     eventName,
				ServiceType: "event_consumer",
				DurationMS:  duration.Milliseconds(),
				Status:      status,
				RequestSize: requestSize,
				// ResponseSize is 0 for event_consumer (no response)
			}
			m.writeEntry(entry)
		}()

		// Call the original handler
		err = original(ctx, msg)
		completed = true
		return err
	}
}

// wrapRequestReplyHandler wraps a RequestReplyHandler to capture timing and status.
func (m *AccessLogModule) wrapRequestReplyHandler(
	original types.RequestReplyHandler,
	moduleName, serviceName string,
) types.RequestReplyHandler {
	return func(ctx context.Context, req *types.Msg) ([]byte, error) {
		start := time.Now()
		requestSize := len(req.Data)
		requestID := m.extractRequestID(req)
		if requestID == "" {
			requestID = requestid.GetRequestID(ctx)
		}
		// The line is written when the handler returns and also when it
		// panics: a deferred function runs in both cases. The panic is not
		// recovered here, so it reaches the framework with its own stack.
		completed := false
		var response []byte
		var err error
		defer func() {
			duration := time.Since(start)
			status := StatusSuccess
			if err != nil || !completed {
				status = StatusError
			}

			entry := Entry{
				Timestamp:    start.UTC(),
				RequestID:    requestID,
				Module:       moduleName,
				Service:      serviceName,
				ServiceType:  "request_reply",
				DurationMS:   duration.Milliseconds(),
				Status:       status,
				RequestSize:  requestSize,
				ResponseSize: len(response),
			}
			m.writeEntry(entry)
		}()

		// Call the original handler
		response, err = original(ctx, req)
		completed = true
		return response, err
	}
}

// wrapQueueGroupHandler wraps a QueueGroupHandler to capture timing and status.
func (m *AccessLogModule) wrapQueueGroupHandler(
	original types.QueueGroupHandler,
	moduleName, serviceName string,
) types.QueueGroupHandler {
	return func(ctx context.Context, msg *types.Msg) error {
		start := time.Now()
		requestSize := len(msg.Data)
		requestID := m.extractRequestID(msg)

		// The line is written when the handler returns and also when it
		// panics: a deferred function runs in both cases. The panic is not
		// recovered here, so it reaches the framework with its own stack.
		completed := false
		var err error
		defer func() {
			duration := time.Since(start)
			status := StatusSuccess
			if err != nil || !completed {
				status = StatusError
			}

			entry := Entry{
				Timestamp:   start.UTC(),
				RequestID:   requestID,
				Module:      moduleName,
				Service:     serviceName,
				ServiceType: "queue_group",
				DurationMS:  duration.Milliseconds(),
				Status:      status,
				RequestSize: requestSize,
				// ResponseSize is 0 for queue_group (fire-and-forget)
			}
			m.writeEntry(entry)
		}()

		// Call the original handler
		err = original(ctx, msg)
		completed = true
		return err
	}
}

// wrapStreamConsumerHandler wraps a StreamConsumerHandler to capture timing and status.
// For batch handlers, we log one entry for the entire batch.
func (m *AccessLogModule) wrapStreamConsumerHandler(
	original types.StreamConsumerHandler,
	moduleName, serviceName string,
) types.StreamConsumerHandler {
	return func(ctx context.Context, msgs []*types.Msg) error {
		start := time.Now()

		// Calculate total request size across all messages
		requestSize := 0
		for _, msg := range msgs {
			requestSize += len(msg.Data)
		}

		// Use request ID from first message if available
		var requestID string
		if len(msgs) > 0 {
			requestID = m.extractRequestID(msgs[0])
		}

		// The line is written when the handler returns and also when it
		// panics: a deferred function runs in both cases. The panic is not
		// recovered here, so it reaches the framework with its own stack.
		completed := false
		var err error
		defer func() {
			duration := time.Since(start)
			status := StatusSuccess
			if err != nil || !completed {
				status = StatusError
			}

			entry := Entry{
				Timestamp:   start.UTC(),
				RequestID:   requestID,
				Module:      moduleName,
				Service:     serviceName,
				ServiceType: "stream_consumer",
				DurationMS:  duration.Milliseconds(),
				Status:      status,
				RequestSize: requestSize,
				// ResponseSize is 0 for stream_consumer
			}
			m.writeEntry(entry)
		}()

		// Call the original handler
		err = original(ctx, msgs)
		completed = true
		return err
	}
}

// wrapEventStreamConsumerHandler wraps an EventStreamConsumerHandler to capture timing and status.
// For batch handlers, we log one entry for the entire batch.
func (m *AccessLogModule) wrapEventStreamConsumerHandler(
	original types.EventStreamConsumerHandler,
	moduleName, eventName string,
) types.EventStreamConsumerHandler {
	return func(ctx context.Context, msgs []*types.Msg) error {
		start := time.Now()

		// Calculate total request size across all messages
		requestSize := 0
		for _, msg := range msgs {
			requestSize += len(msg.Data)
		}

		// Use request ID from first message if available
		var requestID string
		if len(msgs) > 0 {
			requestID = m.extractRequestID(msgs[0])
		}
		if requestID == "" {
			requestID = requestid.GetRequestID(ctx)
		}

		// The line is written when the handler returns and also when it
		// panics: a deferred function runs in both cases. The panic is not
		// recovered here, so it reaches the framework with its own stack.
		completed := false
		var err error
		defer func() {
			duration := time.Since(start)
			status := StatusSuccess
			if err != nil || !completed {
				status = StatusError
			}

			entry := Entry{
				Timestamp:   start.UTC(),
				RequestID:   requestID,
				Module:      moduleName,
				Service:     eventName,
				ServiceType: "event_stream_consumer",
				DurationMS:  duration.Milliseconds(),
				Status:      status,
				RequestSize: requestSize,
				// ResponseSize is 0 for event_stream_consumer
			}
			m.writeEntry(entry)
		}()

		// Call the original handler
		err = original(ctx, msgs)
		completed = true
		return err
	}
}

// extractRequestID extracts the request ID from message headers. The header
// is written by the sender, so the value is sanitised (reqid.Sanitize: at
// most reqid.MaxLen bytes of a fixed character set) before it reaches a log
// line: as it came, it could forge fields of a text line or make a line as
// long as the sender liked.
func (m *AccessLogModule) extractRequestID(msg *types.Msg) string {
	if msg.Header == nil {
		return ""
	}
	values := msg.Header[m.requestIDHeader]
	if len(values) > 0 {
		return reqid.Sanitize(values[0])
	}
	return ""
}

// wrapChannelService creates proxy channels for access logging.
// It returns modified in/out channels that log access when messages pass through.
func (m *AccessLogModule) wrapChannelService(
	originalIn chan *types.Msg,
	originalOut chan *types.Msg,
	moduleName, serviceName string,
) (proxyIn chan *types.Msg, proxyOut chan *types.Msg) {
	// Return original channels unchanged during shutdown to prevent goroutine leaks
	if m.shutdown.Load() {
		return originalIn, originalOut
	}

	// Get buffer size from original channels using reflection
	bufferSizeIn := reflect.ValueOf(originalIn).Cap()
	bufferSizeOut := reflect.ValueOf(originalOut).Cap()

	// Create proxy channels with same buffer size as originals
	proxyIn = make(chan *types.Msg, bufferSizeIn)
	proxyOut = make(chan *types.Msg, bufferSizeOut)

	// Create proxy context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is stored in proxy.cancel and called in Stop()

	proxy := &channelProxy{
		serviceName: serviceName,
		moduleName:  moduleName,
		originalIn:  originalIn,
		originalOut: originalOut,
		proxyIn:     proxyIn,
		proxyOut:    proxyOut,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Store proxy for cleanup, under the module and the service: two
	// modules may each have a channel service of one name, and keyed by the
	// service alone the second replaced the first, whose two goroutines
	// were then never stopped. A proxy that is replaced is stopped.
	key := moduleName + "/" + serviceName
	m.channelProxiesMu.Lock()
	if replaced, ok := m.channelProxies[key]; ok {
		replaced.cancel()
	}
	m.channelProxies[key] = proxy
	m.channelProxiesMu.Unlock()

	// Start proxy goroutines
	proxy.wg.Add(2)
	go m.proxyInboundChannel(proxy)
	go m.proxyOutboundChannel(proxy)

	return proxyIn, proxyOut
}

// proxyInboundChannel reads from proxyIn, logs request, forwards to originalIn.
func (m *AccessLogModule) proxyInboundChannel(proxy *channelProxy) {
	defer proxy.wg.Done()

	for {
		select {
		case <-proxy.ctx.Done():
			return
		case msg, ok := <-proxy.proxyIn:
			if !ok {
				// Channel closed, also close the original (using Once to prevent double-close)
				proxy.closeInOnce.Do(func() {
					close(proxy.originalIn)
				})
				return
			}

			// Check for shutdown. A nil message has nothing to log and is
			// forwarded as it is: the channel router drops it.
			if m.shutdown.Load() || msg == nil {
				// Forward without logging during shutdown
				select {
				case proxy.originalIn <- msg:
				case <-proxy.ctx.Done():
					return
				}
				continue
			}

			// Log inbound request
			entry := Entry{
				Timestamp:   time.Now().UTC(),
				RequestID:   m.extractRequestID(msg),
				Module:      proxy.moduleName,
				Service:     proxy.serviceName,
				ServiceType: "channel",
				DurationMS:  0, // No duration for separate request log
				Status:      StatusSuccess,
				RequestSize: len(msg.Data),
				// ResponseSize is 0 for inbound request
			}
			m.writeEntry(entry)

			// Forward to original channel
			select {
			case proxy.originalIn <- msg:
			case <-proxy.ctx.Done():
				return
			}
		}
	}
}

// proxyOutboundChannel reads from originalOut, logs response, forwards to proxyOut.
func (m *AccessLogModule) proxyOutboundChannel(proxy *channelProxy) {
	defer proxy.wg.Done()

	for {
		select {
		case <-proxy.ctx.Done():
			return
		case msg, ok := <-proxy.originalOut:
			if !ok {
				// Channel closed, also close the proxy (using Once to prevent double-close)
				proxy.closeOutOnce.Do(func() {
					close(proxy.proxyOut)
				})
				return
			}

			// Check for shutdown. A nil message has nothing to log and is
			// forwarded as it is: the channel router drops it.
			if m.shutdown.Load() || msg == nil {
				// Forward without logging during shutdown
				select {
				case proxy.proxyOut <- msg:
				case <-proxy.ctx.Done():
					return
				}
				continue
			}

			// Log outbound response
			entry := Entry{
				Timestamp:    time.Now().UTC(),
				RequestID:    m.extractRequestID(msg),
				Module:       proxy.moduleName,
				Service:      proxy.serviceName,
				ServiceType:  "channel",
				DurationMS:   0, // No duration for separate response log
				Status:       StatusSuccess,
				RequestSize:  0, // RequestSize is 0 for outbound response
				ResponseSize: len(msg.Data),
			}
			m.writeEntry(entry)

			// Forward to proxy channel
			select {
			case proxy.proxyOut <- msg:
			case <-proxy.ctx.Done():
				return
			}
		}
	}
}

// writeEntry formats and writes an entry to the output.
// Write failures are logged to stderr but do not propagate to prevent
// logging failures from impacting service handlers.
func (m *AccessLogModule) writeEntry(entry Entry) {
	// Held for the whole write, so that stop cannot close the log under
	// it; the flag is read under it, so a write either finishes before
	// stop goes on or does not begin.
	m.writes.RLock()
	defer m.writes.RUnlock()

	// Skip writes during shutdown
	if m.shutdown.Load() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	line := m.formatter.Format(entry, m.fields)
	lineWithNewline := line + "\n"

	// Write line with newline in a single call
	if _, err := m.writer.Write([]byte(lineWithNewline)); err != nil {
		// Log to stderr on write failure (critical error)
		fmt.Fprintf(os.Stderr, "CRITICAL: Failed to write access log entry: %v\n", err)
	}
}
