package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// AuditAdapterPort provides a type-safe interface for saving custom audit entries.
//
// Note: The adapter is safe for concurrent use. Multiple goroutines can call
// SaveAuditTrail and AsyncSaveAuditTrail simultaneously.
type AuditAdapterPort interface {
	// SaveAuditTrail saves a list of audit entries synchronously.
	// Each entry is sent as a separate message to the channel and waits for confirmation.
	// The Timestamp field of each entry will be set by the audit module.
	// It returns ErrConfirmationUnavailable, without sending anything, while
	// the framework's channel routers do not run (a module's Start or Stop).
	SaveAuditTrail(ctx context.Context, entries []Entry) (int, error)

	// AsyncSaveAuditTrail saves a list of audit entries asynchronously (fire-and-forget).
	// Each entry is sent as a separate message to the channel.
	// It does not wait for confirmation and returns immediately after sending all entries.
	// The Timestamp field of each entry will be set by the audit module.
	AsyncSaveAuditTrail(ctx context.Context, entries []Entry) error
}

// auditAdapter implements AuditAdapterPort using channel-based communication.
type auditAdapter struct {
	inChan  chan *types.Msg
	outChan chan *types.Msg
	// consumer is the consumer module name, sent as the Reply of a
	// synchronous request so the channel router delivers the confirmation
	// to this consumer's out channel only.
	consumer string
	// mu serialises the synchronous saves on outChan. It belongs to the
	// channel, not to the adapter: a module that builds two adapters under
	// one consumer name gets the same confirmation channel twice, and two
	// saves reading it at the same time would each drop the other's
	// confirmations.
	mu *sync.Mutex
	// routers tells whether the audit-trail router runs, which delivers the
	// confirmations; nil when the container cannot tell (a test double).
	routers types.ChannelRouterInspector
}

// AuditTrailService is the name of the audit module's channel service.
const AuditTrailService = "audit-trail"

// ErrConfirmationUnavailable is returned by SaveAuditTrail when no
// confirmation can arrive: the channel router that delivers the audit
// module's confirmations is not running. It runs from after every module has
// started until shutdown begins, so SaveAuditTrail cannot be confirmed from a
// module's Start, Stop, RegisterServices or SetDependencyServiceContainer. The
// call sends nothing in that case, so it can be repeated later without writing
// an entry twice; use AsyncSaveAuditTrail there, which does not wait.
var ErrConfirmationUnavailable = errors.New("audit: SaveAuditTrail cannot be confirmed while the audit-trail channel router is not running (in a module's Start or Stop, or during shutdown); use AsyncSaveAuditTrail")

// RouterCheckInterval is how often a SaveAuditTrail waiting for
// confirmations checks that the audit-trail router still runs.
const RouterCheckInterval = 50 * time.Millisecond

// confirmationLocks holds the mutex of every confirmation channel an
// adapter was built on (see auditAdapter.mu). There is one channel per
// consumer module of the audit trail, so it stays small.
var confirmationLocks sync.Map // chan *types.Msg -> *sync.Mutex

// ConfirmationLock returns the mutex that serialises the synchronous saves
// reading the confirmation channel out.
func ConfirmationLock(out chan *types.Msg) *sync.Mutex {
	if mu, ok := confirmationLocks.Load(out); ok {
		if lock, ok := mu.(*sync.Mutex); ok {
			return lock
		}
	}
	fresh := &sync.Mutex{}
	if mu, loaded := confirmationLocks.LoadOrStore(out, fresh); loaded {
		if lock, ok := mu.(*sync.Mutex); ok {
			return lock
		}
	}
	return fresh
}

// NewAuditAdapter creates a new audit adapter from a service container.
//
// The adapter wraps the "audit-trail" channel service and provides
// type-safe methods for saving audit entries. The consumerModuleName parameter
// identifies the module consuming the audit service and ensures it receives
// a dedicated response channel.
//
// Returns an error if the container is nil or the channel service is not found.
//
// Example:
//
//	func (m *MyModule) SetDependencyServiceContainer(dep string, container types.ServiceContainer) {
//	    if dep == "audit" {
//	        adapter, err := audit.NewAuditAdapter(container, m.Name())
//	        if err != nil {
//	            // Handle error
//	        }
//	        m.auditAdapter = adapter
//	    }
//	}
func NewAuditAdapter(container types.ServiceContainer, consumerModuleName string) (AuditAdapterPort, error) {
	if container == nil {
		return nil, fmt.Errorf("audit adapter requires non-nil ServiceContainer")
	}

	inChan, outChan, err := container.GetChannelService(AuditTrailService, consumerModuleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get audit-trail channel service: %w", err)
	}

	adapter := &auditAdapter{
		inChan:   inChan,
		outChan:  outChan,
		consumer: consumerModuleName,
		mu:       ConfirmationLock(outChan),
	}
	if routers, ok := container.(types.ChannelRouterInspector); ok {
		adapter.routers = routers
	}
	return adapter, nil
}

// routerRunning reports whether the audit-trail router runs; true when the
// container cannot tell.
func (a *auditAdapter) routerRunning() bool {
	return a.routers == nil || a.routers.ChannelRouterRunning(AuditTrailService)
}

// SaveAuditTrail saves a list of audit entries synchronously.
//
// Each entry is marshaled to JSON and sent as a separate message to the channel,
// and the function returns once every entry is confirmed. Any number of
// entries can be saved in one call.
// The Timestamp field of each entry will be set by the audit module before writing.
//
// Returns the count of entries confirmed as written and the first error
// encountered: an entry the audit module could not write to its log is not
// counted. When ctx ends first, the count is that of the entries confirmed so
// far; entries already sent may still be written, and their confirmations
// are not taken for those of a later call.
//
// The confirmations are delivered by the framework's channel router, which
// runs from after every module has started until shutdown begins. Outside
// that time (a module's Start or Stop) the call sends nothing and returns
// ErrConfirmationUnavailable at once; use AsyncSaveAuditTrail there. When the
// router stops while the call waits (shutdown began), it returns the count
// confirmed so far and ErrConfirmationUnavailable; the entries already sent
// may still be written. After the audit module was stopped no confirmation
// arrives either, so give the call a context with a deadline.
//
// Example:
//
//	entries := []audit.Entry{
//	    {
//	        EventType:   audit.EventCustomAuditTrail,
//	        ModuleName:  "my-module",
//	        ServiceName: "my-service",
//	        Details: map[string]any{
//	            "action": "user_login",
//	            "user_id": "12345",
//	        },
//	    },
//	}
//	count, err := adapter.SaveAuditTrail(ctx, entries)
func (a *auditAdapter) SaveAuditTrail(ctx context.Context, entries []Entry) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	if !a.routerRunning() {
		return 0, ErrConfirmationUnavailable
	}

	// Every entry of this call carries its ID, which the confirmation echoes.
	call := strconv.FormatUint(auditCalls.Add(1), 36)

	// First loop: marshal all entries (fail fast if any fails)
	messages := make([]*types.Msg, 0, len(entries))
	for i, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal audit entry[%d]: %w", i, err)
		}

		messages = append(messages, &types.Msg{
			Subject: "audit-trail",
			Reply:   a.consumer,
			Data:    data,
			Header:  types.Header{CallHeader: []string{call}},
		})
	}

	// Serialize sync requests to ensure correct response routing
	a.mu.Lock()
	defer a.mu.Unlock()
	// A save queued behind another one can get the lock after shutdown
	// began: check again before anything is sent.
	if !a.routerRunning() {
		return 0, ErrConfirmationUnavailable
	}

	// Confirmations of an earlier call that gave up waiting are still on
	// their way or in the channel. Those already here are dropped now; the
	// loop below drops the rest by their call ID.
	a.dropStale()

	// Send and read confirmations in one loop. Sending everything first
	// filled the channels: with more entries than they hold, the router
	// dropped the confirmations it could not deliver and the call waited
	// for them until its context ended. No more entries are in flight than
	// half of what the confirmation channel holds, which leaves the other
	// half for late confirmations of an earlier call that gave up. Should
	// more than that arrive at once (several calls in a row that gave up,
	// and a reader that falls behind), the router drops what does not fit
	// and this call waits until its context ends: it reports an error,
	// never an entry as saved that was not confirmed.
	window := max(cap(a.outChan)/2, 1)
	sent, received, successCount := 0, 0, 0
	var firstErr error
	// The router can stop while the call waits (shutdown began): no
	// confirmation would arrive any more.
	var routerCheck <-chan time.Time
	if a.routers != nil {
		ticker := time.NewTicker(RouterCheckInterval)
		defer ticker.Stop()
		routerCheck = ticker.C
	}
	for received < len(messages) {
		var sendCh chan *types.Msg
		var next *types.Msg
		if sent < len(messages) && sent-received < window {
			sendCh, next = a.inChan, messages[sent]
		}
		select {
		case sendCh <- next:
			sent++
		case respMsg := <-a.outChan:
			if !IsConfirmationOf(respMsg, call) {
				continue // of an earlier call that stopped waiting
			}
			received++
			if err := ConfirmationError(respMsg); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			successCount++
		case <-routerCheck:
			if a.routerRunning() {
				continue
			}
			// Confirmations the router delivered before it stopped may be in
			// the channel: count them, so that the entries reported as not
			// confirmed were really not confirmed.
			for drained := false; !drained && received < sent; {
				select {
				case respMsg := <-a.outChan:
					if !IsConfirmationOf(respMsg, call) {
						continue
					}
					received++
					if err := ConfirmationError(respMsg); err != nil {
						if firstErr == nil {
							firstErr = err
						}
						continue
					}
					successCount++
				default:
					drained = true
				}
			}
			if received == len(messages) {
				return successCount, firstErr
			}
			return successCount, errors.Join(firstErr, fmt.Errorf("the audit-trail router stopped after %d of %d audit entries were sent and %d confirmed: %w",
				sent, len(messages), received, ErrConfirmationUnavailable))
		case <-ctx.Done():
			// Take the confirmations of the entries already sent, so that
			// they are not read as those of the next call.
			a.waitForResponses(context.Background(), sent-received)
			return successCount, errors.Join(firstErr, fmt.Errorf("context cancelled after %d of %d audit entries were sent and %d confirmed: %w",
				sent, len(messages), received, ctx.Err()))
		}
	}

	return successCount, firstErr
}

// auditCalls numbers the synchronous saves of the process.
var auditCalls atomic.Uint64

// IsConfirmationOf reports whether msg is a confirmation of the save with
// the given call ID (see CallHeader).
func IsConfirmationOf(msg *types.Msg, call string) bool {
	if msg == nil {
		return false
	}
	ids := msg.Header[CallHeader]
	return len(ids) > 0 && ids[0] == call
}

// dropStale empties the confirmation channel without waiting.
func (a *auditAdapter) dropStale() {
	for {
		select {
		case <-a.outChan:
		default:
			return
		}
	}
}

// ConfirmationError returns the error a confirmation message of the
// audit-trail service reports, or nil when the entry was saved.
func ConfirmationError(msg *types.Msg) error {
	if msg == nil {
		return fmt.Errorf("empty audit response")
	}
	var response SaveEntryResponse
	if err := json.Unmarshal(msg.Data, &response); err != nil {
		return fmt.Errorf("failed to parse audit response: %w", err)
	}
	if !response.Success {
		failure := &SaveFailure{Text: response.Error}
		// The confirmation carries the error as text. The one a caller can
		// act on (change the entry) is given back its identity.
		if strings.Contains(response.Error, ErrAmbiguousEntry.Error()) {
			failure.Cause = ErrAmbiguousEntry
		}
		return failure
	}
	return nil
}

// SaveFailure is the error of an entry the audit-trail service did not save:
// Text is what the service reported, Cause the error it stands for when that
// is known (ErrAmbiguousEntry), so that errors.Is finds it.
type SaveFailure struct {
	Text  string
	Cause error
}

// Error returns the service's report.
func (f *SaveFailure) Error() string { return "audit save failed: " + f.Text }

// Unwrap returns the cause, or nil when it is not known.
func (f *SaveFailure) Unwrap() error { return f.Cause }

// waitForResponses drains responses for already-sent entries to avoid leaving
// stale responses in the channel.
func (a *auditAdapter) waitForResponses(_ context.Context, count int) {
	for range count {
		select {
		case <-a.outChan:
			// Discard response
		case <-time.After(20 * time.Millisecond):
			// Timeout waiting for response, stop draining
			return
		}
	}
}

// AsyncSaveAuditTrail saves a list of audit entries asynchronously.
//
// Each entry is marshaled to JSON and sent as a separate message to the channel.
// It does not wait for responses and returns immediately after sending all entries.
// The Timestamp field of each entry will be set by the audit module before writing.
//
// Note: Since this is fire-and-forget, errors during processing on the audit
// module side will not be returned. Use SaveAuditTrail if you need confirmation.
//
// Example:
//
//	entries := []audit.Entry{
//	    {
//	        EventType:   audit.EventCustomAuditTrail,
//	        ModuleName:  "my-module",
//	        Details: map[string]any{
//	            "action": "background_job_completed",
//	        },
//	    },
//	}
//	err := adapter.AsyncSaveAuditTrail(ctx, entries)
func (a *auditAdapter) AsyncSaveAuditTrail(ctx context.Context, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// First loop: marshal all entries (fail fast if any fails)
	messages := make([]*types.Msg, 0, len(entries))
	for i, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal audit entry[%d]: %w", i, err)
		}

		messages = append(messages, &types.Msg{
			Subject: "audit-trail",
			Data:    data,
		})
	}

	// Second loop: send all messages (no reply needed for async)
	for _, msg := range messages {
		select {
		case a.inChan <- msg:
			// Entry sent
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while sending audit entries: %w", ctx.Err())
		}
	}

	return nil
}
