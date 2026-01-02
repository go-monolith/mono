package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// AuditAdapterPort provides a type-safe interface for saving custom audit entries.
//
// Note: The adapter is safe for concurrent use. Multiple goroutines can call
// SaveAuditTrail and AsyncSaveAuditTrail simultaneously.
type AuditAdapterPort interface {
	// SaveAuditTrail saves a list of audit entries synchronously.
	// Each entry is sent as a separate message to the channel and waits for confirmation.
	// The Timestamp field of each entry will be set by the audit module.
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
	mu      sync.Mutex // Protects sync request/response pairs
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

	inChan, outChan, err := container.GetChannelService("audit-trail", consumerModuleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get audit-trail channel service: %w", err)
	}

	return &auditAdapter{
		inChan:  inChan,
		outChan: outChan,
	}, nil
}

// SaveAuditTrail saves a list of audit entries synchronously.
//
// Each entry is marshaled to JSON and sent as a separate message to the channel.
// The function waits for confirmation of each entry before proceeding to the next.
// The Timestamp field of each entry will be set by the audit module before writing.
//
// Returns the count of successfully saved entries and any error encountered.
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

	// First loop: marshal all entries (fail fast if any fails)
	messages := make([]*types.Msg, 0, len(entries))
	for i, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal audit entry[%d]: %w", i, err)
		}

		messages = append(messages, &types.Msg{
			Subject: "audit-trail",
			Reply:   "audit-trail.reply",
			Data:    data,
		})
	}

	// Serialize sync requests to ensure correct response routing
	a.mu.Lock()
	defer a.mu.Unlock()

	// Second loop: send all messages
	sentCount := 0
	for _, msg := range messages {
		select {
		case a.inChan <- msg:
			sentCount++
		case <-ctx.Done():
			// Wait for responses of already sent entries
			a.waitForResponses(context.Background(), sentCount)
			return 0, fmt.Errorf("context cancelled while sending audit entry: %w", ctx.Err())
		}
	}

	// Third loop: wait for all responses
	successCount := 0
	var firstErr error
	for range sentCount {
		select {
		case respMsg := <-a.outChan:
			var response SaveEntryResponse
			if err := json.Unmarshal(respMsg.Data, &response); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to parse audit response: %w", err)
				}
				continue
			}
			if !response.Success {
				if firstErr == nil {
					firstErr = fmt.Errorf("audit save failed: %s", response.Error)
				}
				continue
			}
			successCount++
		case <-ctx.Done():
			return successCount, fmt.Errorf("context cancelled while waiting for audit response: %w", ctx.Err())
		}
	}

	return successCount, firstErr
}

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
