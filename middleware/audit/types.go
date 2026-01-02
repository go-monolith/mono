package audit

import "time"

// EventType represents the type of security event being logged.
type EventType string

const (
	// EventModuleStarted indicates a module was started.
	EventModuleStarted EventType = "module.started"

	// EventModuleStopped indicates a module was stopped.
	EventModuleStopped EventType = "module.stopped"

	// EventConfigurationUpdate indicates a configuration change.
	EventConfigurationUpdate EventType = "configuration.updated"

	// EventServiceRegistered indicates a service was registered.
	EventServiceRegistered EventType = "service.registered"

	// EventCustomAuditTrail indicates a custom audit trail entry from another module.
	EventCustomAuditTrail EventType = "custom.audit_trail"

	// Note: EventModuleRegistered does not exist because module registration
	// occurs before the middleware chain is built, so it cannot be captured.
)

// Entry represents a single audit log entry with tamper-evident hash chaining.
//
// Each entry contains:
//   - Timestamp: UTC timestamp in RFC3339 format
//   - EventType: Type of security event
//   - ModuleName: Name of the module (if applicable)
//   - ServiceName: Name of the service (if applicable)
//   - Details: Event-specific structured data
//   - UserContext: User/request context information
//   - PrevHash: SHA-256 hash of previous entry (empty for first entry)
//   - EntryHash: SHA-256 hash of current entry
//
// Hash chaining ensures that any modification to the audit log can be detected
// by verifying the chain using VerifyChain().
type Entry struct {
	// Timestamp is the UTC time when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// EventType identifies the type of security event.
	EventType EventType `json:"event_type"`

	// ModuleName is the name of the module (if applicable).
	ModuleName string `json:"module_name,omitempty"`

	// ServiceName is the name of the service (if applicable).
	ServiceName string `json:"service_name,omitempty"`

	// Details contains event-specific structured data.
	Details map[string]any `json:"details,omitempty"`

	// UserContext contains user/request context information.
	UserContext string `json:"user_context,omitempty"`

	// PrevHash is the SHA-256 hash of the previous entry (empty for first entry).
	PrevHash string `json:"prev_hash"`

	// EntryHash is the SHA-256 hash of this entry.
	EntryHash string `json:"entry_hash"`
}
