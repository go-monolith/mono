package accesslog

import "time"

// Format defines the output format for access log entries.
type Format int

const (
	// FormatText produces key-value format: ts=... request_id=... module=...
	FormatText Format = iota
	// FormatJSON produces JSON format: {"ts":"...","request_id":"..."}
	FormatJSON
)

// Field identifies a log field that can be included/excluded.
type Field int

const (
	FieldTimestamp Field = iota
	FieldRequestID
	FieldModule
	FieldService
	FieldServiceType
	FieldDurationMS
	FieldStatus
	FieldRequestSize
	FieldResponseSize
)

// AllFields returns all available fields for convenience.
func AllFields() []Field {
	return []Field{
		FieldTimestamp,
		FieldRequestID,
		FieldModule,
		FieldService,
		FieldServiceType,
		FieldDurationMS,
		FieldStatus,
		FieldRequestSize,
		FieldResponseSize,
	}
}

// FieldName returns the string key for a field (used in both text and JSON).
func FieldName(f Field) string {
	switch f {
	case FieldTimestamp:
		return "ts"
	case FieldRequestID:
		return "request_id"
	case FieldModule:
		return "module"
	case FieldService:
		return "service"
	case FieldServiceType:
		return "service_type"
	case FieldDurationMS:
		return "duration_ms"
	case FieldStatus:
		return "status"
	case FieldRequestSize:
		return "request_size"
	case FieldResponseSize:
		return "response_size"
	default:
		return "unknown"
	}
}

// Status represents the outcome of a handler execution.
type Status string

const (
	// StatusSuccess indicates the handler completed without error.
	StatusSuccess Status = "success"
	// StatusError indicates the handler returned an error.
	StatusError Status = "error"
)

// Entry represents a single access log entry.
// This struct is used internally for formatting.
type Entry struct {
	// Timestamp is the UTC time when the request started.
	Timestamp time.Time

	// RequestID is extracted from the configured header (default: X-Request-ID).
	RequestID string

	// Module is the name of the module that registered the service.
	Module string

	// Service is the name of the service handler.
	Service string

	// ServiceType is the service type: "channel", "request_reply", "queue_group", "stream_consumer".
	ServiceType string

	// DurationMS is the handler execution time in milliseconds.
	DurationMS int64

	// Status is the outcome: "success" or "error".
	Status Status

	// RequestSize is the size of the request data in bytes.
	RequestSize int

	// ResponseSize is the size of the response data in bytes (0 for non-request-reply).
	ResponseSize int
}
