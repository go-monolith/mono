package accesslog

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Formatter formats an Entry to a string for output.
type Formatter interface {
	Format(entry Entry, fields []Field) string
}

// TextFormatter produces key=value format output.
type TextFormatter struct{}

// Format formats an entry as key=value pairs.
//
// Example output: ts=2024-01-15T10:30:00Z request_id=abc123 module=order service=place-order method=request_reply status=success duration_ms=45 request_size=1024 response_size=512
func (f *TextFormatter) Format(entry Entry, fields []Field) string {
	fieldSet := make(map[Field]bool)
	for _, field := range fields {
		fieldSet[field] = true
	}

	var parts []string

	if fieldSet[FieldTimestamp] {
		parts = append(parts, "ts="+entry.Timestamp.Format(time.RFC3339))
	}
	if fieldSet[FieldRequestID] {
		parts = append(parts, "request_id="+entry.RequestID)
	}
	if fieldSet[FieldModule] {
		parts = append(parts, "module="+entry.Module)
	}
	if fieldSet[FieldService] {
		parts = append(parts, "service="+entry.Service)
	}
	if fieldSet[FieldServiceType] {
		parts = append(parts, "service_type="+entry.ServiceType)
	}
	if fieldSet[FieldStatus] {
		parts = append(parts, "status="+string(entry.Status))
	}
	if fieldSet[FieldDurationMS] {
		parts = append(parts, "duration_ms="+strconv.FormatInt(entry.DurationMS, 10))
	}
	if fieldSet[FieldRequestSize] {
		parts = append(parts, "request_size="+strconv.Itoa(entry.RequestSize))
	}
	if fieldSet[FieldResponseSize] {
		parts = append(parts, "response_size="+strconv.Itoa(entry.ResponseSize))
	}

	return strings.Join(parts, " ")
}

// JSONFormatter produces JSON format output.
type JSONFormatter struct{}

// Format formats an entry as a JSON object.
//
// Example output: {"ts":"2024-01-15T10:30:00Z","request_id":"abc123","module":"order","service":"place-order","method":"request_reply","status":"success","duration_ms":45,"request_size":1024,"response_size":512}
func (f *JSONFormatter) Format(entry Entry, fields []Field) string {
	fieldSet := make(map[Field]bool)
	for _, field := range fields {
		fieldSet[field] = true
	}

	data := make(map[string]any)

	if fieldSet[FieldTimestamp] {
		data["ts"] = entry.Timestamp.Format(time.RFC3339)
	}
	if fieldSet[FieldRequestID] {
		data["request_id"] = entry.RequestID
	}
	if fieldSet[FieldModule] {
		data["module"] = entry.Module
	}
	if fieldSet[FieldService] {
		data["service"] = entry.Service
	}
	if fieldSet[FieldServiceType] {
		data["service_type"] = entry.ServiceType
	}
	if fieldSet[FieldStatus] {
		data["status"] = string(entry.Status)
	}
	if fieldSet[FieldDurationMS] {
		data["duration_ms"] = entry.DurationMS
	}
	if fieldSet[FieldRequestSize] {
		data["request_size"] = entry.RequestSize
	}
	if fieldSet[FieldResponseSize] {
		data["response_size"] = entry.ResponseSize
	}

	result, err := json.Marshal(data)
	if err != nil {
		// Fallback to error message
		return `{"error":"failed to marshal access log entry"}`
	}
	return string(result)
}

// newFormatter creates the appropriate formatter based on format type.
func newFormatter(format Format) Formatter {
	switch format {
	case FormatJSON:
		return &JSONFormatter{}
	default:
		return &TextFormatter{}
	}
}
