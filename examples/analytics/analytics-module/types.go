package analytics

import (
	"context"
	"time"
)

// TrackEventRequest represents a request to track an analytics event
type TrackEventRequest struct {
	EventType  string         `json:"event_type"` // e.g., "page_view", "button_click"
	UserID     string         `json:"user_id"`    // User identifier
	Properties map[string]any `json:"properties"` // Event-specific properties
	Timestamp  time.Time      `json:"timestamp"`  // Event timestamp
}

// TrackEventResponse represents the response after tracking an event
type TrackEventResponse struct {
	Success    bool   `json:"success"`     // Processing status
	EventID    string `json:"event_id"`    // Generated event ID
	TotalCount int    `json:"total_count"` // Current total count of events
}

// GetEventRequest represents a request to retrieve an event by ID
type GetEventRequest struct {
	EventID string `json:"event_id"` // Event ID to retrieve
}

// GetEventResponse represents the response for getting an event
type GetEventResponse struct {
	Found      bool           `json:"found"`       // Whether the event was found
	EventID    string         `json:"event_id"`    // Event ID
	EventType  string         `json:"event_type"`  // Event type
	UserID     string         `json:"user_id"`     // User ID
	Properties map[string]any `json:"properties"`  // Event properties
	Timestamp  time.Time      `json:"timestamp"`   // Event timestamp
	RecordedAt time.Time      `json:"recorded_at"` // When the event was recorded
}

// AnalyticsAdapterPort provides a type-safe interface for analytics operations.
// Note: TrackEvent is not included as it uses a channel-based service pattern.
// Only request-reply services are exposed through the adapter.
type AnalyticsAdapterPort interface {
	GetEvent(ctx context.Context, req *GetEventRequest) (*GetEventResponse, error)
}

// eventRecord represents an internal stored event
type eventRecord struct {
	ID         string
	Request    TrackEventRequest
	RecordedAt time.Time
}
