package requestid

import "context"

// contextKey is an unexported type for context keys to prevent collisions.
type contextKey struct{}

// requestIDKey is the context key for storing request IDs.
// This is internal and NOT exported to keep it private to the middleware.
var requestIDKey = contextKey{}

// HeaderName is the default header name for request IDs.
const HeaderName = "X-Request-ID"

// GetRequestID retrieves the request ID from context.
// Returns empty string if not found.
func GetRequestID(ctx context.Context) string {
	if v := ctx.Value(requestIDKey); v != nil {
		if id, ok := v.(string); ok {
			return id
		}
	}
	return ""
}
