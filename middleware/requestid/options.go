package requestid

import "fmt"

// Option configures a request ID middleware module.
type Option func(*options) error

type options struct {
	HeaderName string
}

func defaultOptions() *options {
	return &options{
		HeaderName: HeaderName, // "X-Request-ID"
	}
}

// WithHeaderName sets the header name for request ID.
// Default: "X-Request-ID"
//
// Example:
//
//	middleware, err := requestid.New(
//	    requestid.WithHeaderName("X-Trace-ID"),
//	)
func WithHeaderName(name string) Option {
	return func(opts *options) error {
		if name == "" {
			return fmt.Errorf("WithHeaderName: header name cannot be empty")
		}
		opts.HeaderName = name
		return nil
	}
}
