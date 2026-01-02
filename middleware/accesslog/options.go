package accesslog

import (
	"fmt"
	"io"
)

// Option configures an access log module.
type Option func(*options) error

// options holds configuration for access log module creation.
type options struct {
	Output          io.Writer
	Format          Format
	Fields          []Field
	RequestIDHeader string
}

// defaultOptions returns default access log module options.
func defaultOptions() *options {
	return &options{
		Output:          nil, // Required - must be set
		Format:          FormatText,
		Fields:          AllFields(),
		RequestIDHeader: "X-Request-ID",
	}
}

// WithOutput sets the output writer for access logs.
// This option is required.
//
// The writer should support concurrent writes (e.g., *os.File) as the access
// log module writes from multiple goroutines.
//
// Example:
//
//	accessFile, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//	accesslog.New(accesslog.WithOutput(accessFile))
func WithOutput(w io.Writer) Option {
	return func(opts *options) error {
		if w == nil {
			return fmt.Errorf("WithOutput: access log output writer cannot be nil")
		}
		opts.Output = w
		return nil
	}
}

// WithFormat sets the output format (FormatText or FormatJSON).
// Default: FormatText
//
// Example:
//
//	accesslog.New(
//	    accesslog.WithOutput(os.Stdout),
//	    accesslog.WithFormat(accesslog.FormatJSON),
//	)
func WithFormat(format Format) Option {
	return func(opts *options) error {
		opts.Format = format
		return nil
	}
}

// WithFields configures which fields appear in the output.
// Pass nil or empty slice to use all fields.
// Default: AllFields()
//
// Example:
//
//	accesslog.New(
//	    accesslog.WithOutput(os.Stdout),
//	    accesslog.WithFields([]accesslog.Field{
//	        accesslog.FieldTimestamp,
//	        accesslog.FieldService,
//	        accesslog.FieldDurationMS,
//	        accesslog.FieldStatus,
//	    }),
//	)
func WithFields(fields []Field) Option {
	return func(opts *options) error {
		if len(fields) == 0 {
			opts.Fields = AllFields()
		} else {
			opts.Fields = fields
		}
		return nil
	}
}

// WithRequestIDHeader sets the header key to extract request ID from.
// Default: "X-Request-ID"
//
// Example:
//
//	accesslog.New(
//	    accesslog.WithOutput(os.Stdout),
//	    accesslog.WithRequestIDHeader("X-Correlation-ID"),
//	)
func WithRequestIDHeader(header string) Option {
	return func(opts *options) error {
		if header == "" {
			return fmt.Errorf("WithRequestIDHeader: header cannot be empty")
		}
		opts.RequestIDHeader = header
		return nil
	}
}
