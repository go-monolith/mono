package audit

import (
	"context"
	"fmt"
	"io"
)

// Option configures an audit module.
type Option func(*options) error

// options holds configuration for audit module creation.
type options struct {
	Output          io.Writer
	EnableChaining  bool
	LastSavedHash   string
	UserContextFunc func(context.Context) string
}

// defaultOptions returns default audit module options.
func defaultOptions() *options {
	return &options{
		EnableChaining: false,
		LastSavedHash:  "",
		UserContextFunc: func(ctx context.Context) string {
			return ""
		},
	}
}

// WithOutput sets the output writer for audit logs.
//
// The writer should support concurrent writes (e.g., *os.File) as the audit
// module writes from multiple goroutines.
//
// Example:
//
//	auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
//	audit.New(audit.WithOutput(auditFile))
func WithOutput(w io.Writer) Option {
	return func(opts *options) error {
		if w == nil {
			return fmt.Errorf("WithOutput: audit output writer cannot be nil")
		}
		opts.Output = w
		return nil
	}
}

// WithHashChaining enables hash chaining for tamper-evidence with an optional
// initial hash to continue an existing chain.
//
// When enabled, each audit entry contains:
//   - prev_hash: SHA-256 hash of previous entry
//   - entry_hash: SHA-256 hash of current entry
//
// This creates a tamper-evident chain that can be verified using audit.VerifyChain().
//
// The lastSavedHash parameter allows resuming an existing audit chain:
//   - Empty string: starts a new chain (first entry has empty prev_hash)
//   - Non-empty string: continues from this hash (first entry uses it as prev_hash)
//
// Example:
//
//	audit.New(audit.WithHashChaining(""))           // Start new chain
//	audit.New(audit.WithHashChaining(lastHash))    // Continue existing chain
func WithHashChaining(lastSavedHash string) Option {
	return func(opts *options) error {
		opts.EnableChaining = true
		opts.LastSavedHash = lastSavedHash
		return nil
	}
}

// WithUserContext sets a function to extract user context from context.Context.
//
// The function is called for each audit event to populate the UserContext field.
// This is useful for tracking which user triggered each event.
//
// Example:
//
//	audit.New(audit.WithUserContext(func(ctx context.Context) string {
//	    if user, ok := ctx.Value(userKey).(string); ok {
//	        return user
//	    }
//	    return "system"
//	}))
func WithUserContext(fn func(context.Context) string) Option {
	return func(opts *options) error {
		if fn == nil {
			return fmt.Errorf("WithUserContext: user context function cannot be nil")
		}
		opts.UserContextFunc = fn
		return nil
	}
}
