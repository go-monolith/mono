package types

// Logger provides structured logging with levels and context.
//
// The Logger interface wraps log/slog for framework-wide structured logging
// with module-specific context.
//
// See docs/spec/foundation.md for detailed design documentation.
type Logger interface {
	// Debug logs a debug-level message
	Debug(msg string, args ...any)

	// Info logs an info-level message
	Info(msg string, args ...any)

	// Warn logs a warning-level message
	Warn(msg string, args ...any)

	// Error logs an error-level message
	Error(msg string, args ...any)

	// With returns a new logger with additional context fields
	With(args ...any) Logger

	// WithModule returns a new logger with module name context
	WithModule(moduleName string) Logger

	// WithError returns a new logger with error context
	WithError(err error) Logger
}

// LoggerFactory creates logger instances.
type LoggerFactory interface {
	// NewLogger creates a logger for a specific module
	NewLogger(moduleName string) Logger

	// SetLevel sets the global log level
	SetLevel(level LogLevel)

	// GetLevel returns the current log level
	GetLevel() LogLevel
}

// LogLevel represents logging severity levels.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// LogFormat specifies log output format.
type LogFormat int

const (
	LogFormatJSON LogFormat = iota
	LogFormatText
)
