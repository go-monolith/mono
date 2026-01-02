package logger

import (
	"io"
	"log/slog"
	"os"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// slogLogger wraps slog.Logger to implement the types.Logger interface.
type slogLogger struct {
	logger *slog.Logger
	output io.Writer
}

// Debug logs a debug-level message.
func (l *slogLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

// Info logs an info-level message.
func (l *slogLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

// Warn logs a warning-level message.
func (l *slogLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

// Error logs an error-level message.
func (l *slogLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

// With returns a new logger with additional context fields.
func (l *slogLogger) With(args ...any) types.Logger {
	return &slogLogger{
		logger: l.logger.With(args...),
		output: l.output,
	}
}

// WithModule returns a new logger with module name context.
// If moduleName is empty, returns the logger unchanged to avoid "module=" in logs.
func (l *slogLogger) WithModule(moduleName string) types.Logger {
	if moduleName == "" {
		return l
	}
	return l.With("module", moduleName)
}

// WithError returns a new logger with error context.
func (l *slogLogger) WithError(err error) types.Logger {
	return l.With("error", err)
}

// Flush flushes any buffered log output when supported.
func (l *slogLogger) Flush() error {
	if l == nil || l.output == nil {
		return nil
	}
	if flusher, ok := l.output.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	if syncer, ok := l.output.(interface{ Sync() error }); ok {
		if file, ok := l.output.(*os.File); ok {
			info, err := file.Stat()
			if err == nil && !info.Mode().IsRegular() {
				return nil
			}
		}
		return syncer.Sync()
	}
	return nil
}
