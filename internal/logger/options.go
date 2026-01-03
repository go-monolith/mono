package logger

import (
	"errors"
	"io"
	"log/slog"

	"github.com/go-monolith/mono/pkg/types"
)

// WithLogLevel sets the log level.
func WithLogLevel(level types.LogLevel) LoggerOption {
	return func(opts *loggerOptions) error {
		opts.Level = level
		return nil
	}
}

// WithLogFormat sets the log format (JSON or Text).
func WithLogFormat(format types.LogFormat) LoggerOption {
	return func(opts *loggerOptions) error {
		opts.Format = format
		return nil
	}
}

// WithOutput sets the output writer.
func WithOutput(w io.Writer) LoggerOption {
	return func(opts *loggerOptions) error {
		if w == nil {
			return errors.New("output writer cannot be nil")
		}
		opts.Output = w
		return nil
	}
}

// WithAddSource enables/disables source location in logs.
func WithAddSource(enable bool) LoggerOption {
	return func(opts *loggerOptions) error {
		opts.AddSource = enable
		return nil
	}
}

// WithReplaceAttr sets a custom attribute replacement function.
func WithReplaceAttr(fn func([]string, slog.Attr) slog.Attr) LoggerOption {
	return func(opts *loggerOptions) error {
		opts.ReplaceAttr = fn
		return nil
	}
}
