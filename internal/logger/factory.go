package logger

import (
	"io"
	"log/slog"
	"os"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// loggerFactory implements types.LoggerFactory.
type loggerFactory struct {
	handler slog.Handler
	level   *slog.LevelVar
	output  io.Writer
}

// LoggerOption is a functional option for configuring loggers.
type LoggerOption func(*loggerOptions) error

// NewLoggerFactory creates a new logger factory with the given options.
func NewLoggerFactory(opts ...LoggerOption) (types.LoggerFactory, error) {
	// Apply options
	options := defaultOptions()
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	// Create level var for dynamic level control
	levelVar := new(slog.LevelVar)
	levelVar.Set(toSlogLevel(options.Level))

	// Create handler
	handler := createHandler(options, levelVar)

	return &loggerFactory{
		handler: handler,
		level:   levelVar,
		output:  options.Output,
	}, nil
}

// NewLogger creates a logger for a specific module.
// If moduleName is empty, returns a logger without the "module" attribute.
func (f *loggerFactory) NewLogger(moduleName string) types.Logger {
	baseLogger := slog.New(f.handler)
	if moduleName == "" {
		return &slogLogger{logger: baseLogger, output: f.output}
	}
	return &slogLogger{
		logger: baseLogger.With("module", moduleName),
		output: f.output,
	}
}

// SetLevel sets the global log level.
func (f *loggerFactory) SetLevel(level types.LogLevel) {
	f.level.Set(toSlogLevel(level))
}

// GetLevel returns the current log level.
func (f *loggerFactory) GetLevel() types.LogLevel {
	return fromSlogLevel(f.level.Level())
}

// createHandler creates a slog handler based on options.
func createHandler(opts loggerOptions, levelVar *slog.LevelVar) slog.Handler {
	handlerOpts := &slog.HandlerOptions{
		Level:     levelVar,
		AddSource: opts.AddSource,
	}

	if opts.ReplaceAttr != nil {
		handlerOpts.ReplaceAttr = opts.ReplaceAttr
	}

	switch opts.Format {
	case types.LogFormatJSON:
		return slog.NewJSONHandler(opts.Output, handlerOpts)
	case types.LogFormatText:
		return slog.NewTextHandler(opts.Output, handlerOpts)
	default:
		return slog.NewTextHandler(opts.Output, handlerOpts)
	}
}

// loggerOptions holds configuration for logger creation.
type loggerOptions struct {
	Level       types.LogLevel
	Format      types.LogFormat
	Output      io.Writer
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// defaultOptions returns default logger options.
func defaultOptions() loggerOptions {
	return loggerOptions{
		Level:       types.LogLevelInfo,
		Format:      types.LogFormatText,
		Output:      os.Stdout,
		AddSource:   false,
		ReplaceAttr: nil,
	}
}
