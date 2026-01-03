package logger

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// mockFlusher implements io.Writer with Flush() method
type mockFlusher struct {
	buf         *bytes.Buffer
	flushCalled bool
	flushErr    error
}

func (m *mockFlusher) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}

func (m *mockFlusher) Flush() error {
	m.flushCalled = true
	return m.flushErr
}

// mockSyncer implements io.Writer with Sync() method
type mockSyncer struct {
	buf        *bytes.Buffer
	syncCalled bool
	syncErr    error
}

func (m *mockSyncer) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}

func (m *mockSyncer) Sync() error {
	m.syncCalled = true
	return m.syncErr
}

// TestFlush_NilLogger tests Flush() with nil logger
func TestFlush_NilLogger(t *testing.T) {
	var l *slogLogger
	err := l.Flush()
	if err != nil {
		t.Errorf("Flush() with nil logger should return nil error, got %v", err)
	}
}

// TestFlush_NilOutput tests Flush() with nil output
func TestFlush_NilOutput(t *testing.T) {
	l := &slogLogger{
		logger: slog.Default(),
		output: nil,
	}
	err := l.Flush()
	if err != nil {
		t.Errorf("Flush() with nil output should return nil error, got %v", err)
	}
}

// TestFlush_FlusherInterface tests Flush() with Flusher interface
func TestFlush_FlusherInterface(t *testing.T) {
	tests := []struct {
		name      string
		flushErr  error
		expectErr bool
	}{
		{
			name:      "Flush success",
			flushErr:  nil,
			expectErr: false,
		},
		{
			name:      "Flush error",
			flushErr:  errors.New("flush failed"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flusher := &mockFlusher{
				buf:      &bytes.Buffer{},
				flushErr: tt.flushErr,
			}
			l := &slogLogger{
				logger: slog.New(slog.NewTextHandler(flusher.buf, nil)),
				output: flusher,
			}

			err := l.Flush()
			if !flusher.flushCalled {
				t.Error("Flush() should have been called on flusher")
			}
			if (err != nil) != tt.expectErr {
				t.Errorf("Flush() error = %v, expectErr = %v", err, tt.expectErr)
			}
		})
	}
}

// TestFlush_SyncerInterface tests Flush() with Syncer interface
func TestFlush_SyncerInterface(t *testing.T) {
	tests := []struct {
		name      string
		syncErr   error
		expectErr bool
	}{
		{
			name:      "Sync success",
			syncErr:   nil,
			expectErr: false,
		},
		{
			name:      "Sync error",
			syncErr:   errors.New("sync failed"),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer := &mockSyncer{
				buf:     &bytes.Buffer{},
				syncErr: tt.syncErr,
			}
			l := &slogLogger{
				logger: slog.New(slog.NewTextHandler(syncer.buf, nil)),
				output: syncer,
			}

			err := l.Flush()
			if !syncer.syncCalled {
				t.Error("Sync() should have been called on syncer")
			}
			if (err != nil) != tt.expectErr {
				t.Errorf("Flush() error = %v, expectErr = %v", err, tt.expectErr)
			}
		})
	}
}

// TestFlush_FileOutput tests Flush() with os.File output
func TestFlush_FileOutput(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test-flush-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	l := &slogLogger{
		logger: slog.New(slog.NewTextHandler(tmpFile, nil)),
		output: tmpFile,
	}

	err = l.Flush()
	if err != nil {
		t.Errorf("Flush() with regular file should not error, got %v", err)
	}
}

// TestFlush_StdoutStderr tests Flush() with stdout/stderr (non-regular files)
func TestFlush_StdoutStderr(t *testing.T) {
	tests := []struct {
		name   string
		output *os.File
	}{
		{
			name:   "Stdout",
			output: os.Stdout,
		},
		{
			name:   "Stderr",
			output: os.Stderr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &slogLogger{
				logger: slog.New(slog.NewTextHandler(tt.output, nil)),
				output: tt.output,
			}

			err := l.Flush()
			// Should return nil for non-regular files like stdout/stderr
			if err != nil {
				t.Errorf("Flush() with %s should return nil, got %v", tt.name, err)
			}
		})
	}
}

// TestFlush_NoFlushOrSync tests Flush() with writer that has neither Flush nor Sync
func TestFlush_NoFlushOrSync(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &slogLogger{
		logger: slog.New(slog.NewTextHandler(buf, nil)),
		output: buf,
	}

	err := l.Flush()
	if err != nil {
		t.Errorf("Flush() with plain writer should return nil, got %v", err)
	}
}

// TestToSlogLevel tests conversion from types.LogLevel to slog.Level
func TestToSlogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    types.LogLevel
		expected slog.Level
	}{
		{
			name:     "Debug level",
			level:    types.LogLevelDebug,
			expected: slog.LevelDebug,
		},
		{
			name:     "Info level",
			level:    types.LogLevelInfo,
			expected: slog.LevelInfo,
		},
		{
			name:     "Warn level",
			level:    types.LogLevelWarn,
			expected: slog.LevelWarn,
		},
		{
			name:     "Error level",
			level:    types.LogLevelError,
			expected: slog.LevelError,
		},
		{
			name:     "Unknown level defaults to Info",
			level:    types.LogLevel(99),
			expected: slog.LevelInfo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSlogLevel(tt.level)
			if result != tt.expected {
				t.Errorf("toSlogLevel(%v) = %v, want %v", tt.level, result, tt.expected)
			}
		})
	}
}

// TestFromSlogLevel tests conversion from slog.Level to types.LogLevel
func TestFromSlogLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    slog.Level
		expected types.LogLevel
	}{
		{
			name:     "Debug level",
			level:    slog.LevelDebug,
			expected: types.LogLevelDebug,
		},
		{
			name:     "Info level",
			level:    slog.LevelInfo,
			expected: types.LogLevelInfo,
		},
		{
			name:     "Warn level",
			level:    slog.LevelWarn,
			expected: types.LogLevelWarn,
		},
		{
			name:     "Error level",
			level:    slog.LevelError,
			expected: types.LogLevelError,
		},
		{
			name:     "Unknown level defaults to Info",
			level:    slog.Level(99),
			expected: types.LogLevelInfo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromSlogLevel(tt.level)
			if result != tt.expected {
				t.Errorf("fromSlogLevel(%v) = %v, want %v", tt.level, result, tt.expected)
			}
		})
	}
}

// TestSlogLogger_Methods tests all slogLogger methods for basic functionality
func TestSlogLogger_Methods(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := &slogLogger{
		logger: slog.New(slog.NewTextHandler(buf, nil)),
		output: buf,
	}

	// Test logging methods
	logger.Debug("debug message", "key", "value")
	logger.Info("info message", "key", "value")
	logger.Warn("warn message", "key", "value")
	logger.Error("error message", "key", "value")

	// Verify output contains messages
	output := buf.String()
	if output == "" {
		t.Error("Logger should have produced output")
	}
}

// TestSlogLogger_With tests With() method
func TestSlogLogger_With(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := &slogLogger{
		logger: slog.New(slog.NewTextHandler(buf, nil)),
		output: buf,
	}

	newLogger := logger.With("context", "test")
	if newLogger == nil {
		t.Fatal("With() should return a new logger")
	}

	// Verify the new logger is a different instance
	if logger == newLogger {
		t.Error("With() should return a new logger instance")
	}

	// Verify output is preserved
	newSlogLogger, ok := newLogger.(*slogLogger)
	if !ok {
		t.Fatal("With() should return *slogLogger")
	}
	if newSlogLogger.output != buf {
		t.Error("With() should preserve output writer")
	}
}

// TestSlogLogger_WithModule tests WithModule() method
func TestSlogLogger_WithModule(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := &slogLogger{
		logger: slog.New(slog.NewTextHandler(buf, nil)),
		output: buf,
	}

	tests := []struct {
		name       string
		moduleName string
		shouldWrap bool
	}{
		{
			name:       "Non-empty module name",
			moduleName: "test-module",
			shouldWrap: true,
		},
		{
			name:       "Empty module name returns same logger",
			moduleName: "",
			shouldWrap: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newLogger := logger.WithModule(tt.moduleName)
			if newLogger == nil {
				t.Fatal("WithModule() should return a logger")
			}

			if tt.shouldWrap {
				if logger == newLogger {
					t.Error("WithModule() with non-empty name should return a new logger instance")
				}
			} else {
				if logger != newLogger {
					t.Error("WithModule() with empty name should return the same logger instance")
				}
			}
		})
	}
}

// TestSlogLogger_WithError tests WithError() method
func TestSlogLogger_WithError(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := &slogLogger{
		logger: slog.New(slog.NewTextHandler(buf, nil)),
		output: buf,
	}

	testErr := errors.New("test error")
	newLogger := logger.WithError(testErr)
	if newLogger == nil {
		t.Fatal("WithError() should return a new logger")
	}

	// Verify the new logger is a different instance
	if logger == newLogger {
		t.Error("WithError() should return a new logger instance")
	}
}
