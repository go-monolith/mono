package logger

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestNewDefaultLogger(t *testing.T) {
	t.Run("returns non-nil logger", func(t *testing.T) {
		logger := NewDefaultLogger()
		if logger == nil {
			t.Fatal("NewDefaultLogger() returned nil")
		}
	})

	t.Run("implements Logger interface", func(t *testing.T) {
		logger := NewDefaultLogger()
		// Verify interface implementation by type assertion
		_ = types.Logger(logger)
	})

	t.Run("can log messages without panic", func(t *testing.T) {
		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			w.Close()
			os.Stdout = oldStdout
		}()

		logger := NewDefaultLogger()

		// Log some messages - should not panic
		logger.Info("test info message")
		logger.Debug("test debug message")
		logger.Warn("test warn message")
		logger.Error("test error message")

		w.Close()
		os.Stdout = oldStdout

		// Read captured output
		var buf bytes.Buffer
		io.Copy(&buf, r)
		output := buf.String()

		// Verify at least one message was written (Info level)
		if !strings.Contains(output, "test info message") {
			t.Errorf("expected info message in output, got: %s", output)
		}

		// Verify text format (should contain level and msg fields)
		if !strings.Contains(output, "level=") || !strings.Contains(output, "msg=") {
			t.Errorf("expected text format with level= and msg= fields, got: %s", output)
		}
	})

	t.Run("uses Info level by default", func(t *testing.T) {
		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			w.Close()
			os.Stdout = oldStdout
		}()

		logger := NewDefaultLogger()

		// Debug should not appear (below Info level)
		logger.Debug("debug message")
		// Info should appear
		logger.Info("info message")

		w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		io.Copy(&buf, r)
		output := buf.String()

		// Debug message should NOT appear
		if strings.Contains(output, "debug message") {
			t.Errorf("expected debug message to be filtered out, got: %s", output)
		}

		// Info message should appear
		if !strings.Contains(output, "info message") {
			t.Errorf("expected info message in output, got: %s", output)
		}
	})

	t.Run("returns slogLogger type", func(t *testing.T) {
		logger := NewDefaultLogger()

		// Type assert to concrete type
		slogLog, ok := logger.(*slogLogger)
		if !ok {
			t.Fatal("expected logger to be *slogLogger type")
		}

		// Verify internal logger is set
		if slogLog.logger == nil {
			t.Error("expected internal slog.Logger to be non-nil")
		}

		// Verify output is stdout
		if slogLog.output != os.Stdout {
			t.Errorf("expected output to be os.Stdout, got %v", slogLog.output)
		}
	})

	t.Run("uses slog.TextHandler", func(t *testing.T) {
		// Capture stdout
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			w.Close()
			os.Stdout = oldStdout
		}()

		logger := NewDefaultLogger()
		logger.Info("test message", "key", "value")

		w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		io.Copy(&buf, r)
		output := buf.String()

		// TextHandler format: time=... level=INFO msg="..." key=value
		// Verify text format characteristics
		if !strings.Contains(output, "time=") {
			t.Errorf("expected time= in text format, got: %s", output)
		}
		if !strings.Contains(output, "level=INFO") {
			t.Errorf("expected level=INFO in text format, got: %s", output)
		}
		if !strings.Contains(output, "msg=") {
			t.Errorf("expected msg= in text format, got: %s", output)
		}
		if !strings.Contains(output, "key=value") {
			t.Errorf("expected key=value in text format, got: %s", output)
		}
	})

	t.Run("logger can create child loggers with With", func(t *testing.T) {
		// Capture stdout BEFORE creating logger
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			w.Close()
			os.Stdout = oldStdout
		}()

		logger := NewDefaultLogger()

		// With should return a new logger with additional fields
		childLogger := logger.With("module", "test")
		if childLogger == nil {
			w.Close()
			os.Stdout = oldStdout
			t.Fatal("With() returned nil logger")
		}

		// Child logger should be usable
		childLogger.Info("child log message")

		w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		io.Copy(&buf, r)
		output := buf.String()

		// Verify child logger output contains the context field
		if !strings.Contains(output, "module=test") {
			t.Errorf("expected module=test in child logger output, got: %s", output)
		}
		if !strings.Contains(output, "child log message") {
			t.Errorf("expected message in child logger output, got: %s", output)
		}
	})
}
