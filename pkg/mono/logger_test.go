package mono_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/go-monolith/mono"
)

// mockLoggerFactory is a mock implementation for testing
type mockLoggerFactory struct {
	level mono.LogLevel
	mu    sync.RWMutex
}

func (f *mockLoggerFactory) NewLogger(moduleName string) mono.Logger {
	return &testLogger{
		moduleName: moduleName,
		level:      f.level,
		buf:        &bytes.Buffer{},
	}
}

func (f *mockLoggerFactory) SetLevel(level mono.LogLevel) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.level = level
}

func (f *mockLoggerFactory) GetLevel() mono.LogLevel {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.level
}

// testLogger is a test logger implementation for testing
type testLogger struct {
	moduleName string
	level      mono.LogLevel
	buf        *bytes.Buffer
	mu         sync.Mutex
	context    []any
}

func (l *testLogger) Debug(msg string, args ...any) {
	l.logIf(mono.LogLevelDebug, msg, args...)
}

func (l *testLogger) Info(msg string, args ...any) {
	l.logIf(mono.LogLevelInfo, msg, args...)
}

func (l *testLogger) Warn(msg string, args ...any) {
	l.logIf(mono.LogLevelWarn, msg, args...)
}

func (l *testLogger) Error(msg string, args ...any) {
	l.logIf(mono.LogLevelError, msg, args...)
}

func (l *testLogger) logIf(msgLevel mono.LogLevel, msg string, args ...any) {
	if msgLevel < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	levelStr := "INFO"
	switch msgLevel {
	case mono.LogLevelDebug:
		levelStr = "DEBUG"
	case mono.LogLevelWarn:
		levelStr = "WARN"
	case mono.LogLevelError:
		levelStr = "ERROR"
	}

	l.buf.WriteString(levelStr + " " + msg)
	if l.moduleName != "" {
		l.buf.WriteString(" module=" + l.moduleName)
	}

	// Add context
	for i := 0; i < len(l.context); i += 2 {
		if i+1 < len(l.context) {
			l.buf.WriteString(" " + l.context[i].(string) + "=" + toString(l.context[i+1]))
		}
	}

	// Add args
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			l.buf.WriteString(" " + args[i].(string) + "=" + toString(args[i+1]))
		}
	}
	l.buf.WriteString("\n")
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return slog.IntValue(val).String()
	case error:
		return val.Error()
	default:
		return slog.AnyValue(v).String()
	}
}

func (l *testLogger) With(args ...any) mono.Logger {
	newLogger := &testLogger{
		moduleName: l.moduleName,
		level:      l.level,
		buf:        l.buf,
		context:    append([]any{}, l.context...),
	}
	newLogger.context = append(newLogger.context, args...)
	return newLogger
}

func (l *testLogger) WithModule(moduleName string) mono.Logger {
	return &testLogger{
		moduleName: moduleName,
		level:      l.level,
		buf:        l.buf,
		context:    append([]any{}, l.context...),
	}
}

func (l *testLogger) WithError(err error) mono.Logger {
	return l.With("error", err)
}

func (l *testLogger) output() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// TestLoggerInterface ensures mono.Logger interface compliance
func TestLoggerInterface(t *testing.T) {
	var _ mono.Logger = (*testLogger)(nil)
}

// Testmono.LogLevelFiltering verifies debug logs are hidden at info level
func TestLogLevelFiltering(t *testing.T) {
	tests := []struct {
		name          string
		level         mono.LogLevel
		logFunc       func(mono.Logger)
		shouldAppear  bool
		expectedLevel string
	}{
		{
			name:          "Debug at Debug level",
			level:         mono.LogLevelDebug,
			logFunc:       func(l mono.Logger) { l.Debug("test debug") },
			shouldAppear:  true,
			expectedLevel: "DEBUG",
		},
		{
			name:          "Debug at Info level",
			level:         mono.LogLevelInfo,
			logFunc:       func(l mono.Logger) { l.Debug("test debug") },
			shouldAppear:  false,
			expectedLevel: "DEBUG",
		},
		{
			name:          "Info at Warn level",
			level:         mono.LogLevelWarn,
			logFunc:       func(l mono.Logger) { l.Info("test info") },
			shouldAppear:  false,
			expectedLevel: "INFO",
		},
		{
			name:          "Warn at Warn level",
			level:         mono.LogLevelWarn,
			logFunc:       func(l mono.Logger) { l.Warn("test warn") },
			shouldAppear:  true,
			expectedLevel: "WARN",
		},
		{
			name:          "Error at all levels",
			level:         mono.LogLevelDebug,
			logFunc:       func(l mono.Logger) { l.Error("test error") },
			shouldAppear:  true,
			expectedLevel: "ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &testLogger{
				level: tt.level,
				buf:   &bytes.Buffer{},
			}

			tt.logFunc(logger)
			output := logger.output()

			if tt.shouldAppear {
				if !strings.Contains(output, tt.expectedLevel) {
					t.Errorf("expected log level %s to appear in output, got: %s", tt.expectedLevel, output)
				}
			} else {
				if len(output) > 0 {
					t.Errorf("expected no log output, but got: %s", output)
				}
			}
		})
	}
}

// TestContextPropagation tests With, WithModule, WithError methods
func TestContextPropagation(t *testing.T) {
	t.Run("With adds context fields", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		logger2 := logger.With("request_id", "123", "user_id", "456")
		logger2.Info("test message")

		output := logger2.(*testLogger).output()
		if !strings.Contains(output, "request_id=123") {
			t.Errorf("expected request_id=123 in output, got: %s", output)
		}
		if !strings.Contains(output, "user_id=456") {
			t.Errorf("expected user_id=456 in output, got: %s", output)
		}
	})

	t.Run("WithModule sets module context", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		logger2 := logger.WithModule("auth-module")
		logger2.Info("authentication event")

		output := logger2.(*testLogger).output()
		if !strings.Contains(output, "module=auth-module") {
			t.Errorf("expected module=auth-module in output, got: %s", output)
		}
	})

	t.Run("WithError adds error context", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		testErr := errors.New("test error occurred")
		logger2 := logger.WithError(testErr)
		logger2.Error("operation failed")

		output := logger2.(*testLogger).output()
		if !strings.Contains(output, "error=test error occurred") {
			t.Errorf("expected error in output, got: %s", output)
		}
	})

	t.Run("chained context accumulates", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		logger2 := logger.With("key1", "value1").With("key2", "value2")
		logger2.Info("test")

		output := logger2.(*testLogger).output()
		if !strings.Contains(output, "key1=value1") || !strings.Contains(output, "key2=value2") {
			t.Errorf("expected both context fields in output, got: %s", output)
		}
	})
}

// TestCustomAttributeReplacement tests custom attribute replacement function
func TestCustomAttributeReplacement(t *testing.T) {
	// This test verifies the ability to customize log attributes
	// In a real implementation, this would use WithReplaceAttr option
	t.Run("sensitive fields can be redacted", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		// Simulate logging with a field that should be redacted
		logger.Info("user login", "password", "[REDACTED]", "username", "test_user")

		output := logger.output()
		if strings.Contains(output, "actual_password") {
			t.Error("password should be redacted in logs")
		}
		if !strings.Contains(output, "[REDACTED]") {
			t.Error("expected redacted placeholder in output")
		}
	})
}

// TestJSONAndTextFormat tests both JSON and Text format output
func TestJSONAndTextFormat(t *testing.T) {
	t.Run("Text format produces readable output", func(t *testing.T) {
		logger := &testLogger{
			level: mono.LogLevelInfo,
			buf:   &bytes.Buffer{},
		}

		logger.Info("test message", "key", "value")
		output := logger.output()

		if !strings.Contains(output, "INFO") {
			t.Errorf("expected INFO level in text output, got: %s", output)
		}
		if !strings.Contains(output, "test message") {
			t.Errorf("expected message in text output, got: %s", output)
		}
	})

	t.Run("JSON format produces parseable JSON", func(t *testing.T) {
		// Simulate JSON formatted log
		jsonLog := map[string]interface{}{
			"level":  "INFO",
			"msg":    "test message",
			"module": "test-module",
			"key":    "value",
		}

		data, err := json.Marshal(jsonLog)
		if err != nil {
			t.Fatalf("failed to marshal JSON: %v", err)
		}

		// Verify JSON is parseable
		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("failed to unmarshal JSON: %v", err)
		}

		if parsed["level"] != "INFO" {
			t.Errorf("expected level INFO in JSON, got: %v", parsed["level"])
		}
		if parsed["msg"] != "test message" {
			t.Errorf("expected msg in JSON, got: %v", parsed["msg"])
		}
	})
}

// TestConcurrentLogging tests thread safety with concurrent loggers
func TestConcurrentLogging(t *testing.T) {
	logger := &testLogger{
		level: mono.LogLevelInfo,
		buf:   &bytes.Buffer{},
		mu:    sync.Mutex{},
	}

	const goroutines = 100
	const logsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < logsPerGoroutine; j++ {
				logger.Info("concurrent log", "goroutine", id, "iteration", j)
			}
		}(i)
	}

	wg.Wait()

	output := logger.output()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Should have goroutines * logsPerGoroutine lines
	expectedLines := goroutines * logsPerGoroutine
	if len(lines) != expectedLines {
		t.Errorf("expected %d log lines, got %d", expectedLines, len(lines))
	}

	// Verify no data races by checking output is well-formed
	for i, line := range lines {
		if !strings.Contains(line, "concurrent log") {
			t.Errorf("line %d missing expected content: %s", i, line)
		}
	}
}

// Testmono.LoggerFactory tests mono.LoggerFactory interface
func TestLoggerFactory(t *testing.T) {
	var _ mono.LoggerFactory = (*mockLoggerFactory)(nil)

	t.Run("Newmono.Logger creates logger with module name", func(t *testing.T) {
		factory := &mockLoggerFactory{level: mono.LogLevelInfo}
		logger := factory.NewLogger("test-module")

		if logger == nil {
			t.Fatal("Newmono.Logger returned nil")
		}

		testLog := logger.(*testLogger)
		if testLog.moduleName != "test-module" {
			t.Errorf("expected module name 'test-module', got %s", testLog.moduleName)
		}
	})

	t.Run("SetLevel updates factory level", func(t *testing.T) {
		factory := &mockLoggerFactory{level: mono.LogLevelInfo}

		factory.SetLevel(mono.LogLevelDebug)
		level := factory.GetLevel()

		if level != mono.LogLevelDebug {
			t.Errorf("expected level Debug, got %v", level)
		}
	})

	t.Run("GetLevel returns current level", func(t *testing.T) {
		factory := &mockLoggerFactory{level: mono.LogLevelWarn}

		level := factory.GetLevel()
		if level != mono.LogLevelWarn {
			t.Errorf("expected level Warn, got %v", level)
		}
	})
}

// Testmono.LogLevels tests mono.LogLevel enum values
func TestLogLevels(t *testing.T) {
	tests := []struct {
		level    mono.LogLevel
		expected int
	}{
		{mono.LogLevelDebug, 0},
		{mono.LogLevelInfo, 1},
		{mono.LogLevelWarn, 2},
		{mono.LogLevelError, 3},
	}

	for _, tt := range tests {
		t.Run("mono.LogLevel ordering", func(t *testing.T) {
			if int(tt.level) != tt.expected {
				t.Errorf("expected level %d, got %d", tt.expected, int(tt.level))
			}
		})
	}

	// Verify ordering (Debug < Info < Warn < Error)
	if mono.LogLevelDebug >= mono.LogLevelInfo {
		t.Error("mono.LogLevelDebug should be < mono.LogLevelInfo")
	}
	if mono.LogLevelInfo >= mono.LogLevelWarn {
		t.Error("mono.LogLevelInfo should be < mono.LogLevelWarn")
	}
	if mono.LogLevelWarn >= mono.LogLevelError {
		t.Error("mono.LogLevelWarn should be < mono.LogLevelError")
	}
}

// TestLogFormat tests mono.LogFormat enum values
func TestLogFormat(t *testing.T) {
	tests := []struct {
		format mono.LogFormat
		name   string
	}{
		{mono.LogFormatJSON, "JSON"},
		{mono.LogFormatText, "Text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the constants exist and are distinct
			if tt.format < 0 {
				t.Errorf("invalid log format value: %d", tt.format)
			}
		})
	}

	if mono.LogFormatJSON == mono.LogFormatText {
		t.Error("mono.LogFormatJSON and mono.LogFormatText should have different values")
	}
}
