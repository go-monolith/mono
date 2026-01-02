package logger

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestWithAddSource tests WithAddSource option
func TestWithAddSource(t *testing.T) {
	tests := []struct {
		name      string
		addSource bool
	}{
		{
			name:      "AddSource enabled",
			addSource: true,
		},
		{
			name:      "AddSource disabled",
			addSource: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			factory, err := NewLoggerFactory(
				WithOutput(buf),
				WithLogFormat(types.LogFormatText),
				WithAddSource(tt.addSource),
			)
			if err != nil {
				t.Fatalf("Failed to create logger factory: %v", err)
			}

			logger := factory.NewLogger("test")
			logger.Info("test message")

			output := buf.String()
			if tt.addSource {
				// When AddSource is enabled, output should contain source file info
				if !strings.Contains(output, "source=") {
					t.Error("Expected source info in log output when AddSource is enabled")
				}
			} else {
				// When AddSource is disabled, output should not contain source file info
				if strings.Contains(output, "source=") {
					t.Error("Did not expect source info in log output when AddSource is disabled")
				}
			}
		})
	}
}

// TestWithReplaceAttr tests WithReplaceAttr option
func TestWithReplaceAttr(t *testing.T) {
	tests := []struct {
		name        string
		replaceAttr func([]string, slog.Attr) slog.Attr
		expectMsg   string
	}{
		{
			name: "Replace message attribute",
			replaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.MessageKey {
					return slog.String(slog.MessageKey, "REPLACED")
				}
				return a
			},
			expectMsg: "REPLACED",
		},
		{
			name: "Remove time attribute",
			replaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
			expectMsg: "", // time should be removed
		},
		{
			name:        "Nil replaceAttr",
			replaceAttr: nil,
			expectMsg:   "test message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			opts := []LoggerOption{
				WithOutput(buf),
				WithLogFormat(types.LogFormatText),
			}
			if tt.replaceAttr != nil {
				opts = append(opts, WithReplaceAttr(tt.replaceAttr))
			}

			factory, err := NewLoggerFactory(opts...)
			if err != nil {
				t.Fatalf("Failed to create logger factory: %v", err)
			}

			logger := factory.NewLogger("test")
			logger.Info("test message")

			output := buf.String()
			if tt.expectMsg != "" {
				if !strings.Contains(output, tt.expectMsg) {
					t.Errorf("Expected message %q in output, got: %s", tt.expectMsg, output)
				}
			}

			// Verify time attribute removal
			if tt.name == "Remove time attribute" {
				if strings.Contains(output, "time=") {
					t.Error("Expected time attribute to be removed from output")
				}
			}
		})
	}
}

// TestCreateHandler tests createHandler with all format/level combinations
func TestCreateHandler(t *testing.T) {
	tests := []struct {
		name          string
		format        types.LogFormat
		level         types.LogLevel
		addSource     bool
		replaceAttr   func([]string, slog.Attr) slog.Attr
		expectedType  string
		testMessage   string
		shouldContain []string
		shouldNotLog  bool // For level filtering test
	}{
		{
			name:          "JSON handler with Debug level",
			format:        types.LogFormatJSON,
			level:         types.LogLevelDebug,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "json",
			testMessage:   "debug test",
			shouldContain: []string{`"msg":"debug test"`},
		},
		{
			name:          "JSON handler with Info level",
			format:        types.LogFormatJSON,
			level:         types.LogLevelInfo,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "json",
			testMessage:   "info test",
			shouldContain: []string{`"msg":"info test"`},
		},
		{
			name:          "JSON handler with Warn level",
			format:        types.LogFormatJSON,
			level:         types.LogLevelWarn,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "json",
			testMessage:   "warn test",
			shouldContain: []string{`"msg":"warn test"`, `"level":"WARN"`},
		},
		{
			name:          "JSON handler with Error level",
			format:        types.LogFormatJSON,
			level:         types.LogLevelError,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "json",
			testMessage:   "error test",
			shouldContain: []string{`"msg":"error test"`, `"level":"ERROR"`},
		},
		{
			name:          "Text handler with Debug level",
			format:        types.LogFormatText,
			level:         types.LogLevelDebug,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "text",
			testMessage:   "debug test",
			shouldContain: []string{"msg=\"debug test\""},
		},
		{
			name:          "Text handler with Info level",
			format:        types.LogFormatText,
			level:         types.LogLevelInfo,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "text",
			testMessage:   "info test",
			shouldContain: []string{"msg=\"info test\""},
		},
		{
			name:          "Text handler with Warn level",
			format:        types.LogFormatText,
			level:         types.LogLevelWarn,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "text",
			testMessage:   "warn test",
			shouldContain: []string{"msg=\"warn test\"", "level=WARN"},
		},
		{
			name:          "Text handler with Error level",
			format:        types.LogFormatText,
			level:         types.LogLevelError,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "text",
			testMessage:   "error test",
			shouldContain: []string{"msg=\"error test\"", "level=ERROR"},
		},
		{
			name:          "Unknown format defaults to Text",
			format:        types.LogFormat(99),
			level:         types.LogLevelInfo,
			addSource:     false,
			replaceAttr:   nil,
			expectedType:  "text",
			testMessage:   "default test",
			shouldContain: []string{"msg=\"default test\""},
		},
		{
			name:          "Handler with AddSource",
			format:        types.LogFormatJSON,
			level:         types.LogLevelInfo,
			addSource:     true,
			replaceAttr:   nil,
			expectedType:  "json",
			testMessage:   "source test",
			shouldContain: []string{`"msg":"source test"`, `"source":`},
		},
		{
			name:      "Handler with ReplaceAttr",
			format:    types.LogFormatJSON,
			level:     types.LogLevelInfo,
			addSource: false,
			replaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == "test-key" {
					return slog.String("test-key", "MODIFIED")
				}
				return a
			},
			expectedType:  "json",
			testMessage:   "replace attr test",
			shouldContain: []string{`"test-key":"MODIFIED"`},
		},
		{
			name:         "Level filtering - Debug not logged at Info level",
			format:       types.LogFormatText,
			level:        types.LogLevelInfo,
			addSource:    false,
			replaceAttr:  nil,
			expectedType: "text",
			testMessage:  "debug should not appear",
			shouldNotLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			levelVar := new(slog.LevelVar)
			levelVar.Set(toSlogLevel(tt.level))

			opts := loggerOptions{
				Level:       tt.level,
				Format:      tt.format,
				Output:      buf,
				AddSource:   tt.addSource,
				ReplaceAttr: tt.replaceAttr,
			}

			handler := createHandler(opts, levelVar)
			logger := slog.New(handler)

			// Test different log levels
			if tt.shouldNotLog {
				// First verify logger works at Info level
				logger.Info("control message")
				if buf.String() == "" {
					t.Fatal("Logger is not producing any output at all")
				}
				buf.Reset()

				// Then test that Debug messages are filtered when level is Info
				logger.Debug(tt.testMessage)
				output := buf.String()
				if output != "" {
					t.Errorf("Debug message should not be logged at Info level, got: %s", output)
				}
			} else {
				// Log based on the test's level
				switch tt.level {
				case types.LogLevelDebug:
					logger.Debug(tt.testMessage)
				case types.LogLevelInfo:
					logger.Info(tt.testMessage)
				case types.LogLevelWarn:
					logger.Warn(tt.testMessage)
				case types.LogLevelError:
					logger.Error(tt.testMessage)
				}

				// For ReplaceAttr test, add custom key
				if tt.replaceAttr != nil {
					buf.Reset()
					logger.Info(tt.testMessage, "test-key", "original")
				}

				output := buf.String()
				if output == "" {
					t.Error("Handler should have produced output")
				}

				// Verify expected content
				for _, expected := range tt.shouldContain {
					if !strings.Contains(output, expected) {
						t.Errorf("Expected output to contain %q, got: %s", expected, output)
					}
				}
			}
		})
	}
}

// TestWithOutput tests WithOutput option
func TestWithOutput(t *testing.T) {
	tests := []struct {
		name      string
		output    *bytes.Buffer
		expectErr bool
	}{
		{
			name:      "Valid output",
			output:    &bytes.Buffer{},
			expectErr: false,
		},
		{
			name:      "Nil output should error",
			output:    nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []LoggerOption
			if tt.output != nil {
				opts = append(opts, WithOutput(tt.output))
			} else {
				// Create an option that will be applied with nil output
				opts = append(opts, func(o *loggerOptions) error {
					return WithOutput(nil)(o)
				})
			}

			_, err := NewLoggerFactory(opts...)
			if (err != nil) != tt.expectErr {
				t.Errorf("WithOutput() error = %v, expectErr = %v", err, tt.expectErr)
			}
		})
	}
}

// TestWithLogLevel tests WithLogLevel option
func TestWithLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		level types.LogLevel
	}{
		{
			name:  "Debug level",
			level: types.LogLevelDebug,
		},
		{
			name:  "Info level",
			level: types.LogLevelInfo,
		},
		{
			name:  "Warn level",
			level: types.LogLevelWarn,
		},
		{
			name:  "Error level",
			level: types.LogLevelError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			factory, err := NewLoggerFactory(
				WithOutput(buf),
				WithLogLevel(tt.level),
			)
			if err != nil {
				t.Fatalf("Failed to create logger factory: %v", err)
			}

			// Verify level was set correctly
			currentLevel := factory.GetLevel()
			if currentLevel != tt.level {
				t.Errorf("GetLevel() = %v, want %v", currentLevel, tt.level)
			}
		})
	}
}

// TestWithLogFormat tests WithLogFormat option
func TestWithLogFormat(t *testing.T) {
	tests := []struct {
		name   string
		format types.LogFormat
	}{
		{
			name:   "JSON format",
			format: types.LogFormatJSON,
		},
		{
			name:   "Text format",
			format: types.LogFormatText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			factory, err := NewLoggerFactory(
				WithOutput(buf),
				WithLogFormat(tt.format),
			)
			if err != nil {
				t.Fatalf("Failed to create logger factory: %v", err)
			}

			logger := factory.NewLogger("test")
			logger.Info("format test")

			output := buf.String()
			if tt.format == types.LogFormatJSON {
				if !strings.Contains(output, "{") || !strings.Contains(output, "}") {
					t.Error("Expected JSON format output")
				}
			} else {
				if strings.HasPrefix(output, "{") {
					t.Error("Expected text format output, got JSON")
				}
			}
		})
	}
}

// TestLoggerFactory_SetLevel tests SetLevel method
func TestLoggerFactory_SetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	factory, err := NewLoggerFactory(
		WithOutput(buf),
		WithLogLevel(types.LogLevelInfo),
	)
	if err != nil {
		t.Fatalf("Failed to create logger factory: %v", err)
	}

	// Change level to Debug
	factory.SetLevel(types.LogLevelDebug)
	if factory.GetLevel() != types.LogLevelDebug {
		t.Errorf("SetLevel(Debug) failed, GetLevel() = %v", factory.GetLevel())
	}

	// Change level to Error
	factory.SetLevel(types.LogLevelError)
	if factory.GetLevel() != types.LogLevelError {
		t.Errorf("SetLevel(Error) failed, GetLevel() = %v", factory.GetLevel())
	}
}

// TestLoggerFactory_NewLogger tests NewLogger method
func TestLoggerFactory_NewLogger(t *testing.T) {
	tests := []struct {
		name         string
		moduleName   string
		expectModule bool
	}{
		{
			name:         "Logger with module name",
			moduleName:   "test-module",
			expectModule: true,
		},
		{
			name:         "Logger without module name",
			moduleName:   "",
			expectModule: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			factory, err := NewLoggerFactory(
				WithOutput(buf),
				WithLogFormat(types.LogFormatText),
			)
			if err != nil {
				t.Fatalf("Failed to create logger factory: %v", err)
			}

			logger := factory.NewLogger(tt.moduleName)
			if logger == nil {
				t.Fatal("NewLogger() returned nil")
			}

			logger.Info("test message")
			output := buf.String()

			if tt.expectModule {
				if !strings.Contains(output, "module="+tt.moduleName) {
					t.Errorf("Expected module name %q in output, got: %s", tt.moduleName, output)
				}
			} else {
				if strings.Contains(output, "module=") {
					t.Error("Did not expect module attribute in output")
				}
			}
		})
	}
}

// TestLoggerOptionError tests error handling in logger options
func TestLoggerOptionError(t *testing.T) {
	// Create a custom option that returns an error
	errorOption := func(opts *loggerOptions) error {
		return errors.New("custom option error")
	}

	buf := &bytes.Buffer{}
	_, err := NewLoggerFactory(
		WithOutput(buf),
		errorOption,
	)

	if err == nil {
		t.Error("Expected error from custom option, got nil")
	}
	if err.Error() != "custom option error" {
		t.Errorf("Expected error message 'custom option error', got: %v", err)
	}
}

// TestDefaultOptions tests default logger options
func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()

	if opts.Level != types.LogLevelInfo {
		t.Errorf("Default level = %v, want %v", opts.Level, types.LogLevelInfo)
	}
	if opts.Format != types.LogFormatText {
		t.Errorf("Default format = %v, want %v", opts.Format, types.LogFormatText)
	}
	if opts.AddSource != false {
		t.Error("Default AddSource should be false")
	}
	if opts.ReplaceAttr != nil {
		t.Error("Default ReplaceAttr should be nil")
	}
}
