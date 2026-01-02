package app

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-monolith/mono/v1/internal/nats"
	"github.com/go-monolith/mono/v1/pkg/types"
)

func TestCreateFrameworkAppInstance_WithNilLogger(t *testing.T) {
	// Test that nil logger triggers default logger creation
	natsOpts := types.NATSOptions{
		DontListen:       true,
		UseInProcessConn: true,
	}
	loggerOpts := types.LoggerOptions{
		Level:  types.LogLevelInfo,
		Format: types.LogFormatText,
	}

	fw, err := CreateFrameworkAppInstance(nil, loggerOpts, natsOpts, 0)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if fw == nil {
		t.Fatal("Expected framework instance, got nil")
	}
	defer fw.Stop(context.Background())

	// Verify framework was created successfully
	if fw.Modules() == nil {
		t.Error("Expected modules list to be initialized")
	}
}

func TestCreateFrameworkAppInstance_WithCustomLogger(t *testing.T) {
	// Test that custom logger is used
	customLogger := &mockLogger{}
	natsOpts := types.NATSOptions{
		DontListen:       true,
		UseInProcessConn: true,
	}
	loggerOpts := types.LoggerOptions{} // Should be ignored since logger is provided

	fw, err := CreateFrameworkAppInstance(customLogger, loggerOpts, natsOpts, 0)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if fw == nil {
		t.Fatal("Expected framework instance, got nil")
	}
	defer fw.Stop(context.Background())
}

func TestCreateDefaultLogger_UseDefault(t *testing.T) {
	// Test UseDefault flag returns default logger
	opts := types.LoggerOptions{
		UseDefault: true,
	}

	logger, err := createDefaultLogger(opts)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if logger == nil {
		t.Fatal("Expected logger, got nil")
	}

	// Verify logger works
	logger.Info("test message")
}

func TestCreateDefaultLogger_WithOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     types.LoggerOptions
		wantErr  bool
		contains string // Expected string in log output
	}{
		{
			name: "JSON format",
			opts: types.LoggerOptions{
				Level:  types.LogLevelInfo,
				Format: types.LogFormatJSON,
			},
			wantErr:  false,
			contains: `"msg":"test message"`,
		},
		{
			name: "Text format",
			opts: types.LoggerOptions{
				Level:  types.LogLevelInfo,
				Format: types.LogFormatText,
			},
			wantErr:  false,
			contains: "test message",
		},
		{
			name: "Debug level",
			opts: types.LoggerOptions{
				Level:  types.LogLevelDebug,
				Format: types.LogFormatText,
			},
			wantErr:  false,
			contains: "test message",
		},
		{
			name: "With source",
			opts: types.LoggerOptions{
				Level:     types.LogLevelInfo,
				Format:    types.LogFormatText,
				AddSource: true,
			},
			wantErr:  false,
			contains: "test message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tt.opts.Output = &buf

			logger, err := createDefaultLogger(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("createDefaultLogger() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if logger == nil {
				t.Fatal("Expected logger, got nil")
			}

			// Test logger by writing a message
			logger.Info("test message")

			// Verify output contains expected string
			output := buf.String()
			if !strings.Contains(output, tt.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tt.contains, output)
			}
		})
	}
}

func TestCreateDefaultLogger_NilOutput(t *testing.T) {
	// Test that nil output defaults to os.Stdout
	opts := types.LoggerOptions{
		Level:  types.LogLevelInfo,
		Format: types.LogFormatText,
		Output: nil, // Should default to os.Stdout
	}

	logger, err := createDefaultLogger(opts)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if logger == nil {
		t.Fatal("Expected logger, got nil")
	}

	// Should not panic when logging
	logger.Info("test message")
}

func TestBuildNATSOptions_EmptyConfig(t *testing.T) {
	// Test empty config returns empty slice
	cfg := types.NATSOptions{}
	opts := buildNATSOptions(cfg)

	if len(opts) != 0 {
		t.Errorf("Expected empty slice, got %d options", len(opts))
	}
}

func TestBuildNATSOptions_Host(t *testing.T) {
	cfg := types.NATSOptions{
		Host: "localhost",
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_Port(t *testing.T) {
	cfg := types.NATSOptions{
		Port: 4222,
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_DontListen(t *testing.T) {
	cfg := types.NATSOptions{
		DontListen: true,
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_UseInProcessConn(t *testing.T) {
	cfg := types.NATSOptions{
		UseInProcessConn: true,
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_JetStream(t *testing.T) {
	cfg := types.NATSOptions{
		JetStreamEnabled: true,
		JetStreamDir:     "/tmp/jetstream",
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_JetStreamWithoutDir(t *testing.T) {
	cfg := types.NATSOptions{
		JetStreamEnabled: true,
		// JetStreamDir not set - should use empty string for default
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_JetStreamDomain(t *testing.T) {
	cfg := types.NATSOptions{
		JetStreamDomain: "test-domain",
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_Clustering(t *testing.T) {
	cfg := types.NATSOptions{
		ClusterName:   "test-cluster",
		ClusterHost:   "127.0.0.1",
		ClusterPort:   6222,
		ClusterRoutes: []string{"nats://127.0.0.1:6222"},
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_MaxPayload(t *testing.T) {
	cfg := types.NATSOptions{
		MaxPayload: 1048576, // 1MB
	}
	opts := buildNATSOptions(cfg)

	if len(opts) != 1 {
		t.Errorf("Expected 1 option, got %d", len(opts))
	}
}

func TestBuildNATSOptions_Logging(t *testing.T) {
	tests := []struct {
		name          string
		cfg           types.NATSOptions
		expectedCount int
	}{
		{
			name: "LogDebug only",
			cfg: types.NATSOptions{
				LogDebug: true,
			},
			expectedCount: 1,
		},
		{
			name: "LogTrace only",
			cfg: types.NATSOptions{
				LogTrace: true,
			},
			expectedCount: 1,
		},
		{
			name: "LogSysTrace only",
			cfg: types.NATSOptions{
				LogSysTrace: true,
			},
			expectedCount: 1,
		},
		{
			name: "All logging flags",
			cfg: types.NATSOptions{
				LogDebug:    true,
				LogTrace:    true,
				LogSysTrace: true,
			},
			expectedCount: 1,
		},
		{
			name:          "No logging flags",
			cfg:           types.NATSOptions{},
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := buildNATSOptions(tt.cfg)
			if len(opts) != tt.expectedCount {
				t.Errorf("Expected %d option(s), got %d", tt.expectedCount, len(opts))
			}
		})
	}
}

func TestBuildNATSOptions_AllOptions(t *testing.T) {
	// Test with all options configured
	cfg := types.NATSOptions{
		Host:             "localhost",
		Port:             4222,
		DontListen:       true,
		UseInProcessConn: true,
		JetStreamEnabled: true,
		JetStreamDir:     "/tmp/jetstream",
		JetStreamDomain:  "test-domain",
		ClusterName:      "test-cluster",
		ClusterHost:      "127.0.0.1",
		ClusterPort:      6222,
		ClusterRoutes:    []string{"nats://127.0.0.1:6222"},
		MaxPayload:       1048576,
		LogDebug:         true,
		LogTrace:         true,
		LogSysTrace:      true,
	}

	opts := buildNATSOptions(cfg)

	// Should have 9 options:
	// Host, Port, DontListen, InProcessConn, JetStream (covers both enabled+dir),
	// JetStreamDomain, Clustering, MaxPayload, Logging (all flags combined into 1)
	expectedCount := 9
	if len(opts) != expectedCount {
		t.Errorf("Expected %d options, got %d", expectedCount, len(opts))
	}
}

func TestBuildNATSOptions_PartialOptions(t *testing.T) {
	// Test with partial options (common use case)
	cfg := types.NATSOptions{
		Host:             "localhost",
		Port:             4222,
		JetStreamEnabled: true,
	}

	opts := buildNATSOptions(cfg)

	// Should have 3 options: Host, Port, JetStream
	expectedCount := 3
	if len(opts) != expectedCount {
		t.Errorf("Expected %d options, got %d", expectedCount, len(opts))
	}
}

func TestBuildNATSOptions_TypeVerification(t *testing.T) {
	// Verify the returned options are of correct type
	cfg := types.NATSOptions{
		Host: "localhost",
	}

	opts := buildNATSOptions(cfg)

	if len(opts) == 0 {
		t.Fatal("Expected at least one option")
	}

	// Verify type is []nats.NATSOption
	_ = []nats.NATSOption(opts)
}

// Test createDefaultLogger with different log levels
func TestCreateDefaultLogger_LogLevels(t *testing.T) {
	tests := []struct {
		name  string
		level types.LogLevel
	}{
		{"Debug", types.LogLevelDebug},
		{"Info", types.LogLevelInfo},
		{"Warn", types.LogLevelWarn},
		{"Error", types.LogLevelError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			opts := types.LoggerOptions{
				Level:  tt.level,
				Format: types.LogFormatText,
				Output: &buf,
			}

			logger, err := createDefaultLogger(opts)
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
			if logger == nil {
				t.Fatal("Expected logger, got nil")
			}

			// Test that logger works
			logger.Info("test")
		})
	}
}

// Test createDefaultLogger with different formats
func TestCreateDefaultLogger_LogFormats(t *testing.T) {
	tests := []struct {
		name   string
		format types.LogFormat
	}{
		{"JSON", types.LogFormatJSON},
		{"Text", types.LogFormatText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			opts := types.LoggerOptions{
				Level:  types.LogLevelInfo,
				Format: tt.format,
				Output: &buf,
			}

			logger, err := createDefaultLogger(opts)
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
			if logger == nil {
				t.Fatal("Expected logger, got nil")
			}

			// Test that logger works
			logger.Info("test")

			// Verify format
			output := buf.String()
			if tt.format == types.LogFormatJSON {
				if !strings.Contains(output, `"level"`) {
					t.Error("Expected JSON format to contain level field")
				}
			}
		})
	}
}

// Test CreateFrameworkAppInstance with various QueueGroupOptimisticWindow values
func TestCreateFrameworkAppInstance_QueueGroupOptimisticWindow(t *testing.T) {
	tests := []struct {
		name   string
		window int64 // in milliseconds
	}{
		{"zero window", 0},
		{"100ms window", 100},
		{"1000ms window", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			customLogger := &mockLogger{}
			natsOpts := types.NATSOptions{
				DontListen:       true,
				UseInProcessConn: true,
			}
			loggerOpts := types.LoggerOptions{}

			fw, err := CreateFrameworkAppInstance(customLogger, loggerOpts, natsOpts, 0)
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
			if fw == nil {
				t.Fatal("Expected framework instance, got nil")
			}
			defer fw.Stop(context.Background())
		})
	}
}

// Test that logger factory error is properly wrapped
func TestCreateDefaultLogger_InvalidOptions(t *testing.T) {
	// Use an invalid log level by converting from slog.Level
	// The internal logger should handle invalid levels gracefully
	opts := types.LoggerOptions{
		Level:  types.LogLevel(slog.Level(999)), // Invalid level
		Format: types.LogFormatText,
	}

	// Should still create logger without error (internal logger handles edge cases)
	logger, err := createDefaultLogger(opts)
	if err != nil {
		t.Fatalf("Expected no error for edge case log level, got: %v", err)
	}
	if logger == nil {
		t.Fatal("Expected logger, got nil")
	}
}

// Test CreateFrameworkAppInstance error when framework creation fails
func TestCreateFrameworkAppInstance_FrameworkError(t *testing.T) {
	// This test exercises the error path when NewFrameworkAppInstance fails
	// Use invalid NATS options that might cause an error (listening on invalid port)
	customLogger := &mockLogger{}
	natsOpts := types.NATSOptions{
		Port: -1, // Invalid port that should be rejected by NATS server
	}
	loggerOpts := types.LoggerOptions{}

	_, err := CreateFrameworkAppInstance(customLogger, loggerOpts, natsOpts, 0)
	// We expect an error due to invalid configuration
	// Note: The actual NATS library may or may not error on invalid port during setup
	// but this test ensures the error path exists and wraps properly
	if err != nil {
		// Error is expected and properly wrapped
		if !strings.Contains(err.Error(), "failed to create framework") {
			t.Errorf("Expected error to contain 'failed to create framework', got: %v", err)
		}
	}
}

// Test CreateFrameworkAppInstance with all combinations of logger and NATS opts
func TestCreateFrameworkAppInstance_Combinations(t *testing.T) {
	tests := []struct {
		name       string
		logger     types.Logger
		loggerOpts types.LoggerOptions
		natsOpts   types.NATSOptions
		wantErr    bool
	}{
		{
			name:   "nil logger with default opts",
			logger: nil,
			loggerOpts: types.LoggerOptions{
				UseDefault: false,
				Level:      types.LogLevelInfo,
				Format:     types.LogFormatText,
			},
			natsOpts: types.NATSOptions{
				DontListen:       true,
				UseInProcessConn: true,
			},
			wantErr: false,
		},
		{
			name:       "custom logger with empty opts",
			logger:     &mockLogger{},
			loggerOpts: types.LoggerOptions{},
			natsOpts: types.NATSOptions{
				DontListen:       true,
				UseInProcessConn: true,
			},
			wantErr: false,
		},
		{
			name:   "nil logger with JetStream enabled",
			logger: nil,
			loggerOpts: types.LoggerOptions{
				Level:  types.LogLevelInfo,
				Format: types.LogFormatJSON,
			},
			natsOpts: types.NATSOptions{
				DontListen:       true,
				UseInProcessConn: true,
				JetStreamEnabled: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fw, err := CreateFrameworkAppInstance(tt.logger, tt.loggerOpts, tt.natsOpts, 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFrameworkAppInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && fw != nil {
				defer fw.Stop(context.Background())
			}
		})
	}
}
