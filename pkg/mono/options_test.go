package mono_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// TestDefaultConfig verifies default configuration values
func TestDefaultConfig(t *testing.T) {
	cfg := mono.DefaultConfig()

	t.Run("NATS default host", func(t *testing.T) {
		if cfg.NATSOptions.Host != "127.0.0.1" {
			t.Errorf("default NATS host = %q, want %q", cfg.NATSOptions.Host, "127.0.0.1")
		}
	})

	t.Run("NATS default port", func(t *testing.T) {
		if cfg.NATSOptions.Port != 4222 {
			t.Errorf("default NATS port = %d, want %d", cfg.NATSOptions.Port, 4222)
		}
	})

	t.Run("default shutdown timeout", func(t *testing.T) {
		expected := 30 * time.Second
		if cfg.ShutdownTimeout != expected {
			t.Errorf("default shutdown timeout = %v, want %v", cfg.ShutdownTimeout, expected)
		}
	})

	t.Run("default logger is nil", func(t *testing.T) {
		if cfg.Logger != nil {
			t.Errorf("default logger should be nil, got %v", cfg.Logger)
		}
	})

	t.Run("default logger options", func(t *testing.T) {
		if cfg.LoggerOptions.Level != mono.LogLevelInfo {
			t.Errorf("default log level = %v, want mono.LogLevelInfo", cfg.LoggerOptions.Level)
		}
		if cfg.LoggerOptions.Format != mono.LogFormatText {
			t.Errorf("default log format = %v, want mono.LogFormatText", cfg.LoggerOptions.Format)
		}
		if cfg.LoggerOptions.Output != nil {
			t.Errorf("default log output should be nil, got %v", cfg.LoggerOptions.Output)
		}
		if cfg.LoggerOptions.AddSource {
			t.Error("default AddSource should be false")
		}
		if cfg.LoggerOptions.UseDefault {
			t.Error("default UseDefault should be false")
		}
	})
}

// TestWithNATSPort tests NATS port configuration
func TestWithNATSPort(t *testing.T) {
	tests := []struct {
		name      string
		port      int
		wantError bool
		errorMsg  string
	}{
		{"valid port 4222", 4222, false, ""},
		{"valid port 1024", 1024, false, ""},
		{"valid port 65535", 65535, false, ""},
		{"invalid port too low", 1023, true, "port must be between 1024 and 65535"},
		{"invalid port too high", 65536, true, "port must be between 1024 and 65535"},
		{"invalid port zero", 0, true, "port must be between 1024 and 65535"},
		{"invalid port negative", -1, true, "port must be between 1024 and 65535"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSPort(tt.port)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.Port != tt.port {
					t.Errorf("port = %d, want %d", cfg.NATSOptions.Port, tt.port)
				}
			}
		})
	}
}

// TestWithNATSHost tests NATS host configuration
func TestWithNATSHost(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		wantError bool
		errorMsg  string
	}{
		{"valid localhost", "127.0.0.1", false, ""},
		{"valid any address", "0.0.0.0", false, ""},
		{"valid hostname", "localhost", false, ""},
		{"invalid empty host", "", true, "host cannot be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSHost(tt.host)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.Host != tt.host {
					t.Errorf("host = %q, want %q", cfg.NATSOptions.Host, tt.host)
				}
			}
		})
	}
}

// TestWithShutdownTimeout tests shutdown timeout configuration
func TestWithShutdownTimeout(t *testing.T) {
	tests := []struct {
		name      string
		timeout   time.Duration
		wantError bool
		errorMsg  string
	}{
		{"valid 1 second", time.Second, false, ""},
		{"valid 30 seconds", 30 * time.Second, false, ""},
		{"valid 5 minutes", 5 * time.Minute, false, ""},
		{"invalid 500ms", 500 * time.Millisecond, true, "timeout must be at least 1 second"},
		{"invalid 0", 0, true, "timeout must be at least 1 second"},
		{"invalid negative", -time.Second, true, "timeout must be at least 1 second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithShutdownTimeout(tt.timeout)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.ShutdownTimeout != tt.timeout {
					t.Errorf("shutdown timeout = %v, want %v", cfg.ShutdownTimeout, tt.timeout)
				}
			}
		})
	}
}

// TestWithLogger tests logger configuration
func TestWithLogger(t *testing.T) {
	t.Run("valid logger", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := mono.DefaultConfig()
		err := mono.WithLogger(logger)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg.Logger != logger {
			t.Error("logger not set correctly")
		}
	})

	t.Run("nil logger", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithLogger(nil)(cfg)

		if err == nil {
			t.Error("expected error for nil logger, got nil")
		}

		if !strings.Contains(err.Error(), "logger cannot be nil") {
			t.Errorf("error message = %q, want to contain %q", err.Error(), "logger cannot be nil")
		}
	})
}

// TestWithNATSLogging tests NATS logging configuration
func TestWithNATSLogging(t *testing.T) {
	t.Run("default NATS logging flags are disabled", func(t *testing.T) {
		cfg := mono.DefaultConfig()

		if cfg.NATSOptions.LogDebug {
			t.Error("LogDebug should be disabled by default")
		}
		if cfg.NATSOptions.LogTrace {
			t.Error("LogTrace should be disabled by default")
		}
		if cfg.NATSOptions.LogSysTrace {
			t.Error("LogSysTrace should be disabled by default")
		}
	})

	t.Run("enable debug logging only", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithNATSLogging(true, false, false)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !cfg.NATSOptions.LogDebug {
			t.Error("LogDebug should be enabled")
		}
		if cfg.NATSOptions.LogTrace {
			t.Error("LogTrace should be disabled")
		}
		if cfg.NATSOptions.LogSysTrace {
			t.Error("LogSysTrace should be disabled")
		}
	})

	t.Run("enable all logging flags", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithNATSLogging(true, true, true)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !cfg.NATSOptions.LogDebug {
			t.Error("LogDebug should be enabled")
		}
		if !cfg.NATSOptions.LogTrace {
			t.Error("LogTrace should be enabled")
		}
		if !cfg.NATSOptions.LogSysTrace {
			t.Error("LogSysTrace should be enabled")
		}
	})

	t.Run("disable all logging flags", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		// Pre-enable all flags
		cfg.NATSOptions.LogDebug = true
		cfg.NATSOptions.LogTrace = true
		cfg.NATSOptions.LogSysTrace = true

		err := mono.WithNATSLogging(false, false, false)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg.NATSOptions.LogDebug {
			t.Error("LogDebug should be disabled")
		}
		if cfg.NATSOptions.LogTrace {
			t.Error("LogTrace should be disabled")
		}
		if cfg.NATSOptions.LogSysTrace {
			t.Error("LogSysTrace should be disabled")
		}
	})

	t.Run("WithLogger and WithNATSLogging composition", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		logger := &mockLogger{}

		err := mono.WithLogger(logger)(cfg)
		if err != nil {
			t.Errorf("WithLogger failed: %v", err)
		}

		err = mono.WithNATSLogging(true, false, false)(cfg)
		if err != nil {
			t.Errorf("WithNATSLogging failed: %v", err)
		}

		if cfg.Logger != logger {
			t.Error("logger not set correctly")
		}

		if !cfg.NATSOptions.LogDebug {
			t.Error("LogDebug should be enabled")
		}
	})
}

// TestOptionComposition tests applying multiple options
func TestOptionComposition(t *testing.T) {
	t.Run("multiple valid options", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		logger := &mockLogger{}

		options := []mono.MonoFrameworkOption{
			mono.WithNATSPort(5222),
			mono.WithNATSHost("0.0.0.0"),
			mono.WithShutdownTimeout(60 * time.Second),
			mono.WithLogger(logger),
		}

		for _, opt := range options {
			if err := opt(cfg); err != nil {
				t.Errorf("unexpected error applying option: %v", err)
			}
		}

		if cfg.NATSOptions.Port != 5222 {
			t.Errorf("port = %d, want 5222", cfg.NATSOptions.Port)
		}
		if cfg.NATSOptions.Host != "0.0.0.0" {
			t.Errorf("host = %q, want '0.0.0.0'", cfg.NATSOptions.Host)
		}
		if cfg.ShutdownTimeout != 60*time.Second {
			t.Errorf("shutdown timeout = %v, want 60s", cfg.ShutdownTimeout)
		}
		if cfg.Logger != logger {
			t.Error("logger not set correctly")
		}
	})

	t.Run("last-wins semantics for same option", func(t *testing.T) {
		cfg := mono.DefaultConfig()

		// Apply same option multiple times
		options := []mono.MonoFrameworkOption{
			mono.WithNATSPort(4222),
			mono.WithNATSPort(5222),
			mono.WithNATSPort(6222), // Last one wins
		}

		for _, opt := range options {
			if err := opt(cfg); err != nil {
				t.Errorf("unexpected error applying option: %v", err)
			}
		}

		if cfg.NATSOptions.Port != 6222 {
			t.Errorf("port = %d, want 6222 (last value)", cfg.NATSOptions.Port)
		}
	})

	t.Run("stop on first error", func(t *testing.T) {
		cfg := mono.DefaultConfig()

		options := []mono.MonoFrameworkOption{
			mono.WithNATSPort(4222),                    // Valid
			mono.WithNATSPort(99999),                   // Invalid - should error
			mono.WithShutdownTimeout(60 * time.Second), // Should not be applied
		}

		var firstError error
		for _, opt := range options {
			if err := opt(cfg); err != nil {
				firstError = err
				break
			}
		}

		if firstError == nil {
			t.Error("expected error from invalid port option")
		}

		// Shutdown timeout should still be default since we stopped on error
		if cfg.ShutdownTimeout != 30*time.Second {
			t.Errorf("shutdown timeout = %v, want default 30s (error should have stopped option processing)", cfg.ShutdownTimeout)
		}
	})
}

// TestConfigurationErrorWrapping tests that options use proper error wrapping
func TestConfigurationErrorWrapping(t *testing.T) {
	t.Run("invalid port error is ConfigurationError", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithNATSPort(99999)(cfg)

		if !monoerrors.IsConfigurationError(err) {
			t.Error("expected ConfigurationError, got different error type")
		}

		confErr, ok := monoerrors.GetConfigurationError(err)
		if !ok {
			t.Fatal("failed to extract ConfigurationError")
		}

		if confErr.OptionName != "WithNATSPort" {
			t.Errorf("option name = %q, want 'WithNATSPort'", confErr.OptionName)
		}

		if confErr.Value != 99999 {
			t.Errorf("option value = %v, want 99999", confErr.Value)
		}
	})

	t.Run("nil logger error is ConfigurationError", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithLogger(nil)(cfg)

		if !monoerrors.IsConfigurationError(err) {
			t.Error("expected ConfigurationError, got different error type")
		}
	})
}

// TestCrossFieldValidation tests validation that depends on multiple fields
func TestCrossFieldValidation(t *testing.T) {
	// This test demonstrates where cross-field validation would go
	// Currently not implemented, but structure shows how it would work
	t.Run("example cross-field validation structure", func(t *testing.T) {
		cfg := mono.DefaultConfig()

		// Apply options
		_ = mono.WithNATSPort(4222)(cfg)
		_ = mono.WithNATSHost("127.0.0.1")(cfg)
		_ = mono.WithShutdownTimeout(30 * time.Second)(cfg)

		// In NewMonoApplication(), cross-field validation would check:
		// - NATS host + port combination is valid
		// - Shutdown timeout is reasonable for the number of modules
		// - Logger is set or default logger will be created

		// For now, just verify individual fields are set correctly
		if cfg.NATSOptions.Host == "" {
			t.Error("NATS host should not be empty")
		}
		if cfg.NATSOptions.Port == 0 {
			t.Error("NATS port should not be zero")
		}
		if cfg.ShutdownTimeout == 0 {
			t.Error("shutdown timeout should not be zero")
		}
	})
}

// TestWithLogLevel tests log level configuration
func TestWithLogLevel(t *testing.T) {
	tests := []struct {
		name      string
		level     mono.LogLevel
		wantError bool
		errorMsg  string
	}{
		{"valid mono.LogLevelDebug", mono.LogLevelDebug, false, ""},
		{"valid mono.LogLevelInfo", mono.LogLevelInfo, false, ""},
		{"valid mono.LogLevelWarn", mono.LogLevelWarn, false, ""},
		{"valid mono.LogLevelError", mono.LogLevelError, false, ""},
		{"invalid level too low", mono.LogLevel(-1), true, "invalid log level"},
		{"invalid level too high", mono.LogLevel(999), true, "invalid log level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithLogLevel(tt.level)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.LoggerOptions.Level != tt.level {
					t.Errorf("log level = %v, want %v", cfg.LoggerOptions.Level, tt.level)
				}
			}
		})
	}
}

// TestWithLogFormat tests log format configuration
func TestWithLogFormat(t *testing.T) {
	tests := []struct {
		name      string
		format    mono.LogFormat
		wantError bool
		errorMsg  string
	}{
		{"valid mono.LogFormatJSON", mono.LogFormatJSON, false, ""},
		{"valid mono.LogFormatText", mono.LogFormatText, false, ""},
		{"invalid format", mono.LogFormat(999), true, "invalid log format"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithLogFormat(tt.format)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.LoggerOptions.Format != tt.format {
					t.Errorf("log format = %v, want %v", cfg.LoggerOptions.Format, tt.format)
				}
			}
		})
	}
}

// TestWithLogOutput tests log output configuration
func TestWithLogOutput(t *testing.T) {
	t.Run("valid output writer", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := mono.DefaultConfig()
		err := mono.WithLogOutput(&buf)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg.LoggerOptions.Output != &buf {
			t.Error("output writer not set correctly")
		}
	})

	t.Run("nil output writer", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithLogOutput(nil)(cfg)

		if err == nil {
			t.Error("expected error for nil output writer, got nil")
		}

		if !strings.Contains(err.Error(), "output writer cannot be nil") {
			t.Errorf("error message = %q, want to contain %q", err.Error(), "output writer cannot be nil")
		}
	})
}

// TestWithLogSource tests log source configuration
func TestWithLogSource(t *testing.T) {
	tests := []struct {
		name   string
		enable bool
	}{
		{"enable source", true},
		{"disable source", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithLogSource(tt.enable)(cfg)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if cfg.LoggerOptions.AddSource != tt.enable {
				t.Errorf("AddSource = %v, want %v", cfg.LoggerOptions.AddSource, tt.enable)
			}
		})
	}
}

// TestWithCustomLogger tests custom logger injection
func TestWithCustomLogger(t *testing.T) {
	t.Run("valid custom logger", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := mono.DefaultConfig()
		err := mono.WithCustomLogger(logger)(cfg)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if cfg.Logger != logger {
			t.Error("logger not set correctly")
		}

		if !cfg.LoggerOptions.UseDefault {
			t.Error("UseDefault should be true when custom logger is set")
		}
	})

	t.Run("nil custom logger", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		err := mono.WithCustomLogger(nil)(cfg)

		if err == nil {
			t.Error("expected error for nil custom logger, got nil")
		}

		if !strings.Contains(err.Error(), "logger cannot be nil") {
			t.Errorf("error message = %q, want to contain %q", err.Error(), "logger cannot be nil")
		}
	})
}

// TestLoggerOptionsComposition tests applying multiple logger options
func TestLoggerOptionsComposition(t *testing.T) {
	t.Run("multiple logger options", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		var buf bytes.Buffer

		options := []mono.MonoFrameworkOption{
			mono.WithLogLevel(mono.LogLevelDebug),
			mono.WithLogFormat(mono.LogFormatJSON),
			mono.WithLogOutput(&buf),
			mono.WithLogSource(true),
		}

		for _, opt := range options {
			if err := opt(cfg); err != nil {
				t.Errorf("unexpected error applying option: %v", err)
			}
		}

		if cfg.LoggerOptions.Level != mono.LogLevelDebug {
			t.Errorf("log level = %v, want mono.LogLevelDebug", cfg.LoggerOptions.Level)
		}
		if cfg.LoggerOptions.Format != mono.LogFormatJSON {
			t.Errorf("log format = %v, want mono.LogFormatJSON", cfg.LoggerOptions.Format)
		}
		if cfg.LoggerOptions.Output != &buf {
			t.Error("output writer not set correctly")
		}
		if !cfg.LoggerOptions.AddSource {
			t.Error("AddSource should be true")
		}
	})
}

// TestWithJetStreamDomain tests JetStream domain configuration
func TestWithJetStreamDomain(t *testing.T) {
	tests := []struct {
		name      string
		domain    string
		wantError bool
		errorMsg  string
	}{
		{"valid domain", "production", false, ""},
		{"valid domain with dash", "prod-env", false, ""},
		{"invalid empty domain", "", true, "JetStream domain cannot be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithJetStreamDomain(tt.domain)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestWithJetStreamStorageDir tests JetStream storage directory configuration
func TestWithJetStreamStorageDir(t *testing.T) {
	tests := []struct {
		name      string
		dir       string
		wantError bool
		errorMsg  string
	}{
		{"valid directory", "./jetstream", false, ""},
		{"valid absolute path", "/var/lib/jetstream", false, ""},
		{"invalid empty directory", "", true, "storage directory cannot be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithJetStreamStorageDir(tt.dir)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestWithNATSClustering tests NATS clustering configuration
func TestWithNATSClustering(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		clusterHost string
		clusterPort int
		routes      []string
		wantError   bool
		errorMsg    string
	}{
		{"valid clustering", "my-cluster", "127.0.0.1", 6222, []string{"nats://node1:6222", "nats://node2:6222"}, false, ""},
		{"valid single route", "cluster1", "127.0.0.1", 6222, []string{"nats://node1:6222"}, false, ""},
		{"valid seed node (no routes)", "my-cluster", "127.0.0.1", 6222, nil, false, ""},
		{"invalid empty cluster name", "", "127.0.0.1", 6222, []string{"nats://node1:6222"}, true, "cluster name cannot be empty"},
		{"invalid empty cluster host", "my-cluster", "", 6222, []string{"nats://node1:6222"}, true, "cluster host cannot be empty"},
		{"invalid cluster port too low", "my-cluster", "127.0.0.1", 1023, []string{"nats://node1:6222"}, true, "cluster port must be between 1024 and 65535"},
		{"invalid cluster port too high", "my-cluster", "127.0.0.1", 65536, []string{"nats://node1:6222"}, true, "cluster port must be between 1024 and 65535"},
		{"invalid empty route in list", "my-cluster", "127.0.0.1", 6222, []string{"nats://node1:6222", ""}, true, "cluster route at index 1 cannot be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSClustering(tt.clusterName, tt.clusterHost, tt.clusterPort, tt.routes)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestWithNATSMaxPayload tests NATS max payload configuration
func TestWithNATSMaxPayload(t *testing.T) {
	tests := []struct {
		name      string
		bytes     int32
		wantError bool
		errorMsg  string
	}{
		{"valid 1KB (minimum)", 1024, false, ""},
		{"valid 1MB", 1024 * 1024, false, ""},
		{"valid 8MB (maximum)", 8388608, false, ""},
		{"invalid too small", 512, true, "max payload must be at least"},
		{"invalid too large", 10 * 1024 * 1024, true, "max payload must be at most"},
		{"invalid zero", 0, true, "max payload must be at least"},
		{"invalid negative", -1024, true, "max payload must be at least"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSMaxPayload(tt.bytes)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestWithNATSConfigFile tests NATS config file configuration
func TestWithNATSConfigFile(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantError bool
		errorMsg  string
	}{
		{"valid absolute path", "/etc/nats/server.conf", false, ""},
		{"valid relative path", "./nats.conf", false, ""},
		{"valid path with spaces", "/path/to/nats config.conf", false, ""},
		{"invalid empty path", "", true, "config file path cannot be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()

			// Verify default is empty
			if cfg.NATSOptions.ConfigFile != "" {
				t.Error("expected ConfigFile to be empty by default")
			}

			err := mono.WithNATSConfigFile(tt.path)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.ConfigFile != tt.path {
					t.Errorf("config file = %q, want %q", cfg.NATSOptions.ConfigFile, tt.path)
				}
			}
		})
	}
}

// TestWithNATSConfigFile_ErrorIsConfigurationError tests error type
func TestWithNATSConfigFile_ErrorIsConfigurationError(t *testing.T) {
	cfg := mono.DefaultConfig()
	err := mono.WithNATSConfigFile("")(cfg)

	if !monoerrors.IsConfigurationError(err) {
		t.Error("expected ConfigurationError, got different error type")
	}

	confErr, ok := monoerrors.GetConfigurationError(err)
	if !ok {
		t.Fatal("failed to extract ConfigurationError")
	}

	if confErr.OptionName != "WithNATSConfigFile" {
		t.Errorf("option name = %q, want 'WithNATSConfigFile'", confErr.OptionName)
	}
}

// TestWithNATSConfigFile_Composition tests config file can be combined with other NATS options
func TestWithNATSConfigFile_Composition(t *testing.T) {
	cfg := mono.DefaultConfig()

	// Apply config file with other NATS options
	// Config file provides base settings, other options override
	options := []mono.MonoFrameworkOption{
		mono.WithNATSConfigFile("/etc/nats/server.conf"),
		mono.WithNATSPort(4333),      // Override port from config file
		mono.WithNATSHost("0.0.0.0"), // Override host from config file
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			t.Fatalf("option composition failed: %v", err)
		}
	}

	// Verify all options were applied
	if cfg.NATSOptions.ConfigFile != "/etc/nats/server.conf" {
		t.Errorf("config file = %q, want '/etc/nats/server.conf'", cfg.NATSOptions.ConfigFile)
	}
	if cfg.NATSOptions.Port != 4333 {
		t.Errorf("port = %d, want 4333", cfg.NATSOptions.Port)
	}
	if cfg.NATSOptions.Host != "0.0.0.0" {
		t.Errorf("host = %q, want '0.0.0.0'", cfg.NATSOptions.Host)
	}
}

// TestWithStartupReadyTimeout tests NATS ready timeout configuration
func TestWithStartupReadyTimeout(t *testing.T) {
	t.Run("default ready timeout", func(t *testing.T) {
		cfg := mono.DefaultConfig()
		expected := 10 * time.Second
		if cfg.NATSOptions.StartupReadyTimeout != expected {
			t.Errorf("default StartupReadyTimeout = %v, want %v", cfg.NATSOptions.StartupReadyTimeout, expected)
		}
	})

	tests := []struct {
		name      string
		timeout   time.Duration
		wantError bool
		errorMsg  string
	}{
		{"valid 3s (minimum)", 3 * time.Second, false, ""},
		{"valid 10s", 10 * time.Second, false, ""},
		{"valid 30s", 30 * time.Second, false, ""},
		{"valid 60s (maximum)", 60 * time.Second, false, ""},
		{"invalid below minimum (2s)", 2 * time.Second, true, "ready timeout must be between 3s and 60s"},
		{"invalid below minimum (500ms)", 500 * time.Millisecond, true, "ready timeout must be between 3s and 60s"},
		{"invalid above maximum (61s)", 61 * time.Second, true, "ready timeout must be between 3s and 60s"},
		{"invalid zero", 0, true, "ready timeout must be between 3s and 60s"},
		{"invalid negative", -1 * time.Second, true, "ready timeout must be between 3s and 60s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithStartupReadyTimeout(tt.timeout)(cfg)

			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.StartupReadyTimeout != tt.timeout {
					t.Errorf("StartupReadyTimeout = %v, want %v", cfg.NATSOptions.StartupReadyTimeout, tt.timeout)
				}
			}
		})
	}
}

// TestWithStartupReadyTimeout_ErrorIsConfigurationError tests error type
func TestWithStartupReadyTimeout_ErrorIsConfigurationError(t *testing.T) {
	cfg := mono.DefaultConfig()
	err := mono.WithStartupReadyTimeout(0)(cfg)

	if !monoerrors.IsConfigurationError(err) {
		t.Error("expected ConfigurationError, got different error type")
	}

	confErr, ok := monoerrors.GetConfigurationError(err)
	if !ok {
		t.Fatal("failed to extract ConfigurationError")
	}

	if confErr.OptionName != "WithStartupReadyTimeout" {
		t.Errorf("option name = %q, want 'WithStartupReadyTimeout'", confErr.OptionName)
	}
}
