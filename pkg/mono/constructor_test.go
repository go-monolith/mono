package mono_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// TestNewMonoApplication_NoOptions tests creating an app with default options
func TestNewMonoApplication_NoOptions(t *testing.T) {
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("unexpected error creating app with defaults: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_ValidOptions tests creating an app with valid options
func TestNewMonoApplication_ValidOptions(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(5222),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithShutdownTimeout(60*time.Second),
		mono.WithLogLevel(mono.LogLevelDebug),
	)
	if err != nil {
		t.Fatalf("unexpected error creating app: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_InvalidOptions tests that invalid options return errors
func TestNewMonoApplication_InvalidOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []mono.MonoFrameworkOption
		errorMsg string
	}{
		{
			name:     "invalid port too low",
			opts:     []mono.MonoFrameworkOption{mono.WithNATSPort(500)},
			errorMsg: "port must be between 1024 and 65535",
		},
		{
			name:     "invalid port too high",
			opts:     []mono.MonoFrameworkOption{mono.WithNATSPort(99999)},
			errorMsg: "port must be between 1024 and 65535",
		},
		{
			name:     "invalid empty host",
			opts:     []mono.MonoFrameworkOption{mono.WithNATSHost("")},
			errorMsg: "host cannot be empty",
		},
		{
			name:     "invalid shutdown timeout",
			opts:     []mono.MonoFrameworkOption{mono.WithShutdownTimeout(500 * time.Millisecond)},
			errorMsg: "timeout must be at least 1 second",
		},
		{
			name:     "nil logger",
			opts:     []mono.MonoFrameworkOption{mono.WithLogger(nil)},
			errorMsg: "logger cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, err := mono.NewMonoApplication(tt.opts...)

			if err == nil {
				t.Error("expected error, got nil")
				// Clean up if app was created
				if app != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = app.Stop(ctx)
				}
				return
			}

			if !strings.Contains(err.Error(), tt.errorMsg) {
				t.Errorf("error message = %q, want to contain %q", err.Error(), tt.errorMsg)
			}

			if app != nil {
				t.Error("expected nil app when error occurs")
			}
		})
	}
}

// TestNewMonoApplication_WithLogger tests using a custom logger
func TestNewMonoApplication_WithLogger(t *testing.T) {
	customLogger := &mockLogger{}

	app, err := mono.NewMonoApplication(
		mono.WithLogger(customLogger),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_WithNATSOptions tests NATS configuration options
func TestNewMonoApplication_WithNATSOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []mono.MonoFrameworkOption
	}{
		{
			name: "with JetStream storage dir",
			opts: []mono.MonoFrameworkOption{
				mono.WithJetStreamStorageDir("/tmp/test-jetstream"),
			},
		},
		{
			name: "with JetStream domain",
			opts: []mono.MonoFrameworkOption{
				mono.WithJetStreamStorageDir("/tmp/test-jetstream"),
				mono.WithJetStreamDomain("test-domain"),
			},
		},
		{
			name: "with clustering",
			opts: []mono.MonoFrameworkOption{
				mono.WithNATSClustering("test-cluster", "127.0.0.1", 6222, []string{"nats://localhost:6223"}),
			},
		},
		{
			name: "with max payload",
			opts: []mono.MonoFrameworkOption{
				mono.WithNATSMaxPayload(2 * 1024 * 1024), // 2 MB
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app, err := mono.NewMonoApplication(tt.opts...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if app == nil {
				t.Fatal("expected non-nil app")
			}

			// Clean up
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := app.Stop(ctx); err != nil {
				t.Errorf("failed to stop app: %v", err)
			}
		})
	}
}

// TestNewMonoApplication_OptionsAppliedInOrder tests "last wins" semantics
func TestNewMonoApplication_OptionsAppliedInOrder(t *testing.T) {
	// Apply same option multiple times - last one should win
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(4222),
		mono.WithNATSPort(5222),
		mono.WithNATSPort(6222), // This should be the final value
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// We can't directly verify the port was set to 6222 since it's internal,
	// but we can verify the app was created successfully
	// which means all options were applied

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_StopsOnFirstError tests that option processing stops on first error
func TestNewMonoApplication_StopsOnFirstError(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(4222),                  // Valid
		mono.WithNATSPort(99999),                 // Invalid - should error
		mono.WithShutdownTimeout(60*time.Second), // Should not be applied
	)

	if err == nil {
		t.Error("expected error from invalid port option")
		// Clean up if app was somehow created
		if app != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = app.Stop(ctx)
		}
		return
	}

	if app != nil {
		t.Error("expected nil app when error occurs")
	}

	if !strings.Contains(err.Error(), "port must be between") {
		t.Errorf("error message = %q, want to contain port validation error", err.Error())
	}
}

// TestNewMonoApplication_InterfaceHidesImplementation tests that only interface is exposed
func TestNewMonoApplication_InterfaceHidesImplementation(t *testing.T) {
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify we can call interface methods
	modules := app.Modules()
	if modules == nil {
		t.Error("Modules() should return non-nil slice")
	}

	health := app.Health(context.Background())
	if health.Timestamp.IsZero() {
		t.Error("Health() should return valid health status")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_DefaultLoggerCreated tests that default logger is created when not provided
func TestNewMonoApplication_DefaultLoggerCreated(t *testing.T) {
	// Don't provide a logger - should create default
	app, err := mono.NewMonoApplication(
		mono.WithLogLevel(mono.LogLevelDebug),
		mono.WithLogFormat(mono.LogFormatJSON),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// App should have been created with default logger
	// We can't directly verify this, but if creation succeeded, logger was created

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_DefaultAuditLoggerCreated tests that default audit logger is created
func TestNewMonoApplication_DefaultAuditLoggerCreated(t *testing.T) {
	// Don't provide an audit logger - should create default with io.Discard
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// App should have been created with default audit logger (io.Discard)
	// We can't directly verify this, but if creation succeeded, audit logger was created

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}

// TestNewMonoApplication_AllOptionsComposition tests applying many options together
func TestNewMonoApplication_AllOptionsComposition(t *testing.T) {
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(5222),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithShutdownTimeout(45*time.Second),
		mono.WithLogLevel(mono.LogLevelDebug),
		mono.WithLogFormat(mono.LogFormatJSON),
		mono.WithLogSource(true),
		mono.WithJetStreamStorageDir("/tmp/test-jetstream"),
		mono.WithJetStreamDomain("test"),
		mono.WithNATSMaxPayload(2*1024*1024),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("expected non-nil app")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Stop(ctx); err != nil {
		t.Errorf("failed to stop app: %v", err)
	}
}
