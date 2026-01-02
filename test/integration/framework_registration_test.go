//go:build integration
// +build integration

package integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-monolith/mono"
)

// simpleTestModule is a minimal module for testing registration
type simpleTestModule struct {
	name string
}

func (m *simpleTestModule) Name() string {
	return m.name
}

func (m *simpleTestModule) Start(ctx context.Context) error {
	return nil
}

func (m *simpleTestModule) Stop(ctx context.Context) error {
	return nil
}

// simpleTestPlugin is a minimal plugin module for testing registration
type simpleTestPlugin struct {
	name      string
	container mono.ServiceContainer
}

func (p *simpleTestPlugin) Name() string {
	return p.name
}

func (p *simpleTestPlugin) Start(ctx context.Context) error {
	return nil
}

func (p *simpleTestPlugin) Stop(ctx context.Context) error {
	return nil
}

func (p *simpleTestPlugin) SetContainer(container mono.ServiceContainer) {
	p.container = container
}

func (p *simpleTestPlugin) Container() mono.ServiceContainer {
	return p.container
}

func TestRegisterModuleAfterStart(t *testing.T) {
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Start the application
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}
	defer app.Stop(context.Background())

	// Attempt to register a module after Start() - should fail
	module := &simpleTestModule{name: "test-module"}
	err = app.Register(module)

	if err == nil {
		t.Fatal("Expected error when registering module after Start(), got nil")
	}

	// Verify error message contains human-readable state
	errMsg := err.Error()
	if !strings.Contains(errMsg, "cannot register module") {
		t.Errorf("Expected error message to contain 'cannot register module', got: %s", errMsg)
	}

	if !strings.Contains(errMsg, "Running") {
		t.Errorf("Expected error message to contain 'Running' state, got: %s", errMsg)
	}
}

func TestRegisterPluginAfterStart(t *testing.T) {
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Start the application
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}
	defer app.Stop(context.Background())

	// Attempt to register a plugin after Start() - should fail
	plugin := &simpleTestPlugin{name: "test-plugin"}
	err = app.RegisterPlugin(plugin, "test-alias")

	if err == nil {
		t.Fatal("Expected error when registering plugin after Start(), got nil")
	}

	// Verify error message contains human-readable state
	errMsg := err.Error()
	if !strings.Contains(errMsg, "cannot register plugin") {
		t.Errorf("Expected error message to contain 'cannot register plugin', got: %s", errMsg)
	}

	if !strings.Contains(errMsg, "Running") {
		t.Errorf("Expected error message to contain 'Running' state, got: %s", errMsg)
	}
}

func TestLogger(t *testing.T) {
	app, err := mono.NewMonoApplication()
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}
	defer app.Stop(context.Background())

	// Get the logger from the framework
	logger := app.Logger()

	if logger == nil {
		t.Fatal("Expected Logger() to return a non-nil logger")
	}

	// Test that the logger can be used for basic logging
	// This shouldn't panic or error
	logger.Info("Test log message from integration test")
	logger.Debug("Debug message", "key", "value")
	logger.Warn("Warning message")
	logger.Error("Error message")

	// Test With methods
	ctxLogger := logger.With("context", "test")
	if ctxLogger == nil {
		t.Fatal("Expected With() to return a non-nil logger")
	}

	moduleLogger := logger.WithModule("test-module")
	if moduleLogger == nil {
		t.Fatal("Expected WithModule() to return a non-nil logger")
	}

	// Test that modules can use the framework's logger
	ctx := context.Background()
	module := &simpleTestModule{name: "logger-test-module"}
	if err := app.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}

	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}

	// Get logger again after start
	startedLogger := app.Logger()
	if startedLogger == nil {
		t.Fatal("Expected Logger() to return a non-nil logger after Start()")
	}

	// Use the logger with module context
	startedLogger.WithModule("logger-test-module").Info("Module using framework logger")
}
