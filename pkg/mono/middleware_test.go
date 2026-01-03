package mono_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/internal/app"
	"github.com/go-monolith/mono/middleware/audit"
)

// testModule implements a simple module for testing middleware hooks
type testModule struct {
	name         string
	dependencies []string
	eventBus     mono.EventBus
	container    mono.ServiceContainer
}

func (m *testModule) Name() string {
	return m.name
}

func (m *testModule) Dependencies() []string {
	return m.dependencies
}

// SetDependencyServiceContainer implements mono.DependentModule interface.
func (m *testModule) SetDependencyServiceContainer(_ string, _ mono.ServiceContainer) {
	// No-op for tests
}

func (m *testModule) Start(_ context.Context) error {
	return nil
}

func (m *testModule) Stop(_ context.Context) error {
	return nil
}

func (m *testModule) SetEventBus(eventBus mono.EventBus) {
	m.eventBus = eventBus
}

func (m *testModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container

	// Register a request-reply service to trigger OnServiceRegistration
	err := container.RegisterRequestReplyService(
		"test",
		func(ctx context.Context, msg *mono.Msg) ([]byte, error) {
			return []byte("test response"), nil
		},
	)
	return err
}

// TestMiddleware_AuditModuleLifecycleEvents tests that audit middleware captures module lifecycle events
func TestMiddleware_AuditModuleLifecycleEvents(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware (hash chaining disabled by default)
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware first (so it observes everything)
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module
	testMod := &testModule{name: "test-module"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Stop framework to capture stop events
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop framework: %v", err)
	}

	// Parse audit log
	logLines := strings.Split(strings.TrimSpace(auditLog.String()), "\n")
	if len(logLines) == 0 {
		t.Fatal("No audit log entries found")
	}

	// Verify we have lifecycle events
	// Note: Module registration events occur before middleware chain is built,
	// so we only check for started/stopped events
	var foundStarted, foundStopped bool
	for _, line := range logLines {
		if line == "" {
			continue
		}

		var entry audit.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Logf("Failed to unmarshal log line: %s", line)
			continue
		}

		// Check for test-module events
		if entry.ModuleName == "test-module" {
			switch entry.EventType {
			case audit.EventModuleStarted:
				foundStarted = true
				t.Logf("Found module started event: %+v", entry)
			case audit.EventModuleStopped:
				foundStopped = true
				t.Logf("Found module stopped event: %+v", entry)
			}
		}
	}

	// Verify lifecycle events were captured
	// Note: Module registered events are not captured because they occur
	// before the middleware chain is built during framework.Start()
	if !foundStarted {
		t.Error("Module started event not found in audit log")
	}
	if !foundStopped {
		t.Error("Module stopped event not found in audit log")
	}
}

// TestMiddleware_AuditServiceRegistration tests that audit middleware captures service registration events
func TestMiddleware_AuditServiceRegistration(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware (hash chaining disabled by default)
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module (which registers a service)
	testMod := &testModule{name: "service-test"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Parse audit log
	logLines := strings.Split(strings.TrimSpace(auditLog.String()), "\n")

	// Look for service registration event
	var foundServiceReg bool
	for _, line := range logLines {
		if line == "" {
			continue
		}

		var entry audit.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		if entry.EventType == audit.EventServiceRegistered {
			if entry.ServiceName == "test" {
				foundServiceReg = true
				t.Logf("Found service registration: %+v", entry)

				// Verify service type is in details
				if serviceType, ok := entry.Details["service_type"].(string); ok {
					if serviceType != "request_reply" {
						t.Errorf("Expected service_type 'request_reply', got '%s'", serviceType)
					}
				} else {
					t.Error("Service type not found in details")
				}
				break
			}
		}
	}

	if !foundServiceReg {
		t.Error("Service registration event not found in audit log")
		t.Logf("Audit log:\n%s", auditLog.String())
	}
}

// TestMiddleware_EventPassThrough tests that audit middleware doesn't modify events (observer pattern)
func TestMiddleware_EventPassThrough(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware (hash chaining disabled by default)
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module
	testMod := &testModule{name: "passthrough-test"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify module started successfully (middleware didn't block it)
	// Check that the module's EventBus was set
	if testMod.eventBus == nil {
		t.Error("Module's EventBus was not set - middleware may have blocked initialization")
	}

	// Check that the module's ServiceContainer was set
	if testMod.container == nil {
		t.Error("Module's ServiceContainer was not set - middleware may have blocked service registration")
	}

	// Verify audit log captured events but didn't prevent them
	if auditLog.Len() == 0 {
		t.Error("Audit log is empty - middleware may not be observing events")
	}
}

// TestMiddleware_HashChaining tests that hash chaining works correctly
func TestMiddleware_HashChaining(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware with hash chaining enabled
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
		audit.WithHashChaining(""), // Enable hash chaining with new chain
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module
	testMod := &testModule{name: "hash-test"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Parse audit log and collect entries
	logLines := strings.Split(strings.TrimSpace(auditLog.String()), "\n")
	var entries []audit.Entry

	for _, line := range logLines {
		if line == "" {
			continue
		}

		var entry audit.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	// Verify hash chain
	if len(entries) < 2 {
		t.Fatalf("Expected at least 2 entries for hash chain verification, got %d", len(entries))
	}

	// First entry should have empty prev_hash
	if entries[0].PrevHash != "" {
		t.Errorf("First entry should have empty prev_hash, got '%s'", entries[0].PrevHash)
	}

	// First entry should have non-empty entry_hash
	if entries[0].EntryHash == "" {
		t.Error("First entry should have non-empty entry_hash")
	}

	// Subsequent entries should have prev_hash matching previous entry_hash
	for i := 1; i < len(entries); i++ {
		if entries[i].PrevHash != entries[i-1].EntryHash {
			t.Errorf("Entry %d: prev_hash '%s' doesn't match previous entry_hash '%s'",
				i, entries[i].PrevHash, entries[i-1].EntryHash)
		}
		if entries[i].EntryHash == "" {
			t.Errorf("Entry %d: entry_hash is empty", i)
		}
	}

	t.Logf("Hash chain verified: %d entries", len(entries))
}

// TestMiddleware_MultipleModules tests middleware with multiple modules
func TestMiddleware_MultipleModules(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware (hash chaining disabled by default)
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register multiple test modules
	moduleA := &testModule{name: "module-a"}
	moduleB := &testModule{name: "module-b", dependencies: []string{"module-a"}}
	moduleC := &testModule{name: "module-c", dependencies: []string{"module-b"}}

	if err := fw.Register(moduleA); err != nil {
		t.Fatalf("Failed to register module A: %v", err)
	}
	if err := fw.Register(moduleB); err != nil {
		t.Fatalf("Failed to register module B: %v", err)
	}
	if err := fw.Register(moduleC); err != nil {
		t.Fatalf("Failed to register module C: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Parse audit log
	logLines := strings.Split(strings.TrimSpace(auditLog.String()), "\n")

	// Count events per module
	moduleCounts := make(map[string]int)
	for _, line := range logLines {
		if line == "" {
			continue
		}

		var entry audit.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		if entry.ModuleName != "" {
			moduleCounts[entry.ModuleName]++
		}
	}

	// Verify all modules have events
	expectedModules := []string{"module-a", "module-b", "module-c"}
	for _, moduleName := range expectedModules {
		if count := moduleCounts[moduleName]; count == 0 {
			t.Errorf("Module '%s' has no audit events", moduleName)
		} else {
			t.Logf("Module '%s': %d events", moduleName, count)
		}
	}
}

// TestMiddleware_AuditTimestamps tests that audit entries have valid timestamps
func TestMiddleware_AuditTimestamps(t *testing.T) {
	logger := &mockLogger{}
	auditLog := &bytes.Buffer{}

	// Create audit middleware (hash chaining disabled by default)
	auditModule, err := audit.New(
		audit.WithOutput(auditLog),
	)
	if err != nil {
		t.Fatalf("Failed to create audit module: %v", err)
	}

	// Create framework
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register audit middleware
	if err := fw.Register(auditModule); err != nil {
		t.Fatalf("Failed to register audit module: %v", err)
	}

	// Register test module
	testMod := &testModule{name: "timestamp-test"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Record start time
	startTime := time.Now().UTC()

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Record end time
	endTime := time.Now().UTC()

	// Parse audit log
	logLines := strings.Split(strings.TrimSpace(auditLog.String()), "\n")

	for _, line := range logLines {
		if line == "" {
			continue
		}

		var entry audit.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Logf("Failed to unmarshal: %s", line)
			continue
		}

		// Verify timestamp is within test window
		if entry.Timestamp.Before(startTime.Add(-1*time.Second)) || entry.Timestamp.After(endTime.Add(1*time.Second)) {
			t.Errorf("Entry timestamp %v is outside test window [%v, %v]",
				entry.Timestamp, startTime, endTime)
		}

		// Verify timestamp is in UTC
		if entry.Timestamp.Location() != time.UTC {
			t.Errorf("Entry timestamp should be in UTC, got %v", entry.Timestamp.Location())
		}
	}
}

// TestMiddleware_NoAuditModule tests framework works without audit middleware
func TestMiddleware_NoAuditModule(t *testing.T) {
	logger := &mockLogger{}

	// Create framework without audit middleware
	fw, err := app.NewFrameworkAppInstance(logger, 0)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}
	defer fw.Stop(context.Background())

	// Register test module
	testMod := &testModule{name: "no-audit-test"}
	if err := fw.Register(testMod); err != nil {
		t.Fatalf("Failed to register test module: %v", err)
	}

	// Start framework
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start framework: %v", err)
	}

	// Verify module started successfully without audit
	if testMod.eventBus == nil {
		t.Error("Module's EventBus was not set")
	}
	if testMod.container == nil {
		t.Error("Module's ServiceContainer was not set")
	}
}
