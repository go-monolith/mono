package nats

import (
	"testing"
)

func TestNATSLoggerAdapter_Noticef(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Noticef("test notice: %s", "hello")

	if !logger.hasMessage("INFO", "test notice: hello") {
		t.Error("expected Info message with 'test notice: hello'")
	}
}

func TestNATSLoggerAdapter_Warnf(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Warnf("test warning: %d", 42)

	if !logger.hasMessage("WARN", "test warning: 42") {
		t.Error("expected Warn message with 'test warning: 42'")
	}
}

func TestNATSLoggerAdapter_Errorf(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Errorf("test error: %v", "failure")

	if !logger.hasMessage("ERROR", "test error: failure") {
		t.Error("expected Error message with 'test error: failure'")
	}
}

func TestNATSLoggerAdapter_Fatalf(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Fatalf("fatal error: %s", "critical")

	if !logger.hasMessage("ERROR", "[FATAL] fatal error: critical") {
		t.Error("expected Error message with '[FATAL] fatal error: critical'")
	}
}

func TestNATSLoggerAdapter_Debugf(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Debugf("debug info: %d", 123)

	if !logger.hasMessage("DEBUG", "debug info: 123") {
		t.Error("expected Debug message with 'debug info: 123'")
	}
}

func TestNATSLoggerAdapter_Tracef(t *testing.T) {
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	adapter.Tracef("trace info: %s", "details")

	if !logger.hasMessage("DEBUG", "[TRACE] trace info: details") {
		t.Error("expected Debug message with '[TRACE] trace info: details'")
	}
}

func TestNATSLoggerAdapter_ImplementsServerLogger(t *testing.T) {
	// This is a compile-time check that natsLoggerAdapter implements server.Logger
	// If it doesn't, the code won't compile
	logger := newMockLogger()
	adapter := NewNATSLoggerAdapter(logger)

	// Verify adapter is not nil
	if adapter == nil {
		t.Error("expected non-nil adapter")
	}
}
