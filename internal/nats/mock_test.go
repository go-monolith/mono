package nats

import (
	"strings"
	"sync"

	"github.com/go-monolith/mono/pkg/types"
)

// mockLogger implements types.Logger interface for testing.
// This is a shared mock used across all test files in the nats package.
type mockLogger struct {
	mu         sync.Mutex
	debugCalls []string
	infoCalls  []string
	warnCalls  []string
	errorCalls []string
	messages   []logMessage
}

type logMessage struct {
	level string
	msg   string
	args  []any
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		debugCalls: make([]string, 0),
		infoCalls:  make([]string, 0),
		warnCalls:  make([]string, 0),
		errorCalls: make([]string, 0),
		messages:   make([]logMessage, 0),
	}
}

func (m *mockLogger) Debug(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.debugCalls = append(m.debugCalls, msg)
	m.messages = append(m.messages, logMessage{level: "DEBUG", msg: msg, args: args})
}

func (m *mockLogger) Info(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoCalls = append(m.infoCalls, msg)
	m.messages = append(m.messages, logMessage{level: "INFO", msg: msg, args: args})
}

func (m *mockLogger) Warn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warnCalls = append(m.warnCalls, msg)
	m.messages = append(m.messages, logMessage{level: "WARN", msg: msg, args: args})
}

func (m *mockLogger) Error(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCalls = append(m.errorCalls, msg)
	m.messages = append(m.messages, logMessage{level: "ERROR", msg: msg, args: args})
}

func (m *mockLogger) With(_ ...any) types.Logger {
	return m
}

func (m *mockLogger) WithModule(_ string) types.Logger {
	return m
}

func (m *mockLogger) WithError(_ error) types.Logger {
	return m
}

func (m *mockLogger) hasMessage(level, msgSubstring string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range m.messages {
		if msg.level == level && strings.Contains(msg.msg, msgSubstring) {
			return true
		}
	}
	return false
}
