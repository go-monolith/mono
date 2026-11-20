package nats

import (
	"context"
	"net"
	"strings"
	"testing"
)

// FreeTestPort returns a TCP port that was free a moment ago.
func FreeTestPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to release the port: %v", err)
	}
	return port
}

// StartConfigFileManager starts a manager with a config file of the given
// content and the options, and stops it when the test ends.
func StartConfigFileManager(t *testing.T, logger *mockLogger, content string, opts ...NATSOption) *natsManager {
	t.Helper()
	path := t.TempDir() + "/nats.conf"
	if err := writeTestFile(path, content); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	mgr, err := NewNATSManager(logger, append([]NATSOption{WithConfigFile(path)}, opts...)...)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}
	t.Cleanup(func() {
		if err := mgr.Stop(context.Background()); err != nil {
			t.Errorf("failed to stop NATS manager: %v", err)
		}
	})
	internal, ok := mgr.(*natsManager)
	if !ok {
		t.Fatal("failed to cast manager to *natsManager")
	}
	return internal
}

// ListenerIP returns the address the client listener of the manager is bound to.
func ListenerIP(t *testing.T, m *natsManager) net.IP {
	t.Helper()
	addr, ok := m.server.Addr().(*net.TCPAddr)
	if !ok || addr == nil {
		t.Fatalf("server has no TCP listener: %v", m.server.Addr())
	}
	return addr.IP
}

// StartedLogHost returns the host attribute of the "NATS server started" line.
func StartedLogHost(t *testing.T, logger *mockLogger) string {
	t.Helper()
	logger.mu.Lock()
	defer logger.mu.Unlock()
	for _, m := range logger.messages {
		if m.msg != "NATS server started" {
			continue
		}
		for i := 0; i+1 < len(m.args); i += 2 {
			if m.args[i] == "host" {
				host, _ := m.args[i+1].(string)
				return host
			}
		}
	}
	t.Fatal(`no "NATS server started" line with a host`)
	return ""
}

// AllInterfacesWarnings counts the warnings about a listener on all interfaces.
func AllInterfacesWarnings(logger *mockLogger) int {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	n := 0
	for _, msg := range logger.warnCalls {
		if strings.Contains(msg, "all interfaces") {
			n++
		}
	}
	return n
}

// An explicit WithHost wins over a config file that sets no host, also when
// it is the default (127.0.0.1): the server must not fall back to
// nats-server's default of every interface.
func TestNATSManager_ConfigFile_ExplicitLoopbackHostIsKept(t *testing.T) {
	logger := newMockLogger()
	m := StartConfigFileManager(t, logger, "max_payload: 2MB\n",
		WithHost("127.0.0.1"), WithPort(FreeTestPort(t)))

	if ip := ListenerIP(t, m); !ip.IsLoopback() {
		t.Errorf("listener bound to %s, want loopback", ip)
	}
	if host := StartedLogHost(t, logger); host != "127.0.0.1" {
		t.Errorf("logged host %q, want 127.0.0.1", host)
	}
	if n := AllInterfacesWarnings(logger); n != 0 {
		t.Errorf("%d all-interfaces warnings, want none", n)
	}
}

// A config file without a host, and no WithHost, keeps nats-server's default
// (every interface). The log must say so instead of the framework default.
func TestNATSManager_ConfigFile_NoHostIsReported(t *testing.T) {
	logger := newMockLogger()
	m := StartConfigFileManager(t, logger, "max_payload: 2MB\n", WithPort(FreeTestPort(t)))

	ip := ListenerIP(t, m)
	if !ip.IsUnspecified() {
		t.Fatalf("listener bound to %s, want the unspecified address", ip)
	}
	if host := StartedLogHost(t, logger); host != ip.String() {
		t.Errorf("logged host %q, want the bound address %s", host, ip)
	}
	if n := AllInterfacesWarnings(logger); n != 1 {
		t.Errorf("%d all-interfaces warnings, want 1", n)
	}
	// The framework's own client still connects (through loopback).
	if _, err := m.Connection(); err != nil {
		t.Errorf("framework client not connected: %v", err)
	}
}

// A host in the config file is the operator's choice and is not warned about.
func TestNATSManager_ConfigFile_FileHostIsNotWarnedAbout(t *testing.T) {
	logger := newMockLogger()
	m := StartConfigFileManager(t, logger, "host: 127.0.0.1\n", WithPort(FreeTestPort(t)))

	if ip := ListenerIP(t, m); !ip.IsLoopback() {
		t.Errorf("listener bound to %s, want loopback", ip)
	}
	if n := AllInterfacesWarnings(logger); n != 0 {
		t.Errorf("%d all-interfaces warnings, want none", n)
	}
}

// WithMaxPayload wins over the config file also when it is the default (1MB).
func TestNATSManager_ConfigFile_ExplicitDefaultMaxPayloadIsKept(t *testing.T) {
	const oneMB = 1024 * 1024
	tests := []struct {
		name string
		opts []NATSOption
		want int64
	}{
		{name: "explicit default overrides the file", opts: []NATSOption{WithMaxPayload(oneMB)}, want: oneMB},
		{name: "without the option the file is kept", want: 524288},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]NATSOption{WithHost("127.0.0.1"), WithPort(FreeTestPort(t))}, tt.opts...)
			m := StartConfigFileManager(t, newMockLogger(), "max_payload: 524288\n", opts...)
			conn, err := m.Connection()
			if err != nil {
				t.Fatalf("no connection: %v", err)
			}
			if got := conn.MaxPayload(); got != tt.want {
				t.Errorf("server max payload %d, want %d", got, tt.want)
			}
		})
	}
}

func TestListenAddress(t *testing.T) {
	host, port := ListenAddress(&net.TCPAddr{IP: net.IPv4zero, Port: 4333}, "127.0.0.1", 4222)
	if host != "0.0.0.0" || port != 4333 {
		t.Errorf("got %s:%d, want 0.0.0.0:4333", host, port)
	}
	host, port = ListenAddress(nil, "127.0.0.1", 4222)
	if host != "127.0.0.1" || port != 4222 {
		t.Errorf("without a listener got %s:%d, want the fallback 127.0.0.1:4222", host, port)
	}
}
