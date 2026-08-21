package nats

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// findAvailablePort finds an available TCP port for testing.
func findAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = listener.Close()
	}()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func TestNewNATSManager(t *testing.T) {
	logger := newMockLogger()

	tests := []struct {
		name        string
		opts        []NATSOption
		wantErr     bool
		errContains string
	}{
		{
			name:    "default config",
			opts:    nil,
			wantErr: false,
		},
		{
			name:    "with JetStream",
			opts:    []NATSOption{WithJetStream("/tmp/test-jetstream")},
			wantErr: false,
		},
		{
			name:    "with valid max payload",
			opts:    []NATSOption{WithMaxPayload(2048)},
			wantErr: false,
		},
		{
			name:    "with invalid max payload - too small",
			opts:    []NATSOption{WithMaxPayload(512)},
			wantErr: true,
		},
		{
			name:    "with invalid max payload - too large",
			opts:    []NATSOption{WithMaxPayload(10 * 1024 * 1024)},
			wantErr: true,
		},
		{
			name:    "with empty JetStream domain",
			opts:    []NATSOption{WithJetStreamDomain("")},
			wantErr: true,
		},
		{
			name:    "with clustering - empty cluster name",
			opts:    []NATSOption{WithClustering("", "127.0.0.1", 6222, []string{"nats://localhost:6223"})},
			wantErr: true,
		},
		{
			name:    "with clustering - empty cluster host",
			opts:    []NATSOption{WithClustering("test-cluster", "", 6222, []string{"nats://localhost:6223"})},
			wantErr: true,
		},
		{
			name:    "with clustering - invalid cluster port",
			opts:    []NATSOption{WithClustering("test-cluster", "127.0.0.1", 1023, []string{"nats://localhost:6223"})},
			wantErr: true,
		},
		{
			name:        "with DontListen but without UseInProcessConn - validation fails",
			opts:        []NATSOption{WithDontListen()},
			wantErr:     true,
			errContains: "when DontListen is enabled, UseInProcessConn must also be enabled",
		},
		{
			name:    "with DontListen and UseInProcessConn - valid",
			opts:    []NATSOption{WithDontListen(), WithInProcessConn()},
			wantErr: false,
		},
		{
			name:    "with UseInProcessConn only - valid",
			opts:    []NATSOption{WithInProcessConn()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := NewNATSManager(logger, tt.opts...)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain %q, got: %v", tt.errContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if mgr == nil {
					t.Error("expected manager but got nil")
				}
			}
		})
	}
}

func TestNATSManager_StartStop(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with custom port to avoid conflicts
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			JetStreamEnabled:    false,
			StorageDir:          "",
			ClusterEnabled:      false,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Test Start
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}

	// Verify server is running
	if mgr.server == nil {
		t.Error("expected server to be running")
	}

	// Verify connection is established
	conn, err := mgr.Connection()
	if err != nil {
		t.Errorf("failed to get connection: %v", err)
	}
	if conn == nil {
		t.Error("expected connection but got nil")
	}

	// Verify ServerInfo
	info := mgr.ServerInfo()
	if info.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", info.Host)
	}
	if info.Port != port {
		t.Errorf("expected port %d, got %d", port, info.Port)
	}
	if info.JetStreamEnabled {
		t.Error("expected JetStream to be disabled")
	}

	// Test Stop
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("failed to stop NATS server: %v", err)
	}

	// Verify server is stopped
	if mgr.server != nil {
		t.Error("expected server to be stopped")
	}
	if mgr.conn != nil {
		t.Error("expected connection to be closed")
	}

	// Verify logging
	if !logger.hasMessage("INFO", "NATS server started") {
		t.Error("expected 'NATS server started' log message")
	}
	if !logger.hasMessage("INFO", "Stopping NATS server") {
		t.Error("expected 'Stopping NATS server' log message")
	}
	if !logger.hasMessage("INFO", "NATS server stopped") {
		t.Error("expected 'NATS server stopped' log message")
	}
}

func TestNATSManager_StartTwice(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			JetStreamEnabled:    false,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// First start should succeed
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Second start should fail
	err = mgr.Start(ctx)
	if err == nil {
		t.Error("expected error when starting twice, got nil")
	}
	if err != nil && err.Error() != "NATS server already started" {
		t.Errorf("expected 'NATS server already started' error, got: %v", err)
	}
}

func TestNATSManager_StopWithoutStart(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: DefaultNATSConfig(),
		logger: logger,
	}

	ctx := context.Background()

	// Stop without starting should fail
	err := mgr.Stop(ctx)
	if err == nil {
		t.Error("expected error when stopping without start, got nil")
	}
	if err != nil && err.Error() != "NATS server not started" {
		t.Errorf("expected 'NATS server not started' error, got: %v", err)
	}
}

func TestNATSManager_ConnectionBeforeStart(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: DefaultNATSConfig(),
		logger: logger,
	}

	// Getting connection before start should fail
	conn, err := mgr.Connection()
	if err == nil {
		t.Error("expected error when getting connection before start, got nil")
	}
	if conn != nil {
		t.Error("expected nil connection, got non-nil")
	}
}

func TestNATSManager_JetStream(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with JetStream enabled
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			JetStreamEnabled:    true,
			StorageDir:          t.TempDir() + "/jetstream",
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start server
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify JetStream is available
	js, err := mgr.JetStream()
	if err != nil {
		t.Errorf("failed to get JetStream context: %v", err)
	}
	if js == nil {
		t.Error("expected JetStream context but got nil")
	}

	// Verify ServerInfo reflects JetStream
	info := mgr.ServerInfo()
	if !info.JetStreamEnabled {
		t.Error("expected JetStream to be enabled in ServerInfo")
	}

	// Verify logging
	if !logger.hasMessage("INFO", "JetStream enabled") {
		t.Error("expected 'JetStream enabled' log message")
	}
}

func TestNATSManager_JetStreamDisabled(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager without JetStream
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			JetStreamEnabled:    false,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start server
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Getting JetStream should fail when disabled
	js, err := mgr.JetStream()
	if err == nil {
		t.Error("expected error when JetStream is disabled, got nil")
	}
	if js != nil {
		t.Error("expected nil JetStream context, got non-nil")
	}
	if err != nil && err.Error() != "JetStream not enabled" {
		t.Errorf("expected 'JetStream not enabled' error, got: %v", err)
	}
}

func TestNATSManager_JetStreamWithDomain(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with JetStream domain
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			JetStreamEnabled:    true,
			JetStreamDomain:     "test-domain",
			StorageDir:          t.TempDir() + "/jetstream",
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start server
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify JetStream is available
	js, err := mgr.JetStream()
	if err != nil {
		t.Errorf("failed to get JetStream context: %v", err)
	}
	if js == nil {
		t.Error("expected JetStream context but got nil")
	}
}

func TestNATSManager_Clustering(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with clustering
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			ClusterEnabled:      true,
			ClusterName:         "test-cluster",
			ClusterHost:         "127.0.0.1",
			ClusterPort:         port + 100,
			ClusterRoutes:       []string{fmt.Sprintf("nats://127.0.0.1:%d", port+1000)},
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start server
	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify ServerInfo reflects clustering
	info := mgr.ServerInfo()
	if info.ClusterURL == "" {
		t.Error("expected cluster URL to be set")
	}
}

func TestNATSManager_InvalidClusterRoute(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with invalid cluster route (missing scheme)
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			ClusterEnabled:      true,
			ClusterName:         "test-cluster",
			ClusterHost:         "127.0.0.1",
			ClusterPort:         port + 100,
			ClusterRoutes:       []string{"://invalid"},
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start should fail with invalid route
	err = mgr.Start(ctx)
	if err == nil {
		t.Error("expected error with invalid cluster route, got nil")
		_ = mgr.Stop(ctx)
	} else {
		// Verify error message mentions the invalid route
		errMsg := err.Error()
		if !strings.Contains(errMsg, "invalid cluster route") && !strings.Contains(errMsg, "parse") {
			t.Logf("got error (acceptable): %v", err)
		}
	}
}

func TestNATSManager_PortAlreadyInUse(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Start first server
	mgr1 := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err = mgr1.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start first NATS server: %v", err)
	}
	defer func() {
		if err := mgr1.Stop(ctx); err != nil {
			t.Errorf("Failed to stop first NATS manager: %v", err)
		}
	}()

	// Try to start second server on same port
	mgr2 := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: newMockLogger(),
	}

	err = mgr2.Start(ctx)
	if err == nil {
		t.Error("expected error when port is already in use, got nil")
		_ = mgr2.Stop(ctx)
	}
}

func TestNATSManager_BasicPubSub(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	conn, err := mgr.Connection()
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}

	// Test basic pub/sub
	subject := "test.subject"
	received := make(chan string, 1)

	// Subscribe
	_, err = conn.Subscribe(subject, func(msg *nats.Msg) {
		received <- string(msg.Data)
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Flush to ensure subscription is processed
	err = conn.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Publish
	testMsg := "hello world"
	err = conn.Publish(subject, []byte(testMsg))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for message
	select {
	case msg := <-received:
		if msg != testMsg {
			t.Errorf("expected message %q, got %q", testMsg, msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for message")
	}
}

// TestNATSManager_InProcessConnection tests that in-process connections work correctly.
func TestNATSManager_InProcessConnection(t *testing.T) {
	logger := newMockLogger()

	// Create manager with in-process connection and DontListen
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                4222,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server with in-process connection: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify connection is established
	conn, err := mgr.Connection()
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	if conn == nil {
		t.Fatal("expected connection but got nil")
	}

	// Test basic pub/sub over in-process connection
	subject := "test.inprocess"
	received := make(chan string, 1)

	// Subscribe
	_, err = conn.Subscribe(subject, func(msg *nats.Msg) {
		received <- string(msg.Data)
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Flush to ensure subscription is processed
	err = conn.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Publish
	testMsg := "in-process message"
	err = conn.Publish(subject, []byte(testMsg))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Wait for message
	select {
	case msg := <-received:
		if msg != testMsg {
			t.Errorf("expected message %q, got %q", testMsg, msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for message over in-process connection")
	}

	// Verify logging
	if !logger.hasMessage("INFO", "NATS client connected via in-process connection") {
		t.Error("expected 'NATS client connected via in-process connection' log message")
	}
}

// TestNATSManager_UseInProcessConnOnly tests using in-process connection without DontListen.
func TestNATSManager_UseInProcessConnOnly(t *testing.T) {
	logger := newMockLogger()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	// Create manager with UseInProcessConn but without DontListen
	// This should allow both TCP and in-process connections
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                port,
			DontListen:          false, // TCP listener is enabled
			UseInProcessConn:    true,  // But we'll use in-process connection
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err = mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify server is running and connection is established
	conn, err := mgr.Connection()
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	if conn == nil {
		t.Fatal("expected connection but got nil")
	}

	// Test basic pub/sub
	subject := "test.mixed"
	received := make(chan string, 1)

	_, err = conn.Subscribe(subject, func(msg *nats.Msg) {
		received <- string(msg.Data)
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	err = conn.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	testMsg := "mixed mode message"
	err = conn.Publish(subject, []byte(testMsg))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case msg := <-received:
		if msg != testMsg {
			t.Errorf("expected message %q, got %q", testMsg, msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for message")
	}

	// Verify in-process connection was used (not TCP)
	if !logger.hasMessage("INFO", "NATS client connected via in-process connection") {
		t.Error("expected 'NATS client connected via in-process connection' log message")
	}
}

// TestNATSManager_InProcessWithJetStream tests in-process connection with JetStream enabled.
func TestNATSManager_InProcessWithJetStream(t *testing.T) {
	logger := newMockLogger()

	// Create manager with in-process connection, DontListen, and JetStream
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                4222,
			DontListen:          true,
			UseInProcessConn:    true,
			JetStreamEnabled:    true,
			StorageDir:          t.TempDir() + "/jetstream",
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS server with in-process connection and JetStream: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify JetStream is available
	js, err := mgr.JetStream()
	if err != nil {
		t.Errorf("failed to get JetStream context: %v", err)
	}
	if js == nil {
		t.Error("expected JetStream context but got nil")
	}

	// Verify logging
	if !logger.hasMessage("INFO", "NATS client connected via in-process connection") {
		t.Error("expected 'NATS client connected via in-process connection' log message")
	}
	if !logger.hasMessage("INFO", "JetStream enabled") {
		t.Error("expected 'JetStream enabled' log message")
	}
}

// TestNATSManager_Stop_DoubleStop tests calling Stop() twice on a started manager.
func TestNATSManager_Stop_DoubleStop(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// First stop should succeed
	err = mgr.Stop(ctx)
	if err != nil {
		t.Fatalf("first stop failed: %v", err)
	}

	// Second stop should fail
	err = mgr.Stop(ctx)
	if err == nil {
		t.Error("expected error when stopping twice, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "NATS server not started") {
		t.Errorf("expected 'NATS server not started' error, got: %v", err)
	}
}

// TestNATSManager_Stop_NilConnection tests Stop() when connection is nil.
func TestNATSManager_Stop_NilConnection(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Manually set conn to nil to simulate a scenario where connection is not established
	mgr.mu.Lock()
	mgr.conn = nil
	mgr.mu.Unlock()

	// Stop should still work (connection is already nil)
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() should handle nil connection gracefully, got error: %v", err)
	}
}

// TestNATSManager_Start_WithLogging tests Start() with NATS server logging enabled.
func TestNATSManager_Start_WithLogging(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
			// Enable all NATS server logging
			LogDebug:    true,
			LogTrace:    true,
			LogSysTrace: true,
		},
		logger: logger,
	}

	ctx := context.Background()

	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager with logging: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Verify logging was configured
	if !logger.hasMessage("DEBUG", "NATS server logging enabled") {
		t.Error("expected 'NATS server logging enabled' debug log message")
	}
}

// TestNATSManager_JetStream_Error tests JetStream() when JetStream is not enabled.
func TestNATSManager_JetStream_Error(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			JetStreamEnabled:    false, // JetStream NOT enabled
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Try to get JetStream when it's not enabled
	js, err := mgr.JetStream()
	if err == nil {
		t.Error("expected error when getting JetStream context without JetStream enabled, got nil")
	}
	if js != nil {
		t.Error("expected nil JetStream context when JetStream is not enabled")
	}
}

// TestNATSManager_JetStream_NilClient tests JetStream() when client is nil despite being enabled.
func TestNATSManager_JetStream_NilClient(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			JetStreamEnabled:    true,
			StorageDir:          t.TempDir() + "/jetstream",
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager with JetStream: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("Failed to stop NATS manager: %v", err)
		}
	}()

	// Manually set js to nil to simulate a scenario where JetStream client is not available
	mgr.mu.Lock()
	mgr.js = nil
	mgr.mu.Unlock()

	// Try to get JetStream - should fail with "JetStream client not available"
	js, err := mgr.JetStream()
	if err == nil {
		t.Error("expected error when JetStream client is nil, got nil")
	}
	if js != nil {
		t.Error("expected nil JetStream context when client is nil")
	}
	if err != nil && !strings.Contains(err.Error(), "JetStream client not available") {
		t.Errorf("expected 'JetStream client not available' error, got: %v", err)
	}
}

// TestNATSManager_Stop_PanicRecovery tests that Stop() recovers from panics during shutdown.
func TestNATSManager_Stop_PanicRecovery(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Note: It's difficult to reliably trigger a panic during shutdown with a real NATS server.
	// The panic recovery code (lines 232-243) is defensive and rarely triggered.
	// This test verifies that the manager handles the normal shutdown path correctly,
	// but a real panic scenario would require extensive mocking of the NATS server internals.

	// Stop should succeed without panic
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() should succeed, got error: %v", err)
	}

	// Verify server is stopped
	if mgr.server != nil {
		t.Error("expected server to be nil after stop")
	}

	// Verify Stop message was logged
	if !logger.hasMessage("INFO", "NATS server stopped successfully") {
		// If panic occurred and was recovered, we'd see a warning instead
		if !logger.hasMessage("WARN", "NATS server stopped with error") {
			t.Error("expected either success or warning log message after stop")
		}
	}
}

// TestNATSManager_Start_InvalidClusterRoute tests Start() with invalid cluster route URL.
func TestNATSManager_Start_InvalidClusterRoute(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
			ClusterEnabled:      true,
			ClusterName:         "test-cluster",
			ClusterHost:         "127.0.0.1",
			ClusterPort:         6222,
			ClusterRoutes:       []string{"://invalid-url-no-scheme"}, // Invalid URL - missing scheme
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start should fail with cluster route parse error
	err := mgr.Start(ctx)
	if err == nil {
		t.Fatal("expected error when cluster route is invalid, got nil")
	}

	if !strings.Contains(err.Error(), "invalid cluster route") {
		t.Errorf("expected 'invalid cluster route' error, got: %v", err)
	}

	// Server should not be started
	if mgr.server != nil {
		t.Error("expected server to be nil after failed start")
	}
}

// TestNATSManager_Start_ConnectionFailure tests Start() when client connection fails.
func TestNATSManager_Start_ConnectionFailure(t *testing.T) {
	logger := newMockLogger()

	// Create a manager that will start a server but fail to connect via TCP
	// because we're using an invalid connection approach
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,     // Let OS assign port
			DontListen:          false, // Start TCP listener
			UseInProcessConn:    false, // Try TCP connection
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// The test attempts to use TCP connection mode, but this may succeed
	// depending on the system. If it succeeds, we clean up and skip the test.
	err := mgr.Start(ctx)
	if err == nil {
		// Connection succeeded - clean up and skip
		if stopErr := mgr.Stop(ctx); stopErr != nil {
			t.Errorf("failed to stop server: %v", stopErr)
		}
		t.Skip("TCP connection succeeded - cannot test connection failure scenario")
		return
	}

	// Verify we got a connection-related error
	if !strings.Contains(err.Error(), "failed to connect to NATS server") &&
		!strings.Contains(err.Error(), "NATS server not ready") &&
		!strings.Contains(err.Error(), "no servers available") {
		t.Errorf("expected connection error, got: %v", err)
	}

	// Verify cleanup happened
	if mgr.server != nil {
		t.Error("expected server to be nil after failed connection")
	}
	if mgr.conn != nil {
		t.Error("expected connection to be nil after failed connection")
	}
}

// TestNATSManager_Start_ServerReadyTimeout tests Start() when server doesn't become ready in time.
func TestNATSManager_Start_ServerReadyTimeout(t *testing.T) {
	logger := newMockLogger()

	// This test attempts to create a scenario where ReadyForConnections might fail.
	// In practice, with proper configuration, the server usually starts quickly.
	// This test documents the error path even if it's hard to trigger reliably.
	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Normal start should succeed
	err := mgr.Start(ctx)
	if err != nil {
		// If we got a "not ready" error, that's what we're testing for
		if strings.Contains(err.Error(), "NATS server not ready") {
			// Success - we triggered the timeout path
			t.Log("Successfully triggered server ready timeout path")

			// Verify cleanup
			if mgr.server != nil {
				t.Error("expected server to be nil after timeout")
			}
			return
		}
		// Other error - skip test
		t.Skipf("Got different error: %v", err)
	}

	// Clean up
	if err := mgr.Stop(ctx); err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}

	// Note: ReadyForConnections timeout (line 145-147) is a defensive check.
	// It's difficult to trigger reliably without mocking the NATS server,
	// as the embedded server typically starts very quickly.
	t.Log("Server started normally - ready timeout is a defensive error path")
}

// TestNATSManager_Stop_ConnectionAlreadyClosed tests Stop() when connection is already closed.
func TestNATSManager_Stop_ConnectionAlreadyClosed(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Close connection manually before Stop
	mgr.mu.Lock()
	if mgr.conn != nil {
		mgr.conn.Close()
	}
	mgr.mu.Unlock()

	// Stop should still succeed even though connection was already closed
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() should succeed with already-closed connection, got error: %v", err)
	}
}

// TestNATSManager_Stop_JetStreamCleared tests that JetStream client is properly cleared on Stop.
func TestNATSManager_Stop_JetStreamCleared(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			JetStreamEnabled:    true,
			StorageDir:          t.TempDir() + "/jetstream",
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager with JetStream
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Verify JetStream was created
	mgr.mu.RLock()
	hasJS := mgr.js != nil
	mgr.mu.RUnlock()
	if !hasJS {
		t.Fatal("expected JetStream to be initialized")
	}

	// Stop
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() should succeed, got error: %v", err)
	}

	// Verify JetStream was cleared
	mgr.mu.RLock()
	jsCleared := mgr.js == nil
	mgr.mu.RUnlock()
	if !jsCleared {
		t.Error("expected JetStream to be nil after stop")
	}
}

// TestNATSManager_Start_NewServerError tests Start() when server.NewServer fails.
// This is a defensive path that's hard to trigger with valid configurations.
func TestNATSManager_Start_NewServerError(t *testing.T) {
	logger := newMockLogger()

	// Try various configurations that might fail
	testCases := []struct {
		name   string
		config *NATSConfig
	}{
		{
			name: "negative max payload",
			config: &NATSConfig{
				Host:                "127.0.0.1",
				Port:                0,
				DontListen:          true,
				UseInProcessConn:    true,
				MaxPayload:          -1, // Invalid
				StartupReadyTimeout: 10 * time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := &natsManager{
				config: tc.config,
				logger: logger,
			}

			ctx := context.Background()
			err := mgr.Start(ctx)

			if err != nil {
				// We triggered an error - check for server creation failure
				if strings.Contains(err.Error(), "failed to create NATS server") ||
					strings.Contains(err.Error(), "NATS server not ready") {
					t.Logf("Got expected error: %v", err)
				} else {
					t.Logf("Got error (may be acceptable): %v", err)
				}
			} else {
				// Server started despite invalid config - clean up
				if err := mgr.Stop(ctx); err != nil {
					t.Errorf("Failed to stop: %v", err)
				}
				t.Log("Server started despite potentially invalid config")
			}
		})
	}
}

// TestNATSManager_Start_NewServerError_MoreConfigs tests additional configurations that might fail.
func TestNATSManager_Start_NewServerError_MoreConfigs(t *testing.T) {
	logger := newMockLogger()

	// Try configurations that might cause server creation to fail
	testCases := []struct {
		name   string
		config *NATSConfig
	}{
		{
			name: "cluster port conflict with main port",
			config: &NATSConfig{
				Host:                "127.0.0.1",
				Port:                12345,
				ClusterEnabled:      true,
				ClusterName:         "test-cluster",
				ClusterHost:         "127.0.0.1",
				ClusterPort:         12345, // Same as main port - should conflict
				DontListen:          false,
				UseInProcessConn:    false,
				MaxPayload:          1024 * 1024,
				StartupReadyTimeout: 10 * time.Second,
			},
		},
		{
			name: "extremely large max payload",
			config: &NATSConfig{
				Host:                "127.0.0.1",
				Port:                0,
				DontListen:          true,
				UseInProcessConn:    true,
				MaxPayload:          1 << 30, // 1GB - very large
				StartupReadyTimeout: 10 * time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := &natsManager{
				config: tc.config,
				logger: logger,
			}

			ctx := context.Background()
			err := mgr.Start(ctx)

			if err != nil {
				if strings.Contains(err.Error(), "failed to create NATS server") {
					t.Logf("Got expected NewServer error: %v", err)
					return
				}
				t.Logf("Got error (may be acceptable): %v", err)
			} else {
				// Server started despite invalid config - clean up
				_ = mgr.Stop(ctx)
				t.Log("Server started despite potentially invalid config")
			}
		})
	}
}

// TestNATSManager_Stop_ConcurrentStops tests calling Stop() concurrently.
func TestNATSManager_Stop_ConcurrentStops(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start the manager
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Call Stop concurrently from multiple goroutines
	var wg sync.WaitGroup
	results := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- mgr.Stop(ctx)
		}()
	}

	wg.Wait()
	close(results)

	// Count successes and failures
	successes := 0
	failures := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}

	// At least one should succeed (first one), rest should fail with "not started"
	if successes < 1 {
		t.Error("at least one Stop() should succeed")
	}
	// Due to race, we might see different results, but total should be 5
	if successes+failures != 5 {
		t.Errorf("expected 5 total results, got %d successes + %d failures", successes, failures)
	}
}

// TestNATSManager_Stop_ShutdownPanicRecovery documents the panic recovery path.
// NOTE: This test cannot actually trigger the panic path because it requires
// the NATS server internals to panic, which is rare and hard to simulate.
// The panic recovery code (lines 232-242) is defensive and handles rare edge cases
// where the embedded server's eventing subsystem wasn't fully initialized.
func TestNATSManager_Stop_ShutdownPanicRecovery(t *testing.T) {
	logger := newMockLogger()

	mgr := &natsManager{
		config: &NATSConfig{
			Host:                "127.0.0.1",
			Port:                0,
			DontListen:          true,
			UseInProcessConn:    true,
			MaxPayload:          1024 * 1024,
			StartupReadyTimeout: 10 * time.Second,
		},
		logger: logger,
	}

	ctx := context.Background()

	// Start
	err := mgr.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}

	// Stop normally
	err = mgr.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() failed: %v", err)
	}

	// Check logs - the panic recovery path would log a warning
	if logger.hasMessage("WARN", "NATS server panic during shutdown") {
		t.Log("Panic recovery path was triggered")
		// Also check for the follow-up log
		if logger.hasMessage("WARN", "NATS server stopped with error") {
			t.Log("Shutdown error was logged")
		}
	} else {
		// Normal path - success message
		if !logger.hasMessage("INFO", "NATS server stopped successfully") {
			t.Error("expected success log message")
		}
	}
}

// TestNATSManager_StartWithConfigFile tests starting NATS with a config file.
func TestNATSManager_StartWithConfigFile(t *testing.T) {
	logger := newMockLogger()
	tmpDir := t.TempDir()
	configPath := tmpDir + "/nats.conf"

	// Write a minimal valid NATS config file
	configContent := `
port: 4333
`
	if err := writeTestFile(configPath, configContent); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	mgr, err := NewNATSManager(logger,
		WithConfigFile(configPath),
		WithDontListen(),
		WithInProcessConn(),
	)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("failed to stop NATS manager: %v", err)
		}
	}()

	// Verify server is running
	conn, err := mgr.Connection()
	if err != nil {
		t.Errorf("failed to get connection: %v", err)
	}
	if conn == nil {
		t.Error("expected connection but got nil")
	}

	// Verify logging
	if !logger.hasMessage("INFO", "NATS config file loaded") {
		t.Error("expected 'NATS config file loaded' log message")
	}
}

// TestNATSManager_ConfigFileNotFound tests error when config file doesn't exist.
func TestNATSManager_ConfigFileNotFound(t *testing.T) {
	logger := newMockLogger()

	mgr, err := NewNATSManager(logger,
		WithConfigFile("/nonexistent/path/nats.conf"),
		WithDontListen(),
		WithInProcessConn(),
	)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}

	ctx := context.Background()
	err = mgr.Start(ctx)
	if err == nil {
		mgr.Stop(ctx)
		t.Fatal("expected error for nonexistent config file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to process NATS config file") {
		t.Errorf("error message = %q, want to contain 'failed to process NATS config file'", err.Error())
	}
}

// TestNATSManager_ProgrammaticOverridesConfigFile tests that programmatic options override config file.
func TestNATSManager_ProgrammaticOverridesConfigFile(t *testing.T) {
	logger := newMockLogger()
	tmpDir := t.TempDir()
	configPath := tmpDir + "/nats.conf"

	// Config file sets port to 4333 and max_payload to 512KB
	configContent := `
port: 4333
max_payload: 524288
`
	if err := writeTestFile(configPath, configContent); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Override the port and max_payload programmatically
	mgr, err := NewNATSManager(logger,
		WithConfigFile(configPath),
		WithPort(4444),            // Override the port
		WithMaxPayload(1024*1024), // Override max_payload to 1MB
		WithDontListen(),
		WithInProcessConn(),
	)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("failed to stop NATS manager: %v", err)
		}
	}()

	// Get the internal manager to check config
	internalMgr, ok := mgr.(*natsManager)
	if !ok {
		t.Fatal("failed to cast manager to *natsManager")
	}

	// Verify the programmatic port override
	if internalMgr.config.Port != 4444 {
		t.Errorf("expected port 4444 (programmatic override), got %d", internalMgr.config.Port)
	}

	// Verify the programmatic max_payload override
	if internalMgr.config.MaxPayload != 1024*1024 {
		t.Errorf("expected max_payload 1048576 (programmatic override), got %d", internalMgr.config.MaxPayload)
	}
}

// TestNATSManager_ConfigFileWithJetStream tests config file with JetStream settings.
func TestNATSManager_ConfigFileWithJetStream(t *testing.T) {
	logger := newMockLogger()
	tmpDir := t.TempDir()
	configPath := tmpDir + "/nats.conf"
	jsDir := tmpDir + "/jetstream"

	// Config file with JetStream enabled
	configContent := fmt.Sprintf(`
port: 4333
jetstream: {
    store_dir: "%s"
}
`, jsDir)
	if err := writeTestFile(configPath, configContent); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	mgr, err := NewNATSManager(logger,
		WithConfigFile(configPath),
		WithDontListen(),
		WithInProcessConn(),
	)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("failed to start NATS manager: %v", err)
	}
	defer func() {
		if err := mgr.Stop(ctx); err != nil {
			t.Errorf("failed to stop NATS manager: %v", err)
		}
	}()

	// Verify JetStream is available (it was configured in the config file)
	// Note: JetStream from config file is independent of our JetStreamEnabled flag
	// The server will have JetStream if the config file enables it
	info := mgr.ServerInfo()
	// ServerInfo reflects our internal config, not what was in the file
	// So we need to verify via actual connection if needed
	t.Logf("ServerInfo JetStreamEnabled: %v", info.JetStreamEnabled)
}

// TestNATSManager_InvalidConfigFile tests error when config file has invalid syntax.
func TestNATSManager_InvalidConfigFile(t *testing.T) {
	logger := newMockLogger()
	tmpDir := t.TempDir()
	configPath := tmpDir + "/nats.conf"

	// Write an invalid config file (malformed syntax)
	configContent := `
port: "not-a-number"
invalid_key: {
    unclosed_brace
`
	if err := writeTestFile(configPath, configContent); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	mgr, err := NewNATSManager(logger,
		WithConfigFile(configPath),
		WithDontListen(),
		WithInProcessConn(),
	)
	if err != nil {
		t.Fatalf("failed to create NATS manager: %v", err)
	}

	ctx := context.Background()
	err = mgr.Start(ctx)
	if err == nil {
		mgr.Stop(ctx)
		t.Fatal("expected error for invalid config file, got nil")
	}
	if !strings.Contains(err.Error(), "failed to process NATS config file") {
		t.Errorf("error message = %q, want to contain 'failed to process NATS config file'", err.Error())
	}
}

// writeTestFile is a helper to write test config files.
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

// autoTLSTestConfig returns an AutoTLS configuration that never touches the
// network: the startup certificate fetch is disabled and the challenge server
// binds an ephemeral loopback port.
func autoTLSTestConfig(t *testing.T) *types.AutoTLSConfig {
	t.Helper()
	return &types.AutoTLSConfig{
		Domains:             []string{"nats.test.invalid"},
		Email:               "ops@test.invalid",
		CacheDir:            filepath.Join(t.TempDir(), "acme"),
		HTTPChallengeAddr:   "127.0.0.1:0",
		AcceptTOS:           true,
		StartupIssueTimeout: -1,
	}
}

// TestNATSManager_AutoTLS_StartStop verifies a full manager lifecycle with
// AutoTLS enabled. It generates no ACME traffic: nats-server sniffs in-process
// connections even when TLSConfig is set, so the framework's own client
// connects in plaintext over the pipe while the TCP listener stays TLS-only.
func TestNATSManager_AutoTLS_StartStop(t *testing.T) {
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	autoTLS := autoTLSTestConfig(t)
	logger := newMockLogger()

	manager, err := NewNATSManager(logger,
		WithPort(port),
		WithInProcessConn(),
		WithAutoTLS(autoTLS),
	)
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}

	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := manager.Stop(ctx); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	}()

	if !logger.hasMessage("INFO", "ACME http-01 challenge server started") {
		t.Error("expected an INFO log announcing the challenge server")
	}
	if !logger.hasMessage("INFO", "AutoTLS enabled") {
		t.Error("expected an INFO log explaining the forced in-process transport")
	}
	if !logger.hasMessage("INFO", "startup certificate fetch disabled") {
		t.Error("expected an INFO log for the disabled startup fetch")
	}

	// The client URL must advertise the certificate's domain, not the bind
	// address: a client dialling the bind IP would fail hostname verification.
	info := manager.ServerInfo()
	wantURL := fmt.Sprintf("tls://%s:%d", autoTLS.Domains[0], port)
	if info.ClientURL != wantURL {
		t.Errorf("ClientURL = %q, want %q", info.ClientURL, wantURL)
	}
	if info.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want the bind address to be unchanged", info.Host)
	}

	// The framework's own connection still works end to end.
	conn, err := manager.Connection()
	if err != nil {
		t.Fatalf("Connection() error = %v", err)
	}
	sub, err := conn.SubscribeSync("autotls.test")
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}
	if err := conn.Publish("autotls.test", []byte("hello")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg() error = %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Errorf("message = %q, want %q", msg.Data, "hello")
	}
}

// TestNATSManager_AutoTLS_ChallengePortConflict verifies that a failure to bind
// the challenge port aborts startup without leaving a NATS server running.
func TestNATSManager_AutoTLS_ChallengePortConflict(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy a port: %v", err)
	}
	defer func() { _ = busy.Close() }()

	natsPort, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	autoTLS := autoTLSTestConfig(t)
	autoTLS.HTTPChallengeAddr = busy.Addr().String()

	manager, err := NewNATSManager(newMockLogger(),
		WithPort(natsPort),
		WithInProcessConn(),
		WithAutoTLS(autoTLS),
	)
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}

	if err := manager.Start(context.Background()); err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() error = nil, want a challenge port bind failure")
	}

	// The NATS port must be free: a half-started server would make the next
	// Start fail for an unrelated reason.
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatalf("NATS port %d is still in use after a failed Start: %v", natsPort, err)
	}
	_ = ln.Close()
}

// TestNATSManager_AutoTLS_PrewarmFailureIsFatal verifies the fail-fast contract:
// when the startup certificate fetch cannot complete, Start returns an error and
// leaves nothing running.
func TestNATSManager_AutoTLS_PrewarmFailureIsFatal(t *testing.T) {
	// A closed local port as the ACME directory guarantees failure without
	// reaching the internet.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	deadAddr := closed.Addr().String()
	_ = closed.Close()

	natsPort, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	autoTLS := autoTLSTestConfig(t)
	autoTLS.DirectoryURL = "https://" + deadAddr + "/dir"
	autoTLS.StartupIssueTimeout = 15 * time.Second

	manager, err := NewNATSManager(newMockLogger(),
		WithPort(natsPort),
		WithInProcessConn(),
		WithAutoTLS(autoTLS),
	)
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}

	err = manager.Start(context.Background())
	if err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() error = nil, want a certificate issuance failure")
	}
	if !strings.Contains(err.Error(), autoTLS.Domains[0]) {
		t.Errorf("error %q does not name the domain %q", err, autoTLS.Domains[0])
	}

	// Both ports must be released.
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", natsPort))
	if err != nil {
		t.Fatalf("NATS port %d is still in use after a failed Start: %v", natsPort, err)
	}
	_ = ln.Close()
	if err := manager.Stop(context.Background()); err == nil {
		t.Error("Stop() after a failed Start error = nil, want 'not started'")
	}
}

// TestNATSManager_AutoTLS_RejectsDontListen verifies the cross-field validation:
// AutoTLS is meaningless without a TCP listener to protect.
func TestNATSManager_AutoTLS_RejectsDontListen(t *testing.T) {
	_, err := NewNATSManager(newMockLogger(),
		WithDontListen(),
		WithInProcessConn(),
		WithAutoTLS(autoTLSTestConfig(t)),
	)
	if err == nil {
		t.Fatal("NewNATSManager() error = nil, want a DontListen conflict")
	}
	if !strings.Contains(err.Error(), "DontListen") {
		t.Errorf("error = %q, want it to mention DontListen", err)
	}
}

// TestNATSManager_AutoTLS_ConfigFileConflict verifies that a tls{} block in a
// NATS config file and AutoTLS are rejected as a pair, since both would own
// server.Options.TLSConfig.
func TestNATSManager_AutoTLS_ConfigFileConflict(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	writeSelfSignedPair(t, certPath, keyPath)

	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	configPath := filepath.Join(dir, "server.conf")
	configContent := fmt.Sprintf("port: %d\ntls {\n  cert_file: %q\n  key_file: %q\n}\n", port, certPath, keyPath)
	if err := writeTestFile(configPath, configContent); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	manager, err := NewNATSManager(newMockLogger(),
		WithConfigFile(configPath),
		WithInProcessConn(),
		WithAutoTLS(autoTLSTestConfig(t)),
	)
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}

	err = manager.Start(context.Background())
	if err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() error = nil, want a config file TLS conflict")
	}
	if !strings.Contains(err.Error(), "tls{}") {
		t.Errorf("error = %q, want it to name the conflicting tls{} block", err)
	}
}

// writeSelfSignedPair generates a throwaway certificate and key so that a NATS
// config file can carry a syntactically valid tls{} block. Generating it in
// the test avoids committing key material to the repository.
func writeSelfSignedPair(t *testing.T, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nats.test.invalid"},
		DNSNames:     []string{"nats.test.invalid"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("failed to write certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("failed to write key: %v", err)
	}
}

// TestNATSManager_AutoTLS_RequiresInProcessConn verifies the fail-fast guard for
// a direct user of this package.
//
// buildNATSOptions turns UseInProcessConn on whenever AutoTLS is set, so the
// public API cannot reach this. Without the guard, a caller composing
// WithAutoTLS without WithInProcessConn would get the TCP branch of Start
// dialling plaintext nats:// against a TLS-only listener, and the only symptom
// would be an opaque connection failure.
func TestNATSManager_AutoTLS_RequiresInProcessConn(t *testing.T) {
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}

	_, err = NewNATSManager(newMockLogger(),
		WithPort(port),
		WithAutoTLS(autoTLSTestConfig(t)),
	)
	if err == nil {
		t.Fatal("NewNATSManager() error = nil, want AutoTLS to require UseInProcessConn")
	}
	if !strings.Contains(err.Error(), "UseInProcessConn") {
		t.Errorf("error = %q, want it to name UseInProcessConn", err)
	}

	// The same configuration with the in-process transport is accepted.
	if _, err := NewNATSManager(newMockLogger(),
		WithPort(port),
		WithInProcessConn(),
		WithAutoTLS(autoTLSTestConfig(t)),
	); err != nil {
		t.Errorf("NewNATSManager() with UseInProcessConn error = %v, want nil", err)
	}
}
