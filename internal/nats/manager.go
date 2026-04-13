// Package nats provides NATS server management including embedded server startup,
// client connections, and JetStream configuration.
package nats

import (
	"context"
	"fmt"
	"net/url"
	"runtime/debug"
	"sync"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSManager manages the embedded NATS server.
//
// See docs/spec/monolith-framework/design.md NATS Server Manager Module section.
type NATSManager interface {
	// Start starts the embedded NATS server
	Start(ctx context.Context) error

	// Stop gracefully stops the NATS server
	Stop(ctx context.Context) error

	// Connection returns a client connection to the embedded server
	Connection() (*nats.Conn, error)

	// JetStream returns a JetStream client if enabled
	JetStream() (jetstream.JetStream, error)

	// ServerInfo returns information about the running server
	ServerInfo() ServerInfo
}

// ServerInfo contains information about the running NATS server.
type ServerInfo struct {
	Host             string
	Port             int
	ClientURL        string
	ClusterURL       string
	JetStreamEnabled bool
}

// natsManager implements NATSManager interface.
type natsManager struct {
	config *NATSConfig
	server *server.Server
	conn   *nats.Conn
	js     jetstream.JetStream
	mu     sync.RWMutex
	logger types.Logger
}

// NewNATSManager creates a new NATS manager with the given options.
func NewNATSManager(logger types.Logger, opts ...NATSOption) (NATSManager, error) {
	config := DefaultNATSConfig()
	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, fmt.Errorf("failed to apply NATS option: %w", err)
		}
	}

	// Validate configuration constraints
	if config.DontListen && !config.UseInProcessConn {
		return nil, fmt.Errorf("invalid configuration: when DontListen is enabled, UseInProcessConn must also be enabled")
	}

	return &natsManager{
		config: config,
		logger: logger,
	}, nil
}

// Start starts the embedded NATS server.
func (m *natsManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server != nil {
		return fmt.Errorf("NATS server already started")
	}

	// Create NATS server options - either from config file or defaults
	var opts *server.Options
	if m.config.ConfigFile != "" {
		var err error
		opts, err = server.ProcessConfigFile(m.config.ConfigFile)
		if err != nil {
			return fmt.Errorf("failed to process NATS config file %q: %w", m.config.ConfigFile, err)
		}
		m.logger.Info("NATS config file loaded", "path", m.config.ConfigFile)

		// Sync framework config from the config file values (for fields not programmatically set)
		// This ensures client connection uses correct host/port from config file
		// Only sync if the value was NOT explicitly set via programmatic options
		if !m.config.HostSet && opts.Host != "" {
			m.config.Host = opts.Host
		}
		if !m.config.PortSet && opts.Port != 0 {
			m.config.Port = opts.Port
		}
	} else {
		opts = &server.Options{}
	}

	// Apply host/port configuration
	// When using config file: only apply if explicitly set (programmatic override)
	// When not using config file: always apply from m.config (includes defaults)
	if m.config.ConfigFile != "" {
		// Config file mode: only override if explicitly set
		if m.config.HostSet {
			opts.Host = m.config.Host
		}
		if m.config.PortSet {
			opts.Port = m.config.Port
		}
	} else {
		// No config file: always apply m.config values (defaults or programmatic)
		if m.config.Host != "" {
			opts.Host = m.config.Host
		}
		if m.config.Port > 0 {
			opts.Port = m.config.Port
		}
	}
	// DontListen is only applied if explicitly enabled (default is false)
	if m.config.DontListen {
		opts.DontListen = true
	}
	// MaxPayload: When using config file, only apply if different from default (1MB)
	// to avoid overriding config file settings with defaults
	defaultMaxPayload := int32(1024 * 1024)
	if m.config.ConfigFile != "" {
		if m.config.MaxPayload > 0 && m.config.MaxPayload != defaultMaxPayload {
			opts.MaxPayload = m.config.MaxPayload
		}
	} else {
		if m.config.MaxPayload > 0 {
			opts.MaxPayload = m.config.MaxPayload
		}
	}

	// Always set NoSigs for embedded server to prevent signal handling conflicts
	opts.NoSigs = true

	// Configure JetStream if enabled
	if m.config.JetStreamEnabled {
		opts.JetStream = true
		opts.StoreDir = m.config.StorageDir
		if m.config.JetStreamDomain != "" {
			opts.JetStreamDomain = m.config.JetStreamDomain
		}
	}

	// Configure clustering if enabled
	if m.config.ClusterEnabled {
		opts.Cluster = server.ClusterOpts{
			Name: m.config.ClusterName,
			Host: m.config.ClusterHost,
			Port: m.config.ClusterPort,
		}
		// Parse cluster routes as URLs
		for _, route := range m.config.ClusterRoutes {
			u, err := url.Parse(route)
			if err != nil {
				return fmt.Errorf("invalid cluster route %q: %w", route, err)
			}
			opts.Routes = append(opts.Routes, u)
		}
	}

	// Create and start the server
	ns, err := server.NewServer(opts)
	if err != nil {
		return fmt.Errorf("failed to create NATS server: %w", err)
	}

	// Configure NATS server logger if any logging flag is enabled
	// SetLoggerV2 must be called before Start()
	if m.config.LogDebug || m.config.LogTrace || m.config.LogSysTrace {
		natsLogger := NewNATSLoggerAdapter(m.logger)
		ns.SetLoggerV2(natsLogger, m.config.LogDebug, m.config.LogTrace, m.config.LogSysTrace)
		m.logger.Debug("NATS server logging enabled",
			"debug", m.config.LogDebug,
			"trace", m.config.LogTrace,
			"sysTrace", m.config.LogSysTrace)
	}

	m.server = ns

	// Start the server in a goroutine
	go ns.Start()

	// Wait for server to be ready
	if !ns.ReadyForConnections(m.config.StartupReadyTimeout) {
		ns.Shutdown()
		m.server = nil
		return fmt.Errorf("NATS server not ready after %s timeout", m.config.StartupReadyTimeout)
	}

	logAttrs := []any{
		"host", m.config.Host,
		"port", m.config.Port,
		"jetstream", m.config.JetStreamEnabled,
	}
	if m.config.ClusterEnabled {
		logAttrs = append(logAttrs,
			"cluster_name", m.config.ClusterName,
			"cluster_host", m.config.ClusterHost,
			"cluster_port", m.config.ClusterPort,
			"cluster_routes", m.config.ClusterRoutes,
		)
	}
	m.logger.Info("NATS server started", logAttrs...)

	// Create client connection
	var conn *nats.Conn
	if m.config.UseInProcessConn {
		// Use in-process connection via net.Pipe
		conn, err = nats.Connect("", nats.InProcessServer(ns))
		if err != nil {
			ns.Shutdown()
			m.server = nil
			return fmt.Errorf("failed to create in-process connection to NATS server: %w", err)
		}
		m.logger.Info("NATS client connected via in-process connection")
	} else {
		// Use TCP connection
		clientURL := fmt.Sprintf("nats://%s:%d", m.config.Host, m.config.Port)
		conn, err = nats.Connect(clientURL)
		if err != nil {
			ns.Shutdown()
			m.server = nil
			return fmt.Errorf("failed to connect to NATS server: %w", err)
		}
		m.logger.Info("NATS client connected via TCP", "url", clientURL)
	}

	m.conn = conn

	// Create JetStream client if enabled
	if m.config.JetStreamEnabled {
		js, err := jetstream.New(conn)
		if err != nil {
			conn.Close()
			ns.Shutdown()
			m.server = nil
			m.conn = nil
			return fmt.Errorf("failed to create JetStream client: %w", err)
		}
		m.js = js
		m.logger.Info("JetStream enabled", "storage_dir", m.config.StorageDir)
	}

	return nil
}

// Stop gracefully stops the NATS server.
func (m *natsManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.server == nil {
		return fmt.Errorf("NATS server not started")
	}

	m.logger.Info("Stopping NATS server")

	// Close client connection first to prevent new requests
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}

	// Clear JetStream client
	m.js = nil

	// Shutdown the server with panic recovery
	// The embedded NATS server may panic during shutdown if certain subsystems
	// (like eventing) were not fully initialized
	var shutdownErr error
	serverRef := m.server // Capture server reference before any operations
	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				m.logger.Warn("NATS server panic during shutdown (recovered)",
					"panic", r,
					"stack", string(stack))
				shutdownErr = fmt.Errorf("NATS server shutdown panic: %v", r)
				// Attempt WaitForShutdown even after panic, as server may still be running
				if serverRef != nil {
					serverRef.WaitForShutdown()
				}
			}
		}()
		serverRef.Shutdown()
		serverRef.WaitForShutdown()
	}()

	m.server = nil

	if shutdownErr != nil {
		// Return nil to allow graceful shutdown to continue.
		// This prevents a NATS shutdown panic from blocking the entire
		// application shutdown sequence. The server handle is cleared
		// and resources are released as much as possible.
		m.logger.Warn("NATS server stopped with error", "error", shutdownErr)
		return nil
	}

	m.logger.Info("NATS server stopped successfully")
	return nil
}

// Connection returns a client connection to the embedded server.
func (m *natsManager) Connection() (*nats.Conn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.conn == nil {
		return nil, fmt.Errorf("NATS connection not available")
	}

	return m.conn, nil
}

// JetStream returns a JetStream client if enabled.
func (m *natsManager) JetStream() (jetstream.JetStream, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.config.JetStreamEnabled {
		return nil, fmt.Errorf("JetStream not enabled")
	}

	if m.js == nil {
		return nil, fmt.Errorf("JetStream client not available")
	}

	return m.js, nil
}

// ServerInfo returns information about the running server.
func (m *natsManager) ServerInfo() ServerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info := ServerInfo{
		Host:             m.config.Host,
		Port:             m.config.Port,
		ClientURL:        fmt.Sprintf("nats://%s:%d", m.config.Host, m.config.Port),
		JetStreamEnabled: m.config.JetStreamEnabled,
	}

	if m.config.ClusterEnabled {
		info.ClusterURL = fmt.Sprintf("nats://%s:%d", m.config.ClusterHost, m.config.ClusterPort)
	}

	return info
}
