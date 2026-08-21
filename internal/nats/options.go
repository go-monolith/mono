package nats

import (
	"fmt"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// NATSOption is a functional option for NATS configuration.
type NATSOption func(*NATSConfig) error

// NATSConfig holds NATS server configuration.
type NATSConfig struct {
	Host             string
	Port             int
	DontListen       bool
	UseInProcessConn bool
	JetStreamEnabled bool
	JetStreamDomain  string
	StorageDir       string
	ClusterEnabled   bool
	ClusterName      string
	ClusterHost      string
	ClusterPort      int
	ClusterRoutes    []string
	MaxPayload       int32
	// StartupReadyTimeout is the maximum time to wait for the NATS server to be ready for connections.
	// Defaults to 10 seconds.
	StartupReadyTimeout time.Duration

	// NATS server logging flags (passed to SetLoggerV2)
	LogDebug    bool // Enable debug-level NATS server logging
	LogTrace    bool // Enable trace-level NATS server logging
	LogSysTrace bool // Enable system trace logging (internal NATS operations)

	// ConfigFile is the path to a NATS server configuration file.
	// When specified, the file is processed using server.ProcessConfigFile() during Start().
	// Programmatic options override settings from the config file.
	ConfigFile string

	// Tracking flags to distinguish between default values and explicitly set values.
	// These are used to properly handle config file + programmatic option merging.
	//
	// When using a config file, we need to know if Host/Port were explicitly set
	// via programmatic options (should override config file) or still at defaults
	// (should use config file values). Host/Port specifically need tracking because:
	// 1. They are synced back to m.config for client connection URL construction
	// 2. They have non-zero defaults that would otherwise always override config file
	//
	// Other fields like MaxPayload, DontListen, etc. use default-value detection
	// or are additive (JetStream, Clustering) so they don't need explicit tracking.
	HostSet bool // true if Host was explicitly set via WithHost option
	PortSet bool // true if Port was explicitly set via WithPort option

	// AutoTLS holds the ACME certificate configuration for the client
	// listener. Nil means AutoTLS is disabled; the pointer itself is the
	// "explicitly set" marker, so no companion AutoTLSSet flag is needed.
	AutoTLS *types.AutoTLSConfig
}

// DefaultNATSConfig returns a NATSConfig with sensible defaults.
func DefaultNATSConfig() *NATSConfig {
	return &NATSConfig{
		Host:                "127.0.0.1",
		Port:                4222,
		DontListen:          false,
		UseInProcessConn:    false,
		JetStreamEnabled:    false,
		JetStreamDomain:     "",
		StorageDir:          "./jetstream",
		ClusterEnabled:      false,
		ClusterName:         "",
		ClusterHost:         "",
		ClusterPort:         0,
		ClusterRoutes:       []string{},
		MaxPayload:          1024 * 1024,      // 1 MB default
		StartupReadyTimeout: 10 * time.Second, // 10 seconds default
		LogDebug:            false,            // Disabled by default
		LogTrace:            false,            // Disabled by default
		LogSysTrace:         false,            // Disabled by default
	}
}

// WithHost sets the NATS server host address.
func WithHost(host string) NATSOption {
	return func(cfg *NATSConfig) error {
		if host == "" {
			return fmt.Errorf("host cannot be empty")
		}
		cfg.Host = host
		cfg.HostSet = true
		return nil
	}
}

// WithPort sets the NATS server port.
func WithPort(port int) NATSOption {
	return func(cfg *NATSConfig) error {
		if port < 1024 || port > 65535 {
			return fmt.Errorf("port must be between 1024 and 65535")
		}
		cfg.Port = port
		cfg.PortSet = true
		return nil
	}
}

// WithDontListen prevents the server from listening on TCP.
// Useful for embedded servers that only need in-process connections.
// Note: When enabled, WithInProcessConn must also be used.
func WithDontListen() NATSOption {
	return func(cfg *NATSConfig) error {
		cfg.DontListen = true
		return nil
	}
}

// WithInProcessConn enables in-process client connections instead of TCP.
// This uses net.Pipe() for direct client-server communication without network overhead.
// Can be used independently or required when DontListen is enabled.
func WithInProcessConn() NATSOption {
	return func(cfg *NATSConfig) error {
		cfg.UseInProcessConn = true
		return nil
	}
}

// WithJetStream enables JetStream with the specified storage directory.
func WithJetStream(storageDir string) NATSOption {
	return func(cfg *NATSConfig) error {
		cfg.JetStreamEnabled = true
		if storageDir != "" {
			cfg.StorageDir = storageDir
		}
		return nil
	}
}

// WithJetStreamDomain sets the JetStream domain.
func WithJetStreamDomain(domain string) NATSOption {
	return func(cfg *NATSConfig) error {
		if domain == "" {
			return fmt.Errorf("JetStream domain cannot be empty")
		}
		cfg.JetStreamDomain = domain
		return nil
	}
}

// WithClustering enables clustering with the specified cluster name, host, port, and routes.
// For a seed node, routes can be empty/nil. For non-seed nodes, routes should point to seed nodes.
func WithClustering(clusterName, clusterHost string, clusterPort int, routes []string) NATSOption {
	return func(cfg *NATSConfig) error {
		if clusterName == "" {
			return fmt.Errorf("cluster name cannot be empty")
		}
		if clusterHost == "" {
			return fmt.Errorf("cluster host cannot be empty")
		}
		if clusterPort < 1024 || clusterPort > 65535 {
			return fmt.Errorf("cluster port must be between 1024 and 65535")
		}
		cfg.ClusterEnabled = true
		cfg.ClusterName = clusterName
		cfg.ClusterHost = clusterHost
		cfg.ClusterPort = clusterPort
		// Defensive copy to prevent unintended slice sharing
		if len(routes) > 0 {
			cfg.ClusterRoutes = make([]string, len(routes))
			copy(cfg.ClusterRoutes, routes)
		}
		return nil
	}
}

// WithMaxPayload sets the maximum payload size in bytes.
func WithMaxPayload(bytes int32) NATSOption {
	return func(cfg *NATSConfig) error {
		const minPayload = 1024
		const maxPayload = 8388608
		if bytes < minPayload {
			return fmt.Errorf("max payload must be at least %d bytes (1 KB), got %d", minPayload, bytes)
		}
		if bytes > maxPayload {
			return fmt.Errorf("max payload must be at most %d bytes (8 MB), got %d", maxPayload, bytes)
		}
		cfg.MaxPayload = bytes
		return nil
	}
}

// WithLogging configures NATS server logging through the framework logger.
// When any flag is enabled, NATS server logs are routed to the framework's logger.
//
// Parameters:
//   - debug: enables debug-level logging (verbose operational info)
//   - trace: enables trace-level logging (message tracing)
//   - sysTrace: enables system trace logging (internal NATS operations)
func WithLogging(debug, trace, sysTrace bool) NATSOption {
	return func(cfg *NATSConfig) error {
		cfg.LogDebug = debug
		cfg.LogTrace = trace
		cfg.LogSysTrace = sysTrace
		return nil
	}
}

// WithStartupReadyTimeout sets the maximum time to wait for the NATS server to be ready for connections.
// The timeout must be between 3 seconds and 60 seconds.
func WithStartupReadyTimeout(timeout time.Duration) NATSOption {
	return func(cfg *NATSConfig) error {
		if timeout < 3*time.Second || timeout > 60*time.Second {
			return fmt.Errorf("ready timeout must be between 3s and 60s, got %s", timeout)
		}
		cfg.StartupReadyTimeout = timeout
		return nil
	}
}

// WithConfigFile sets the path to a NATS server configuration file.
// The file is processed using server.ProcessConfigFile() during Start().
//
// Override behavior:
//   - Host/Port: Programmatic options (WithHost/WithPort) override config file values.
//     If not explicitly set, config file values are used for client connection.
//   - MaxPayload: Only applied if different from default (1MB), preserving config file value.
//   - DontListen: Only applied if explicitly enabled (default is false).
//   - JetStream/Clustering: Programmatic options override or merge with config file settings.
//
// The path cannot be empty. File existence and validity are checked during Start().
func WithConfigFile(path string) NATSOption {
	return func(cfg *NATSConfig) error {
		if path == "" {
			return fmt.Errorf("config file path cannot be empty")
		}
		cfg.ConfigFile = path
		return nil
	}
}

// WithAutoTLS enables automatic ACME (Let's Encrypt) certificate management for
// the NATS client listener.
//
// The configuration is validated immediately and stored as a deep copy, so
// later mutation of the caller's Domains slice cannot affect the framework.
func WithAutoTLS(autoTLS *types.AutoTLSConfig) NATSOption {
	return func(cfg *NATSConfig) error {
		if autoTLS == nil {
			return fmt.Errorf("AutoTLS config cannot be nil")
		}
		if err := autoTLS.Validate(); err != nil {
			return fmt.Errorf("invalid AutoTLS configuration: %w", err)
		}
		cfg.AutoTLS = autoTLS.Clone()
		return nil
	}
}
