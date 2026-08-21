package mono

import (
	"fmt"
	"io"
	"time"

	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// MonoFrameworkOption is a functional option for configuring the framework.
//
// Options follow the functional options pattern for clean, composable configuration.
// Each option validates its parameters and returns an error if validation fails.
//
// Example:
//
//	app, err := mono.NewMonoApplication(
//	    mono.WithNATSPort(4222),
//	    mono.WithShutdownTimeout(30*time.Second),
//	    mono.WithLogger(myLogger),
//	)
//
// See docs/spec/foundation.md for detailed documentation.
type MonoFrameworkOption func(*types.MonoFrameworkConfig) error

// DefaultConfig returns a MonoFrameworkConfig with sensible defaults.
// This is primarily useful for testing.
func DefaultConfig() *types.MonoFrameworkConfig {
	return defaultConfig()
}

// defaultConfig returns a MonoFrameworkConfig with sensible defaults.
func defaultConfig() *types.MonoFrameworkConfig {
	return &types.MonoFrameworkConfig{
		NATSOptions: types.NATSOptions{
			Host:                "127.0.0.1",
			Port:                4222,
			StartupReadyTimeout: 10 * time.Second,
		},
		LoggerOptions: types.LoggerOptions{
			Level:      types.LogLevelInfo,
			Format:     types.LogFormatText,
			Output:     nil, // Will use os.Stdout if not set
			AddSource:  false,
			UseDefault: false,
		},
		Logger:                     nil, // Will use default logger if not set
		ShutdownTimeout:            30 * time.Second,
		QueueGroupOptimisticWindow: 0, // Disabled by default
	}
}

// WithNATSPort sets the NATS server port.
// The port must be between 1024 and 65535.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithNATSPort(4222))
func WithNATSPort(port int) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if port < 1024 || port > 65535 {
			return errors.WrapInvalidConfiguration(0, "WithNATSPort", port,
				"port must be between 1024 and 65535")
		}
		cfg.NATSOptions.Port = port
		return nil
	}
}

// WithNATSHost sets the NATS server host address.
// The host cannot be empty.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithNATSHost("0.0.0.0"))
func WithNATSHost(host string) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if host == "" {
			return errors.WrapInvalidConfiguration(0, "WithNATSHost", host,
				"host cannot be empty")
		}
		cfg.NATSOptions.Host = host
		return nil
	}
}

// WithNATSDontListen prevents the NATS server from listening on TCP.
// Useful for embedded servers that only need in-process connections.
//
// IMPORTANT: When DontListen is enabled, you MUST also enable UseInProcessConn
// via WithNATSInProcessConn(), otherwise the server will start but be unreachable.
//
// Example:
//
//	mono.NewMonoApplication(
//	    mono.WithNATSDontListen(),
//	    mono.WithNATSInProcessConn(), // Required when using DontListen
//	)
func WithNATSDontListen() MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		cfg.NATSOptions.DontListen = true
		return nil
	}
}

// WithNATSInProcessConn enables in-process client connections instead of TCP.
// This uses net.Pipe() for direct client-server communication without network overhead.
// Typically used with WithNATSDontListen() for fully in-process NATS communication.
//
// Example:
//
//	mono.NewMonoApplication(
//	    mono.WithNATSDontListen(),
//	    mono.WithNATSInProcessConn(),
//	)
func WithNATSInProcessConn() MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		cfg.NATSOptions.UseInProcessConn = true
		return nil
	}
}

// WithNATSConfigFile sets the path to a NATS server configuration file.
// The file is processed using NATS server.ProcessConfigFile() during startup.
//
// When both a config file and programmatic options (like WithNATSPort) are specified,
// the config file provides base settings and programmatic options override them.
// This allows using a standard NATS config file while customizing specific settings.
//
// The path cannot be empty. File existence and validity are checked during Start().
//
// Example:
//
//	// Use config file only
//	mono.NewMonoApplication(
//	    mono.WithNATSConfigFile("/etc/nats/server.conf"),
//	)
//
//	// Config file with programmatic overrides
//	mono.NewMonoApplication(
//	    mono.WithNATSConfigFile("/etc/nats/server.conf"),
//	    mono.WithNATSPort(4333), // Overrides port from config file
//	)
func WithNATSConfigFile(path string) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if path == "" {
			return errors.WrapInvalidConfiguration(0, "WithNATSConfigFile", path,
				"config file path cannot be empty")
		}
		cfg.NATSOptions.ConfigFile = path
		return nil
	}
}

// WithNATSAutoTLS enables automatic ACME (Let's Encrypt) TLS certificates for
// the embedded NATS server's client listener.
//
// Scope: client-to-server connections only. Cluster routes, gateways,
// leafnodes, websocket and MQTT listeners are unaffected and stay plaintext
// unless configured separately - see types.AutoTLSConfig for why an ACME
// certificate does not fit the route path.
//
// The framework starts an HTTP listener (":80" by default, see
// AutoTLSConfig.HTTPChallengeAddr) that answers ACME http-01 challenges for the
// whole lifetime of the application - renewals re-run the challenge - and
// installs a lazily-populated tls.Config on the NATS client port. Certificates
// are renewed in the background with no restart or reload.
//
// External clients must connect with tls:// using one of the configured domain
// names, which is what ServerInfo().ClientURL reports. Connecting by IP address
// fails: the certificate is selected from the TLS SNI extension, which nats.go
// derives from the URL host.
//
// Enabling AutoTLS makes TLS mandatory for external clients - plaintext TCP
// connections are rejected - so it is a breaking change for an existing
// deployment with plaintext clients.
//
// It also switches the framework's own internal NATS client to an in-process
// connection: a loopback TCP dial could not satisfy hostname verification
// against a public-domain certificate. This is transparent to modules.
//
// Only the http-01 challenge is supported. See docs/spec/foundation.md.
//
// Example:
//
//	app, err := mono.NewMonoApplication(
//	    mono.WithNATSHost("0.0.0.0"),
//	    mono.WithNATSAutoTLS(types.AutoTLSConfig{
//	        Domains:   []string{"nats.example.com"},
//	        Email:     "ops@example.com",
//	        CacheDir:  "/var/lib/mono/acme",
//	        AcceptTOS: true,
//	    }),
//	)
func WithNATSAutoTLS(autoTLS types.AutoTLSConfig) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if err := autoTLS.Validate(); err != nil {
			return errors.WrapInvalidConfiguration(0, "WithNATSAutoTLS", autoTLS.Domains, err.Error())
		}
		cfg.NATSOptions.AutoTLS = autoTLS.Clone()
		return nil
	}
}

// WithShutdownTimeout sets the graceful shutdown timeout.
// The timeout must be at least 1 second to allow proper cleanup.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithShutdownTimeout(30*time.Second))
func WithShutdownTimeout(timeout time.Duration) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if timeout < time.Second {
			return errors.WrapInvalidConfiguration(0, "WithShutdownTimeout", timeout,
				"timeout must be at least 1 second")
		}
		cfg.ShutdownTimeout = timeout
		return nil
	}
}

// WithLogger sets a custom logger for the framework.
// The logger cannot be nil.
//
// To enable NATS server logging, use WithNATSLogging in addition to this option.
//
// Example:
//
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	mono.NewMonoApplication(mono.WithLogger(logger))
func WithLogger(logger types.Logger) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if logger == nil {
			return errors.WrapInvalidConfiguration(0, "WithLogger", nil,
				"logger cannot be nil")
		}
		cfg.Logger = logger
		return nil
	}
}

// WithNATSLogging configures NATS server logging flags.
// All flags are disabled by default.
// When any flag is enabled, NATS server logs are routed through the framework's
// logging system with a "nats-server" component identifier.
//
// Parameters:
//   - debug: enables debug-level logging (verbose operational info)
//   - trace: enables trace-level logging (message tracing)
//   - sysTrace: enables system trace logging (internal NATS operations)
//
// This option is independent of WithLogger and can be used with either a
// custom logger or the default framework logger.
//
// Example:
//
//	mono.NewMonoApplication(
//	    mono.WithLogger(logger),
//	    mono.WithNATSLogging(true, false, false),  // Enable debug logging only
//	)
func WithNATSLogging(debug, trace, sysTrace bool) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		cfg.NATSOptions.LogDebug = debug
		cfg.NATSOptions.LogTrace = trace
		cfg.NATSOptions.LogSysTrace = sysTrace
		return nil
	}
}

// WithLogLevel sets the log level for the framework logger.
// Valid levels are: LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithLogLevel(mono.LogLevelDebug))
func WithLogLevel(level types.LogLevel) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if level < types.LogLevelDebug || level > types.LogLevelError {
			return errors.WrapInvalidConfiguration(0, "WithLogLevel", level,
				"invalid log level")
		}
		cfg.LoggerOptions.Level = level
		return nil
	}
}

// WithLogFormat sets the log format (JSON or Text).
// Valid formats are: LogFormatJSON, LogFormatText.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithLogFormat(mono.LogFormatJSON))
func WithLogFormat(format types.LogFormat) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if format != types.LogFormatJSON && format != types.LogFormatText {
			return errors.WrapInvalidConfiguration(0, "WithLogFormat", format,
				"invalid log format")
		}
		cfg.LoggerOptions.Format = format
		return nil
	}
}

// WithLogOutput sets the output writer for logs.
// The writer cannot be nil.
//
// Example:
//
//	var buf bytes.Buffer
//	mono.NewMonoApplication(mono.WithLogOutput(&buf))
func WithLogOutput(w io.Writer) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if w == nil {
			return errors.WrapInvalidConfiguration(0, "WithLogOutput", nil,
				"output writer cannot be nil")
		}
		cfg.LoggerOptions.Output = w
		return nil
	}
}

// WithLogSource enables/disables source file and line number in log entries.
// When enabled, logs will include the file:line where the log was called.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithLogSource(true))
func WithLogSource(enable bool) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		cfg.LoggerOptions.AddSource = enable
		return nil
	}
}

// WithCustomLogger allows injection of a custom logger instance.
// This bypasses the logger factory and uses the provided logger directly.
// The logger cannot be nil.
//
// Example:
//
//	customLogger := myLoggerFactory.NewLogger("application")
//	mono.NewMonoApplication(mono.WithCustomLogger(customLogger))
func WithCustomLogger(logger types.Logger) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if logger == nil {
			return errors.WrapInvalidConfiguration(0, "WithCustomLogger", nil,
				"logger cannot be nil")
		}
		cfg.Logger = logger
		cfg.LoggerOptions.UseDefault = true // Mark that custom logger is in use
		return nil
	}
}

// WithJetStreamDomain sets the JetStream domain for the NATS server.
// The domain cannot be empty.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithJetStreamDomain("production"))
func WithJetStreamDomain(domain string) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if domain == "" {
			return errors.WrapInvalidConfiguration(0, "WithJetStreamDomain", domain,
				"JetStream domain cannot be empty")
		}
		cfg.NATSOptions.JetStreamDomain = domain
		return nil
	}
}

// WithJetStreamStorageDir sets the JetStream storage directory.
// The directory path cannot be empty.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithJetStreamStorageDir("./data/jetstream"))
func WithJetStreamStorageDir(dir string) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if dir == "" {
			return errors.WrapInvalidConfiguration(0, "WithJetStreamStorageDir", dir,
				"storage directory cannot be empty")
		}
		cfg.NATSOptions.JetStreamEnabled = true
		cfg.NATSOptions.JetStreamDir = dir
		return nil
	}
}

// WithNATSClustering enables NATS clustering with the specified configuration.
//
// Parameters:
//   - clusterName: Name of the NATS cluster (must not be empty)
//   - clusterHost: Host address for cluster communication (e.g., "127.0.0.1")
//   - clusterPort: Port for cluster communication (e.g., 6222), must be between 1024 and 65535
//   - routes: URLs to other cluster nodes (e.g., "nats://node1:6222")
//
// For a seed node (the first node in a cluster), routes can be empty.
// For non-seed nodes, routes should point to the seed node's cluster URL.
//
// Example (seed node):
//
//	mono.NewMonoApplication(
//	    mono.WithNATSClustering("my-cluster", "127.0.0.1", 6222, nil),
//	)
//
// Example (non-seed node):
//
//	mono.NewMonoApplication(
//	    mono.WithNATSClustering("my-cluster", "127.0.0.1", 6223, []string{"nats://127.0.0.1:6222"}),
//	)
func WithNATSClustering(clusterName, clusterHost string, clusterPort int, routes []string) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if clusterName == "" {
			return errors.WrapInvalidConfiguration(0, "WithNATSClustering", clusterName,
				"cluster name cannot be empty")
		}
		if clusterHost == "" {
			return errors.WrapInvalidConfiguration(0, "WithNATSClustering", clusterHost,
				"cluster host cannot be empty")
		}
		if clusterPort < 1024 || clusterPort > 65535 {
			return errors.WrapInvalidConfiguration(0, "WithNATSClustering", clusterPort,
				"cluster port must be between 1024 and 65535")
		}
		// Validate route format (basic validation)
		for i, route := range routes {
			if route == "" {
				return errors.WrapInvalidConfiguration(0, "WithNATSClustering", route,
					fmt.Sprintf("cluster route at index %d cannot be empty", i))
			}
		}
		cfg.NATSOptions.ClusterName = clusterName
		cfg.NATSOptions.ClusterHost = clusterHost
		cfg.NATSOptions.ClusterPort = clusterPort
		// Defensive copy to prevent unintended slice sharing
		if len(routes) > 0 {
			cfg.NATSOptions.ClusterRoutes = make([]string, len(routes))
			copy(cfg.NATSOptions.ClusterRoutes, routes)
		} else {
			cfg.NATSOptions.ClusterRoutes = nil
		}
		return nil
	}
}

// WithNATSMaxPayload sets the maximum payload size for NATS messages.
// The size must be between 1KB (1024 bytes) and 8MB (8388608 bytes).
//
// Example:
//
//	mono.NewMonoApplication(mono.WithNATSMaxPayload(2 * 1024 * 1024)) // 2 MB
func WithNATSMaxPayload(bytes int32) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		const minPayload = 1024    // 1 KB
		const maxPayload = 8388608 // 8 MB

		if bytes < minPayload {
			return errors.WrapInvalidConfiguration(0, "WithNATSMaxPayload", bytes,
				fmt.Sprintf("max payload must be at least %d bytes (1 KB), got %d", minPayload, bytes))
		}
		if bytes > maxPayload {
			return errors.WrapInvalidConfiguration(0, "WithNATSMaxPayload", bytes,
				fmt.Sprintf("max payload must be at most %d bytes (8 MB), got %d", maxPayload, bytes))
		}
		cfg.NATSOptions.MaxPayload = bytes
		return nil
	}
}

// WithStartupReadyTimeout sets the maximum time to wait for the NATS server to be ready.
// The timeout must be between 3 seconds and 60 seconds. Default is 10 seconds.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithStartupReadyTimeout(15 * time.Second))
func WithStartupReadyTimeout(timeout time.Duration) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if timeout < 3*time.Second || timeout > 60*time.Second {
			return errors.WrapInvalidConfiguration(0, "WithStartupReadyTimeout", timeout,
				fmt.Sprintf("ready timeout must be between 3s and 60s, got %s", timeout))
		}
		cfg.NATSOptions.StartupReadyTimeout = timeout
		return nil
	}
}

// WithQueueGroupOptimisticWindow enables optimistic publish mode for queue group services.
//
// When set to a duration > 0, queue group clients will use request-reply (with ACK) for
// the first send, then switch to fire-and-forget publish for subsequent sends within the
// specified window. This improves performance while maintaining service availability detection.
//
// The window must be at least 100 milliseconds to be meaningful. Common values are 1-5 seconds.
// When set to 0 (default), optimistic mode is disabled and all sends use ACK.
//
// Example:
//
//	mono.NewMonoApplication(mono.WithQueueGroupOptimisticWindow(1 * time.Second))
func WithQueueGroupOptimisticWindow(window time.Duration) MonoFrameworkOption {
	return func(cfg *types.MonoFrameworkConfig) error {
		if window != 0 && window < 100*time.Millisecond {
			return errors.WrapInvalidConfiguration(0, "WithQueueGroupOptimisticWindow", window,
				"window must be 0 (disabled) or at least 100ms")
		}
		cfg.QueueGroupOptimisticWindow = window
		return nil
	}
}
