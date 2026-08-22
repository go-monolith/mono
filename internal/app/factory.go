package app

import (
	"fmt"
	"os"
	"time"

	internallogger "github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/internal/nats"
	"github.com/go-monolith/mono/pkg/types"
)

// CreateFrameworkAppInstance creates a new MonoFramework instance with the given configuration.
// This is called directly by pkg/mono.NewMonoApplication().
//
// Parameters:
//   - logger: Optional custom logger. If nil, a default logger is created based on loggerOpts.
//   - loggerOpts: Logger configuration options (level, format, output, etc.)
//   - natsOpts: NATS server configuration options
//   - queueGroupOptimisticWindow: Optimistic publish window for queue groups (0 = disabled)
//
// Returns the created MonoFramework or an error if creation fails.
func CreateFrameworkAppInstance(logger types.Logger, loggerOpts types.LoggerOptions, natsOpts types.NATSOptions, queueGroupOptimisticWindow time.Duration) (types.MonoFramework, error) {
	// Initialize logger if not provided
	if logger == nil {
		var err error
		logger, err = createDefaultLogger(loggerOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create default logger: %w", err)
		}
	}

	// Convert public NATS options to internal NATS options
	internalNatsOpts := buildNATSOptions(natsOpts)

	// Call internal framework constructor with config
	fwApp, err := NewFrameworkAppInstance(logger, queueGroupOptimisticWindow, internalNatsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create framework: %w", err)
	}

	return fwApp, nil
}

// createDefaultLogger creates a logger using the provided logger options.
func createDefaultLogger(opts types.LoggerOptions) (types.Logger, error) {
	// If UseDefault is true, a custom logger was set via WithCustomLogger
	// and we should have already caught this case (cfg.Logger != nil)
	// This is a safety check
	if opts.UseDefault {
		return internallogger.NewDefaultLogger(), nil
	}

	// Set output to stdout if not specified
	output := opts.Output
	if output == nil {
		output = os.Stdout
	}

	// Create logger factory with options
	factory, err := internallogger.NewLoggerFactory(
		internallogger.WithLogLevel(opts.Level),
		internallogger.WithLogFormat(opts.Format),
		internallogger.WithOutput(output),
		internallogger.WithAddSource(opts.AddSource),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger factory: %w", err)
	}

	// Create framework level logger with an empty module name
	return factory.NewLogger(""), nil
}

// Default values for NATS options - used to detect explicit overrides when config file is used.
const (
	defaultNATSHost       = "127.0.0.1"
	defaultNATSPort       = 4222
	defaultNATSMaxPayload = int32(1024 * 1024) // 1 MB
)

// buildNATSOptions converts NATSOptions to internal nats.NATSOption slice.
func buildNATSOptions(cfg types.NATSOptions) []nats.NATSOption {
	var opts []nats.NATSOption

	// Track if a config file is being used - when using a config file,
	// we only add other options if they differ from defaults (i.e., explicitly set)
	hasConfigFile := cfg.ConfigFile != ""

	// Add config file first so other options can override its settings
	if hasConfigFile {
		opts = append(opts, nats.WithConfigFile(cfg.ConfigFile))
	}

	// Add Host and Port configuration
	// When using config file, only add if explicitly changed from defaults
	if cfg.Host != "" && (!hasConfigFile || cfg.Host != defaultNATSHost) {
		opts = append(opts, nats.WithHost(cfg.Host))
	}
	if cfg.Port > 0 && (!hasConfigFile || cfg.Port != defaultNATSPort) {
		opts = append(opts, nats.WithPort(cfg.Port))
	}

	// Add DontListen if enabled
	if cfg.DontListen {
		opts = append(opts, nats.WithDontListen())
	}

	// Add UseInProcessConn if enabled.
	//
	// AutoTLS forces it on: the framework's own client dials the loopback
	// address, which cannot satisfy hostname verification against a
	// public-domain certificate. Doing it here rather than inside the manager
	// keeps the translated options an honest reflection of behaviour.
	if cfg.UseInProcessConn || cfg.AutoTLS != nil {
		opts = append(opts, nats.WithInProcessConn())
	}

	// Add AutoTLS (ACME) configuration if enabled
	if cfg.AutoTLS != nil {
		opts = append(opts, nats.WithAutoTLS(cfg.AutoTLS))
	}

	// Add JetStream configuration if enabled
	if cfg.JetStreamEnabled {
		// Use configured directory or empty string for default
		opts = append(opts, nats.WithJetStream(cfg.JetStreamDir))
	}

	// Add JetStream domain if configured
	if cfg.JetStreamDomain != "" {
		opts = append(opts, nats.WithJetStreamDomain(cfg.JetStreamDomain))
	}

	// Add clustering configuration if configured
	if cfg.ClusterName != "" {
		opts = append(opts, nats.WithClustering(
			cfg.ClusterName,
			cfg.ClusterHost,
			cfg.ClusterPort,
			cfg.ClusterRoutes,
		))
	}

	// Add max payload if configured
	// When using config file, only add if explicitly changed from defaults
	if cfg.MaxPayload > 0 && (!hasConfigFile || cfg.MaxPayload != defaultNATSMaxPayload) {
		opts = append(opts, nats.WithMaxPayload(cfg.MaxPayload))
	}

	// Add ready timeout if configured
	if cfg.StartupReadyTimeout > 0 {
		opts = append(opts, nats.WithStartupReadyTimeout(cfg.StartupReadyTimeout))
	}

	// Add logging configuration if any logging flag is enabled
	if cfg.LogDebug || cfg.LogTrace || cfg.LogSysTrace {
		opts = append(opts, nats.WithLogging(cfg.LogDebug, cfg.LogTrace, cfg.LogSysTrace))
	}

	return opts
}
