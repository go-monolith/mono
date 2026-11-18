package mono

import (
	"fmt"

	"github.com/go-monolith/mono/internal/app"
	"github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// NewMonoApplication creates a new MonoFramework application instance with the given options.
//
// The monolith application is created with sensible defaults that can be overridden using
// functional options. If no logger is provided, a default logger is created.
// If no audit logger is provided, audit logging is disabled (uses io.Discard).
//
// Example:
//
//	app, err := mono.NewMonoApplication(
//	    mono.WithNATSPort(4222),
//	    mono.WithShutdownTimeout(30*time.Second),
//	    mono.WithLogLevel(mono.LogLevelDebug),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer app.Stop(context.Background())
//
// See docs/spec/foundation.md for detailed documentation.
func NewMonoApplication(opts ...MonoFrameworkOption) (MonoApplication, error) {
	// Start with default configuration
	cfg := defaultConfig()

	// Apply all options in order, stopping on first error
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Cross-field validation, once every option has been applied. An unset
	// drain timeout follows the shutdown timeout; an explicit one must fit in it.
	switch {
	case cfg.ShutdownDrainTimeout <= 0:
		cfg.ShutdownDrainTimeout = min(types.DefaultShutdownDrainTimeout, cfg.ShutdownTimeout)
	case cfg.ShutdownDrainTimeout > cfg.ShutdownTimeout:
		return nil, errors.WrapInvalidConfiguration(0, "WithShutdownDrainTimeout", cfg.ShutdownDrainTimeout,
			fmt.Sprintf("drain timeout must not exceed the shutdown timeout (%s)", cfg.ShutdownTimeout))
	}

	if err := validateClusterOptions(cfg.NATSOptions); err != nil {
		return nil, err
	}
	if ct := cfg.NATSOptions.ClientTLS; ct != nil {
		switch {
		case cfg.NATSOptions.AutoTLS != nil:
			return nil, errors.WrapInvalidConfiguration(0, "WithNATSClientTLS", ct.CertFile,
				"client TLS cannot be combined with WithNATSAutoTLS: both configure the client listener's certificate")
		case cfg.NATSOptions.DontListen:
			return nil, errors.WrapInvalidConfiguration(0, "WithNATSClientTLS", ct.CertFile,
				"client TLS cannot be combined with WithNATSDontListen: there is no client listener to protect")
		}
	}
	if cfg.JetStreamDefaultReplicas > 1 && cfg.NATSOptions.ClusterName == "" && cfg.NATSOptions.ConfigFile == "" {
		return nil, errors.WrapInvalidConfiguration(0, "WithJetStreamDefaultReplicas", cfg.JetStreamDefaultReplicas,
			"more than one replica requires clustering (WithNATSClustering or a cluster{} block in a NATS config file)")
	}

	// Call internal app directly to create framework.
	// Both packages share types from pkg/types, so we can pass config structs directly.
	frameworkAppInstance, err := app.CreateFrameworkAppInstance(cfg.Logger, cfg.LoggerOptions, cfg.NATSOptions, app.Settings{
		QueueGroupOptimisticWindow: cfg.QueueGroupOptimisticWindow,
		DefaultServiceConcurrency:  cfg.DefaultServiceConcurrency,
		ShutdownTimeout:            cfg.ShutdownTimeout,
		ShutdownDrainTimeout:       cfg.ShutdownDrainTimeout,
		DeadlineSkewAllowance:      cfg.DeadlineSkewAllowance,
		JetStreamReadyTimeout:      cfg.JetStreamReadyTimeout,
		JetStreamDefaultReplicas:   cfg.JetStreamDefaultReplicas,
		StatsLogInterval:           cfg.SubscriptionStatsLogInterval,
		ServiceDiscovery:           cfg.ServiceDiscovery,
		StrictModuleBoundaries:     cfg.StrictModuleBoundaries,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create framework: %w", err)
	}

	return frameworkAppInstance, nil
}

// validateClusterOptions checks the clustering options against each other.
// A NATS config file may provide what the options leave out (server_name, a
// cluster{} block), so with one the checks are made at Start instead, on the
// merged configuration.
func validateClusterOptions(o types.NATSOptions) error {
	if o.ConfigFile != "" {
		return nil
	}
	clustered := o.ClusterName != ""
	if o.ClusterTLSCertFile != "" && !clustered {
		return errors.WrapInvalidConfiguration(0, "WithNATSClusterTLS", o.ClusterTLSCertFile,
			"cluster TLS requires WithNATSClustering")
	}
	if (o.ClusterUsername != "" || o.ClusterPassword != "") && !clustered {
		return errors.WrapInvalidConfiguration(0, "WithNATSClusterAuth", o.ClusterUsername,
			"cluster credentials require WithNATSClustering")
	}
	if clustered && o.JetStreamEnabled && o.ServerName == "" {
		return errors.WrapInvalidConfiguration(0, "WithNATSServerName", "",
			"clustered JetStream requires a server name that is unique within the cluster and stable across restarts")
	}
	if clustered && o.JetStreamEnabled && len(o.ClusterRoutes) == 0 {
		return errors.WrapInvalidConfiguration(0, "WithNATSClustering", o.ClusterName,
			"clustered JetStream requires at least one route: list the cluster's servers, this one included")
	}
	return nil
}
