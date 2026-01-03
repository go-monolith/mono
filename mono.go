package mono

import (
	"fmt"

	"github.com/go-monolith/mono/internal/app"
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

	// Call internal app directly to create framework.
	// Both packages share types from pkg/types, so we can pass config structs directly.
	frameworkAppInstance, err := app.CreateFrameworkAppInstance(cfg.Logger, cfg.LoggerOptions, cfg.NATSOptions, cfg.QueueGroupOptimisticWindow)
	if err != nil {
		return nil, fmt.Errorf("failed to create framework: %w", err)
	}

	return frameworkAppInstance, nil
}
