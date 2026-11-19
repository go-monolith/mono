package fsjetstream

import (
	"errors"
	"log/slog"
)

// Option is a functional option for configuring the Module.
type Option func(*PluginModule) error

// WithLogger sets a custom logger for the module. Without it the module logs
// to slog.Default() as it is when Start runs (not when New is called), so an
// application may replace the default logger after constructing the plugin.
// A nil logger is rejected with an error from New.
func WithLogger(logger *slog.Logger) Option {
	return func(m *PluginModule) error {
		if logger == nil {
			return errors.New("fs-jetstream: WithLogger: logger cannot be nil")
		}
		m.logger = logger
		m.customLogger = true
		return nil
	}
}
