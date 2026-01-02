package fsjetstream

import (
	"log/slog"
)

// Option is a functional option for configuring the Module.
type Option func(*PluginModule) error

// WithLogger sets a custom logger for the module.
func WithLogger(logger *slog.Logger) Option {
	return func(m *PluginModule) error {
		m.logger = logger
		return nil
	}
}
