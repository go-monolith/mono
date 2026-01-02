package nats

import (
	"fmt"

	"github.com/go-monolith/mono/v1/pkg/types"
	"github.com/nats-io/nats-server/v2/server"
)

// Ensure natsLoggerAdapter implements server.Logger interface at compile time
var _ server.Logger = (*natsLoggerAdapter)(nil)

// natsLoggerAdapter adapts the framework Logger to NATS server.Logger interface.
// This allows NATS server logs to be routed through the framework's logging system.
type natsLoggerAdapter struct {
	logger types.Logger
}

// NewNATSLoggerAdapter creates a new adapter that bridges NATS server logging
// to the framework's logger.
func NewNATSLoggerAdapter(logger types.Logger) server.Logger {
	return &natsLoggerAdapter{
		logger: logger.With("component", "nats-server"),
	}
}

// Noticef logs notice-level messages (maps to Info)
func (a *natsLoggerAdapter) Noticef(format string, v ...any) {
	a.logger.Info(fmt.Sprintf(format, v...))
}

// Warnf logs warning-level messages
func (a *natsLoggerAdapter) Warnf(format string, v ...any) {
	a.logger.Warn(fmt.Sprintf(format, v...))
}

// Errorf logs error-level messages
func (a *natsLoggerAdapter) Errorf(format string, v ...any) {
	a.logger.Error(fmt.Sprintf(format, v...))
}

// Fatalf logs fatal-level messages (maps to Error, does NOT call os.Exit)
// The framework manages shutdown, so we log as Error instead of terminating.
func (a *natsLoggerAdapter) Fatalf(format string, v ...any) {
	a.logger.Error(fmt.Sprintf("[FATAL] "+format, v...))
}

// Debugf logs debug-level messages
func (a *natsLoggerAdapter) Debugf(format string, v ...any) {
	a.logger.Debug(fmt.Sprintf(format, v...))
}

// Tracef logs trace-level messages (maps to Debug with trace marker)
func (a *natsLoggerAdapter) Tracef(format string, v ...any) {
	a.logger.Debug(fmt.Sprintf("[TRACE] "+format, v...))
}
