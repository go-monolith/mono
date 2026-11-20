package nats

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-monolith/mono/pkg/types"
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

// FatalCapture is the logger installed on the embedded server. It keeps the
// server's fatal messages, which are the only place the server says why it
// cannot start (it does not return an error, it stops becoming ready), and
// logs them at Error whatever the logging flags are. Every other line goes
// to Forward, when there is one.
type FatalCapture struct {
	logger  types.Logger
	forward server.Logger

	mu    sync.Mutex
	last  string
	fatal chan string
}

// NewFatalCapture returns a FatalCapture that logs fatal messages to logger
// and passes the server's other lines to forward (nil: they are dropped, as
// they are without a logging flag).
func NewFatalCapture(logger types.Logger, forward server.Logger) *FatalCapture {
	return &FatalCapture{
		logger:  logger.With("component", "nats-server"),
		forward: forward,
		fatal:   make(chan string, 1),
	}
}

// Fatal delivers the first fatal message the server reports.
func (c *FatalCapture) Fatal() <-chan string { return c.fatal }

// Last returns the last fatal message, or "" when there was none.
func (c *FatalCapture) Last() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

// Fatalf keeps and logs a fatal message. It does not exit the process: the
// framework manages shutdown.
func (c *FatalCapture) Fatalf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	c.mu.Lock()
	c.last = msg
	c.mu.Unlock()
	select {
	case c.fatal <- msg:
	default:
	}
	c.logger.Error("[FATAL] " + msg)
}

// Noticef forwards a notice.
func (c *FatalCapture) Noticef(format string, v ...any) {
	if c.forward != nil {
		c.forward.Noticef(format, v...)
	}
}

// Warnf forwards a warning.
func (c *FatalCapture) Warnf(format string, v ...any) {
	if c.forward != nil {
		c.forward.Warnf(format, v...)
	}
}

// Errorf forwards an error.
func (c *FatalCapture) Errorf(format string, v ...any) {
	if c.forward != nil {
		c.forward.Errorf(format, v...)
	}
}

// Debugf forwards a debug line.
func (c *FatalCapture) Debugf(format string, v ...any) {
	if c.forward != nil {
		c.forward.Debugf(format, v...)
	}
}

// Tracef forwards a trace line.
func (c *FatalCapture) Tracef(format string, v ...any) {
	if c.forward != nil {
		c.forward.Tracef(format, v...)
	}
}

// RedactConfigError returns err unless its text holds a URL: a route,
// gateway or leafnode URL the config file parser rejects is quoted in its
// error, credentials included. Such a line is cut before the URL, which is
// malformed and so cannot be parsed to remove the password from it; the file
// position at its start says where to look.
func RedactConfigError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	if !strings.Contains(text, "://") {
		return err
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		at := strings.Index(line, "://")
		if at < 0 {
			continue
		}
		cut := strings.LastIndexByte(line[:at], ' ')
		if cut < 0 {
			cut = 0
		}
		lines[i] = strings.TrimRight(line[:cut], " ") + " (the URL is not shown: it may hold credentials)"
	}
	return errors.New(strings.Join(lines, "\n"))
}
