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
		fatal:   make(chan string, fatalBacklog),
	}
}

// fatalBacklog is how many fatal messages are kept for Fatal until they are
// read; more than that are logged and kept as Last only.
const fatalBacklog = 16

// Fatal delivers the fatal messages the server reports, in order.
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
	c.logger.Error("[FATAL] " + RedactURLText(msg))
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

// RedactConfigError returns err unless its text may hold a URL: a route,
// gateway or leafnode URL the config file parser rejects is quoted in its
// error, credentials included. Such a line is cut before the URL, which is
// malformed and so cannot be parsed to remove the password from it; the file
// position at its start says where to look. The returned error carries the
// text only.
func RedactConfigError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	redacted := RedactURLText(text)
	if redacted == text {
		return err
	}
	return errors.New(redacted)
}

// urlErrorMarker is how nats-server introduces a URL it could not parse
// ("error parsing route url [...]"), whatever the URL looks like.
const urlErrorMarker = " url ["

// urlParseMarker is how the Go URL parser quotes what it could not parse
// (`parse "user:pw@proxy:3128": first path segment in URL cannot contain
// colon`), which nats-server passes on for a leafnode proxy URL.
const urlParseMarker = `parse "`

// urlNotShown replaces what is cut from a line.
const urlNotShown = " (the URL is not shown: it may hold credentials)"

// RedactURLText cuts every line of text before a URL it may quote: after
// the config parser's own marker for a URL it rejected (which need not have
// a scheme: "user:pass@host:6222" is rejected for its first colon), after
// the Go URL parser's, and before anything with "://" in it.
func RedactURLText(text string) string {
	if !strings.Contains(text, "://") && !strings.Contains(text, urlErrorMarker) && !strings.Contains(text, urlParseMarker) {
		return text
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		cut := -1
		if at := strings.Index(line, urlErrorMarker); at >= 0 {
			cut = at + len(" url")
		}
		if at := strings.Index(line, urlParseMarker); at >= 0 && (cut < 0 || at+len("parse") < cut) {
			cut = at + len("parse")
		}
		if at := strings.Index(line, "://"); at >= 0 {
			before := max(strings.LastIndexByte(line[:at], ' '), 0)
			if cut < 0 || before < cut {
				cut = before
			}
		}
		if cut < 0 {
			continue
		}
		lines[i] = strings.TrimRight(line[:cut], " ") + urlNotShown
	}
	return strings.Join(lines, "\n")
}
