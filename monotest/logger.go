package monotest

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// testLogger implements types.Logger on top of testing.TB.Logf and keeps the
// Warn and Error lines so that assertions can look at them.
type testLogger struct {
	tb     testing.TB
	prefix string
	mu     *sync.Mutex
	lines  *[]string
}

func newTestLogger(tb testing.TB) *testLogger {
	lines := make([]string, 0)
	return &testLogger{tb: tb, mu: &sync.Mutex{}, lines: &lines}
}

func (l *testLogger) log(level, msg string, args ...any) {
	var b strings.Builder
	b.WriteString(level)
	b.WriteString(" ")
	if l.prefix != "" {
		b.WriteString(l.prefix)
		b.WriteString(" ")
	}
	b.WriteString(msg)
	for i := 0; i+1 < len(args); i += 2 {
		fmt.Fprintf(&b, " %v=%v", args[i], args[i+1])
	}
	line := b.String()
	l.mu.Lock()
	*l.lines = append(*l.lines, line)
	l.mu.Unlock()
	if l.tb != nil {
		l.tb.Helper()
		l.tb.Log(line)
	}
}

func (l *testLogger) Debug(msg string, args ...any) { l.log("DEBUG", msg, args...) }
func (l *testLogger) Info(msg string, args ...any)  { l.log("INFO", msg, args...) }
func (l *testLogger) Warn(msg string, args ...any)  { l.log("WARN", msg, args...) }
func (l *testLogger) Error(msg string, args ...any) { l.log("ERROR", msg, args...) }

func (l *testLogger) With(args ...any) types.Logger {
	var b strings.Builder
	b.WriteString(l.prefix)
	for i := 0; i+1 < len(args); i += 2 {
		fmt.Fprintf(&b, "%v=%v ", args[i], args[i+1])
	}
	c := *l
	c.prefix = strings.TrimSpace(b.String())
	return &c
}

func (l *testLogger) WithModule(name string) types.Logger { return l.With("module", name) }
func (l *testLogger) WithError(err error) types.Logger    { return l.With("error", err) }

// Lines returns a copy of every line logged so far.
func (l *testLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), *l.lines...)
}
