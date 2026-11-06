package nats

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func startManagerForClientTests(t *testing.T, logger *mockLogger, extra ...NATSOption) *natsManager {
	t.Helper()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	opts := append([]NATSOption{WithHost("127.0.0.1"), WithPort(port)}, extra...)
	mgrIface, err := NewNATSManager(logger, opts...)
	if err != nil {
		t.Fatalf("NewNATSManager: %v", err)
	}
	mgr := mgrIface.(*natsManager)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Stop(context.Background()) })
	return mgr
}

func TestClientOptions_DefaultsAndOverrides(t *testing.T) {
	t.Run("default connection name", func(t *testing.T) {
		mgr := startManagerForClientTests(t, newMockLogger())
		conn, _ := mgr.Connection()
		if conn.Opts.Name != DefaultClientName {
			t.Fatalf("connection name = %q, want %q", conn.Opts.Name, DefaultClientName)
		}
		if conn.Opts.AsyncErrorCB == nil || conn.Opts.DisconnectedErrCB == nil || conn.Opts.ReconnectedCB == nil || conn.Opts.ClosedCB == nil {
			t.Fatal("framework connection handlers should be installed")
		}
		if conn.Opts.DrainTimeout != connDrainTimeout {
			t.Fatalf("drain timeout = %s, want %s", conn.Opts.DrainTimeout, connDrainTimeout)
		}
	})

	t.Run("configured name", func(t *testing.T) {
		mgr := startManagerForClientTests(t, newMockLogger(), WithClientName("orders-api"))
		conn, _ := mgr.Connection()
		if conn.Opts.Name != "orders-api" {
			t.Fatalf("connection name = %q, want orders-api", conn.Opts.Name)
		}
	})

	t.Run("user options are applied after the defaults and can override them", func(t *testing.T) {
		mgr := startManagerForClientTests(t, newMockLogger(),
			WithClientOptions(nats.Name("from-user-option"), nats.ReconnectWait(3*time.Second)))
		conn, _ := mgr.Connection()
		if conn.Opts.Name != "from-user-option" {
			t.Fatalf("connection name = %q, want the user override", conn.Opts.Name)
		}
		if conn.Opts.ReconnectWait != 3*time.Second {
			t.Fatalf("reconnect wait = %s, want 3s", conn.Opts.ReconnectWait)
		}
	})

	t.Run("in-process connection gets the same options", func(t *testing.T) {
		mgr := startManagerForClientTests(t, newMockLogger(), WithDontListen(), WithInProcessConn(), WithClientName("inproc"))
		conn, _ := mgr.Connection()
		if conn.Opts.Name != "inproc" || conn.Opts.AsyncErrorCB == nil {
			t.Fatalf("in-process connection missing client options: name=%q", conn.Opts.Name)
		}
	})

	t.Run("option validation", func(t *testing.T) {
		if err := WithClientName("")(&NATSConfig{}); err == nil {
			t.Fatal("empty client name must be rejected")
		}
		if err := WithClientOptions(nil)(&NATSConfig{}); err == nil {
			t.Fatal("nil client option must be rejected")
		}
		cfg := &NATSConfig{}
		if err := WithClientOptions(nats.Name("a"), nats.Name("b"))(cfg); err != nil || len(cfg.ClientOptions) != 2 {
			t.Fatalf("options not appended: %v %d", err, len(cfg.ClientOptions))
		}
	})
}

// TestClientOptions_SlowConsumerIsLogged forces a slow consumer on the
// framework connection and checks the framework logger receives a structured
// Error line with the subject, instead of nats.go's stderr fallback.
func TestClientOptions_SlowConsumerIsLogged(t *testing.T) {
	logger := newMockLogger()
	mgr := startManagerForClientTests(t, logger)
	conn, _ := mgr.Connection()

	release := make(chan struct{})
	defer close(release)
	sub, err := conn.QueueSubscribe("services.orders.check-stock", "check-stock", func(*nats.Msg) { <-release })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.SetPendingLimits(1, -1); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// One message blocks the handler, one fills the buffer, the rest are dropped.
	for i := 0; i < 5; i++ {
		if err := conn.Publish("services.orders.check-stock", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if line := logger.findError("slow consumer"); line != "" {
			for _, want := range []string{"subject services.orders.check-stock", "queue_group check-stock", "module orders", "service check-stock"} {
				if !strings.Contains(line, want) {
					t.Fatalf("slow consumer line lacks %q: %s", want, line)
				}
			}
			// The dropped count is a positive number (how many depends on
			// dispatch timing; the message text itself also says "dropped").
			m := regexp.MustCompile(`dropped (\d+)`).FindStringSubmatch(line)
			if m == nil || m[1] == "0" {
				t.Fatalf("slow consumer line lacks a positive dropped count: %s", line)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no slow consumer log line from the framework logger")
}

func TestHandleClientError_NonSlowConsumer(t *testing.T) {
	logger := newMockLogger()
	mgr := &natsManager{logger: logger, config: DefaultNATSConfig()}
	mgr.handleClientError(nil, nil, nats.ErrBadSubscription)
	if logger.findError("NATS client error") == "" {
		t.Fatal("expected a generic client error line")
	}
}

func TestParseServiceSubject(t *testing.T) {
	if m, s, ok := parseServiceSubject("services.orders.check-stock"); !ok || m != "orders" || s != "check-stock" {
		t.Fatalf("got %q %q %v", m, s, ok)
	}
	if _, _, ok := parseServiceSubject("events.orders.created.v1"); ok {
		t.Fatal("event subjects are not service subjects")
	}
}

// findError returns the first ERROR line whose message contains substr,
// rendered with its fields, or "" when there is none.
func (m *mockLogger) findError(substr string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range m.messages {
		if msg.level == "ERROR" && strings.Contains(msg.msg, substr) {
			return fmt.Sprintf("%s %v", msg.msg, msg.args)
		}
	}
	return ""
}
