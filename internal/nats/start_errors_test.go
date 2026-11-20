package nats

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats-server/v2/server"
)

// writeConfigFile writes a NATS config file and returns its path.
func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestNATSManager_AutoTLS_RejectsAllowNonTLS: AutoTLS makes TLS mandatory. A
// config file's allow_non_tls would let plaintext clients in beside it.
func TestNATSManager_AutoTLS_RejectsAllowNonTLS(t *testing.T) {
	port, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewNATSManager(newMockLogger(),
		WithPort(port),
		WithInProcessConn(),
		WithAutoTLS(autoTLSTestConfig(t)),
		WithConfigFile(writeConfigFile(t, "allow_non_tls: true\n")),
	)
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}
	err = manager.Start(context.Background())
	if err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() succeeded with AutoTLS and allow_non_tls")
	}
	if !strings.Contains(err.Error(), "allow_non_tls") || !strings.Contains(err.Error(), "AutoTLS") {
		t.Fatalf("error = %v, want it to name allow_non_tls and AutoTLS", err)
	}
}

// TestNATSManager_StartReportsTheServersFatalError: a server that cannot
// start says why (the port is in use). Start used to wait the whole ready
// timeout and return "not ready" without the reason, which was logged
// nowhere.
func TestNATSManager_StartReportsTheServersFatalError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()
	port := busy.Addr().(*net.TCPAddr).Port

	logger := newMockLogger()
	manager, err := NewNATSManager(logger, WithPort(port), WithStartupReadyTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}
	started := time.Now()
	err = manager.Start(context.Background())
	if err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() succeeded on a port that is in use")
	}
	if !strings.Contains(err.Error(), "address already in use") || !strings.Contains(err.Error(), fmt.Sprint(port)) {
		t.Errorf("error = %v, want the server's reason and the port", err)
	}
	if waited := time.Since(started); waited > 3*time.Second {
		t.Errorf("Start waited %v for a server that had already failed", waited)
	}
}

// TestNATSManager_IPv6Host: an IPv6 host is put in brackets in the URL the
// framework's own client dials.
func TestNATSManager_IPv6Host(t *testing.T) {
	probe, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is not available: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	manager, err := NewNATSManager(newMockLogger(), WithHost("::1"), WithPort(port))
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start() with an IPv6 host: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()
	info := manager.ServerInfo()
	if want := fmt.Sprintf("nats://[::1]:%d", port); info.ClientURL != want {
		t.Errorf("ClientURL = %q, want %q", info.ClientURL, want)
	}
}

// TestNATSManager_ConfigFileRouteErrorHidesCredentials: a route URL the
// config file parser rejects comes back in its error, password included. The
// framework never quotes a route.
func TestNATSManager_ConfigFileRouteErrorHidesCredentials(t *testing.T) {
	for name, route := range map[string]string{
		"bad escape":      "nats://admin:s3cr%zzet@10.0.0.1:6222",
		"slash":           "nats://admin:s3cr/et@10.0.0.1:6222",
		"hash":            "nats://admin:s3cr#et@10.0.0.1:6222",
		"at and bad port": "nats://admin:s3cr@et@10.0.0.1:notaport",
		"space":           "nats://admin:s3cr et@10.0.0.1:6222",
		// Without a scheme the error holds no "://".
		"no scheme":         "route_admin:s3cret@10.0.0.1:6222",
		"no scheme, digit":  "1admin:s3cret@10.0.0.1:6222",
		"scheme-less slash": "//admin:s3cr%zzet@10.0.0.1:6222",
	} {
		t.Run(name, func(t *testing.T) {
			port, err := findAvailablePort()
			if err != nil {
				t.Fatal(err)
			}
			conf := fmt.Sprintf("cluster {\n  name: c1\n  port: -1\n  routes: [%q]\n}\n", route)
			manager, err := NewNATSManager(newMockLogger(), WithPort(port), WithInProcessConn(), WithConfigFile(writeConfigFile(t, conf)))
			if err != nil {
				if strings.Contains(err.Error(), "s3cr") {
					t.Fatalf("NewNATSManager error quotes the password: %v", err)
				}
				return
			}
			err = manager.Start(context.Background())
			if err == nil {
				_ = manager.Stop(context.Background())
				t.Skip("the parser accepts this route")
			}
			if strings.Contains(err.Error(), "s3cr") || strings.Contains(err.Error(), "et@") {
				t.Fatalf("the error quotes the route's credentials: %v", err)
			}
			if !strings.Contains(err.Error(), "route") {
				t.Errorf("the error does not say that a route is at fault: %v", err)
			}
		})
	}
}

// TestFatalCapture: fatal messages are kept and logged whatever the flags
// are; the server's other lines reach the log only through a forwarder.
func TestFatalCapture(t *testing.T) {
	t.Run("without a forwarder only fatal messages are logged", func(t *testing.T) {
		logger := newMockLogger()
		c := NewFatalCapture(logger, nil)
		c.Noticef("Starting nats-server")
		c.Warnf("slow consumer")
		c.Errorf("an error")
		c.Debugf("debug")
		c.Tracef("trace")
		logger.mu.Lock()
		lines := len(logger.messages)
		logger.mu.Unlock()
		if lines != 0 {
			t.Fatalf("%d server lines reached the log without a logging flag", lines)
		}
		if c.Last() != "" {
			t.Fatalf("Last() = %q before any fatal message", c.Last())
		}
		c.Fatalf("Error listening on port: %s", "127.0.0.1:4222")
		c.Fatalf("second %d", 2)
		if !logger.hasMessage("ERROR", "[FATAL] Error listening on port: 127.0.0.1:4222") {
			t.Fatal("the fatal message was not logged")
		}
		select {
		case first := <-c.Fatal():
			if first != "Error listening on port: 127.0.0.1:4222" {
				t.Fatalf("first fatal message = %q", first)
			}
		default:
			t.Fatal("no fatal message delivered")
		}
		if c.Last() != "second 2" {
			t.Fatalf("Last() = %q", c.Last())
		}
	})
	t.Run("with a forwarder every line is passed on", func(t *testing.T) {
		logger := newMockLogger()
		c := NewFatalCapture(newMockLogger(), NewNATSLoggerAdapter(logger))
		c.Noticef("notice %d", 1)
		c.Warnf("warn")
		c.Errorf("error")
		c.Debugf("debug")
		c.Tracef("trace")
		for level, msg := range map[string]string{"INFO": "notice 1", "WARN": "warn", "ERROR": "error", "DEBUG": "debug"} {
			if !logger.hasMessage(level, msg) {
				t.Errorf("%s line %q was not forwarded", level, msg)
			}
		}
		if !logger.hasMessage("DEBUG", "[TRACE] trace") {
			t.Error("the trace line was not forwarded")
		}
	})
}

func TestRedactConfigError(t *testing.T) {
	if RedactConfigError(nil) != nil {
		t.Fatal("nil is not kept")
	}
	plain := fmt.Errorf("nats.conf:3:1: unknown field %q", "foo")
	if RedactConfigError(plain) != plain {
		t.Fatal("an error without a URL is not returned as it is")
	}
	in := fmt.Errorf("%s\n%s\n%s",
		`nats.conf:2:1: unknown field "foo"`,
		`nats.conf:4:13: error parsing route url ["nats://admin:s3cr et@10.0.0.1:6222"]`,
		`nats://admin:hunter2@host`)
	got := RedactConfigError(in).Error()
	// The parser's marker is enough, also for a URL without a scheme.
	if got := RedactURLText(`nats.conf:4:13: error parsing route url ["route_admin:s3cret@10.0.0.1:6222"]`); strings.Contains(got, "s3cret") || !strings.Contains(got, "error parsing route url") {
		t.Fatalf("scheme-less URL: %s", got)
	}
	if got := RedactURLText("Error listening on port: 127.0.0.1:4222"); got != "Error listening on port: 127.0.0.1:4222" {
		t.Fatalf("a message without a URL was changed: %s", got)
	}
	for _, secret := range []string{"s3cr", "et@", "hunter2", "admin"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted error still holds %q: %s", secret, got)
		}
	}
	for _, kept := range []string{`nats.conf:2:1: unknown field "foo"`, "nats.conf:4:13: error parsing route url", "the URL is not shown"} {
		if !strings.Contains(got, kept) {
			t.Fatalf("redacted error lacks %q: %s", kept, got)
		}
	}
}

// TestNATSManager_ClientCAUnderConfigFileRouteCA: with the route CA in a
// config file's cluster { tls {} } block, a route CA that is signed by the
// client CA was not found (the block leaves only a certificate pool), so a
// route certificate would have been accepted as a client.
func TestNATSManager_ClientCAUnderConfigFileRouteCA(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	clientCA := newTestCA(t, "client-ca")
	underClient := clientCA.intermediate(t, "under-client")
	dir := t.TempDir()
	certFile, keyFile := serverCA.issue(t, dir, "server", 2)
	routeCert, routeKey := underClient.issue(t, t.TempDir(), "route", 3)
	routeCAFile := underClient.write(t, t.TempDir())
	clientCAFile := clientCA.write(t, t.TempDir())

	port, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	conf := fmt.Sprintf("server_name: n1\nCluster {\n  name: c1\n  port: -1\n  TLS {\n    cert_file: %q\n    key_file: %q\n    CA_File: %q\n    verify: true\n  }\n}\n",
		routeCert, routeKey, routeCAFile)
	path := writeConfigFile(t, conf)
	if got, err := ClusterCAFileOfConfig(path); err != nil || got != routeCAFile {
		t.Fatalf("ClusterCAFileOfConfig = %q, %v, want %q", got, err, routeCAFile)
	}
	manager, err := NewNATSManager(newMockLogger(), WithPort(port), WithInProcessConn(), WithConfigFile(path),
		WithClientTLS(&types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: clientCAFile}))
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}
	err = manager.Start(context.Background())
	if err == nil {
		_ = manager.Stop(context.Background())
		t.Fatal("Start() accepted a route CA that is signed by the client CA")
	}
	if !strings.Contains(err.Error(), "is signed by, the client CA") {
		t.Fatalf("error = %v, want the shared authority named", err)
	}
}

func TestClusterCAFileOfConfig(t *testing.T) {
	for name, tt := range map[string]struct{ conf, want string }{
		"no cluster block":    {"port: 4222\n", ""},
		"cluster without tls": {"cluster { name: c1 }\n", ""},
		"tls without ca_file": {"cluster { tls { verify: true } }\n", ""},
		"ca_file":             {"cluster { tls { ca_file: \"/etc/route-ca.pem\" } }\n", "/etc/route-ca.pem"},
	} {
		got, err := ClusterCAFileOfConfig(writeConfigFile(t, tt.conf))
		if err != nil || got != tt.want {
			t.Errorf("%s: got %q, %v, want %q", name, got, err, tt.want)
		}
	}
	if _, err := ClusterCAFileOfConfig(filepath.Join(t.TempDir(), "missing.conf")); err == nil {
		t.Error("a missing file is not an error")
	}
}

// TestAwaitServerReady_FatalThatDoesNotStopTheServer: not every message the
// server calls fatal keeps it from starting (a profiling port in use). Such
// a server started before the message was captured, and still does.
func TestAwaitServerReady_FatalThatDoesNotStopTheServer(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	logger := newMockLogger()
	conf := fmt.Sprintf("prof_port: %d\n", busy.Addr().(*net.TCPAddr).Port)
	manager, err := NewNATSManager(logger, WithPort(port), WithInProcessConn(), WithConfigFile(writeConfigFile(t, conf)))
	if err != nil {
		t.Fatalf("NewNATSManager() error = %v", err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start() with a profiling port that is in use: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()
	if !logger.hasMessage("ERROR", "[FATAL]") {
		t.Error("the server's message was not logged")
	}
}

// TestRedactURLText_ParseMarker: the Go URL parser's own quoting of what it
// could not parse is cut as well.
func TestRedactURLText_ParseMarker(t *testing.T) {
	got := RedactURLText(`nats.conf:9:5: invalid proxy URL: parse "svc_user:pw@proxy:3128": first path segment in URL cannot contain colon`)
	if strings.Contains(got, "svc_user") || strings.Contains(got, "pw@") || !strings.Contains(got, "invalid proxy URL: parse") {
		t.Fatalf("redacted = %s", got)
	}
}

// TestAwaitServerReady_FailsOnceStartReturnedWithAFatalMessage: a server
// whose Start returned after a fatal message, and that is not ready, has
// failed; the rest of the ready timeout is not waited for. A fatal message
// while Start is still at work does not end the wait.
func TestAwaitServerReady_FailsOnceStartReturnedWithAFatalMessage(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Shutdown()
	// Never started: it does not become ready.
	fatal := NewFatalCapture(newMockLogger(), nil)
	started := make(chan struct{})

	result := make(chan error, 1)
	begun := time.Now()
	go func() { result <- AwaitServerReady(ns, fatal, started, 30*time.Second) }()

	fatal.Fatalf("Can't start JetStream: %s", "store directory is not writable")
	select {
	case err := <-result:
		t.Fatalf("returned %v while Start was still running", err)
	case <-time.After(startedReadyGrace + 500*time.Millisecond):
	}
	close(started)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "Can't start JetStream") || !strings.Contains(err.Error(), "failed to start") {
			t.Fatalf("err = %v, want the server's reason", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("still waiting for the ready timeout after Start returned with a fatal message")
	}
	if waited := time.Since(begun); waited > 10*time.Second {
		t.Fatalf("waited %v", waited)
	}
}

// TestAwaitServerReady_TimeoutNamesTheReportedMessage: when the timeout is
// what ends the wait, the error says so and adds what the server reported.
func TestAwaitServerReady_TimeoutNamesTheReportedMessage(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Shutdown()
	fatal := NewFatalCapture(newMockLogger(), nil)
	fatal.Fatalf("error starting profiler: %s", "address already in use")
	err = AwaitServerReady(ns, fatal, make(chan struct{}), 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "not ready after 300ms") || !strings.Contains(err.Error(), "error starting profiler") {
		t.Fatalf("err = %v", err)
	}
	if err := AwaitServerReady(ns, NewFatalCapture(newMockLogger(), nil), make(chan struct{}), 100*time.Millisecond); err == nil || strings.Contains(err.Error(), "reported") {
		t.Fatalf("err without a fatal message = %v", err)
	}
}
