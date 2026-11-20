//go:build integration
// +build integration

package integration_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// nonLoopbackAddresses returns the addresses of this host other than loopback
// and link-local ones.
func nonLoopbackAddresses(t *testing.T) []net.IP {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("cannot list interface addresses: %v", err)
	}
	var out []net.IP
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ipNet.IP)
	}
	return out
}

// With a config file, an explicit WithNATSHost("127.0.0.1") used to be dropped
// because it equals the default, and the embedded server listened on every
// interface without authentication. The client port must be reachable through
// loopback only.
func TestIntegration_ConfigFile_ExplicitLoopbackHostNotExposed(t *testing.T) {
	others := nonLoopbackAddresses(t)
	if len(others) == 0 {
		t.Skip("this host has no non-loopback address to probe")
	}

	conf := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(conf, []byte("max_payload: 2MB\n"), 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	port := freeTCPPort(t)
	app, err := mono.NewMonoApplication(
		mono.WithNATSConfigFile(conf),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(port),
		mono.WithLogLevel(mono.LogLevelError),
	)
	if err != nil {
		t.Fatalf("failed to create application: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Errorf("failed to stop application: %v", err)
		}
	})

	loopback, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 2*time.Second)
	if err != nil {
		t.Fatalf("client port not reachable through loopback: %v", err)
	}
	if err := loopback.Close(); err != nil {
		t.Errorf("failed to close the loopback connection: %v", err)
	}

	for _, ip := range others {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip.String(), strconv.Itoa(port)), time.Second)
		if err == nil {
			_ = conn.Close()
			t.Errorf("client port %d is reachable through %s, want loopback only", port, ip)
		}
	}
}
