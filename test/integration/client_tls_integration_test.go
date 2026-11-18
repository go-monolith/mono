//go:build integration
// +build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// TestClientTLS_MutualTLS runs one server with route TLS and client mTLS,
// each with its own CA: a client with a certificate from the client CA calls
// a responder through the framework's bus, and the certificates of one CA
// are refused on the other listener.
func TestClientTLS_MutualTLS(t *testing.T) {
	dir := t.TempDir()
	routeCA := newRouteCA(t, dir, "route-ca")
	clientCA := newRouteCA(t, dir, "client-ca")
	routeCert, routeKey := routeCA.issue(t, dir, "route")
	listenerCert, listenerKey := clientCA.issue(t, dir, "listener")
	clientCert, clientKey := clientCA.issue(t, dir, "client")

	port, clusterPort := freeTCPPort(t), freeTCPPort(t)
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithNATSServerName("tls-node"),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(port),
		mono.WithNATSClustering("tls-test", "127.0.0.1", clusterPort, []string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPort)}),
		mono.WithNATSClusterTLS(routeCert, routeKey, routeCA.file),
		mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: listenerCert, KeyFile: listenerKey, ClientCAFile: clientCA.file}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	// The framework's own bus (in-process) answers requests.
	bus := app.EventBus("")
	sub, err := bus.Subscribe("tls.echo", func(_ context.Context, m *mono.Msg) {
		_ = bus.Publish(m.Reply, append([]byte("echo:"), m.Data...))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	url := fmt.Sprintf("tls://127.0.0.1:%d", port)
	tlsOpt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: clientCA.file, CertFile: clientCert, KeyFile: clientKey})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url, tlsOpt, nats.NoReconnect())
	if err != nil {
		t.Fatalf("client with a certificate from the client CA: %v", err)
	}
	defer nc.Close()
	msg, err := nc.Request("tls.echo", []byte("hi"), 2*time.Second)
	if err != nil || string(msg.Data) != "echo:hi" {
		t.Fatalf("request = %v, %v", msg, err)
	}

	refused := func(t *testing.T, opts ...nats.Option) {
		t.Helper()
		nc, err := nats.Connect(url, append([]nats.Option{nats.NoReconnect(), nats.Timeout(2 * time.Second)}, opts...)...)
		if err == nil {
			_, err = nc.Request("tls.echo", nil, time.Second)
			nc.Close()
		}
		if err == nil {
			t.Fatal("connection accepted")
		}
	}
	t.Run("a client without a certificate is refused", func(t *testing.T) {
		opt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: clientCA.file})
		if err != nil {
			t.Fatal(err)
		}
		refused(t, opt)
	})
	t.Run("a route certificate is refused as a client certificate", func(t *testing.T) {
		opt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: clientCA.file, CertFile: routeCert, KeyFile: routeKey})
		if err != nil {
			t.Fatal(err)
		}
		refused(t, opt)
	})
	t.Run("a client certificate is refused on the route port", func(t *testing.T) {
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			t.Fatal(err)
		}
		pool := x509.NewCertPool()
		pool.AddCert(routeCA.cert)
		conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", clusterPort), &tls.Config{
			Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12,
		})
		if err == nil {
			// With TLS 1.3 the server's verdict on the client certificate
			// arrives after the client's side of the handshake.
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, err = conn.Read(make([]byte, 1))
			_ = conn.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("err = %v, want a refused certificate", err)
		}
	})
}

func TestClientTLS_Validation(t *testing.T) {
	dir := t.TempDir()
	clientCA := newRouteCA(t, dir, "client-ca")
	cert, key := clientCA.issue(t, dir, "listener")

	t.Run("cannot be combined with AutoTLS", func(t *testing.T) {
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSAutoTLS(types.AutoTLSConfig{Domains: []string{"example.com"}, CacheDir: t.TempDir(), AcceptTOS: true}),
			mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: cert, KeyFile: key}))
		if err == nil || !strings.Contains(err.Error(), "cannot be combined with WithNATSAutoTLS") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("cannot be combined with DontListen", func(t *testing.T) {
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSDontListen(),
			mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: cert, KeyFile: key}))
		if err == nil || !strings.Contains(err.Error(), "cannot be combined with WithNATSDontListen") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("requires a key file", func(t *testing.T) {
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: cert}))
		if err == nil || !strings.Contains(err.Error(), "needs a certificate file and a key file") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("refuses the route CA as client CA", func(t *testing.T) {
		routeCert, routeKey := clientCA.issue(t, dir, "route")
		clusterPort := freeTCPPort(t)
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSPort(freeTCPPort(t)),
			mono.WithNATSClustering("c", "127.0.0.1", clusterPort, []string{fmt.Sprintf("nats://127.0.0.1:%d", clusterPort)}),
			mono.WithNATSClusterTLS(routeCert, routeKey, clientCA.file),
			mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: cert, KeyFile: key, ClientCAFile: clientCA.file}))
		if err == nil || !strings.Contains(err.Error(), "is, or is signed by, the route CA") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("conflicts with a config file tls block", func(t *testing.T) {
		conf := filepath.Join(dir, "nats.conf")
		if err := os.WriteFile(conf, []byte(fmt.Sprintf("tls { cert_file: %q, key_file: %q }\n", cert, key)), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSConfigFile(conf),
			mono.WithNATSPort(freeTCPPort(t)),
			mono.WithNATSClientTLS(types.ClientTLSConfig{CertFile: cert, KeyFile: key}))
		if err == nil || !strings.Contains(err.Error(), "tls {} block") {
			t.Fatalf("err = %v", err)
		}
	})
}
