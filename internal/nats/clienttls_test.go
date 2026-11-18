package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/pkg/types"
)

func TestApplyClientTLS(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	clientCA := newTestCA(t, "client-ca")
	routeCA := newTestCA(t, "route-ca")
	dir := t.TempDir()
	certFile, keyFile := serverCA.issue(t, dir, "server", 2)
	clientCAFile := clientCA.write(t, t.TempDir())
	routeCAFile := routeCA.write(t, t.TempDir())

	t.Run("nil leaves the options unchanged", func(t *testing.T) {
		opts := &server.Options{}
		if err := applyClientTLS(opts, nil, "", nil); err != nil || opts.TLSConfig != nil {
			t.Fatalf("err = %v, TLSConfig = %v", err, opts.TLSConfig)
		}
	})

	t.Run("TLS without client certificates", func(t *testing.T) {
		opts := &server.Options{}
		if err := applyClientTLS(opts, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile}, "", newMockLogger()); err != nil {
			t.Fatal(err)
		}
		c := opts.TLSConfig
		if c == nil || c.ClientAuth != tls.NoClientCert || opts.TLSVerify {
			t.Fatalf("TLS config = %+v, TLSVerify = %v", c, opts.TLSVerify)
		}
		if len(c.Certificates) != 0 || c.GetCertificate == nil {
			t.Fatal("the certificate must be served through GetCertificate")
		}
		if cert, err := c.GetCertificate(&tls.ClientHelloInfo{}); err != nil || cert.Leaf.Subject.CommonName != "server" {
			t.Fatalf("GetCertificate = %v, %v", cert, err)
		}
	})

	t.Run("mutual TLS", func(t *testing.T) {
		opts := &server.Options{}
		cfg := &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: clientCAFile}
		if err := applyClientTLS(opts, cfg, routeCAFile, nil); err != nil {
			t.Fatal(err)
		}
		if c := opts.TLSConfig; c.ClientAuth != tls.RequireAndVerifyClientCert || c.ClientCAs == nil || !opts.TLSVerify {
			t.Fatalf("TLS config = %+v, TLSVerify = %v", c, opts.TLSVerify)
		}
	})

	clientOnlyCert, clientOnlyKey := serverCA.issueWith(t, t.TempDir(), "client-only", 3, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	expiredCert, expiredKey := serverCA.issueExpired(t, t.TempDir(), "expired", 4)
	// A bundle that holds the route CA next to another CA.
	bundle := filepath.Join(t.TempDir(), "bundle.crt")
	writeFile(t, bundle, append(append([]byte{}, clientCA.pem...), routeCA.pem...))
	underRouteFile := routeCA.intermediate(t, "under-route").write(t, t.TempDir())
	underClientFile := clientCA.intermediate(t, "under-client").write(t, t.TempDir())
	// An expired intermediate under the route CA is still the route's
	// authority: the check is about the signature chain, not the time.
	expiredUnderRouteFile := routeCA.intermediateAt(t, "expired-under-route", time.Now().Add(-3*time.Hour), time.Now().Add(-2*time.Hour)).write(t, t.TempDir())
	notPEM := filepath.Join(t.TempDir(), "not.pem")
	writeFile(t, notPEM, []byte("not a certificate"))
	for _, tt := range []struct {
		name  string
		opts  *server.Options
		cfg   *types.ClientTLSConfig
		route string
		want  string
	}{
		{"missing key", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile}, "", "needs a certificate file and a key file"},
		{"separate CAs pass the check (another failure follows)", &server.Options{AllowNonTLS: true}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: clientCAFile}, routeCAFile, "allow_non_tls"},
		{"config file tls block", &server.Options{TLSConfig: &tls.Config{}}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile}, "", "tls {} block"},
		{"missing certificate file", &server.Options{}, &types.ClientTLSConfig{CertFile: filepath.Join(dir, "nope.crt"), KeyFile: keyFile}, "", "client TLS"},
		{"missing client CA file", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: filepath.Join(dir, "nope.crt")}, "", "client TLS"},
		{"client use only", &server.Options{}, &types.ClientTLSConfig{CertFile: clientOnlyCert, KeyFile: clientOnlyKey}, "", "not valid for TLS server use"},
		{"expired", &server.Options{}, &types.ClientTLSConfig{CertFile: expiredCert, KeyFile: expiredKey}, "", "has expired"},
		{"client CA is the route CA", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: routeCAFile}, routeCAFile, "is, or is signed by, the route CA"},
		{"client CA is an intermediate under the route CA", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: underRouteFile}, routeCAFile, "CN=under-route"},
		{"client CA is an expired intermediate under the route CA", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: expiredUnderRouteFile}, routeCAFile, "CN=expired-under-route"},
		{"route CA is an intermediate under the client CA", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: clientCAFile}, underClientFile, "is signed by, the client CA"},
		{"client CA is the route CA of a config file cluster tls block", &server.Options{Cluster: server.ClusterOpts{TLSConfig: &tls.Config{ClientCAs: routeCA.pool()}}}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: routeCAFile}, "", "the route CA"},
		{"config file allow_non_tls", &server.Options{AllowNonTLS: true}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile}, "", "allow_non_tls"},
		{"config file ocsp", &server.Options{OCSPConfig: &server.OCSPConfig{Mode: server.OCSPModeAuto}}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile}, "", "ocsp"},
		{"client CA without a certificate", &server.Options{}, &types.ClientTLSConfig{ClientCAFile: clientCAFile}, "", "client TLS: client TLS needs a certificate file and a key file"},
		{"client CA bundle holds the route CA", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: bundle}, routeCAFile, "CN=route-ca"},
		{"client CA file without a certificate", &server.Options{}, &types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: notPEM}, routeCAFile, "no certificate found"},
	} {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			err := applyClientTLS(tt.opts, tt.cfg, tt.route, nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewNATSManager_ClientTLSCombinations(t *testing.T) {
	cfg := &types.ClientTLSConfig{CertFile: "c", KeyFile: "k"}
	autoTLS := &types.AutoTLSConfig{Domains: []string{"example.com"}, CacheDir: t.TempDir(), AcceptTOS: true}
	for _, tt := range []struct {
		name string
		opts []NATSOption
		want string
	}{
		{"AutoTLS", []NATSOption{WithInProcessConn(), WithAutoTLS(autoTLS), WithClientTLS(cfg)}, "cannot be combined with AutoTLS"},
		{"DontListen", []NATSOption{WithInProcessConn(), WithDontListen(), WithClientTLS(cfg)}, "cannot be combined with DontListen"},
		{"TCP client", []NATSOption{WithClientTLS(cfg)}, "requires UseInProcessConn"},
		{"nil config", []NATSOption{WithInProcessConn(), WithClientTLS(nil)}, "cannot be nil"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNATSManager(newMockLogger(), tt.opts...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestNATSManager_ClientTLS runs a server with mutual TLS on the client
// listener: the framework's own in-process client works without a
// certificate, TCP clients need one from the client CA, and a renewed server
// certificate is served to new connections.
func TestNATSManager_ClientTLS(t *testing.T) {
	old := clientTLSCheckInterval
	clientTLSCheckInterval = time.Nanosecond
	t.Cleanup(func() { clientTLSCheckInterval = old })

	serverCA := newTestCA(t, "server-ca")
	clientCA := newTestCA(t, "client-ca")
	otherCA := newTestCA(t, "other-ca")
	dir := t.TempDir()
	serverCAFile := serverCA.write(t, t.TempDir())
	clientCAFile := clientCA.write(t, t.TempDir())
	certFile, keyFile := serverCA.issue(t, dir, "server", 10)
	clientCert, clientKey := clientCA.issueWith(t, t.TempDir(), "client", 20, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	otherCert, otherKey := otherCA.issueWith(t, t.TempDir(), "stranger", 30, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	m, err := startClusterNode(t, 0,
		WithInProcessConn(),
		WithJetStream(t.TempDir()),
		WithSystemAccount("admin", "s3cret"),
		WithClientTLS(&types.ClientTLSConfig{CertFile: certFile, KeyFile: keyFile, ClientCAFile: clientCAFile}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// The framework's own client: in-process, no certificate, JetStream works.
	conn, err := m.Connection()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Request("$JS.API.INFO", nil, 2*time.Second); err != nil {
		t.Fatalf("in-process client: %v", err)
	}

	info := m.ServerInfo()
	if !strings.HasPrefix(info.ClientURL, "tls://") {
		t.Errorf("ClientURL = %q, want tls://", info.ClientURL)
	}
	url := fmt.Sprintf("tls://127.0.0.1:%d", info.Port)
	connect := func(opts ...natsgo.Option) (*natsgo.Conn, error) {
		return natsgo.Connect(url, append([]natsgo.Option{natsgo.NoReconnect(), natsgo.Timeout(2 * time.Second)}, opts...)...)
	}

	t.Run("client with a certificate from the client CA", func(t *testing.T) {
		nc, err := connect(natsgo.RootCAs(serverCAFile), natsgo.ClientCert(clientCert, clientKey))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		if _, err := nc.Request("$JS.API.INFO", nil, 2*time.Second); err != nil {
			t.Fatalf("request: %v", err)
		}
	})
	t.Run("system account user with a client certificate", func(t *testing.T) {
		nc, err := connect(natsgo.RootCAs(serverCAFile), natsgo.ClientCert(clientCert, clientKey), natsgo.UserInfo("admin", "s3cret"))
		if err != nil {
			t.Fatal(err)
		}
		nc.Close()
	})
	for _, tt := range []struct {
		name string
		url  string
		opts []natsgo.Option
	}{
		{"without a certificate", url, []natsgo.Option{natsgo.RootCAs(serverCAFile)}},
		{"with a certificate from another CA", url, []natsgo.Option{natsgo.RootCAs(serverCAFile), natsgo.ClientCert(otherCert, otherKey)}},
	} {
		t.Run("refuses a client "+tt.name, func(t *testing.T) {
			nc, err := natsgo.Connect(tt.url, append([]natsgo.Option{natsgo.NoReconnect(), natsgo.Timeout(2 * time.Second)}, tt.opts...)...)
			if err == nil {
				// A client may complete its side of the handshake before the
				// server rejects the certificate: it must not stay usable.
				_, err = nc.Request("$JS.API.INFO", nil, time.Second)
				nc.Close()
			}
			if err == nil {
				t.Fatal("connection accepted")
			}
		})
	}

	t.Run("refuses a plaintext client", func(t *testing.T) {
		// A raw connection: nats.go would switch to TLS by itself when the
		// server's INFO says TLS is required.
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", info.Port), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil || !strings.HasPrefix(string(buf[:n]), "INFO ") || !strings.Contains(string(buf[:n]), `"tls_required":true`) {
			t.Fatalf("INFO = %q, %v", buf[:n], err)
		}
		if _, err := conn.Write([]byte("CONNECT {\"verbose\":false}\r\nPING\r\n")); err != nil {
			t.Fatal(err)
		}
		var got strings.Builder
		for {
			n, err := conn.Read(buf)
			got.Write(buf[:n])
			if err != nil {
				break
			}
		}
		if strings.Contains(got.String(), "PONG") {
			t.Fatalf("a plaintext client got %q", got.String())
		}
	})

	t.Run("varz reports TLS with client verification", func(t *testing.T) {
		v, err := m.server.Varz(nil)
		if err != nil {
			t.Fatal(err)
		}
		if !v.TLSRequired || !v.TLSVerify {
			t.Errorf("tls_required = %v, tls_verify = %v", v.TLSRequired, v.TLSVerify)
		}
	})

	t.Run("a renewed server certificate is served to new connections", func(t *testing.T) {
		serverCA.issue(t, dir, "server", 11)
		nc, err := connect(natsgo.RootCAs(serverCAFile), natsgo.ClientCert(clientCert, clientKey))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		state, err := nc.TLSConnectionState()
		if err != nil {
			t.Fatal(err)
		}
		if s := state.PeerCertificates[0].SerialNumber.Int64(); s != 11 {
			t.Errorf("serial = %d, want the renewed 11", s)
		}
		if !m.logger.(*mockLogger).hasMessage("INFO", "Reloaded the client TLS certificate") {
			t.Error("no reload logged")
		}
	})
	_ = m.Stop(context.Background())
}
