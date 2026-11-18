package nats

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// testCA is a throwaway certificate authority for route TLS tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue writes a leaf certificate for 127.0.0.1/localhost, usable as server
// and client, to dir/<name>.crt and dir/<name>.key, and returns the paths.
func (ca *testCA) issue(t *testing.T, dir, name string, serial int64) (string, string) {
	t.Helper()
	return ca.issueFor(t, dir, name, serial, []net.IP{net.ParseIP("127.0.0.1")})
}

// issueFor is issue with the given IP SANs (nil: a certificate for
// "localhost" only, which does not match a route URL with 127.0.0.1).
func (ca *testCA) issueFor(t *testing.T, dir, name string, serial int64, ips []net.IP) (string, string) {
	t.Helper()
	return ca.issueWith(t, dir, name, serial, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
}

func (ca *testCA) issueWith(t *testing.T, dir, name string, serial int64, ips []net.IP, usages []x509.ExtKeyUsage) (string, string) {
	t.Helper()
	return ca.sign(t, dir, name, &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		DNSNames:     []string{"localhost"},
		IPAddresses:  ips,
	})
}

// issueExpired issues a route certificate that expired a minute ago.
func (ca *testCA) issueExpired(t *testing.T, dir, name string, serial int64) (string, string) {
	t.Helper()
	return ca.sign(t, dir, name, &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-2 * time.Hour),
		NotAfter:     time.Now().Add(-time.Minute),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	})
}

// sign issues tmpl from the CA with a new key and writes the certificate and
// key files.
func (ca *testCA) sign(t *testing.T, dir, name string, tmpl *x509.Certificate) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	writeFile(t, certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFile(t, keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}))
	return certFile, keyFile
}

func (ca *testCA) write(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "ca.crt")
	writeFile(t, p, ca.pem)
	return p
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestApplyClusterTLS(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	caFile := ca.write(t, dir)
	certFile, keyFile := ca.issue(t, dir, "node-a", 2)
	clustered := func() *server.Options { return &server.Options{Cluster: server.ClusterOpts{Port: 6222}} }

	t.Run("empty leaves the options unchanged", func(t *testing.T) {
		opts := clustered()
		if err := applyClusterTLS(opts, "", "", "", nil); err != nil || opts.Cluster.TLSConfig != nil {
			t.Fatalf("err = %v, TLSConfig = %v", err, opts.Cluster.TLSConfig)
		}
	})

	t.Run("mutual TLS with the CA on both sides", func(t *testing.T) {
		opts := clustered()
		if err := applyClusterTLS(opts, certFile, keyFile, caFile, nil); err != nil {
			t.Fatal(err)
		}
		c := opts.Cluster.TLSConfig
		if c == nil || c.ClientAuth != tls.RequireAndVerifyClientCert || c.RootCAs == nil || c.ClientCAs == nil {
			t.Fatalf("TLS config = %+v", c)
		}
		if len(c.Certificates) != 0 || c.GetCertificate == nil || c.GetClientCertificate == nil {
			t.Fatal("the certificate must be served through the callbacks")
		}
		cert, err := c.GetCertificate(&tls.ClientHelloInfo{})
		if err != nil || cert.Leaf == nil || cert.Leaf.Subject.CommonName != "node-a" {
			t.Fatalf("GetCertificate = %v, %v", cert, err)
		}
	})

	reject := []struct {
		name           string
		opts           *server.Options
		cert, key, ca  string
		wantErrContain string
	}{
		{"missing CA", clustered(), certFile, keyFile, "", "needs a certificate, a key and a CA"},
		{"no clustering", &server.Options{}, certFile, keyFile, caFile, "clustering is not enabled"},
		{"config file tls block", &server.Options{Cluster: server.ClusterOpts{Port: 6222, TLSConfig: &tls.Config{}}}, certFile, keyFile, caFile, "cluster { tls {} } block"},
		{"missing certificate file", clustered(), filepath.Join(dir, "nope.crt"), keyFile, caFile, "cluster TLS"},
		{"key does not match", clustered(), certFile, caFile, caFile, "cluster TLS"},
	}
	for _, tt := range reject {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			err := applyClusterTLS(tt.opts, tt.cert, tt.key, tt.ca, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrContain) {
				t.Fatalf("err = %v, want %q", err, tt.wantErrContain)
			}
		})
	}
}

// TestApplyClusterTLS_CertificateChecks: the route certificate must allow
// both TLS uses and chain to the route CA, and be valid now (the reloader of
// pkg/certreload, configured for routes).
func TestApplyClusterTLS_CertificateChecks(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	caFile := ca.write(t, dir)
	other := newTestCA(t, "other-ca")
	clustered := func() *server.Options { return &server.Options{Cluster: server.ClusterOpts{Port: 6222}} }

	serverOnlyCert, serverOnlyKey := ca.issueWith(t, t.TempDir(), "server-only", 6, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	otherCert, otherKey := other.issue(t, t.TempDir(), "node", 5)
	expiredCert, expiredKey := ca.issueExpired(t, t.TempDir(), "node", 7)
	for _, tt := range []struct {
		name, cert, key, want string
	}{
		{"server use only", serverOnlyCert, serverOnlyKey, "certificate is not valid for TLS client use with the route CA"},
		{"another CA", otherCert, otherKey, "with the route CA"},
		{"expired", expiredCert, expiredKey, "certificate has expired"},
	} {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			err := applyClusterTLS(clustered(), tt.cert, tt.key, caFile, nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}

	// On reload, a certificate from another CA is refused with the route
	// wording, and the previous one is kept.
	old := clusterTLSCheckInterval
	clusterTLSCheckInterval = time.Nanosecond
	t.Cleanup(func() { clusterTLSCheckInterval = old })
	certFile, keyFile := ca.issue(t, dir, "node", 10)
	logger := newMockLogger()
	opts := clustered()
	if err := applyClusterTLS(opts, certFile, keyFile, caFile, logger); err != nil {
		t.Fatal(err)
	}
	other.issue(t, dir, "node", 99)
	cert, err := opts.Cluster.TLSConfig.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || cert.Leaf.SerialNumber.Int64() != 10 {
		t.Fatalf("serial = %v, err = %v; want the previous certificate", cert.Leaf.SerialNumber, err)
	}
	if !logger.hasMessage("WARN", "Could not reload the cluster TLS certificate; keeping the previous one") {
		t.Errorf("warn lines = %v", logger.warnCalls)
	}
	ca.issue(t, dir, "node", 11)
	if cert, _ := opts.Cluster.TLSConfig.GetClientCertificate(&tls.CertificateRequestInfo{}); cert.Leaf.SerialNumber.Int64() != 11 {
		t.Errorf("serial = %v, want the renewed certificate", cert.Leaf.SerialNumber)
	}
	if !logger.hasMessage("INFO", "Reloaded the cluster TLS certificate") || !logger.hasMessage("INFO", "Cluster TLS enabled") {
		t.Errorf("info lines = %v", logger.infoCalls)
	}
}

// TestClusterTLS_LiveRenewal renews a server's certificate while it runs:
// routes formed later use the new certificate, and a certificate from
// another CA is refused without breaking the cluster.
func TestClusterTLS_LiveRenewal(t *testing.T) {
	old := clusterTLSCheckInterval
	clusterTLSCheckInterval = time.Nanosecond
	t.Cleanup(func() { clusterTLSCheckInterval = old })

	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	caFile := ca.write(t, dir)
	aCert, aKey := ca.issue(t, dir, "node-a", 20)
	bCert, bKey := ca.issue(t, dir, "node-b", 30)

	pa, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	pb, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	routes := []string{fmt.Sprintf("nats://127.0.0.1:%d", pa), fmt.Sprintf("nats://127.0.0.1:%d", pb)}
	start := func(port int, cert, key string) *natsManager {
		t.Helper()
		m, err := startClusterNode(t, 0, WithClustering("c", "127.0.0.1", port, routes), WithClusterTLS(cert, key, caFile))
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	a := start(pa, aCert, aKey)
	b := start(pb, bCert, bKey)
	if !waitForRoutes(a, 1, 5*time.Second) {
		t.Fatal("no route between a and b")
	}
	restartB := func() {
		t.Helper()
		_ = b.Stop(context.Background())
		b = start(pb, bCert, bKey)
		if !waitForRoutes(b, 1, 5*time.Second) {
			t.Fatal("b did not route to a again")
		}
	}

	// Renew a's certificate from the same CA, then make b reconnect.
	ca.issue(t, dir, "node-a", 21)
	restartB()
	al := a.logger.(*mockLogger)
	al.mu.Lock()
	reloaded := strings.Contains(strings.Join(al.infoCalls, "|"), "Reloaded the cluster TLS certificate")
	al.mu.Unlock()
	if !reloaded {
		t.Error("a did not reload its renewed certificate")
	}

	// A certificate from another CA on a: refused, a keeps the previous one,
	// and b can still route to it.
	other := newTestCA(t, "other-ca")
	other.issue(t, dir, "node-a", 22)
	restartB()
	al.mu.Lock()
	warned := strings.Contains(strings.Join(al.warnCalls, "|"), "Could not reload the cluster TLS certificate")
	al.mu.Unlock()
	if !warned {
		t.Error("a did not refuse the certificate from another CA")
	}
}

// TestClusterTLS_HostMismatch: a server whose certificate does not name the
// route URL's host cannot be dialed by its peers.
func TestClusterTLS_HostMismatch(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	caFile := ca.write(t, dir)
	aCert, aKey := ca.issueFor(t, dir, "node-a", 40, nil) // "localhost" only
	bCert, bKey := ca.issue(t, dir, "node-b", 41)
	pa, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	a, err := startClusterNode(t, 0, WithClustering("c", "127.0.0.1", pa, []string{fmt.Sprintf("nats://127.0.0.1:%d", pa)}), WithClusterTLS(aCert, aKey, caFile))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	// b dials a by 127.0.0.1, which a's certificate does not name.
	if _, err := startClusterNode(t, 0, WithClustering("c", "127.0.0.1", pb, []string{fmt.Sprintf("nats://127.0.0.1:%d", pa)}), WithClusterTLS(bCert, bKey, caFile)); err != nil {
		t.Fatal(err)
	}
	if routeAccepted(a, 0, 2*time.Second) {
		t.Error("a route was formed although a's certificate does not name the route host")
	}
}

func TestWithClusterTLS(t *testing.T) {
	cfg := &NATSConfig{}
	if err := WithClusterTLS("c", "k", "")(cfg); err == nil {
		t.Error("empty CA accepted")
	}
	if err := WithClusterTLS("c", "k", "ca")(cfg); err != nil || cfg.ClusterTLSCAFile != "ca" {
		t.Errorf("err = %v, cfg = %+v", err, cfg)
	}
}
