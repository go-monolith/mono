//go:build integration
// +build integration

package integration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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

	"github.com/go-monolith/mono"
)

// routeCA issues route certificates for 127.0.0.1 in a test directory.
type routeCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newRouteCA(t *testing.T, dir, name string) *routeCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	file := filepath.Join(dir, name+".crt")
	writePEM(t, file, "CERTIFICATE", der)
	return &routeCA{cert: cert, key: key, file: file}
}

func (ca *routeCA) issue(t *testing.T, dir, name string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
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
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", kder)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startRouteNode(t *testing.T, name string, clusterPort int, routes []string, extra ...mono.MonoFrameworkOption) mono.MonoApplication {
	t.Helper()
	opts := append([]mono.MonoFrameworkOption{
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithNATSServerName(name),
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(freeTCPPort(t)),
		mono.WithNATSInProcessConn(),
		mono.WithNATSClustering("tls-test", "127.0.0.1", clusterPort, routes),
		mono.WithNATSClusterAuth("route", "s3cret"),
	}, extra...)
	app, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("%s start: %v", name, err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	return app
}

// routed reports whether a request sent on b reaches a responder on a, which
// needs a route between the two servers.
func routed(t *testing.T, a, b mono.MonoApplication, subject string, wait time.Duration) bool {
	t.Helper()
	bus := a.EventBus("")
	sub, err := bus.Subscribe(subject, func(_ context.Context, m *mono.Msg) {
		_ = bus.Publish(m.Reply, []byte("pong"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := b.EventBus("").RequestWithContext(ctx, subject, nil)
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

// TestClusterTLS_MutualTLSRoutes: servers with certificates from the route CA
// form routes; a server whose certificate comes from another CA, and a
// server without TLS, are refused.
func TestClusterTLS_MutualTLSRoutes(t *testing.T) {
	dir := t.TempDir()
	ca := newRouteCA(t, dir, "route-ca")
	rogue := newRouteCA(t, dir, "rogue-ca")
	aCert, aKey := ca.issue(t, dir, "node-a")
	bCert, bKey := ca.issue(t, dir, "node-b")
	rCert, rKey := rogue.issue(t, dir, "node-r")

	pa, pb := freeTCPPort(t), freeTCPPort(t)
	routes := []string{fmt.Sprintf("nats://127.0.0.1:%d", pa), fmt.Sprintf("nats://127.0.0.1:%d", pb)}
	a := startRouteNode(t, "a", pa, routes, mono.WithNATSClusterTLS(aCert, aKey, ca.file))
	b := startRouteNode(t, "b", pb, routes, mono.WithNATSClusterTLS(bCert, bKey, ca.file))
	if !routed(t, a, b, "tls.ab", 5*time.Second) {
		t.Fatal("servers with certificates from the route CA did not form a route")
	}

	// A server of another CA (its own certificate and CA): it starts, but the
	// cluster refuses its certificate, and it refuses the cluster's.
	r := startRouteNode(t, "r", freeTCPPort(t), routes[:1], mono.WithNATSClusterTLS(rCert, rKey, rogue.file))
	if routed(t, a, r, "tls.ar", 2*time.Second) {
		t.Error("a server with a certificate from another CA was routed")
	}
	// No TLS at all.
	n := startRouteNode(t, "n", freeTCPPort(t), routes[:1])
	if routed(t, a, n, "tls.an", 2*time.Second) {
		t.Error("a server without TLS was routed")
	}
}

func TestClusterTLS_Validation(t *testing.T) {
	dir := t.TempDir()
	ca := newRouteCA(t, dir, "route-ca")
	cert, key := ca.issue(t, dir, "node")

	t.Run("requires clustering", func(t *testing.T) {
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSClusterTLS(cert, key, ca.file))
		if err == nil || !strings.Contains(err.Error(), "cluster TLS requires WithNATSClustering") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing file fails when the server starts", func(t *testing.T) {
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSPort(freeTCPPort(t)),
			mono.WithNATSClustering("c", "127.0.0.1", freeTCPPort(t), []string{"nats://127.0.0.1:1"}),
			mono.WithNATSClusterTLS(filepath.Join(dir, "missing.crt"), key, ca.file))
		if err == nil || !strings.Contains(err.Error(), "cluster TLS") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("own certificate not signed by the CA fails at start", func(t *testing.T) {
		other := newRouteCA(t, dir, "other-ca")
		oCert, oKey := other.issue(t, dir, "stranger")
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSPort(freeTCPPort(t)),
			mono.WithNATSClustering("c", "127.0.0.1", freeTCPPort(t), []string{"nats://127.0.0.1:1"}),
			mono.WithNATSClusterTLS(oCert, oKey, ca.file))
		if err == nil || !strings.Contains(err.Error(), "not signed by the route CA") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("conflicts with a config file tls block", func(t *testing.T) {
		conf := filepath.Join(dir, "nats.conf")
		body := fmt.Sprintf("cluster { tls { cert_file: %q, key_file: %q, ca_file: %q, verify: true } }\n", cert, key, ca.file)
		if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}),
			mono.WithNATSConfigFile(conf),
			mono.WithNATSPort(freeTCPPort(t)),
			mono.WithNATSClustering("c", "127.0.0.1", freeTCPPort(t), []string{"nats://127.0.0.1:1"}),
			mono.WithNATSClusterTLS(cert, key, ca.file))
		if err == nil || !strings.Contains(err.Error(), "cannot be combined with a cluster { tls {} } block") {
			t.Fatalf("err = %v", err)
		}
	})
}
