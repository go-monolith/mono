package helper_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/pkg/helper"
)

type tlsCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newTLSCA(t *testing.T, dir, name string) *tlsCA {
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
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, name+".crt")
	writePEM(t, file, "CERTIFICATE", der)
	return &tlsCA{cert: cert, key: key, file: file}
}

// issue writes dir/<name>.crt and .key for 127.0.0.1 with the given usage
// and common name.
func (ca *tlsCA) issue(t *testing.T, dir, name, cn string, usage x509.ExtKeyUsage) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
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

// startMTLSServer starts a nats-server that requires client certificates
// from clientCA.
func startMTLSServer(t *testing.T, serverCA, clientCA *tlsCA) *server.Server {
	t.Helper()
	dir := t.TempDir()
	certFile, keyFile := serverCA.issue(t, dir, "server", "server", x509.ExtKeyUsageServerAuth)
	tc, err := server.GenTLSConfig(&server.TLSConfigOpts{CertFile: certFile, KeyFile: keyFile, CaFile: clientCA.file, Verify: true})
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true, TLSConfig: tc, TLSVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// peerCN returns the common name of the client certificate the server sees
// on its only client connection, waiting until there is exactly one (after a
// reconnect the server may still list the old connection for a moment).
func peerCN(t *testing.T, s *server.Server) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cz, err := s.Connz(&server.ConnzOptions{Username: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(cz.Conns) == 1 && len(cz.Conns[0].TLSPeerCerts) > 0 {
			return cz.Conns[0].TLSPeerCerts[0].Subject
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections = %+v", cz.Conns)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNATSClientTLS(t *testing.T) {
	dir := t.TempDir()
	serverCA := newTLSCA(t, dir, "server-ca")
	clientCA := newTLSCA(t, dir, "client-ca")
	s := startMTLSServer(t, serverCA, clientCA)
	certDir := t.TempDir()
	certFile, keyFile := clientCA.issue(t, certDir, "client", "client-1", x509.ExtKeyUsageClientAuth)

	opt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{
		CAFile: serverCA.file, CertFile: certFile, KeyFile: keyFile, CheckInterval: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnected := make(chan struct{}, 1)
	nc, err := nats.Connect(s.ClientURL(), opt, nats.ReconnectWait(10*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if cn := peerCN(t, s); !strings.Contains(cn, "client-1") {
		t.Fatalf("client certificate = %q, want client-1", cn)
	}

	// Renew the client certificate, then reconnect: the new one is presented
	// without recreating the connection.
	clientCA.issue(t, certDir, "client", "client-2", x509.ExtKeyUsageClientAuth)
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect")
	}
	if cn := peerCN(t, s); !strings.Contains(cn, "client-2") {
		t.Errorf("client certificate after renewal = %q, want client-2", cn)
	}
}

func TestNATSClientTLS_ServerOnly(t *testing.T) {
	dir := t.TempDir()
	serverCA := newTLSCA(t, dir, "server-ca")
	certFile, keyFile := serverCA.issue(t, dir, "server", "server", x509.ExtKeyUsageServerAuth)
	tc, err := server.GenTLSConfig(&server.TLSConfigOpts{CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true, TLSConfig: tc})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	defer s.Shutdown()

	opt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: serverCA.file})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(s.ClientURL(), opt)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// A server certificate from another CA is refused.
	other := newTLSCA(t, dir, "other-ca")
	opt, err = helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: other.file})
	if err != nil {
		t.Fatal(err)
	}
	if nc, err := nats.Connect(s.ClientURL(), opt, nats.NoReconnect()); err == nil {
		nc.Close()
		t.Fatal("a server certificate from another CA was accepted")
	}
}

func TestNATSClientTLS_Errors(t *testing.T) {
	dir := t.TempDir()
	ca := newTLSCA(t, dir, "ca")
	certFile, keyFile := ca.issue(t, dir, "client", "client", x509.ExtKeyUsageClientAuth)
	serverCert, serverKey := ca.issue(t, dir, "server", "server", x509.ExtKeyUsageServerAuth)
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		cfg  helper.NATSClientTLSConfig
		want string
	}{
		{"no CA", helper.NATSClientTLSConfig{CertFile: certFile, KeyFile: keyFile}, "needs a CA file"},
		{"certificate without key", helper.NATSClientTLSConfig{CAFile: ca.file, CertFile: certFile}, "both a certificate file and a key file"},
		{"key without certificate", helper.NATSClientTLSConfig{CAFile: ca.file, KeyFile: keyFile}, "both a certificate file and a key file"},
		{"missing CA file", helper.NATSClientTLSConfig{CAFile: filepath.Join(dir, "nope")}, "CA file"},
		{"CA file without a certificate", helper.NATSClientTLSConfig{CAFile: notPEM}, "no certificate found"},
		{"server certificate as client certificate", helper.NATSClientTLSConfig{CAFile: ca.file, CertFile: serverCert, KeyFile: serverKey}, "not valid for TLS client use"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := helper.NATSClientTLS(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
	// The option is usable with tls.Config defaults (TLS 1.2 minimum).
	opt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{CAFile: ca.file, CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	var o nats.Options
	if err := opt(&o); err != nil || !o.Secure || o.TLSCertCB == nil || o.RootCAsCB == nil || o.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("options = %+v, err = %v", o, err)
	}
}
