package certreload

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// testCA is a throwaway certificate authority.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
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
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

// leaf describes a certificate to issue.
type leaf struct {
	serial              int64
	usages              []x509.ExtKeyUsage
	unknownUsages       []asn1.ObjectIdentifier
	notBefore, notAfter time.Time
}

// issue writes the certificate and key of l to dir/node.crt and dir/node.key
// and returns the paths. Zero times mean valid from an hour ago for a day.
func (ca *testCA) issue(t *testing.T, dir string, l leaf) (string, string) {
	t.Helper()
	if l.notBefore.IsZero() {
		l.notBefore = time.Now().Add(-time.Hour)
	}
	if l.notAfter.IsZero() {
		l.notAfter = time.Now().Add(24 * time.Hour)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(l.serial), Subject: pkix.Name{CommonName: "node"},
		NotBefore: l.notBefore, NotAfter: l.notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: l.usages, UnknownExtKeyUsage: l.unknownUsages,
		DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, "node.crt"), filepath.Join(dir, "node.key")
	writeFile(t, certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFile(t, keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}))
	return certFile, keyFile
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

var both = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

// testLogger records messages by level.
type testLogger struct {
	mu          sync.Mutex
	info, warns []string
}

func (l *testLogger) Debug(string, ...any) {}
func (l *testLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.info = append(l.info, msg)
}
func (l *testLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *testLogger) Error(string, ...any)           {}
func (l *testLogger) With(...any) types.Logger       { return l }
func (l *testLogger) WithModule(string) types.Logger { return l }
func (l *testLogger) WithError(error) types.Logger   { return l }

func (l *testLogger) counts() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.info), len(l.warns)
}

// TestReloader walks a reloader with roots through renewals and refused
// updates, on a fake clock.
func TestReloader(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "route-ca")
	other := newTestCA(t, "other-ca")
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 10, usages: both})

	logger := &testLogger{}
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), RootsName: "route CA",
		Usages: both, Name: "cluster TLS", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	clock := r.checked
	r.now = func() time.Time { return clock }
	serial := func() int64 {
		t.Helper()
		return r.Certificate().Leaf.SerialNumber.Int64()
	}
	next := func() { clock = clock.Add(DefaultCheckInterval) }
	if s := serial(); s != 10 {
		t.Fatalf("serial = %d, want 10", s)
	}

	// A renewed certificate from the same CA.
	ca.issue(t, dir, leaf{serial: 11, usages: both})
	if s := serial(); s != 10 {
		t.Errorf("within the check interval the old certificate is kept, got %d", s)
	}
	next()
	if s := serial(); s != 11 {
		t.Errorf("after the interval the renewed certificate is served, got %d", s)
	}
	if info, _ := logger.counts(); info != 1 || logger.info[0] != "Reloaded the cluster TLS certificate" {
		t.Errorf("info lines = %v", logger.info)
	}

	// The same content written again (a sync that rewrites files): no reload.
	data, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, certFile, data)
	next()
	if s := serial(); s != 11 {
		t.Errorf("serial = %d after an unchanged rewrite", s)
	}
	if info, _ := logger.counts(); info != 1 {
		t.Errorf("unchanged content reloaded: info lines %v", logger.info)
	}

	// Each refused update keeps the previous certificate, warns once, and the
	// next valid one is loaded.
	refused := []struct {
		name  string
		write func()
	}{
		{"half-written update", func() { writeFile(t, keyFile, []byte("not a key")) }},
		{"another CA", func() { other.issue(t, dir, leaf{serial: 99, usages: both}) }},
		{"server use only", func() {
			ca.issue(t, dir, leaf{serial: 98, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
		}},
		{"expired", func() {
			ca.issue(t, dir, leaf{serial: 97, usages: both, notBefore: time.Now().Add(-2 * time.Hour), notAfter: time.Now().Add(-time.Minute)})
		}},
		{"missing file", func() {
			if err := os.Remove(certFile); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for i, tt := range refused {
		_, warns := logger.counts()
		tt.write()
		next()
		if s := serial(); s != int64(11+i) {
			t.Errorf("%s: serial = %d, want the previous %d", tt.name, s, 11+i)
		}
		if _, w := logger.counts(); w != warns+1 || logger.warns[w-1] != "Could not reload the cluster TLS certificate; keeping the previous one" {
			t.Errorf("%s: warn lines = %v", tt.name, logger.warns)
		}
		// A refused file is retried: the same bad content warns again.
		next()
		serial()
		if _, w := logger.counts(); w != warns+2 {
			t.Errorf("%s: a refused update must be retried at the next check, warn lines = %v", tt.name, logger.warns)
		}
		ca.issue(t, dir, leaf{serial: int64(12 + i), usages: both})
		next()
		if s := serial(); s != int64(12+i) {
			t.Errorf("%s: serial = %d, want %d once a valid certificate is in place", tt.name, s, 12+i)
		}
	}
	if got := r.NotAfter(); got.Before(time.Now()) {
		t.Errorf("NotAfter = %v", got)
	}
}

func TestNew(t *testing.T) {
	ca := newTestCA(t, "ca")
	other := newTestCA(t, "other")
	dir := t.TempDir()
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 1, usages: both})

	t.Run("rejects", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			cfg  func() Config
			want string
		}{
			{"no certificate file", func() Config { return Config{KeyFile: keyFile} }, "a certificate file and a key file are required"},
			{"no key file", func() Config { return Config{CertFile: certFile} }, "a certificate file and a key file are required"},
			{"negative interval", func() Config { return Config{CertFile: certFile, KeyFile: keyFile, CheckInterval: -time.Second} }, "must not be negative"},
			{"missing file", func() Config { return Config{CertFile: filepath.Join(dir, "nope"), KeyFile: keyFile} }, "certificate file"},
			{"key not matching", func() Config { return Config{CertFile: certFile, KeyFile: certFile} }, "loading certificate and key"},
			{"another CA", func() Config {
				c, k := other.issue(t, t.TempDir(), leaf{serial: 2, usages: both})
				return Config{CertFile: c, KeyFile: k, Roots: ca.pool(), RootsName: "route CA", Usages: both}
			}, "certificate is not valid for TLS server use with the route CA"},
			{"another CA, no usages", func() Config {
				c, k := other.issue(t, t.TempDir(), leaf{serial: 2, usages: both})
				return Config{CertFile: c, KeyFile: k, Roots: ca.pool()}
			}, "certificate is not valid with the CA"},
			{"client use missing", func() Config {
				c, k := ca.issue(t, t.TempDir(), leaf{serial: 3, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
				return Config{CertFile: c, KeyFile: k, Roots: ca.pool(), Usages: both}
			}, "TLS client use with the CA"},
			{"server use missing, no roots", func() Config {
				c, k := ca.issue(t, t.TempDir(), leaf{serial: 4, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
				return Config{CertFile: c, KeyFile: k, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			}, "certificate is not valid for TLS server use"},
			{"only an unknown usage, no roots", func() Config {
				c, k := ca.issue(t, t.TempDir(), leaf{serial: 9, unknownUsages: []asn1.ObjectIdentifier{{1, 2, 3, 4}}})
				return Config{CertFile: c, KeyFile: k, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			}, "certificate is not valid for TLS server use"},
			{"not yet valid, no roots", func() Config {
				c, k := ca.issue(t, t.TempDir(), leaf{serial: 5, usages: both, notBefore: time.Now().Add(time.Hour), notAfter: time.Now().Add(2 * time.Hour)})
				return Config{CertFile: c, KeyFile: k}
			}, "has expired or is not yet valid"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(tt.cfg())
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("err = %v, want %q", err, tt.want)
				}
			})
		}
	})

	t.Run("expired is an x509 expiry error, with and without roots", func(t *testing.T) {
		c, k := ca.issue(t, t.TempDir(), leaf{serial: 6, usages: both, notBefore: time.Now().Add(-2 * time.Hour), notAfter: time.Now().Add(-time.Minute)})
		for _, roots := range []*x509.CertPool{nil, ca.pool()} {
			var invalid x509.CertificateInvalidError
			if _, err := New(Config{CertFile: c, KeyFile: k, Roots: roots, Usages: both}); !errors.As(err, &invalid) || invalid.Reason != x509.Expired {
				t.Errorf("roots %v: err = %v, want an expiry error", roots != nil, err)
			}
		}
	})

	t.Run("accepts", func(t *testing.T) {
		noEKU, noEKUKey := ca.issue(t, t.TempDir(), leaf{serial: 7})
		anyEKU, anyEKUKey := ca.issue(t, t.TempDir(), leaf{serial: 8, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
		serverOnly, serverOnlyKey := ca.issue(t, t.TempDir(), leaf{serial: 10, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
		sgc, sgcKey := ca.issue(t, t.TempDir(), leaf{serial: 11, usages: []x509.ExtKeyUsage{x509.ExtKeyUsageNetscapeServerGatedCrypto}})
		for _, tt := range []struct {
			name string
			cfg  Config
		}{
			{"defaults", Config{CertFile: certFile, KeyFile: keyFile}},
			{"roots and both usages", Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), Usages: both}},
			{"no extended key usage, no roots", Config{CertFile: noEKU, KeyFile: noEKUKey, Usages: both}},
			{"no extended key usage, roots", Config{CertFile: noEKU, KeyFile: noEKUKey, Roots: ca.pool(), Usages: both}},
			{"any usage, no roots", Config{CertFile: anyEKU, KeyFile: anyEKUKey, Usages: both}},
			{"Usages any, server only, no roots", Config{CertFile: serverOnly, KeyFile: serverOnlyKey, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}},
			{"Usages any, server only, roots", Config{CertFile: serverOnly, KeyFile: serverOnlyKey, Roots: ca.pool(), Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}},
			{"server gated crypto for server use, no roots", Config{CertFile: sgc, KeyFile: sgcKey, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				r, err := New(tt.cfg)
				if err != nil {
					t.Fatal(err)
				}
				if r.cfg.CheckInterval != DefaultCheckInterval || r.cfg.Name != "TLS" {
					t.Errorf("defaults not applied: %+v", r.cfg)
				}
			})
		}
	})
}

// TestReloader_TLSHandshake serves the certificate to a real handshake
// through the callbacks and picks up a renewal.
func TestReloader_TLSHandshake(t *testing.T) {
	ca := newTestCA(t, "ca")
	dir := t.TempDir()
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 1, usages: both})
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), Usages: both, CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	handshake := func() int64 {
		t.Helper()
		ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			GetCertificate: r.GetCertificate, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.pool(), MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}()
		conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
			GetClientCertificate: r.GetClientCertificate, RootCAs: ca.pool(), ServerName: "localhost", MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
	}
	if s := handshake(); s != 1 {
		t.Fatalf("serial = %d, want 1", s)
	}
	ca.issue(t, dir, leaf{serial: 2, usages: both})
	if s := handshake(); s != 2 {
		t.Fatalf("serial = %d after renewal, want 2", s)
	}
}

// TestReloader_NoRoots: without roots a reload is checked against the
// validity period and the usages, on the reloader's clock.
func TestReloader_NoRoots(t *testing.T) {
	ca := newTestCA(t, "ca")
	dir := t.TempDir()
	server := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 1, usages: server})
	logger := &testLogger{}
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Usages: server, Name: "client TLS", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	clock := r.checked
	r.now = func() time.Time { return clock }

	// A certificate valid from two hours from now is not valid yet.
	ca.issue(t, dir, leaf{serial: 2, usages: server, notBefore: time.Now().Add(2 * time.Hour), notAfter: time.Now().Add(4 * time.Hour)})
	clock = clock.Add(DefaultCheckInterval)
	if s := r.Certificate().Leaf.SerialNumber.Int64(); s != 1 {
		t.Fatalf("serial = %d, a certificate not yet valid must not be served", s)
	}
	if _, w := logger.counts(); w != 1 || logger.warns[0] != "Could not reload the client TLS certificate; keeping the previous one" {
		t.Fatalf("warn lines = %v", logger.warns)
	}
	// Three hours later on the reloader's clock it is, and it is loaded.
	clock = clock.Add(3 * time.Hour)
	if s := r.Certificate().Leaf.SerialNumber.Int64(); s != 2 {
		t.Fatalf("serial = %d, want 2 once it is valid", s)
	}
}

// TestReloader_Concurrent calls the reloader from many goroutines while the
// files change (run with -race).
func TestReloader_Concurrent(t *testing.T) {
	ca := newTestCA(t, "ca")
	dir := t.TempDir()
	certFile, keyFile := ca.issue(t, dir, leaf{serial: 1, usages: both})
	r, err := New(Config{CertFile: certFile, KeyFile: keyFile, Roots: ca.pool(), Usages: both, CheckInterval: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if c, err := r.GetCertificate(nil); err != nil || c == nil || c.Leaf == nil {
					t.Errorf("GetCertificate = %v, %v", c, err)
					return
				}
				_ = r.NotAfter()
			}
		}()
	}
	for serial := int64(2); serial < 6; serial++ {
		ca.issue(t, dir, leaf{serial: serial, usages: both})
	}
	wg.Wait()
	if c, _ := r.GetClientCertificate(nil); c.Leaf.SerialNumber.Int64() != 5 {
		t.Errorf("serial = %d, want the last certificate", c.Leaf.SerialNumber.Int64())
	}
}
