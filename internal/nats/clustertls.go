package nats

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/go-monolith/mono/pkg/types"
)

// clusterTLSCheckInterval is the least time between two checks of the
// certificate files for a renewed certificate. A handshake that comes later
// stats the files and reloads them when they changed.
var clusterTLSCheckInterval = 10 * time.Second

// applyClusterTLS makes every route between the servers of the cluster use
// mutual TLS: each server presents certFile/keyFile, requires a certificate
// from every peer, and accepts only peers whose certificate chains to caFile
// (both on routes it accepts and on routes it opens, where the peer must also
// match the route URL's host). This mirrors a config file's
// cluster { tls { cert_file, key_file, ca_file, verify: true } } block.
//
// The certificate and key are reloaded from disk when their contents change
// (checked at most every clusterTLSCheckInterval, on the next handshake), so a
// renewed certificate (for example a Kubernetes Secret updated by
// cert-manager) is used for new routes without a restart. Routes already
// established keep their session. The CA is read once: a certificate that does
// not chain to it is refused (at start, and on reload, where the previous
// certificate is kept), and changing the CA needs a restart.
//
// Empty paths leave opts unchanged.
func applyClusterTLS(opts *server.Options, certFile, keyFile, caFile string, logger types.Logger) error {
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return errors.New("cluster TLS needs a certificate, a key and a CA file")
	}
	if opts.Cluster.Port == 0 {
		return errors.New("cluster TLS is set but clustering is not enabled: " +
			"use WithNATSClustering or a cluster{} block in the NATS config file")
	}
	if opts.Cluster.TLSConfig != nil {
		return errors.New("WithNATSClusterTLS cannot be combined with a cluster { tls {} } block in the NATS config file: " +
			"use one of the two")
	}

	config, err := server.GenTLSConfig(&server.TLSConfigOpts{
		CertFile: certFile,
		KeyFile:  keyFile,
		CaFile:   caFile,
		Verify:   true,
	})
	if err != nil {
		return fmt.Errorf("cluster TLS: %w", err)
	}
	// As nats-server does for a cluster tls{} block: require a certificate
	// from every peer, and verify peers we dial against the same CA.
	config.ClientAuth = tls.RequireAndVerifyClientCert
	config.RootCAs = config.ClientCAs

	reloader, err := newCertReloader(certFile, keyFile, config.ClientCAs, logger)
	if err != nil {
		return fmt.Errorf("cluster TLS: %w", err)
	}
	if logger != nil {
		// nats-server's /varz cannot show the expiry of a certificate served
		// through callbacks, so log it.
		logger.Info("Cluster TLS enabled", "cert_file", certFile, "not_after", reloader.notAfter())
	}
	// Serve the certificate through callbacks so a renewed one is picked up.
	// Certificates must stay empty: Go uses GetCertificate only when it is.
	config.Certificates = nil
	config.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return reloader.current() }
	config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return reloader.current() }

	opts.Cluster.TLSConfig = config
	return nil
}

// certReloader serves a certificate key pair loaded from files and reloads it
// when the files change. A certificate is only accepted when it chains to the
// route CA: a renewed certificate from another CA would be served to peers
// that still trust only the old CA, and every route that reconnects would
// then fail. It is safe for concurrent use.
type certReloader struct {
	certFile, keyFile string
	roots             *x509.CertPool
	logger            types.Logger
	now               func() time.Time

	mu      sync.Mutex
	cert    *tls.Certificate
	digest  [sha256.Size]byte // of the certificate and key files' contents
	checked time.Time
}

func newCertReloader(certFile, keyFile string, roots *x509.CertPool, logger types.Logger) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile, roots: roots, logger: logger, now: time.Now}
	certPEM, keyPEM, err := r.read()
	if err != nil {
		return nil, err
	}
	cert, err := r.parse(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	r.cert, r.digest, r.checked = cert, digestOf(certPEM, keyPEM), r.now()
	return r, nil
}

func digestOf(certPEM, keyPEM []byte) [sha256.Size]byte {
	h := sha256.New()
	h.Write(certPEM)
	h.Write([]byte{0})
	h.Write(keyPEM)
	var d [sha256.Size]byte
	copy(d[:], h.Sum(nil))
	return d
}

func (r *certReloader) read() ([]byte, []byte, error) {
	certPEM, err := os.ReadFile(r.certFile)
	if err != nil {
		return nil, nil, fmt.Errorf("certificate file: %w", err)
	}
	keyPEM, err := os.ReadFile(r.keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("key file: %w", err)
	}
	return certPEM, keyPEM, nil
}

// parse builds the key pair and checks that its leaf chains to the route CA
// (intermediates in the certificate file are used for the chain).
func (r *certReloader) parse(certPEM, keyPEM []byte) (*tls.Certificate, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("loading certificate and key: %w", err)
	}
	leaf := pair.Leaf
	if leaf == nil {
		if leaf, err = x509.ParseCertificate(pair.Certificate[0]); err != nil {
			return nil, fmt.Errorf("parsing certificate: %w", err)
		}
		pair.Leaf = leaf
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		if c, err := x509.ParseCertificate(der); err == nil {
			intermediates.AddCert(c)
		}
	}
	if r.roots != nil {
		// Each server is TLS server on the routes it accepts and TLS client on
		// the routes it opens, and peers verify both uses: a certificate
		// without one of them would make half of the routes fail.
		for _, use := range []struct {
			usage x509.ExtKeyUsage
			name  string
		}{{x509.ExtKeyUsageServerAuth, "server"}, {x509.ExtKeyUsageClientAuth, "client"}} {
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots:         r.roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{use.usage},
			}); err != nil {
				return nil, fmt.Errorf("certificate is not valid for TLS %s use with the route CA: %w", use.name, err)
			}
		}
	}
	return &pair, nil
}

// notAfter returns the expiry of the current certificate.
func (r *certReloader) notAfter() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert.Leaf.NotAfter
}

// current returns the certificate, reloading it first when the files'
// contents changed since the last check (at most every
// clusterTLSCheckInterval). A reload that fails, for example while a Secret
// update has written the certificate but not yet the key, or a certificate
// from another CA, keeps the previous certificate and is retried at the next
// check.
func (r *certReloader) current() (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.checked) < clusterTLSCheckInterval {
		return r.cert, nil
	}
	r.checked = now
	certPEM, keyPEM, err := r.read()
	if err != nil {
		r.warn(err)
		return r.cert, nil
	}
	digest := digestOf(certPEM, keyPEM)
	if digest == r.digest {
		return r.cert, nil
	}
	cert, err := r.parse(certPEM, keyPEM)
	if err != nil {
		r.warn(err)
		return r.cert, nil
	}
	r.cert, r.digest = cert, digest
	if r.logger != nil {
		r.logger.Info("Reloaded the cluster TLS certificate", "cert_file", r.certFile, "not_after", cert.Leaf.NotAfter)
	}
	return r.cert, nil
}

func (r *certReloader) warn(err error) {
	if r.logger != nil {
		r.logger.Warn("Could not reload the cluster TLS certificate; keeping the previous one", "error", err)
	}
}
