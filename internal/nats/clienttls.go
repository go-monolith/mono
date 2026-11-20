package nats

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/conf"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/go-monolith/mono/pkg/certreload"
	"github.com/go-monolith/mono/pkg/types"
)

// clientTLSCheckInterval is the least time between two checks of the client
// listener's certificate files. A variable so that tests can shorten it.
var clientTLSCheckInterval = certreload.DefaultCheckInterval

// applyClientTLS makes the client listener require TLS with the certificate
// of cfg, served through a reloader so a renewed certificate is used for new
// connections without a restart. With cfg.ClientCAFile every client must
// present a certificate that chains to it. This mirrors a config file's
// top-level tls { cert_file, key_file, ca_file, verify } block.
//
// A client CA that shares a certificate authority with the route CA is
// refused, since every server accepts a certificate from the route CA as a
// cluster peer: routeCAFile is the CA of WithNATSClusterTLS (empty without
// it), and opts.Cluster.TLSConfig, when set (by WithNATSClusterTLS, applied
// before, or a config file's cluster { tls {} } block), gives the route pool.
//
// A nil cfg leaves opts unchanged.
func applyClientTLS(opts *server.Options, cfg *types.ClientTLSConfig, routeCAFile string, logger types.Logger) error {
	if cfg == nil {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("client TLS: %w", err)
	}
	switch {
	case opts.TLSConfig != nil:
		return errors.New("WithNATSClientTLS cannot be combined with a tls {} block in the NATS config file: " +
			"use one of the two")
	case opts.AllowNonTLS:
		// The server would then accept plaintext clients, which skip the
		// client certificate check entirely.
		return errors.New("WithNATSClientTLS cannot be combined with allow_non_tls in the NATS config file: " +
			"it lets plaintext clients in without a certificate")
	case opts.OCSPConfig != nil:
		// nats-server's OCSP stapling reads tls.Config.Certificates, which
		// stays empty for a certificate served through a callback: stapling
		// would be off without a word.
		return errors.New("WithNATSClientTLS cannot be combined with an ocsp setting in the NATS config file: " +
			"OCSP stapling needs the certificate in the config file's tls {} block")
	}
	verify := cfg.ClientCAFile != ""
	if verify {
		var routePool *x509.CertPool
		if opts.Cluster.TLSConfig != nil {
			routePool = opts.Cluster.TLSConfig.ClientCAs
		}
		if err := checkSeparateCAs(cfg.ClientCAFile, routeCAFile, routePool); err != nil {
			return fmt.Errorf("client TLS: %w", err)
		}
	}

	config, err := server.GenTLSConfig(&server.TLSConfigOpts{
		CertFile: cfg.CertFile,
		KeyFile:  cfg.KeyFile,
		CaFile:   cfg.ClientCAFile,
		Verify:   verify,
	})
	if err != nil {
		return fmt.Errorf("client TLS: %w", err)
	}

	// The server certificate is presented to clients, which verify it
	// against their own trust: check its validity period and server use.
	reloader, err := certreload.New(certreload.Config{
		CertFile:      cfg.CertFile,
		KeyFile:       cfg.KeyFile,
		Usages:        []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Name:          "client TLS",
		Logger:        logger,
		CheckInterval: clientTLSCheckInterval,
	})
	if err != nil {
		return fmt.Errorf("client TLS: %w", err)
	}
	if logger != nil {
		// nats-server's /varz cannot show the expiry of a certificate served
		// through a callback, so log it.
		logger.Info("Client TLS enabled", "cert_file", cfg.CertFile, "not_after", reloader.NotAfter(),
			"client_certificates_required", verify)
	}
	// Certificates must stay empty: Go uses GetCertificate only when it is.
	config.Certificates = nil
	config.GetCertificate = reloader.GetCertificate

	opts.TLSConfig = config
	opts.TLSVerify = verify
	return nil
}

// checkSeparateCAs refuses a client CA bundle that shares a certificate
// authority with the routes: a CA of one that is, or chains to, a CA of the
// other. A client certificate from an intermediate under the route CA would
// verify on the route port (peers send their intermediates), and a route
// certificate from an intermediate under the client CA would be accepted as
// a client. routeCAFile (may be empty) gives the route CA certificates,
// routePool (may be nil) the pool the routes verify peers against.
func checkSeparateCAs(clientCAFile, routeCAFile string, routePool *x509.CertPool) error {
	clientCAs, err := readCertificates(clientCAFile)
	if err != nil {
		return fmt.Errorf("client CA file: %w", err)
	}
	var routeCAs []*x509.Certificate
	if routeCAFile != "" {
		if routeCAs, err = readCertificates(routeCAFile); err != nil {
			return fmt.Errorf("route CA file: %w", err)
		}
	}
	if routePool == nil && len(routeCAs) > 0 {
		routePool = x509.NewCertPool()
		for _, r := range routeCAs {
			routePool.AddCert(r)
		}
	}
	if routePool == nil {
		return nil // no route TLS
	}
	shared := func(c *x509.Certificate) error {
		return fmt.Errorf("the client CA %q is, or is signed by, the route CA: "+
			"a client certificate from it would be accepted as a cluster peer; use a separate CA for clients",
			c.Subject.String())
	}
	for _, c := range clientCAs {
		for _, r := range routeCAs {
			if bytes.Equal(c.RawSubject, r.RawSubject) && bytes.Equal(c.RawSubjectPublicKeyInfo, r.RawSubjectPublicKeyInfo) {
				return shared(c)
			}
		}
		if chains(c, routePool, routeCAs) {
			return shared(c)
		}
	}
	clientPool := x509.NewCertPool()
	for _, c := range clientCAs {
		clientPool.AddCert(c)
	}
	for _, r := range routeCAs {
		if chains(r, clientPool, clientCAs) {
			return fmt.Errorf("the route CA %q is, or is signed by, the client CA: "+
				"a route certificate from it would be accepted as a client; use a separate CA for clients",
				r.Subject.String())
		}
	}
	return nil
}

// chains reports whether c is signed by one of the CA certificates of other
// or chains to roots. The validity period is not what is asked: a CA
// certificate that has expired or is not valid yet is still the same
// authority, so the direct signatures are checked without regard to time,
// and the chain is tried at times inside c's own validity as well as now.
func chains(c *x509.Certificate, roots *x509.CertPool, other []*x509.Certificate) bool {
	for _, o := range other {
		if bytes.Equal(c.RawIssuer, o.RawSubject) && c.CheckSignatureFrom(o) == nil {
			return true
		}
	}
	for _, at := range []time.Time{time.Now(), c.NotBefore.Add(time.Second), c.NotBefore.Add(c.NotAfter.Sub(c.NotBefore) / 2)} {
		if _, err := c.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: at, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err == nil {
			return true
		}
	}
	return false
}

// readCertificates parses every CERTIFICATE block of a PEM file.
func readCertificates(path string) ([]*x509.Certificate, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing certificate: %w", err)
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, errors.New("no certificate found")
	}
	return certs, nil
}

// ClusterCAFileOfConfig returns the ca_file of the cluster { tls {} } block
// of the NATS config file at path, or "" when there is none (keys are
// case-insensitive, as nats-server reads them). The options parsed from the
// file hold the route CA only as a certificate pool, whose certificates
// cannot be listed, and checkSeparateCAs needs them to find a route CA that
// is signed by the client CA.
func ClusterCAFileOfConfig(path string) (string, error) {
	m, err := conf.ParseFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to parse NATS config file %q: %w", path, RedactConfigError(err))
	}
	cluster, ok := configBlock(m, "cluster")
	if !ok {
		return "", nil
	}
	tlsBlock, ok := configBlock(cluster, "tls")
	if !ok {
		return "", nil
	}
	for k, v := range tlsBlock {
		if strings.EqualFold(k, "ca_file") {
			if file, ok := v.(string); ok {
				return file, nil
			}
		}
	}
	return "", nil
}

// configBlock returns the block named name (case-insensitive) of a parsed
// config map.
func configBlock(m map[string]any, name string) (map[string]any, bool) {
	for k, v := range m {
		if strings.EqualFold(k, name) {
			block, ok := v.(map[string]any)
			return block, ok
		}
	}
	return nil, false
}
