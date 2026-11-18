package nats

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/go-monolith/mono/pkg/certreload"
	"github.com/go-monolith/mono/pkg/types"
)

// clusterTLSCheckInterval is the least time between two checks of the
// certificate files for a renewed certificate. A handshake that comes later
// reads the files and reloads them when their contents changed. A variable so
// that tests can shorten it.
var clusterTLSCheckInterval = certreload.DefaultCheckInterval

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

	// Each server is TLS server on the routes it accepts and TLS client on
	// the routes it opens, and peers verify both uses: a certificate without
	// one of them would make half of the routes fail. Requiring the chain to
	// the route CA on reload keeps a renewed certificate from another CA,
	// which peers still trusting only the old CA would refuse, from being
	// served.
	reloader, err := certreload.New(certreload.Config{
		CertFile:      certFile,
		KeyFile:       keyFile,
		Roots:         config.ClientCAs,
		RootsName:     "route CA",
		Usages:        []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		Name:          "cluster TLS",
		Logger:        logger,
		CheckInterval: clusterTLSCheckInterval,
	})
	if err != nil {
		return fmt.Errorf("cluster TLS: %w", err)
	}
	if logger != nil {
		// nats-server's /varz cannot show the expiry of a certificate served
		// through callbacks, so log it.
		logger.Info("Cluster TLS enabled", "cert_file", certFile, "not_after", reloader.NotAfter())
	}
	// Serve the certificate through callbacks so a renewed one is picked up.
	// Certificates must stay empty: Go uses GetCertificate only when it is.
	config.Certificates = nil
	config.GetCertificate = reloader.GetCertificate
	config.GetClientCertificate = reloader.GetClientCertificate

	opts.Cluster.TLSConfig = config
	return nil
}
