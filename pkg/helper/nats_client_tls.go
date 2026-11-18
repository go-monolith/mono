package helper

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-monolith/mono/pkg/certreload"
	"github.com/go-monolith/mono/pkg/types"
)

// NATSClientTLSConfig configures NATSClientTLS.
type NATSClientTLSConfig struct {
	// CAFile is the PEM file of the CA (or CAs) the server certificate must
	// chain to. Required. It is read once.
	CAFile string

	// CertFile and KeyFile are the PEM files of this client's certificate and
	// key, for a server that requires client certificates (mutual TLS,
	// types.ClientTLSConfig.ClientCAFile). Both or neither. The certificate
	// must allow TLS client use. They are read again when they change, so a
	// renewed certificate is presented on the next connection.
	CertFile string
	KeyFile  string

	// Logger receives the reload lines of the client certificate. Nil means
	// no logging.
	Logger types.Logger

	// CheckInterval is the least time between two checks of the certificate
	// files. Zero means certreload.DefaultCheckInterval (10 seconds).
	CheckInterval time.Duration
}

// NATSClientTLS returns a nats.go option that connects with TLS, verifying
// the server against cfg.CAFile and, with cfg.CertFile and cfg.KeyFile,
// presenting a client certificate that is reloaded when its files change
// (pkg/certreload), so a client with short-lived certificates keeps
// reconnecting after a renewal without a restart. nats.ClientCert reads its
// files once and would present the expired certificate on reconnect.
//
// The server's certificate must name the host in the server URL. When that
// host is a name and the other servers of a cluster are announced by IP
// address, nats.go verifies them against that name, so a certificate for a
// Kubernetes Service name also covers reconnects to the pods behind it.
//
// It is the client side of mono.WithNATSClientTLS:
//
//	tlsOpt, err := helper.NATSClientTLS(helper.NATSClientTLSConfig{
//	    CAFile:   "/etc/nats-client-tls/ca.crt",
//	    CertFile: "/etc/nats-client-tls/tls.crt",
//	    KeyFile:  "/etc/nats-client-tls/tls.key",
//	})
//	if err != nil {
//	    return err
//	}
//	nc, err := nats.Connect("tls://mono-cluster-client.mono-demo.svc.cluster.local:4222", tlsOpt)
//
// It returns an error when the CA file cannot be read or holds no
// certificate, when only one of CertFile and KeyFile is set, or when the
// client certificate cannot be loaded, has expired or does not allow client
// use.
func NATSClientTLS(cfg NATSClientTLSConfig) (nats.Option, error) {
	if cfg.CAFile == "" {
		return nil, errors.New("NATS client TLS needs a CA file")
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, errors.New("NATS client TLS needs both a certificate file and a key file, or neither")
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("NATS client TLS: CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("NATS client TLS: no certificate found in CA file %s", cfg.CAFile)
	}
	rootsCB := func() (*x509.CertPool, error) { return roots, nil }

	var certCB nats.TLSCertHandler
	if cfg.CertFile != "" {
		reloader, err := certreload.New(certreload.Config{
			CertFile:      cfg.CertFile,
			KeyFile:       cfg.KeyFile,
			Usages:        []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			Name:          "NATS client TLS",
			Logger:        cfg.Logger,
			CheckInterval: cfg.CheckInterval,
		})
		if err != nil {
			return nil, fmt.Errorf("NATS client TLS: %w", err)
		}
		// nats.go calls the handler on every connect, reconnects included.
		certCB = func() (tls.Certificate, error) { return *reloader.Certificate(), nil }
	}
	return nats.ClientTLSConfig(certCB, rootsCB), nil
}
