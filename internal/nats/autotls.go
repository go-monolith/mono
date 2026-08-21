package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// challengeReadHeaderTimeout bounds how long the ACME challenge server waits
// for a client to send its request headers. Required to avoid a slowloris
// exposure on the public challenge port (gosec G112).
const challengeReadHeaderTimeout = 10 * time.Second

// autoTLSHandshakeTimeout is the TLS handshake budget, in seconds, installed on
// server.Options.TLSTimeout when AutoTLS is enabled.
//
// The nats-server default is TLS_TIMEOUT (2s), which is generous for a normal
// handshake but far too short for one that has to obtain a certificate first.
// That only happens when the startup fetch is disabled and the cache is cold,
// but when it does happen a 2s budget guarantees failure.
const autoTLSHandshakeTimeout = 10.0

// autoTLS owns the autocert manager and the HTTP listener that answers ACME
// http-01 challenges for the embedded NATS server.
//
// The challenge listener stays up for the entire life of the process rather
// than only during initial issuance: autocert renews certificates in the
// background and each renewal re-runs the http-01 challenge, so a listener torn
// down after the first issuance would work for roughly sixty days and then
// silently stop renewing.
type autoTLS struct {
	cfg    *types.AutoTLSConfig
	mgr    *autocert.Manager
	srv    *http.Server
	ln     net.Listener
	logger types.Logger
}

// newAutoTLS builds the autocert manager for cfg. The only I/O it performs is
// creating the certificate cache directory.
func newAutoTLS(cfg *types.AutoTLSConfig, logger types.Logger) (*autoTLS, error) {
	if cfg == nil {
		return nil, fmt.Errorf("AutoTLS config cannot be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AutoTLS configuration: %w", err)
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create AutoTLS cache directory %q: %w", cfg.CacheDir, err)
	}

	mgr := &autocert.Manager{
		// Safe: AcceptTOS is required to be true by Validate, so reaching
		// here means the operator has explicitly accepted the CA's terms.
		Prompt:      autocert.AcceptTOS,
		Cache:       autocert.DirCache(cfg.CacheDir),
		HostPolicy:  autocert.HostWhitelist(cfg.Domains...),
		Email:       cfg.Email,
		RenewBefore: cfg.RenewBefore, // zero means the autocert default
	}
	if cfg.DirectoryURL != "" || cfg.DirectoryCAPool != nil {
		client := &acme.Client{DirectoryURL: cfg.DirectoryURL}
		if cfg.DirectoryCAPool != nil {
			client.HTTPClient = &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						RootCAs:    cfg.DirectoryCAPool,
						MinVersion: tls.VersionTLS12,
					},
				},
			}
		}
		mgr.Client = client
	}

	return &autoTLS{cfg: cfg, mgr: mgr, logger: logger}, nil
}

// tlsConfig returns the configuration installed on server.Options.TLSConfig.
//
// nats-server hands this value verbatim to tls.Server, so the lazy
// GetCertificate callback is consulted on every handshake - which is what makes
// background renewal invisible to clients and to the server.
//
// Certificates is deliberately left empty: nats-server's OCSP stapling wrapper
// iterates that slice, so an empty slice makes OCSP a harmless no-op rather
// than an error. NextProtos is left nil because tls-alpn-01 is not supported
// (it would require the validated listener to be on port 443), and advertising
// acme.ALPNProto would claim support the framework cannot honour.
func (a *autoTLS) tlsConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: a.mgr.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}
}

// start binds and serves the http-01 challenge listener.
//
// Binding is synchronous so that a port conflict - most often "permission
// denied" on the privileged default port 80 - is returned as a startup error
// rather than logged from a goroutine nobody is watching.
//
// Calling mgr.HTTPHandler is what enables the http-01 challenge type at all:
// it sets the manager's internal tryHTTP01 flag. Until then autocert offers the
// CA only tls-alpn-01, which this framework cannot answer. start must therefore
// run before any code path that can trigger issuance.
func (a *autoTLS) start() error {
	addr := a.cfg.ChallengeAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind ACME http-01 challenge listener on %q "+
			"(binding a privileged port such as :80 requires root or CAP_NET_BIND_SERVICE): %w", addr, err)
	}
	a.ln = ln
	a.srv = &http.Server{
		Handler:           stripHostPort(a.mgr.HTTPHandler(nil)),
		ReadHeaderTimeout: challengeReadHeaderTimeout,
	}
	go func() {
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("ACME challenge server stopped unexpectedly", "error", err)
		}
	}()
	a.logger.Info("ACME http-01 challenge server started", "addr", ln.Addr().String())
	return nil
}

// stripHostPort removes any port from the request's Host header before the
// autocert handler sees it.
//
// autocert's HTTPHandler passes r.Host straight to the host policy, and
// HostWhitelist matches bare hostnames exactly. A certificate authority always
// connects on port 80, where Go leaves Host without a port, so upstream never
// has to care. But this framework lets the challenge listener bind a different
// address - behind a reverse proxy, or in tests against a local ACME server -
// and then Host arrives as "example.com:5002" and every challenge is answered
// with 403 Forbidden.
//
// Stripping the port matches what autocert itself does when building its HTTPS
// redirect, so no policy decision changes: "evil.test:5002" still becomes
// "evil.test" and is still rejected unless it is a configured domain.
func stripHostPort(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if host, _, err := net.SplitHostPort(r.Host); err == nil && host != "" {
			r = r.Clone(r.Context())
			r.Host = host
		}
		next.ServeHTTP(w, r)
	})
}

// prewarm synchronously obtains a certificate for every configured domain,
// bounded by the configured startup issue timeout. It is a no-op when a valid
// certificate is already in the cache, and returns nil immediately when the
// startup fetch is disabled.
//
// autocert issues one certificate per SNI name rather than a single multi-SAN
// certificate, so every domain must be fetched individually.
func (a *autoTLS) prewarm(ctx context.Context) error {
	timeout := a.cfg.IssueTimeout()
	if timeout < 0 {
		a.logger.Info("AutoTLS startup certificate fetch disabled; certificates will be obtained on first handshake")
		return nil
	}

	// autocert's GetCertificate builds its own internal context and ignores
	// any deadline on the caller's, so the timeout has to be enforced from the
	// outside. Passing a context.WithTimeout down would look correct and do
	// nothing.
	done := make(chan error, 1)
	go func() { done <- a.issueAll() }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("timed out after %s obtaining ACME certificates for %v: "+
			"check that the domains resolve to this host and that the challenge address %q is reachable on port 80",
			timeout, a.cfg.Domains, a.cfg.ChallengeAddr())
	case <-ctx.Done():
		return fmt.Errorf("cancelled while obtaining ACME certificates: %w", ctx.Err())
	}
}

// issueAll fetches a certificate for each domain in turn.
func (a *autoTLS) issueAll() error {
	for _, domain := range a.cfg.Domains {
		hello := &tls.ClientHelloInfo{
			ServerName: domain,
			// Pinned so the autocert cache key, which encodes whether an RSA
			// certificate was requested, matches the key a real handshake from
			// a Go client produces. Without this the startup fetch would warm
			// a cache entry that later handshakes never read.
			CipherSuites:      []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
			SignatureSchemes:  []tls.SignatureScheme{tls.ECDSAWithP256AndSHA256},
			SupportedVersions: []uint16{tls.VersionTLS13, tls.VersionTLS12},
			SupportedCurves:   []tls.CurveID{tls.X25519, tls.CurveP256},
		}
		if _, err := a.mgr.GetCertificate(hello); err != nil {
			return fmt.Errorf("failed to obtain ACME certificate for %q: %w", domain, err)
		}
		a.logger.Info("ACME certificate ready", "domain", domain)
	}
	return nil
}

// stop shuts the challenge server down, honouring the caller's deadline and
// falling back to closing the listener outright if graceful shutdown fails.
//
// autocert.Manager has no exported stop: its renewal timers are plain
// time.Timers that are released when the process exits.
func (a *autoTLS) stop(ctx context.Context) {
	if a == nil {
		return
	}
	if a.srv != nil {
		if err := a.srv.Shutdown(ctx); err != nil {
			a.logger.Warn("ACME challenge server did not shut down gracefully; forcing close", "error", err)
			if closeErr := a.srv.Close(); closeErr != nil {
				a.logger.Warn("failed to force-close the ACME challenge server", "error", closeErr)
			}
		}
		a.srv = nil
		a.ln = nil
		a.logger.Info("ACME challenge server stopped")
		return
	}
	if a.ln != nil {
		// Reached only when the listener was bound but the server never got
		// as far as serving on it.
		if err := a.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			a.logger.Warn("failed to close the ACME challenge listener", "error", err)
		}
		a.ln = nil
	}
}
