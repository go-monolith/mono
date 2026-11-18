// Package certreload serves a TLS certificate and key loaded from files and
// reloads them, without a restart, when the files change. It is the
// certificate source of the framework's route TLS (WithNATSClusterTLS) and
// can be used by any TLS server or client that renews its certificate in
// place, for example from a Kubernetes Secret updated by cert-manager.
//
// A Reloader is plugged into a tls.Config through its callbacks:
//
//	r, err := certreload.New(certreload.Config{
//	    CertFile: "/etc/tls/tls.crt",
//	    KeyFile:  "/etc/tls/tls.key",
//	    Usages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
//	})
//	if err != nil {
//	    return err
//	}
//	config := &tls.Config{GetCertificate: r.GetCertificate}
//
// Behaviour:
//   - The files are read again at most every Config.CheckInterval (10 seconds
//     by default), and only when a certificate is asked for, so an idle
//     process does no I/O.
//   - A change is detected by the SHA-256 digest of the files' contents, so an
//     atomic Secret update (a symlink swap) or a rewrite with the same contents
//     is handled correctly.
//   - A reload that fails (unreadable or half-written files, a key that does
//     not match, a certificate that has expired, is not yet valid, lacks a
//     required usage or does not chain to Config.Roots) keeps the previous
//     certificate, logs a warning and is retried at the next check.
//   - The expiry of the certificate is logged at every reload, since a
//     certificate served through callbacks is invisible to monitoring that
//     reads tls.Config.Certificates.
package certreload

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// DefaultCheckInterval is the least time between two checks of the files
// when Config.CheckInterval is zero.
const DefaultCheckInterval = 10 * time.Second

// Config configures a Reloader.
type Config struct {
	// CertFile and KeyFile are the PEM files of the certificate (with any
	// intermediates after the leaf) and its private key. Both are required.
	CertFile, KeyFile string

	// Roots, when set, is the CA pool the certificate must chain to, checked
	// at load and on every reload: a renewed certificate from another CA is
	// refused and the previous one kept. When nil, only the validity period
	// and the usages are checked.
	Roots *x509.CertPool

	// RootsName names Roots in errors, for example "route CA". Empty means
	// "CA".
	RootsName string

	// Usages are the extended key usages the certificate must allow, each one
	// checked separately (a TLS server certificate needs
	// x509.ExtKeyUsageServerAuth, a client certificate
	// x509.ExtKeyUsageClientAuth). A certificate without the extended key
	// usage extension is valid for any use. Empty, or
	// x509.ExtKeyUsageAny, means no usage check, as in x509.Verify.
	Usages []x509.ExtKeyUsage

	// Name names the certificate in logs, for example "cluster TLS"
	// ("Reloaded the cluster TLS certificate"). Empty means "TLS".
	Name string

	// Logger receives the reload and warning lines. Nil means no logging.
	Logger types.Logger

	// CheckInterval is the least time between two checks of the files.
	// Zero means DefaultCheckInterval; it must not be negative.
	CheckInterval time.Duration
}

// Reloader serves a certificate loaded from files and reloads it when the
// files change. It is safe for concurrent use.
type Reloader struct {
	cfg Config
	now func() time.Time

	mu      sync.Mutex
	cert    *tls.Certificate
	digest  [sha256.Size]byte // of the certificate and key files' contents
	checked time.Time
}

// New loads the certificate and key and checks them (validity period,
// usages, and the chain to Config.Roots when set). It returns an error when
// a file cannot be read or the certificate is not valid: a process should not
// start with a certificate its peers would refuse.
func New(cfg Config) (*Reloader, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, errors.New("a certificate file and a key file are required")
	}
	if cfg.CheckInterval < 0 {
		return nil, fmt.Errorf("check interval must not be negative, got %s", cfg.CheckInterval)
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}
	if cfg.Name == "" {
		cfg.Name = "TLS"
	}
	if cfg.RootsName == "" {
		cfg.RootsName = "CA"
	}
	cfg.Usages = append([]x509.ExtKeyUsage(nil), cfg.Usages...)
	r := &Reloader{cfg: cfg, now: time.Now}
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

// Certificate returns the certificate, reloading it first when the files'
// contents changed since the last check (at most every
// Config.CheckInterval). A reload that fails, for example while a Secret
// update has written the certificate but not yet the key, keeps the previous
// certificate and is retried at the next check, so a certificate is always
// returned.
func (r *Reloader) Certificate() *tls.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.checked) < r.cfg.CheckInterval {
		return r.cert
	}
	r.checked = now
	certPEM, keyPEM, err := r.read()
	if err != nil {
		r.warn(err)
		return r.cert
	}
	digest := digestOf(certPEM, keyPEM)
	if digest == r.digest {
		return r.cert
	}
	cert, err := r.parse(certPEM, keyPEM)
	if err != nil {
		r.warn(err)
		return r.cert
	}
	r.cert, r.digest = cert, digest
	if r.cfg.Logger != nil {
		r.cfg.Logger.Info("Reloaded the "+r.cfg.Name+" certificate", "cert_file", r.cfg.CertFile, "not_after", cert.Leaf.NotAfter)
	}
	return r.cert
}

// GetCertificate is Certificate in the shape of tls.Config.GetCertificate,
// for TLS servers. tls.Config.Certificates must be empty: Go calls
// GetCertificate only then. The error is always nil.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.Certificate(), nil
}

// GetClientCertificate is Certificate in the shape of
// tls.Config.GetClientCertificate, for TLS clients. The error is always nil.
func (r *Reloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return r.Certificate(), nil
}

// NotAfter returns the expiry of the current certificate.
func (r *Reloader) NotAfter() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert.Leaf.NotAfter
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

func (r *Reloader) read() ([]byte, []byte, error) {
	certPEM, err := os.ReadFile(r.cfg.CertFile)
	if err != nil {
		return nil, nil, fmt.Errorf("certificate file: %w", err)
	}
	keyPEM, err := os.ReadFile(r.cfg.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("key file: %w", err)
	}
	return certPEM, keyPEM, nil
}

// usageNames names the usages in errors.
var usageNames = map[x509.ExtKeyUsage]string{
	x509.ExtKeyUsageServerAuth: "server",
	x509.ExtKeyUsageClientAuth: "client",
}

func usageName(u x509.ExtKeyUsage) string {
	if n, ok := usageNames[u]; ok {
		return n
	}
	return fmt.Sprintf("extended key usage %d", u)
}

// parse builds the key pair and checks the leaf: its chain to the roots
// (intermediates in the certificate file are used for the chain) for each
// required usage, or, without roots, its validity period and usages.
func (r *Reloader) parse(certPEM, keyPEM []byte) (*tls.Certificate, error) {
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
	if r.cfg.Roots == nil {
		return &pair, r.checkLeaf(leaf)
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		if c, err := x509.ParseCertificate(der); err == nil {
			intermediates.AddCert(c)
		}
	}
	usages := r.cfg.Usages
	if len(usages) == 0 {
		usages = []x509.ExtKeyUsage{x509.ExtKeyUsageAny}
	}
	for _, usage := range usages {
		if _, err := leaf.Verify(x509.VerifyOptions{
			CurrentTime:   r.now(),
			Roots:         r.cfg.Roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{usage},
		}); err != nil {
			if usage == x509.ExtKeyUsageAny {
				return nil, fmt.Errorf("certificate is not valid with the %s: %w", r.cfg.RootsName, err)
			}
			return nil, fmt.Errorf("certificate is not valid for TLS %s use with the %s: %w", usageName(usage), r.cfg.RootsName, err)
		}
	}
	return &pair, nil
}

// checkLeaf checks the validity period and the required usages of a
// certificate that is not checked against roots.
func (r *Reloader) checkLeaf(leaf *x509.Certificate) error {
	now := r.now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return fmt.Errorf("certificate is not valid: %w", x509.CertificateInvalidError{Cert: leaf, Reason: x509.Expired,
			Detail: fmt.Sprintf("current time %s is outside %s .. %s",
				now.UTC().Format(time.RFC3339), leaf.NotBefore.UTC().Format(time.RFC3339), leaf.NotAfter.UTC().Format(time.RFC3339))})
	}
	if len(leaf.ExtKeyUsage) == 0 && len(leaf.UnknownExtKeyUsage) == 0 {
		return nil // no extended key usage extension: valid for any use
	}
	for _, usage := range r.cfg.Usages {
		if usage == x509.ExtKeyUsageAny {
			continue // as x509.Verify: asking for any usage checks none
		}
		if !allows(leaf, usage) {
			return fmt.Errorf("certificate is not valid for TLS %s use: %w", usageName(usage),
				x509.CertificateInvalidError{Cert: leaf, Reason: x509.IncompatibleUsage})
		}
	}
	return nil
}

// allows reports whether the leaf's extended key usages permit usage, with
// the equivalences x509.Verify applies.
func allows(leaf *x509.Certificate, usage x509.ExtKeyUsage) bool {
	for _, u := range leaf.ExtKeyUsage {
		switch {
		case u == usage, u == x509.ExtKeyUsageAny:
			return true
		case usage == x509.ExtKeyUsageServerAuth &&
			(u == x509.ExtKeyUsageNetscapeServerGatedCrypto || u == x509.ExtKeyUsageMicrosoftServerGatedCrypto):
			return true
		}
	}
	return false
}

func (r *Reloader) warn(err error) {
	if r.cfg.Logger != nil {
		r.cfg.Logger.Warn("Could not reload the "+r.cfg.Name+" certificate; keeping the previous one", "error", err)
	}
}
