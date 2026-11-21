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
	// Zero means DefaultCheckInterval; it must not be negative. Tests that
	// renew a certificate can set a very short interval (time.Nanosecond)
	// instead of waiting.
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
	// checking is set while one caller reads the files (without the lock),
	// since checkStarted. A read that never returns leaves it set: nothing
	// is reloaded until it does, which stallWarned records being said.
	checking     bool
	checkStarted time.Time
	stallWarned  time.Time
	// expiryWarned is when the expiry of the current certificate was last
	// warned about.
	expiryWarned time.Time
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
// returned. Only the caller that makes a check waits for the files; callers
// that arrive meanwhile get the current certificate at once; a read that
// does not return (a file system that hangs) is warned about after
// StalledCheckAfter, and nothing is reloaded until it returns. A certificate
// that has expired without being renewed is warned about (see
// ExpiryWarnInterval).
func (r *Reloader) Certificate() *tls.Certificate {
	r.mu.Lock()
	now := r.now()
	if r.checking || now.Sub(r.checked) < r.cfg.CheckInterval {
		// This is the path of every handshake while a check hangs, so
		// what there is to say is said after the lock is released: a log
		// sink that blocks must not hold the handshakes up as well.
		var say func()
		if r.checking {
			say = r.stalledWarning(now)
		}
		cert := r.cert
		r.mu.Unlock()
		if say != nil {
			say()
		}
		return cert
	}
	// The files are read without the lock held: on a file system that
	// stalls, the read would otherwise hold up every handshake behind this
	// one. While a check runs, the others get the current certificate.
	r.checking = true
	r.checked = now
	r.checkStarted = now
	current := r.digest
	r.mu.Unlock()

	cert, digest, changed, err := r.load(current)

	// What the check has to say is said after the lock is released, as on
	// the path above: the handshakes behind this one wait for the lock.
	var say func()
	r.mu.Lock()
	r.checking = false
	// The read returned: a later one that hangs is said again.
	r.stallWarned = time.Time{}
	switch {
	case err != nil:
		say = func() { r.warn(err) }
	case changed:
		r.cert, r.digest = cert, digest
		r.expiryWarned = time.Time{}
		if logger := r.cfg.Logger; logger != nil {
			notAfter := cert.Leaf.NotAfter
			say = func() {
				logger.Info("Reloaded the "+r.cfg.Name+" certificate", "cert_file", r.cfg.CertFile, "not_after", notAfter)
			}
		}
	default:
		say = r.expiredWarning(now)
	}
	served := r.cert
	r.mu.Unlock()
	if say != nil {
		say()
	}
	return served
}

// load reads the files and, when their contents differ from current, parses
// the certificate. It takes no lock.
func (r *Reloader) load(current [sha256.Size]byte) (cert *tls.Certificate, digest [sha256.Size]byte, changed bool, err error) {
	certPEM, keyPEM, err := r.read()
	if err != nil {
		return nil, digest, false, err
	}
	digest = digestOf(certPEM, keyPEM)
	if digest == current {
		return nil, digest, false, nil
	}
	cert, err = r.parse(certPEM, keyPEM)
	if err != nil {
		return nil, digest, false, err
	}
	return cert, digest, true, nil
}

// ExpiryWarnInterval is the least time between two warnings that the
// certificate being served has expired.
const ExpiryWarnInterval = time.Hour

// StalledCheckAfter is how long a check of the files may run before it is
// warned about (at most every ExpiryWarnInterval). Until it returns the
// certificate is not reloaded.
const StalledCheckAfter = time.Minute

// stalledWarning returns the function that warns that the check of the
// files has not returned (and, as that check cannot, that the certificate
// has expired), or nil when there is nothing to say now. It is called with
// the lock held, by the callers that find a check in progress; what it
// returns is called without it.
func (r *Reloader) stalledWarning(now time.Time) func() {
	if r.cfg.Logger == nil || now.Sub(r.checkStarted) < StalledCheckAfter {
		return nil
	}
	if !r.stallWarned.IsZero() && now.Sub(r.stallWarned) < ExpiryWarnInterval {
		return nil
	}
	r.stallWarned = now
	logger, name := r.cfg.Logger, r.cfg.Name
	certFile, keyFile := r.cfg.CertFile, r.cfg.KeyFile
	readingFor := now.Sub(r.checkStarted)
	var expired time.Time
	if r.cert != nil && r.cert.Leaf != nil && now.After(r.cert.Leaf.NotAfter) {
		expired = r.cert.Leaf.NotAfter
	}
	return func() {
		logger.Warn("Reading the "+name+" certificate files has not returned; the certificate is not reloaded until it does",
			"cert_file", certFile, "key_file", keyFile, "reading_for", readingFor)
		if !expired.IsZero() {
			logger.Warn("The "+name+" certificate has expired and its files were not renewed; peers that check it refuse the connection",
				"cert_file", certFile, "not_after", expired)
		}
	}
}

// expiredWarning returns the warning, due at most every ExpiryWarnInterval,
// that the certificate has expired and its files still hold it, or nil. A
// certificate is checked when it is loaded; one that is never renewed went
// on being served past its expiry without a word, and its peers refuse it.
// Called with the lock held; the warning is written after it is released.
func (r *Reloader) expiredWarning(now time.Time) func() {
	if r.cfg.Logger == nil || r.cert == nil || r.cert.Leaf == nil || !now.After(r.cert.Leaf.NotAfter) {
		return nil
	}
	if !r.expiryWarned.IsZero() && now.Sub(r.expiryWarned) < ExpiryWarnInterval {
		return nil
	}
	r.expiryWarned = now
	logger, name, certFile, notAfter := r.cfg.Logger, r.cfg.Name, r.cfg.CertFile, r.cert.Leaf.NotAfter
	return func() {
		logger.Warn("The "+name+" certificate has expired and its files were not renewed; peers that check it refuse the connection",
			"cert_file", certFile, "not_after", notAfter)
	}
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
