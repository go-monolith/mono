package types

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// DefaultAutoTLSChallengeAddr is the address the ACME http-01 challenge server
// binds to when AutoTLSConfig.HTTPChallengeAddr is empty. Certificate
// authorities always connect to port 80 of the domain being validated.
const DefaultAutoTLSChallengeAddr = ":80"

// DefaultAutoTLSStartupIssueTimeout is the startup certificate fetch budget
// used when AutoTLSConfig.StartupIssueTimeout is zero.
const DefaultAutoTLSStartupIssueTimeout = 60 * time.Second

// Validate checks the AutoTLS configuration for internal consistency and
// returns a descriptive error for the first problem found.
//
// It performs no I/O: filesystem and network problems (an uncreatable cache
// directory, an unreachable challenge port) surface during framework startup
// instead, so that constructing configuration stays free of side effects.
func (c *AutoTLSConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("AutoTLS config cannot be nil")
	}
	if len(c.Domains) == 0 {
		return fmt.Errorf("at least one domain is required")
	}

	seen := make(map[string]struct{}, len(c.Domains))
	for i, d := range c.Domains {
		if d == "" {
			return fmt.Errorf("domain at index %d cannot be empty", i)
		}
		if strings.Contains(d, "*") {
			return fmt.Errorf("wildcard domain %q is not supported: wildcards require the dns-01 challenge, which autocert does not implement", d)
		}
		if strings.Contains(d, "://") || strings.Contains(d, ":") || strings.Contains(d, "/") {
			return fmt.Errorf("domain %q must be a bare hostname, not a URL or host:port", d)
		}
		if !strings.Contains(strings.Trim(d, "."), ".") {
			return fmt.Errorf("domain %q must be a fully qualified domain name", d)
		}
		if d != strings.ToLower(d) {
			return fmt.Errorf("domain %q must be lowercase", d)
		}
		if _, dup := seen[d]; dup {
			return fmt.Errorf("duplicate domain %q", d)
		}
		seen[d] = struct{}{}
	}

	if c.CacheDir == "" {
		return fmt.Errorf("cache directory is required: certificates and the ACME account key must persist across restarts")
	}
	if !c.AcceptTOS {
		return fmt.Errorf("AcceptTOS must be true to acknowledge the certificate authority's Terms of Service")
	}
	if c.HTTPChallengeAddr != "" {
		if _, _, err := net.SplitHostPort(c.HTTPChallengeAddr); err != nil {
			return fmt.Errorf("invalid HTTP challenge address %q: %w", c.HTTPChallengeAddr, err)
		}
	}
	if c.RenewBefore < 0 {
		return fmt.Errorf("renew-before must not be negative, got %s", c.RenewBefore)
	}
	if c.DirectoryURL != "" {
		u, err := url.Parse(c.DirectoryURL)
		if err != nil {
			return fmt.Errorf("invalid ACME directory URL %q: %w", c.DirectoryURL, err)
		}
		if u.Scheme != "https" && u.Scheme != "http" {
			return fmt.Errorf("invalid ACME directory URL %q: scheme must be https (or http for a local test CA)", c.DirectoryURL)
		}
		if u.Host == "" {
			return fmt.Errorf("invalid ACME directory URL %q: missing host", c.DirectoryURL)
		}
	}
	return nil
}

// Clone returns a deep copy of the configuration so that later mutation of the
// caller's Domains slice cannot affect stored configuration. The
// DirectoryCAPool pointer is shared: x509.CertPool is safe for concurrent use
// and is treated as immutable once built.
func (c *AutoTLSConfig) Clone() *AutoTLSConfig {
	if c == nil {
		return nil
	}
	cp := *c
	if c.Domains != nil {
		cp.Domains = make([]string, len(c.Domains))
		copy(cp.Domains, c.Domains)
	}
	return &cp
}

// ChallengeAddr returns the address the http-01 challenge server should bind
// to, applying DefaultAutoTLSChallengeAddr when unset.
func (c *AutoTLSConfig) ChallengeAddr() string {
	if c.HTTPChallengeAddr == "" {
		return DefaultAutoTLSChallengeAddr
	}
	return c.HTTPChallengeAddr
}

// IssueTimeout returns the startup certificate fetch budget, applying
// DefaultAutoTLSStartupIssueTimeout when unset. A negative StartupIssueTimeout
// is returned unchanged and means "skip the startup fetch".
func (c *AutoTLSConfig) IssueTimeout() time.Duration {
	if c.StartupIssueTimeout == 0 {
		return DefaultAutoTLSStartupIssueTimeout
	}
	return c.StartupIssueTimeout
}
