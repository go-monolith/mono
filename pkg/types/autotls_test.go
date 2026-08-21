package types

import (
	"crypto/x509"
	"strings"
	"testing"
	"time"
)

// validAutoTLS returns a minimal configuration that passes Validate, so each
// test case can invalidate exactly one field.
func validAutoTLS() AutoTLSConfig {
	return AutoTLSConfig{
		Domains:   []string{"nats.example.com"},
		Email:     "ops@example.com",
		CacheDir:  "/var/lib/mono/acme",
		AcceptTOS: true,
	}
}

func TestAutoTLSConfigValidate(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*AutoTLSConfig)
		wantErr  bool
		contains string
	}{
		{
			name:   "valid minimal config",
			mutate: func(*AutoTLSConfig) {},
		},
		{
			name: "valid with every optional field",
			mutate: func(c *AutoTLSConfig) {
				c.Domains = []string{"nats.example.com", "events.example.com"}
				c.HTTPChallengeAddr = "127.0.0.1:5002"
				c.DirectoryURL = "https://acme-staging-v02.api.letsencrypt.org/directory"
				c.DirectoryCAPool = x509.NewCertPool()
				c.RenewBefore = 720 * time.Hour
				c.StartupIssueTimeout = 90 * time.Second
			},
		},
		{
			name:     "no domains",
			mutate:   func(c *AutoTLSConfig) { c.Domains = nil },
			wantErr:  true,
			contains: "at least one domain",
		},
		{
			name:     "empty domain",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{""} },
			wantErr:  true,
			contains: "index 0",
		},
		{
			name:     "wildcard domain",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"*.example.com"} },
			wantErr:  true,
			contains: "dns-01",
		},
		{
			name:     "domain with scheme",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"https://nats.example.com"} },
			wantErr:  true,
			contains: "bare hostname",
		},
		{
			name:     "domain with port",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"nats.example.com:4222"} },
			wantErr:  true,
			contains: "bare hostname",
		},
		{
			name:     "domain with path",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"example.com/nats"} },
			wantErr:  true,
			contains: "bare hostname",
		},
		{
			name:     "not fully qualified",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"localhost"} },
			wantErr:  true,
			contains: "fully qualified",
		},
		{
			name:     "uppercase domain",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"NATS.example.com"} },
			wantErr:  true,
			contains: "lowercase",
		},
		{
			name:     "duplicate domain",
			mutate:   func(c *AutoTLSConfig) { c.Domains = []string{"nats.example.com", "nats.example.com"} },
			wantErr:  true,
			contains: "duplicate",
		},
		{
			name:     "no cache dir",
			mutate:   func(c *AutoTLSConfig) { c.CacheDir = "" },
			wantErr:  true,
			contains: "cache directory is required",
		},
		{
			name:     "TOS not accepted",
			mutate:   func(c *AutoTLSConfig) { c.AcceptTOS = false },
			wantErr:  true,
			contains: "AcceptTOS",
		},
		{
			name:     "malformed challenge address",
			mutate:   func(c *AutoTLSConfig) { c.HTTPChallengeAddr = "not-a-host-port" },
			wantErr:  true,
			contains: "challenge address",
		},
		{
			name:     "negative renew-before",
			mutate:   func(c *AutoTLSConfig) { c.RenewBefore = -time.Hour },
			wantErr:  true,
			contains: "renew-before",
		},
		{
			name:     "directory URL with a bad scheme",
			mutate:   func(c *AutoTLSConfig) { c.DirectoryURL = "ftp://acme.example.com/dir" },
			wantErr:  true,
			contains: "scheme must be https",
		},
		{
			name:     "directory URL without a host",
			mutate:   func(c *AutoTLSConfig) { c.DirectoryURL = "https:///dir" },
			wantErr:  true,
			contains: "missing host",
		},
		{
			name:   "http directory URL is allowed for a local test CA",
			mutate: func(c *AutoTLSConfig) { c.DirectoryURL = "http://127.0.0.1:14000/dir" },
		},
		{
			name:   "negative startup issue timeout means lazy issuance",
			mutate: func(c *AutoTLSConfig) { c.StartupIssueTimeout = -1 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validAutoTLS()
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Validate() error = nil, want an error")
				}
				if !strings.Contains(err.Error(), tt.contains) {
					t.Errorf("Validate() error = %q, want it to contain %q", err, tt.contains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestAutoTLSConfigValidateNil(t *testing.T) {
	var cfg *AutoTLSConfig
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() on a nil config error = nil, want an error")
	}
}

func TestAutoTLSConfigClone(t *testing.T) {
	t.Run("copies the domains slice", func(t *testing.T) {
		cfg := validAutoTLS()
		cfg.Domains = []string{"a.example.com", "b.example.com"}

		clone := cfg.Clone()
		cfg.Domains[0] = "mutated.example.com"

		if clone.Domains[0] != "a.example.com" {
			t.Errorf("clone.Domains[0] = %q, want the pre-mutation value", clone.Domains[0])
		}
	})

	t.Run("preserves every field", func(t *testing.T) {
		pool := x509.NewCertPool()
		cfg := validAutoTLS()
		cfg.HTTPChallengeAddr = "127.0.0.1:5002"
		cfg.DirectoryURL = "https://acme.example.com/dir"
		cfg.DirectoryCAPool = pool
		cfg.RenewBefore = time.Hour
		cfg.StartupIssueTimeout = 2 * time.Minute

		clone := cfg.Clone()
		if clone.Email != cfg.Email || clone.CacheDir != cfg.CacheDir ||
			clone.HTTPChallengeAddr != cfg.HTTPChallengeAddr ||
			clone.DirectoryURL != cfg.DirectoryURL ||
			clone.RenewBefore != cfg.RenewBefore ||
			clone.StartupIssueTimeout != cfg.StartupIssueTimeout ||
			clone.AcceptTOS != cfg.AcceptTOS {
			t.Errorf("Clone() = %+v, want a field-for-field copy of %+v", clone, cfg)
		}
		// The pool is shared deliberately: x509.CertPool is treated as
		// immutable once built.
		if clone.DirectoryCAPool != pool {
			t.Error("Clone() did not carry the DirectoryCAPool through")
		}
	})

	t.Run("nil clones to nil", func(t *testing.T) {
		var cfg *AutoTLSConfig
		if clone := cfg.Clone(); clone != nil {
			t.Errorf("Clone() = %v, want nil", clone)
		}
	})
}

func TestAutoTLSConfigDefaults(t *testing.T) {
	cfg := validAutoTLS()

	if got := cfg.ChallengeAddr(); got != DefaultAutoTLSChallengeAddr {
		t.Errorf("ChallengeAddr() = %q, want %q", got, DefaultAutoTLSChallengeAddr)
	}
	if got := cfg.IssueTimeout(); got != DefaultAutoTLSStartupIssueTimeout {
		t.Errorf("IssueTimeout() = %v, want %v", got, DefaultAutoTLSStartupIssueTimeout)
	}

	cfg.HTTPChallengeAddr = "0.0.0.0:8080"
	cfg.StartupIssueTimeout = 5 * time.Second
	if got := cfg.ChallengeAddr(); got != "0.0.0.0:8080" {
		t.Errorf("ChallengeAddr() = %q, want the configured value", got)
	}
	if got := cfg.IssueTimeout(); got != 5*time.Second {
		t.Errorf("IssueTimeout() = %v, want the configured value", got)
	}
}
