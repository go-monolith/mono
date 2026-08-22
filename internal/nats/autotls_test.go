package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// validAutoTLSConfig returns a configuration that passes validation and never
// reaches the network: the startup fetch is disabled and the challenge listener
// binds an ephemeral loopback port.
func validAutoTLSConfig(t *testing.T) *types.AutoTLSConfig {
	t.Helper()
	return &types.AutoTLSConfig{
		Domains:             []string{"nats.test.invalid"},
		Email:               "ops@test.invalid",
		CacheDir:            filepath.Join(t.TempDir(), "acme"),
		HTTPChallengeAddr:   "127.0.0.1:0",
		AcceptTOS:           true,
		StartupIssueTimeout: -1,
	}
}

func TestNewAutoTLS(t *testing.T) {
	t.Run("builds a manager from a valid config", func(t *testing.T) {
		cfg := validAutoTLSConfig(t)
		cfg.RenewBefore = 12 * time.Hour

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if at.mgr.Cache == nil {
			t.Error("Cache is nil, want a DirCache")
		}
		if got, want := at.mgr.Cache, autocert.DirCache(cfg.CacheDir); got != want {
			t.Errorf("Cache = %v, want %v", got, want)
		}
		if at.mgr.HostPolicy == nil {
			t.Fatal("HostPolicy is nil, want a HostWhitelist")
		}
		if at.mgr.Prompt == nil {
			t.Error("Prompt is nil, want autocert.AcceptTOS")
		}
		if at.mgr.Email != cfg.Email {
			t.Errorf("Email = %q, want %q", at.mgr.Email, cfg.Email)
		}
		if at.mgr.RenewBefore != cfg.RenewBefore {
			t.Errorf("RenewBefore = %v, want %v", at.mgr.RenewBefore, cfg.RenewBefore)
		}
		// The client is always constructed, even with no directory URL or CA
		// pool: acme falls back to http.DefaultClient otherwise, which has no
		// timeout and would let a half-open CA hold a connection open long
		// after startup has given up.
		if at.mgr.Client == nil {
			t.Fatal("Client is nil, want an acme.Client with a bounded HTTP client")
		}
		if at.mgr.Client.DirectoryURL != autocert.DefaultACMEDirectory {
			t.Errorf("DirectoryURL = %q, want the Let's Encrypt production default %q",
				at.mgr.Client.DirectoryURL, autocert.DefaultACMEDirectory)
		}
		if at.mgr.Client.HTTPClient == nil {
			t.Fatal("HTTPClient is nil, want a client with an explicit request timeout")
		}
		if at.mgr.Client.HTTPClient.Timeout != acmeRequestTimeout {
			t.Errorf("HTTPClient.Timeout = %v, want %v", at.mgr.Client.HTTPClient.Timeout, acmeRequestTimeout)
		}

		ctx := context.Background()
		if err := at.mgr.HostPolicy(ctx, "nats.test.invalid"); err != nil {
			t.Errorf("HostPolicy rejected a listed domain: %v", err)
		}
		if err := at.mgr.HostPolicy(ctx, "other.test.invalid"); err == nil {
			t.Error("HostPolicy accepted an unlisted domain, want rejection")
		}
	})

	t.Run("directory URL is propagated to the ACME client", func(t *testing.T) {
		cfg := validAutoTLSConfig(t)
		cfg.DirectoryURL = "https://acme.test.invalid/dir"

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if at.mgr.Client == nil {
			t.Fatal("Client is nil, want an acme.Client carrying the directory URL")
		}
		if at.mgr.Client.DirectoryURL != cfg.DirectoryURL {
			t.Errorf("DirectoryURL = %q, want %q", at.mgr.Client.DirectoryURL, cfg.DirectoryURL)
		}
		if at.mgr.Client.HTTPClient == nil || at.mgr.Client.HTTPClient.Timeout != acmeRequestTimeout {
			t.Error("HTTPClient must carry the request timeout even without a DirectoryCAPool")
		}
		// Without a CA pool the transport keeps the standard library defaults,
		// so RootCAs stays nil and the system trust store is used.
		tr, ok := at.mgr.Client.HTTPClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("Transport type = %T, want *http.Transport", at.mgr.Client.HTTPClient.Transport)
		}
		if tr.TLSClientConfig != nil && tr.TLSClientConfig.RootCAs != nil {
			t.Error("RootCAs is pinned, want the system pool when no DirectoryCAPool is configured")
		}
	})

	t.Run("directory CA pool installs a pinned HTTP client", func(t *testing.T) {
		cfg := validAutoTLSConfig(t)
		cfg.DirectoryCAPool = x509.NewCertPool()

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if at.mgr.Client == nil || at.mgr.Client.HTTPClient == nil {
			t.Fatal("HTTPClient is nil, want a client pinned to the configured CA pool")
		}
		tr, ok := at.mgr.Client.HTTPClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("Transport type = %T, want *http.Transport", at.mgr.Client.HTTPClient.Transport)
		}
		if tr.TLSClientConfig.RootCAs != cfg.DirectoryCAPool {
			t.Error("RootCAs is not the configured DirectoryCAPool")
		}
	})

	t.Run("rejects nil and invalid configs", func(t *testing.T) {
		if _, err := newAutoTLS(nil, newMockLogger()); err == nil {
			t.Error("newAutoTLS(nil) error = nil, want an error")
		}
		invalid := validAutoTLSConfig(t)
		invalid.AcceptTOS = false
		if _, err := newAutoTLS(invalid, newMockLogger()); err == nil {
			t.Error("newAutoTLS() with AcceptTOS=false error = nil, want an error")
		}
	})
}

func TestNewAutoTLSCacheDir(t *testing.T) {
	t.Run("creates the cache directory with mode 0700", func(t *testing.T) {
		cfg := validAutoTLSConfig(t)
		if _, err := newAutoTLS(cfg, newMockLogger()); err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		fi, err := os.Stat(cfg.CacheDir)
		if err != nil {
			t.Fatalf("cache directory was not created: %v", err)
		}
		if perm := fi.Mode().Perm(); perm != 0o700 {
			t.Errorf("cache directory mode = %o, want 700", perm)
		}
	})

	t.Run("tolerates an existing cache directory", func(t *testing.T) {
		cfg := validAutoTLSConfig(t)
		if err := os.MkdirAll(cfg.CacheDir, 0o700); err != nil {
			t.Fatalf("failed to pre-create cache dir: %v", err)
		}
		if _, err := newAutoTLS(cfg, newMockLogger()); err != nil {
			t.Errorf("newAutoTLS() with an existing cache dir error = %v, want nil", err)
		}
	})

	t.Run("reports an uncreatable cache directory", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("running as root: an unwritable parent directory is still writable")
		}
		parent := filepath.Join(t.TempDir(), "readonly")
		if err := os.Mkdir(parent, 0o500); err != nil {
			t.Fatalf("failed to create read-only parent: %v", err)
		}
		cfg := validAutoTLSConfig(t)
		cfg.CacheDir = filepath.Join(parent, "acme")

		_, err := newAutoTLS(cfg, newMockLogger())
		if err == nil {
			t.Fatal("newAutoTLS() error = nil, want an error naming the cache directory")
		}
		if !strings.Contains(err.Error(), cfg.CacheDir) {
			t.Errorf("error %q does not name the cache directory %q", err, cfg.CacheDir)
		}
	})
}

func TestAutoTLSTLSConfig(t *testing.T) {
	at, err := newAutoTLS(validAutoTLSConfig(t), newMockLogger())
	if err != nil {
		t.Fatalf("newAutoTLS() error = %v", err)
	}
	tc := at.tlsConfig()

	if tc.GetCertificate == nil {
		t.Error("GetCertificate is nil: nats-server would have no certificate source")
	}
	// Must stay empty: nats-server's OCSP stapling wrapper iterates
	// Certificates, and an empty slice makes it a no-op instead of an error.
	if len(tc.Certificates) != 0 {
		t.Errorf("Certificates has %d entries, want 0", len(tc.Certificates))
	}
	if tc.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x, want %#x", tc.MinVersion, tls.VersionTLS12)
	}
	for _, proto := range tc.NextProtos {
		if proto == acme.ALPNProto {
			t.Errorf("NextProtos advertises %q, but tls-alpn-01 is not supported", acme.ALPNProto)
		}
	}
}

func TestAutoTLSStartStop(t *testing.T) {
	at, err := newAutoTLS(validAutoTLSConfig(t), newMockLogger())
	if err != nil {
		t.Fatalf("newAutoTLS() error = %v", err)
	}
	if err := at.start(); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	addr := at.ln.Addr().String()

	if at.srv.ReadHeaderTimeout == 0 {
		t.Error("ReadHeaderTimeout is 0: the public challenge port would be exposed to slowloris")
	}

	// autocert's HTTPHandler applies the host policy before looking up the
	// challenge token, so the status code distinguishes "unknown token for a
	// domain we serve" from "domain we do not serve at all".
	tests := []struct {
		name       string
		host       string
		path       string
		wantStatus int
	}{
		{"unknown token for a listed domain", "nats.test.invalid", "/.well-known/acme-challenge/nope", http.StatusNotFound},
		{"challenge for an unlisted domain", "other.test.invalid", "/.well-known/acme-challenge/nope", http.StatusForbidden},
		{"non-challenge path redirects to https", "nats.test.invalid", "/", http.StatusFound},
		// The port must be stripped before the host policy runs: autocert
		// passes r.Host through verbatim, and a challenge listener on any port
		// other than 80 would otherwise answer every challenge with 403.
		{"listed domain with a port in the Host header", "nats.test.invalid:5002", "/.well-known/acme-challenge/nope", http.StatusNotFound},
		{"unlisted domain with a port in the Host header", "other.test.invalid:5002", "/.well-known/acme-challenge/nope", http.StatusForbidden},
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://"+addr+tt.path, nil)
			if err != nil {
				t.Fatalf("failed to build request: %v", err)
			}
			req.Host = tt.host
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}

	at.stop(context.Background())
	if at.srv != nil || at.ln != nil {
		t.Error("stop() left the server or listener set")
	}
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Error("challenge listener still accepts connections after stop()")
	}

	// stop must be safe to call again and on a nil receiver.
	at.stop(context.Background())
	var nilAutoTLS *autoTLS
	nilAutoTLS.stop(context.Background())
}

func TestAutoTLSStartPortInUse(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy a port: %v", err)
	}
	defer func() { _ = busy.Close() }()

	cfg := validAutoTLSConfig(t)
	cfg.HTTPChallengeAddr = busy.Addr().String()
	at, err := newAutoTLS(cfg, newMockLogger())
	if err != nil {
		t.Fatalf("newAutoTLS() error = %v", err)
	}

	err = at.start()
	if err == nil {
		at.stop(context.Background())
		t.Fatal("start() error = nil, want an error for an occupied port")
	}
	if !strings.Contains(err.Error(), cfg.HTTPChallengeAddr) {
		t.Errorf("error %q does not name the address %q", err, cfg.HTTPChallengeAddr)
	}
}

func TestAutoTLSChallengeAddrDefault(t *testing.T) {
	cfg := validAutoTLSConfig(t)
	cfg.HTTPChallengeAddr = ""
	if got := cfg.ChallengeAddr(); got != types.DefaultAutoTLSChallengeAddr {
		t.Errorf("ChallengeAddr() = %q, want %q", got, types.DefaultAutoTLSChallengeAddr)
	}
	cfg.HTTPChallengeAddr = "127.0.0.1:5002"
	if got := cfg.ChallengeAddr(); got != "127.0.0.1:5002" {
		t.Errorf("ChallengeAddr() = %q, want the configured address", got)
	}
}

func TestAutoTLSPrewarm(t *testing.T) {
	t.Run("skipped when the startup fetch is disabled", func(t *testing.T) {
		logger := newMockLogger()
		at, err := newAutoTLS(validAutoTLSConfig(t), logger)
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if err := at.prewarm(context.Background()); err != nil {
			t.Errorf("prewarm() error = %v, want nil when disabled", err)
		}
		if !logger.hasMessage("INFO", "startup certificate fetch disabled") {
			t.Error("expected an INFO log explaining that the startup fetch was skipped")
		}
	})

	t.Run("reports issuance failure and names the domain", func(t *testing.T) {
		// A directory endpoint that answers 200 with a body that is not a
		// valid ACME directory makes discovery fail immediately, so autocert
		// cannot begin an order. A 5xx would be retried with backoff and turn
		// this into a slow timeout test instead of an issuance-failure one.
		acmeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, "this is not an ACME directory")
		}))
		defer acmeSrv.Close()

		cfg := validAutoTLSConfig(t)
		cfg.DirectoryURL = acmeSrv.URL + "/dir"
		cfg.StartupIssueTimeout = 20 * time.Second

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if err := at.start(); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		defer at.stop(context.Background())

		err = at.prewarm(context.Background())
		if err == nil {
			t.Fatal("prewarm() error = nil, want an issuance failure")
		}
		if !strings.Contains(err.Error(), cfg.Domains[0]) {
			t.Errorf("error %q does not name the domain %q", err, cfg.Domains[0])
		}
	})

	t.Run("reports a timeout with actionable context", func(t *testing.T) {
		// A directory endpoint that never responds forces the timeout branch.
		block := make(chan struct{})
		acmeSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			<-block
		}))
		defer func() {
			close(block)
			acmeSrv.Close()
		}()

		cfg := validAutoTLSConfig(t)
		cfg.DirectoryURL = acmeSrv.URL + "/dir"
		cfg.StartupIssueTimeout = 150 * time.Millisecond

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if err := at.start(); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		defer at.stop(context.Background())

		err = at.prewarm(context.Background())
		if err == nil {
			t.Fatal("prewarm() error = nil, want a timeout")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf("error = %q, want a timeout message", err)
		}
		if !strings.Contains(err.Error(), cfg.ChallengeAddr()) {
			t.Errorf("error %q does not mention the challenge address", err)
		}
	})

	t.Run("honours caller cancellation", func(t *testing.T) {
		block := make(chan struct{})
		acmeSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			<-block
		}))
		defer func() {
			close(block)
			acmeSrv.Close()
		}()

		cfg := validAutoTLSConfig(t)
		cfg.DirectoryURL = acmeSrv.URL + "/dir"
		cfg.StartupIssueTimeout = time.Minute

		at, err := newAutoTLS(cfg, newMockLogger())
		if err != nil {
			t.Fatalf("newAutoTLS() error = %v", err)
		}
		if err := at.start(); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		defer at.stop(context.Background())

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err = at.prewarm(ctx)
		if err == nil {
			t.Fatal("prewarm() error = nil, want a cancellation error")
		}
		if !strings.Contains(err.Error(), "cancelled") {
			t.Errorf("error = %q, want a cancellation message", err)
		}
	})
}

// TestAutoTLSPrewarmAbandonmentIsLogged verifies that giving up on an in-flight
// certificate request is reported rather than silent.
//
// autocert offers no way to cancel a running GetCertificate, so the request can
// only be abandoned. A supervised restart loop that keeps timing out would
// otherwise accumulate invisible in-flight requests racing on the same cache.
func TestAutoTLSPrewarmAbandonmentIsLogged(t *testing.T) {
	block := make(chan struct{})
	acmeSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block
	}))
	defer func() {
		close(block)
		acmeSrv.Close()
	}()

	cfg := validAutoTLSConfig(t)
	cfg.DirectoryURL = acmeSrv.URL + "/dir"
	cfg.StartupIssueTimeout = 150 * time.Millisecond

	logger := newMockLogger()
	at, err := newAutoTLS(cfg, logger)
	if err != nil {
		t.Fatalf("newAutoTLS() error = %v", err)
	}
	if err := at.start(); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	defer at.stop(context.Background())

	if err := at.prewarm(context.Background()); err == nil {
		t.Fatal("prewarm() error = nil, want a timeout")
	}
	if !logger.hasMessage("WARN", "abandoning an in-flight ACME certificate request") {
		t.Error("expected a WARN log when an in-flight request is abandoned")
	}
}

// TestNewACMEClientAlwaysBounded is the regression guard for the fix to the
// review finding: before it, only the DirectoryCAPool branch produced an
// HTTPClient, so the common configurations fell back to http.DefaultClient,
// which has no timeout.
func TestNewACMEClientAlwaysBounded(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*types.AutoTLSConfig)
		wantURL string
	}{
		{
			name:    "no directory URL and no CA pool",
			mutate:  func(*types.AutoTLSConfig) {},
			wantURL: autocert.DefaultACMEDirectory,
		},
		{
			name:    "directory URL only",
			mutate:  func(c *types.AutoTLSConfig) { c.DirectoryURL = "https://acme.test.invalid/dir" },
			wantURL: "https://acme.test.invalid/dir",
		},
		{
			name:    "CA pool only",
			mutate:  func(c *types.AutoTLSConfig) { c.DirectoryCAPool = x509.NewCertPool() },
			wantURL: autocert.DefaultACMEDirectory,
		},
		{
			name: "directory URL and CA pool",
			mutate: func(c *types.AutoTLSConfig) {
				c.DirectoryURL = "https://acme.test.invalid/dir"
				c.DirectoryCAPool = x509.NewCertPool()
			},
			wantURL: "https://acme.test.invalid/dir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validAutoTLSConfig(t)
			tt.mutate(cfg)

			client := newACMEClient(cfg)
			if client.DirectoryURL != tt.wantURL {
				t.Errorf("DirectoryURL = %q, want %q", client.DirectoryURL, tt.wantURL)
			}
			if client.HTTPClient == nil {
				t.Fatal("HTTPClient is nil: acme would fall back to http.DefaultClient, which has no timeout")
			}
			if client.HTTPClient.Timeout != acmeRequestTimeout {
				t.Errorf("HTTPClient.Timeout = %v, want %v", client.HTTPClient.Timeout, acmeRequestTimeout)
			}
			tr, ok := client.HTTPClient.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("Transport type = %T, want *http.Transport", client.HTTPClient.Transport)
			}
			if cfg.DirectoryCAPool != nil && tr.TLSClientConfig.RootCAs != cfg.DirectoryCAPool {
				t.Error("RootCAs is not the configured DirectoryCAPool")
			}
			// The clone must keep the standard library's connection pooling
			// rather than silently dropping it.
			if tr.Proxy == nil {
				t.Error("Transport lost http.DefaultTransport's settings; it should be a clone")
			}
		})
	}
}

// TestAutoTLSIssueTimeoutDefault documents the default startup fetch budget.
func TestAutoTLSIssueTimeoutDefault(t *testing.T) {
	cfg := validAutoTLSConfig(t)
	cfg.StartupIssueTimeout = 0
	if got := cfg.IssueTimeout(); got != types.DefaultAutoTLSStartupIssueTimeout {
		t.Errorf("IssueTimeout() = %v, want %v", got, types.DefaultAutoTLSStartupIssueTimeout)
	}
	cfg.StartupIssueTimeout = -1
	if got := cfg.IssueTimeout(); got != -1 {
		t.Errorf("IssueTimeout() = %v, want the negative value preserved", got)
	}
}
