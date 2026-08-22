//go:build integration
// +build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// =============================================================================
// End-to-end ACME certificate issuance against Pebble
//
// Pebble is Let's Encrypt's purpose-built ACME test CA. Let's Encrypt's own
// documentation recommends it over the staging environment for CI, because
// staging needs a publicly resolvable domain and inbound port 80. Pebble runs
// as a container rather than a Go dependency so that go.mod stays minimal.
//
// The staging environment is still covered, by nats_autotls_staging_test.go
// behind the acme_staging build tag.
// =============================================================================

const (
	pebbleImage = "ghcr.io/letsencrypt/pebble:latest"
	// pebbleDomain is the name certificates are issued for. Pebble resolves it
	// via the container's /etc/hosts, populated with --add-host.
	pebbleDomain = "mono-acme.test"
	// pebbleChallengePort is Pebble's default http-01 validation port. The
	// framework's challenge listener must bind it so that validation reaches
	// the framework and not something else.
	pebbleChallengePort = 5002
	pebbleDirectoryURL  = "https://localhost:14000/dir"
	// Pebble's order and finalize paths, which share the same order id. Used by
	// startPebbleProxy to rebuild the Location header Pebble omits.
	pebbleOrderPrefix    = "/my-order/"
	pebbleFinalizePrefix = "/finalize-order/"
	pebbleRootURL        = "https://localhost:15000/roots/0"
	pebbleMinicaPath     = "/test/certs/pebble.minica.pem"
)

// requireDocker skips the test unless a usable Docker daemon is present.
//
// The skip is deliberately narrow: on a Linux host with Docker - which is what
// CI runs - this test executes a full ACME order and must pass, not skip.
func requireDocker(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("Pebble ACME test requires Docker host networking, which is Linux-only")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found in PATH; skipping Pebble ACME test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker daemon unavailable (%v); skipping Pebble ACME test: %s", err, out)
	}
}

// startPebbleProxy fronts Pebble with a TLS reverse proxy that supplies the one
// header Pebble omits, and returns the proxy's directory URL and the pool that
// trusts it.
//
// WHY THIS EXISTS. Pebble's FinalizeOrder answers 200 with
// "status": "processing" and no Location header (wfe/wfe.go). RFC 8555 mandates
// Location on order *creation* but not on finalize, so Pebble is compliant -
// but golang.org/x/crypto/acme reads an order's URL exclusively from that
// header (responseOrder, acme/rfc8555.go), so CreateOrderCert falls into
// WaitOrder with an empty URL and fails with
//
//	Post "": unsupported protocol scheme ""
//
// Boulder, which is what Let's Encrypt actually runs, evidently does send it -
// autocert is used against Let's Encrypt in production at scale - so the gap
// appears only against Pebble. It is unfixed as of x/crypto v0.55.0, and
// golang/go#39284 is closed as not-planned.
//
// The proxy supplies the header the real CA would have sent, and nothing else.
// It is guarded on the header being absent, so it becomes inert the moment
// x/crypto stops depending on it - there is nothing here to remember to remove.
//
// This works because Pebble mints its absolute URLs from request.Host and
// honours X-Forwarded-Proto (relativeEndpoint, wfe/wfe.go), so forwarding with
// the proxy's own Host makes every subsequent ACME call - including finalize -
// route back through the proxy.
func startPebbleProxy(t *testing.T, upstreamPool *x509.CertPool) (directoryURL string, proxyPool *x509.CertPool) {
	t.Helper()

	upstream, err := url.Parse("https://localhost:14000")
	if err != nil {
		t.Fatalf("failed to parse the Pebble upstream URL: %v", err)
	}

	const proxyHostHeader = "X-Mono-Proxy-Host"

	rp := httputil.NewSingleHostReverseProxy(upstream)
	rp.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: upstreamPool, MinVersion: tls.VersionTLS12},
	}
	setUpstream := rp.Director
	rp.Director = func(r *http.Request) {
		host := r.Host // the proxy's own address, before the director rewrites it
		setUpstream(r)
		r.Host = host
		r.Header.Set("X-Forwarded-Proto", "https")
		// Carried through so ModifyResponse can rebuild a proxy-facing URL;
		// resp.Request.URL points at the upstream by then.
		r.Header.Set(proxyHostHeader, host)
	}
	rp.ModifyResponse = func(resp *http.Response) error {
		path := resp.Request.URL.Path
		if !strings.HasPrefix(path, pebbleFinalizePrefix) || resp.Header.Get("Location") != "" {
			return nil
		}
		// Pebble's order and finalize URLs share the same opaque id.
		orderID := strings.TrimPrefix(path, pebbleFinalizePrefix)
		resp.Header.Set("Location", "https://"+resp.Request.Header.Get(proxyHostHeader)+pebbleOrderPrefix+orderID)
		return nil
	}
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		t.Errorf("Pebble proxy failed to forward a request: %v", err)
		w.WriteHeader(http.StatusBadGateway)
	}

	srv := httptest.NewTLSServer(rp)
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return srv.URL + "/dir", pool
}

// startPebble runs a Pebble container on the host network and returns the CA
// pool for its directory endpoint and the pool that issued certificates chain
// to. The two are different certificate authorities and must not be conflated:
// the directory is served under pebble.minica.pem, while issued certificates
// chain to the root exposed on the management port.
func startPebble(t *testing.T) (directoryPool, issuedPool *x509.CertPool) {
	t.Helper()

	name := fmt.Sprintf("mono-pebble-%d", os.Getpid())
	_ = exec.Command("docker", "rm", "-f", name).Run()

	// --network host puts Pebble in the test process's network namespace, so
	// 127.0.0.1 inside the container is the loopback the framework's challenge
	// listener binds. Docker still writes a container-private /etc/hosts, so
	// --add-host is what makes the test domain resolve for Pebble's validator.
	//
	// PEBBLE_VA_NOSLEEP removes a randomised 0-15s validation delay that would
	// otherwise dominate the runtime. PEBBLE_WFE_NONCEREJECT disables the
	// deliberate 5% bad-nonce injection, which is realistic but makes the test
	// non-deterministic.
	//
	// PEBBLE_VA_ALWAYS_VALID is deliberately NOT set: it would short-circuit
	// challenge validation entirely, and the test would then pass without ever
	// proving that the framework's http-01 listener works, which is the single
	// most valuable assertion here.
	args := []string{
		"run", "-d", "--rm",
		"--name", name,
		"--network", "host",
		"--add-host", pebbleDomain + ":127.0.0.1",
		"-e", "PEBBLE_VA_NOSLEEP=1",
		"-e", "PEBBLE_WFE_NONCEREJECT=0",
		pebbleImage,
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Skipf("failed to start Pebble container (%v); skipping: %s", err, out)
	}
	t.Cleanup(func() {
		if t.Failed() {
			if logs, err := exec.Command("docker", "logs", name).CombinedOutput(); err == nil {
				t.Logf("Pebble container logs:\n%s", logs)
			}
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	minicaPath := filepath.Join(t.TempDir(), "minica.pem")
	if out, err := exec.Command("docker", "cp", name+":"+pebbleMinicaPath, minicaPath).CombinedOutput(); err != nil {
		// A hard failure rather than a skip: silently falling back to
		// InsecureSkipVerify would hide a real regression.
		t.Fatalf("failed to copy %s out of the Pebble container (%v): %s", pebbleMinicaPath, err, out)
	}
	minicaPEM, err := os.ReadFile(minicaPath)
	if err != nil {
		t.Fatalf("failed to read Pebble minica: %v", err)
	}
	directoryPool = x509.NewCertPool()
	if !directoryPool.AppendCertsFromPEM(minicaPEM) {
		t.Fatal("failed to parse the Pebble minica certificate")
	}

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: directoryPool, MinVersion: tls.VersionTLS12}},
	}
	waitForHTTP(t, client, pebbleDirectoryURL, 45*time.Second)

	rootPEM := fetch(t, client, pebbleRootURL)
	issuedPool = x509.NewCertPool()
	if !issuedPool.AppendCertsFromPEM(rootPEM) {
		t.Fatal("failed to parse the Pebble issuing root")
	}
	// Pebble issues through an intermediate; include it so chain verification
	// does not depend on the server sending a complete chain.
	if interPEM, err := tryFetch(client, "https://localhost:15000/intermediates/0"); err == nil {
		issuedPool.AppendCertsFromPEM(interPEM)
	}

	return directoryPool, issuedPool
}

func waitForHTTP(t *testing.T, client *http.Client, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := tryFetch(client, url); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready within %s: %v", url, timeout, lastErr)
}

func tryFetch(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return body, nil
}

func fetch(t *testing.T, client *http.Client, url string) []byte {
	t.Helper()
	body, err := tryFetch(client, url)
	if err != nil {
		t.Fatalf("failed to fetch %s: %v", url, err)
	}
	return body
}

// autoTLSTestModule is a minimal module providing a request-reply service, used
// to prove that an externally connected TLS client can actually talk to the
// application and not merely complete a handshake.
type autoTLSTestModule struct {
	container mono.ServiceContainer
	mu        sync.Mutex
}

func (m *autoTLSTestModule) Name() string                { return "autotls" }
func (m *autoTLSTestModule) Start(context.Context) error { return nil }
func (m *autoTLSTestModule) Stop(context.Context) error  { return nil }
func (m *autoTLSTestModule) SetEventBus(mono.EventBus)   {}

func (m *autoTLSTestModule) RegisterServices(container mono.ServiceContainer) error {
	m.mu.Lock()
	m.container = container
	m.mu.Unlock()
	return container.RegisterRequestReplyService("echo", func(_ context.Context, msg *mono.Msg) ([]byte, error) {
		return append([]byte("echo:"), msg.Data...), nil
	})
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestAutoTLS_PebbleEndToEnd(t *testing.T) {
	requireDocker(t)
	directoryPool, issuedPool := startPebble(t)

	// Pebble is reached through a proxy that supplies the Location header it
	// omits on finalize; see startPebbleProxy for the full rationale.
	directoryURL, proxyPool := startPebbleProxy(t, directoryPool)

	natsPort := freePort(t)
	cacheDir := filepath.Join(t.TempDir(), "acme")

	autoTLS := types.AutoTLSConfig{
		Domains:           []string{pebbleDomain},
		Email:             "ops@" + pebbleDomain,
		CacheDir:          cacheDir,
		HTTPChallengeAddr: fmt.Sprintf("127.0.0.1:%d", pebbleChallengePort),
		DirectoryURL:      directoryURL,
		DirectoryCAPool:   proxyPool,
		AcceptTOS:         true,
		// A positive timeout means a successful Start already proves that a
		// full ACME order completed through the framework's own http-01
		// listener, before any assertion below runs.
		StartupIssueTimeout: 90 * time.Second,
	}

	app, err := mono.NewMonoApplication(
		mono.WithNATSHost("127.0.0.1"),
		mono.WithNATSPort(natsPort),
		mono.WithNATSAutoTLS(autoTLS),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication() error = %v", err)
	}
	if err := app.Register(&autoTLSTestModule{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v (certificate issuance through Pebble failed)", err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		if err := app.Stop(stopCtx); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	}
	defer stop()

	serverAddr := fmt.Sprintf("tls://127.0.0.1:%d", natsPort)

	t.Run("external TLS client connects and round-trips a request", func(t *testing.T) {
		// Dial by IP with an SNI override: the test host has no DNS entry for
		// the Pebble domain (only the container does), but the certificate is
		// still selected and verified by name.
		nc, err := nats.Connect(serverAddr, nats.Secure(&tls.Config{
			ServerName: pebbleDomain,
			RootCAs:    issuedPool,
			MinVersion: tls.VersionTLS12,
		}))
		if err != nil {
			t.Fatalf("TLS connect failed: %v", err)
		}
		defer nc.Close()

		reply, err := nc.Request("services.autotls.echo", []byte("ping"), 5*time.Second)
		if err != nil {
			t.Fatalf("Request() over TLS failed: %v", err)
		}
		if got, want := string(reply.Data), "echo:ping"; got != want {
			t.Errorf("reply = %q, want %q", got, want)
		}

		state, err := nc.TLSConnectionState()
		if err != nil {
			t.Fatalf("TLSConnectionState() error = %v", err)
		}
		if len(state.PeerCertificates) == 0 {
			t.Fatal("server presented no certificate")
		}
		leaf := state.PeerCertificates[0]
		if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != pebbleDomain {
			t.Errorf("certificate DNSNames = %v, want [%s]", leaf.DNSNames, pebbleDomain)
		}
		if time.Now().After(leaf.NotAfter) {
			t.Errorf("certificate already expired at %s", leaf.NotAfter)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			DNSName:       pebbleDomain,
			Roots:         issuedPool,
			Intermediates: intermediatesFrom(state.PeerCertificates),
		}); err != nil {
			t.Errorf("certificate does not chain to the Pebble root: %v", err)
		}
	})

	t.Run("plaintext clients are rejected", func(t *testing.T) {
		nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", natsPort), nats.Timeout(3*time.Second))
		if err == nil {
			nc.Close()
			t.Fatal("plaintext connect succeeded, want rejection: AutoTLS must make TLS mandatory")
		}
	})

	t.Run("unlisted SNI names are refused", func(t *testing.T) {
		nc, err := nats.Connect(serverAddr, nats.Timeout(5*time.Second), nats.Secure(&tls.Config{
			ServerName: "not-in-the-allowlist.test",
			RootCAs:    issuedPool,
			MinVersion: tls.VersionTLS12,
		}))
		if err == nil {
			nc.Close()
			t.Fatal("handshake for an unlisted domain succeeded, want HostPolicy rejection")
		}
	})

	t.Run("restart serves the cached certificate without reissuing", func(t *testing.T) {
		stop()

		// Point the directory at a closed port: if anything tries to reach the
		// CA on this start, it fails loudly instead of silently reissuing.
		dead := freePort(t)
		cached := autoTLS
		cached.DirectoryURL = fmt.Sprintf("https://127.0.0.1:%d/dir", dead)
		cached.StartupIssueTimeout = 20 * time.Second

		restarted, err := mono.NewMonoApplication(
			mono.WithNATSHost("127.0.0.1"),
			mono.WithNATSPort(freePort(t)),
			mono.WithNATSAutoTLS(cached),
		)
		if err != nil {
			t.Fatalf("NewMonoApplication() error = %v", err)
		}
		if err := restarted.Register(&autoTLSTestModule{}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}

		startCtx, startCancel := context.WithTimeout(context.Background(), time.Minute)
		defer startCancel()
		if err := restarted.Start(startCtx); err != nil {
			t.Fatalf("restart with a warm cache failed, so the certificate was not reused: %v", err)
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		if err := restarted.Stop(stopCtx); err != nil {
			t.Errorf("Stop() error = %v", err)
		}

		// The cache directory must actually hold the certificate.
		entries, err := os.ReadDir(cacheDir)
		if err != nil {
			t.Fatalf("failed to read the cache directory: %v", err)
		}
		var found bool
		for _, e := range entries {
			if strings.Contains(e.Name(), pebbleDomain) {
				found = true
			}
		}
		if !found {
			t.Errorf("cache directory %s holds no entry for %s: %v", cacheDir, pebbleDomain, entries)
		}
	})
}

// intermediatesFrom builds a pool from every certificate the server sent after
// the leaf.
func intermediatesFrom(chain []*x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, c := range chain[1:] {
		pool.AddCert(c)
	}
	return pool
}
