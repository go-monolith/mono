package nats

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func TestApplyClusterAuth(t *testing.T) {
	t.Run("adds credentials to routes without any and keeps a route's own", func(t *testing.T) {
		own := mustParseURL(t, "nats://other:pw@10.0.0.2:6222")
		opts := &server.Options{
			Cluster: server.ClusterOpts{Port: 6222},
			Routes:  []*url.URL{mustParseURL(t, "nats://10.0.0.1:6222"), own, nil},
		}
		if err := applyClusterAuth(opts, "route", "s3cret"); err != nil {
			t.Fatalf("applyClusterAuth: %v", err)
		}
		if opts.Cluster.Username != "route" || opts.Cluster.Password != "s3cret" {
			t.Errorf("cluster authorization = %q/%q", opts.Cluster.Username, opts.Cluster.Password)
		}
		if got := opts.Routes[0].User.String(); got != "route:s3cret" {
			t.Errorf("route 0 user = %q, want route:s3cret", got)
		}
		if opts.Routes[1] != own || opts.Routes[1].User.Username() != "other" {
			t.Errorf("a route with its own credentials must be kept as is, got %v", opts.Routes[1].Redacted())
		}
		if opts.Routes[2] != nil {
			t.Errorf("nil route must stay nil")
		}
	})

	t.Run("does not modify the caller's URL", func(t *testing.T) {
		orig := mustParseURL(t, "nats://10.0.0.1:6222")
		opts := &server.Options{Cluster: server.ClusterOpts{Port: 6222}, Routes: []*url.URL{orig}}
		if err := applyClusterAuth(opts, "route", "s3cret"); err != nil {
			t.Fatalf("applyClusterAuth: %v", err)
		}
		if orig.User != nil {
			t.Errorf("the original URL was modified: %v", orig.Redacted())
		}
	})

	t.Run("no credentials leaves options unchanged", func(t *testing.T) {
		opts := &server.Options{Routes: []*url.URL{mustParseURL(t, "nats://10.0.0.1:6222")}}
		if err := applyClusterAuth(opts, "", ""); err != nil {
			t.Fatalf("applyClusterAuth: %v", err)
		}
		if opts.Routes[0].User != nil || opts.Cluster.Username != "" {
			t.Errorf("options changed without credentials")
		}
	})

	t.Run("credentials without clustering", func(t *testing.T) {
		err := applyClusterAuth(&server.Options{}, "route", "s3cret")
		if err == nil || !strings.Contains(err.Error(), "clustering is not enabled") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestValidateClusteredJetStream(t *testing.T) {
	routes := []*url.URL{mustParseURL(t, "nats://10.0.0.1:6222")}
	tests := []struct {
		name    string
		opts    server.Options
		wantErr string
	}{
		{"not clustered", server.Options{JetStream: true}, ""},
		{"clustered without JetStream", server.Options{Cluster: server.ClusterOpts{Port: 6222}}, ""},
		{"complete", server.Options{JetStream: true, ServerName: "n-0", Cluster: server.ClusterOpts{Port: 6222, Name: "c"}, Routes: routes}, ""},
		{"no server name", server.Options{JetStream: true, Cluster: server.ClusterOpts{Port: 6222, Name: "c"}, Routes: routes}, "requires a server name"},
		{"server name with space", server.Options{JetStream: true, ServerName: "n 0", Cluster: server.ClusterOpts{Port: 6222, Name: "c"}, Routes: routes}, "must not contain whitespace"},
		{"no cluster name", server.Options{JetStream: true, ServerName: "n-0", Cluster: server.ClusterOpts{Port: 6222}, Routes: routes}, "requires a cluster name"},
		{"no routes", server.Options{JetStream: true, ServerName: "n-0", Cluster: server.ClusterOpts{Port: 6222, Name: "c"}}, "requires at least one route"},
		{"no routes but a leafnode remote", server.Options{JetStream: true, ServerName: "n-0", Cluster: server.ClusterOpts{Port: 6222, Name: "c"},
			LeafNode: server.LeafNodeOpts{Remotes: []*server.RemoteLeafOpts{{LocalAccount: "$SYS"}}}}, ""},
		{"no routes and an empty remote list", server.Options{JetStream: true, ServerName: "n-0", Cluster: server.ClusterOpts{Port: 6222, Name: "c"},
			LeafNode: server.LeafNodeOpts{Remotes: []*server.RemoteLeafOpts{}}}, "requires at least one route"},
		{"random cluster port", server.Options{JetStream: true, Cluster: server.ClusterOpts{Port: -1, Name: "c"}, Routes: routes}, "requires a server name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusteredJetStream(&tt.opts)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestRedactURLs(t *testing.T) {
	got := redactURLs([]string{
		"nats://route:s3cret@h:6222",
		"nats://h:6222",
		"://bad s3cret",
		"nats://route:1234/s3cret@h:6222", // unencoded "/" in the password
	})
	want := []string{"nats://route@h:6222", "nats://h:6222", "<invalid>", "<invalid>"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("redactURLs[%d] = %q, want %q", i, got[i], want[i])
		}
		if strings.Contains(got[i], "s3cret") {
			t.Errorf("redactURLs[%d] leaks the password: %q", i, got[i])
		}
	}
}

func TestValidateRouteURL(t *testing.T) {
	tests := []struct {
		raw   string
		valid bool
	}{
		{"nats://h:6222", true},
		{"nats://route:s3cret@h:6222", true},
		{"nats://route:a%2Fb%23c@h:6222", true}, // percent-encoded "/" and "#"
		{"nats://h:6222/", true},
		{"nats://route:abc/s3cret@h:6222", false}, // "/" ends the authority
		{"nats://route:abc#s3cret@h:6222", false}, // "#" starts a fragment
		{"nats://route:abc?s3cret@h:6222", false}, // "?" starts a query
		{"h:6222", false},
		{"nats:opaque", false},
		{"://bad s3cret", false},
	}
	for _, tt := range tests {
		err := ValidateRouteURL(3, tt.raw)
		if (err == nil) != tt.valid {
			t.Errorf("ValidateRouteURL(%q) = %v, want valid=%v", tt.raw, err, tt.valid)
		}
		if err != nil {
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("error leaks the password: %v", err)
			}
			if !strings.Contains(err.Error(), "index 3") {
				t.Errorf("error should name the route by index: %v", err)
			}
		}
	}
}

func TestWithServerNameAndClusterAuthOptions(t *testing.T) {
	cfg := DefaultNATSConfig()
	if err := WithServerName("n-0")(cfg); err != nil || cfg.ServerName != "n-0" {
		t.Fatalf("WithServerName: %v, %q", err, cfg.ServerName)
	}
	if err := WithServerName("")(cfg); err == nil {
		t.Error("empty server name must be rejected")
	}
	if err := WithServerName("n 0")(cfg); err == nil {
		t.Error("server name with whitespace must be rejected")
	}
	if err := WithClusterAuth("route", "pw")(cfg); err != nil || cfg.ClusterUsername != "route" || cfg.ClusterPassword != "pw" {
		t.Fatalf("WithClusterAuth: %v", err)
	}
	if err := WithClusterAuth("route", "")(cfg); err == nil {
		t.Error("empty password must be rejected")
	}
	if err := WithClusterAuth("", "pw")(cfg); err == nil {
		t.Error("empty username must be rejected")
	}
	if err := WithClustering("c", "127.0.0.1", 6222, []string{"nats://route:a/s3cret@h:6222"})(cfg); err == nil ||
		strings.Contains(err.Error(), "s3cret") {
		t.Errorf("WithClustering must reject a malformed route without quoting it: %v", err)
	}
}

// startClusterNode starts a manager with the given options on free ports and
// stops it at the end of the test.
func startClusterNode(t *testing.T, clusterPort int, opts ...NATSOption) (*natsManager, error) {
	t.Helper()
	port, err := findAvailablePort()
	if err != nil {
		t.Fatalf("find port: %v", err)
	}
	all := append([]NATSOption{WithHost("127.0.0.1"), WithPort(port)}, opts...)
	if clusterPort > 0 {
		// A route to itself: clustered JetStream requires at least one.
		self := fmt.Sprintf("nats://127.0.0.1:%d", clusterPort)
		all = append(all, WithClustering("c", "127.0.0.1", clusterPort, []string{self}))
	}
	mgr, err := NewNATSManager(newMockLogger(), all...)
	if err != nil {
		t.Fatalf("NewNATSManager: %v", err)
	}
	m := mgr.(*natsManager)
	if err := m.Start(context.Background()); err != nil {
		return m, err
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m, nil
}

func TestNATSManager_ClusteredJetStreamServerName(t *testing.T) {
	clusterPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	m, err := startClusterNode(t, clusterPort,
		WithServerName("node-a"), WithJetStream(t.TempDir()))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := m.server.Name(); got != "node-a" {
		t.Errorf("server name = %q, want node-a", got)
	}
	if !m.logger.(*mockLogger).hasMessage("INFO", "NATS server started") {
		t.Error("start not logged")
	}
}

func TestNATSManager_ConfigFileClusteredJetStreamWithoutServerName(t *testing.T) {
	clusterPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(t.TempDir(), "nats.conf")
	body := fmt.Sprintf("cluster { name: c, listen: \"127.0.0.1:%d\", routes: [\"nats://127.0.0.1:%d\"] }\njetstream { store_dir: %q }\n",
		clusterPort, clusterPort, t.TempDir())
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = startClusterNode(t, 0, WithConfigFile(conf), WithJetStream(""))
	if err == nil || !strings.Contains(err.Error(), "requires a server name") {
		t.Fatalf("error = %v, want the framework's server name error", err)
	}

	// The same file with a programmatic name starts.
	if _, err := startClusterNode(t, 0, WithConfigFile(conf), WithJetStream(""), WithServerName("node-b")); err != nil {
		t.Fatalf("start with server name: %v", err)
	}
}

// waitForRoutes waits until m has at least n routes, or fails after timeout.
func waitForRoutes(m *natsManager, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.server.NumRoutes() >= n {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestNATSManager_ClusterAuth(t *testing.T) {
	seedPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := startClusterNode(t, seedPort, WithClusterAuth("route", "s3cret"))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	seedRoute := fmt.Sprintf("nats://127.0.0.1:%d", seedPort)

	joinPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	join, err := startClusterNode(t, 0,
		WithClustering("c", "127.0.0.1", joinPort, []string{seedRoute}),
		WithClusterAuth("route", "s3cret"))
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if !waitForRoutes(seed, 1, 5*time.Second) || !waitForRoutes(join, 1, 5*time.Second) {
		t.Fatalf("authenticated route not formed: seed=%d join=%d", seed.server.NumRoutes(), join.server.NumRoutes())
	}
	for _, msg := range join.logger.(*mockLogger).messages {
		if strings.Contains(fmt.Sprint(msg.args...), "s3cret") {
			t.Errorf("password logged in %q: %v", msg.msg, msg.args)
		}
	}

	// A server with the wrong password is refused.
	badPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	bad, err := startClusterNode(t, 0,
		WithClustering("c", "127.0.0.1", badPort, []string{seedRoute}),
		WithClusterAuth("route", "wrong"))
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if waitForRoutes(bad, 1, 1500*time.Millisecond) {
		t.Errorf("a route with the wrong password was accepted")
	}
}

// TestNATSManager_ClusteringMergesConfigFile checks that WithClustering is
// merged into the config file's cluster{} block instead of replacing it: the
// route authorization only the file sets is still required.
func TestNATSManager_ClusteringMergesConfigFile(t *testing.T) {
	seedPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(t.TempDir(), "nats.conf")
	body := "cluster { name: c, authorization { user: route, password: fromfile } }\n"
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	seed, err := startClusterNode(t, seedPort, WithConfigFile(conf))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if seed.server.ClusterAddr() == nil {
		t.Fatal("seed has no cluster listener")
	}

	seedRoute := fmt.Sprintf("nats://127.0.0.1:%d", seedPort)
	noAuthPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	noAuth, err := startClusterNode(t, 0, WithClustering("c", "127.0.0.1", noAuthPort, []string{seedRoute}))
	if err != nil {
		t.Fatalf("no auth: %v", err)
	}
	if waitForRoutes(noAuth, 1, 1500*time.Millisecond) {
		t.Fatal("the file's route authorization was dropped: a route without credentials was accepted")
	}

	authPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	withAuth, err := startClusterNode(t, 0,
		WithClustering("c", "127.0.0.1", authPort, []string{seedRoute}),
		WithClusterAuth("route", "fromfile"))
	if err != nil {
		t.Fatalf("with auth: %v", err)
	}
	if !waitForRoutes(withAuth, 1, 5*time.Second) {
		t.Fatal("route with the file's credentials not formed")
	}
}

// TestNATSManager_InvalidRouteDoesNotLeakPassword covers a route that bypassed
// the option check (set on the config directly): Start fails and neither the
// error nor the log contains the password.
func TestNATSManager_InvalidRouteDoesNotLeakPassword(t *testing.T) {
	port, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	logger := newMockLogger()
	m := &natsManager{
		config: &NATSConfig{
			Host: "127.0.0.1", Port: port, StartupReadyTimeout: 5 * time.Second, MaxPayload: 1024 * 1024,
			ClusterEnabled: true, ClusterName: "c", ClusterHost: "127.0.0.1", ClusterPort: port + 1,
			ClusterRoutes: []string{"nats://route:abc/s3cret@h:6222"},
		},
		logger: logger,
	}
	err = m.Start(context.Background())
	if err == nil {
		_ = m.Stop(context.Background())
		t.Fatal("expected an error for a malformed route")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks the password: %v", err)
	}
	for _, msg := range logger.messages {
		if strings.Contains(fmt.Sprint(msg.args...), "s3cret") {
			t.Errorf("log %q leaks the password", msg.msg)
		}
	}
}

// TestNATSManager_ClusterAuthOnConfigFileRoutes checks that credentials are
// added to the config file's routes that have none.
func TestNATSManager_ClusterAuthOnConfigFileRoutes(t *testing.T) {
	seedPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startClusterNode(t, seedPort, WithClusterAuth("route", "s3cret")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ownPort, err := findAvailablePort()
	if err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(t.TempDir(), "nats.conf")
	body := fmt.Sprintf("cluster { name: c, listen: \"127.0.0.1:%d\", routes: [\"nats://127.0.0.1:%d\", \"nats://other:pw@127.0.0.1:1\"] }\n",
		ownPort, seedPort)
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	node, err := startClusterNode(t, 0, WithConfigFile(conf), WithClusterAuth("route", "s3cret"))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	if !waitForRoutes(node, 1, 5*time.Second) {
		t.Fatal("the file route without credentials did not get them")
	}
	// A route with credentials of its own keeps them: see TestApplyClusterAuth.
}
