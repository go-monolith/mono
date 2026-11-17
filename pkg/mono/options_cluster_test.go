package mono_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

func TestWithNATSServerName(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"statefulset pod name", "orders-0", ""},
		{"empty", "", "server name cannot be empty"},
		{"space", "orders 0", "must not contain whitespace"},
		{"tab", "orders\t0", "must not contain whitespace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSServerName(tt.value)(cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.ServerName != tt.value {
					t.Errorf("ServerName = %q, want %q", cfg.NATSOptions.ServerName, tt.value)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if !monoerrors.IsConfigurationError(err) {
				t.Errorf("error should be an invalid configuration error: %v", err)
			}
		})
	}
}

func TestWithNATSClusterAuth(t *testing.T) {
	tests := []struct {
		name, user, pass, wantErr string
	}{
		{"valid", "route", "s3cret", ""},
		{"empty username", "", "s3cret", "cluster username cannot be empty"},
		{"empty password", "route", "", "cluster password cannot be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mono.DefaultConfig()
			err := mono.WithNATSClusterAuth(tt.user, tt.pass)(cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.NATSOptions.ClusterUsername != tt.user || cfg.NATSOptions.ClusterPassword != tt.pass {
					t.Errorf("credentials not stored: %+v", cfg.NATSOptions)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), tt.pass) && tt.pass != "" {
				t.Errorf("error must not contain the password: %v", err)
			}
		})
	}
}

// TestNewMonoApplication_ClusterValidation checks the cross-field rules of the
// clustering options, which NewMonoApplication applies once every option is
// known and before it starts the embedded server. Successful combinations
// start a server and are covered by the internal/nats manager tests.
func TestNewMonoApplication_ClusterValidation(t *testing.T) {
	route := []string{"nats://127.0.0.1:16299"}

	tests := []struct {
		name    string
		opts    func(t *testing.T) []mono.MonoFrameworkOption
		wantErr string
		// wantConfigErr says whether the error is the framework's own
		// configuration error (a cross-field rule) or a later failure.
		wantConfigErr bool
	}{
		{
			name: "clustered JetStream without server name",
			opts: func(t *testing.T) []mono.MonoFrameworkOption {
				return []mono.MonoFrameworkOption{
					mono.WithNATSClustering("c", "127.0.0.1", 16298, route),
					mono.WithJetStreamStorageDir(filepath.Join(t.TempDir(), "js")),
				}
			},
			wantErr:       "clustered JetStream requires a server name",
			wantConfigErr: true,
		},
		{
			name: "clustered JetStream without routes",
			opts: func(t *testing.T) []mono.MonoFrameworkOption {
				return []mono.MonoFrameworkOption{
					mono.WithNATSServerName("n-0"),
					mono.WithNATSClustering("c", "127.0.0.1", 16298, nil),
					mono.WithJetStreamStorageDir(filepath.Join(t.TempDir(), "js")),
				}
			},
			wantErr:       "clustered JetStream requires at least one route",
			wantConfigErr: true,
		},
		{
			name: "cluster credentials without clustering",
			opts: func(_ *testing.T) []mono.MonoFrameworkOption {
				return []mono.MonoFrameworkOption{mono.WithNATSClusterAuth("route", "s3cret")}
			},
			wantErr:       "cluster credentials require WithNATSClustering",
			wantConfigErr: true,
		},
		{
			// A config file may provide server_name and a cluster{} block,
			// so the rules are checked at Start on the merged configuration:
			// here the (missing) file is what fails, not the rules.
			name: "config file defers the rules to Start",
			opts: func(t *testing.T) []mono.MonoFrameworkOption {
				return []mono.MonoFrameworkOption{
					mono.WithNATSConfigFile(filepath.Join(t.TempDir(), "missing.conf")),
					mono.WithNATSClusterAuth("route", "s3cret"),
				}
			},
			wantErr: "failed to process NATS config file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mono.NewMonoApplication(tt.opts(t)...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if got := monoerrors.IsConfigurationError(err); got != tt.wantConfigErr {
				t.Errorf("IsConfigurationError = %v, want %v (%v)", got, tt.wantConfigErr, err)
			}
		})
	}
}

// TestNewMonoApplication_SeedNodeWithoutJetStream guards the documented seed
// node form: clustering without routes, JetStream or a server name.
func TestNewMonoApplication_SeedNodeWithoutJetStream(t *testing.T) {
	clientPort, clusterPort := freePort(t), freePort(t)
	app, err := mono.NewMonoApplication(
		mono.WithNATSPort(clientPort),
		mono.WithNATSClustering("seed-cluster", "127.0.0.1", clusterPort, nil),
	)
	if err != nil {
		t.Fatalf("seed node without JetStream must be accepted: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })
}

// freePort returns a free TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestWithNATSSystemAccount(t *testing.T) {
	cfg := mono.DefaultConfig()
	if err := mono.WithNATSSystemAccount("admin", "pw")(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.NATSOptions.SystemAccountUsername != "admin" || cfg.NATSOptions.SystemAccountPassword != "pw" {
		t.Errorf("NATSOptions = %+v", cfg.NATSOptions)
	}
	for _, tc := range []struct{ user, pass, want string }{
		{"", "pw", "username cannot be empty"},
		{"admin", "", "password cannot be empty"},
	} {
		err := mono.WithNATSSystemAccount(tc.user, tc.pass)(mono.DefaultConfig())
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("(%q, %q): err = %v, want %q", tc.user, tc.pass, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "pw") && tc.pass != "" {
			t.Errorf("error leaks the password: %v", err)
		}
	}
}
