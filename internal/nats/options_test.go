package nats

import (
	"strings"
	"testing"
)

// TestDefaultNATSConfig verifies that DefaultNATSConfig returns sensible defaults.
func TestDefaultNATSConfig(t *testing.T) {
	cfg := DefaultNATSConfig()

	if cfg == nil {
		t.Fatal("DefaultNATSConfig() returned nil")
	}

	// Verify default values
	if cfg.Host != "127.0.0.1" {
		t.Errorf("expected default Host to be '127.0.0.1', got '%s'", cfg.Host)
	}

	if cfg.Port != 4222 {
		t.Errorf("expected default Port to be 4222, got %d", cfg.Port)
	}

	if cfg.JetStreamEnabled {
		t.Error("expected JetStreamEnabled to be false by default")
	}

	if cfg.JetStreamDomain != "" {
		t.Errorf("expected JetStreamDomain to be empty by default, got '%s'", cfg.JetStreamDomain)
	}

	if cfg.StorageDir != "./jetstream" {
		t.Errorf("expected default StorageDir to be './jetstream', got '%s'", cfg.StorageDir)
	}

	if cfg.DontListen {
		t.Error("expected DontListen to be false by default")
	}

	if cfg.UseInProcessConn {
		t.Error("expected UseInProcessConn to be false by default")
	}

	if cfg.ClusterEnabled {
		t.Error("expected ClusterEnabled to be false by default")
	}

	if cfg.ClusterName != "" {
		t.Errorf("expected ClusterName to be empty by default, got '%s'", cfg.ClusterName)
	}

	if len(cfg.ClusterRoutes) != 0 {
		t.Errorf("expected ClusterRoutes to be empty by default, got %v", cfg.ClusterRoutes)
	}

	expectedMaxPayload := int32(1024 * 1024)
	if cfg.MaxPayload != expectedMaxPayload {
		t.Errorf("expected default MaxPayload to be %d (1 MB), got %d", expectedMaxPayload, cfg.MaxPayload)
	}
}

// TestWithJetStream verifies that WithJetStream option enables JetStream.
func TestWithJetStream(t *testing.T) {
	tests := []struct {
		name           string
		storageDir     string
		wantEnabled    bool
		wantStorageDir string
	}{
		{
			name:           "enable with custom storage dir",
			storageDir:     "/custom/path",
			wantEnabled:    true,
			wantStorageDir: "/custom/path",
		},
		{
			name:           "enable with empty storage dir uses default",
			storageDir:     "",
			wantEnabled:    true,
			wantStorageDir: "./jetstream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithJetStream(tt.storageDir)

			if err := opt(cfg); err != nil {
				t.Fatalf("WithJetStream() returned error: %v", err)
			}

			if cfg.JetStreamEnabled != tt.wantEnabled {
				t.Errorf("expected JetStreamEnabled to be %v, got %v", tt.wantEnabled, cfg.JetStreamEnabled)
			}

			if cfg.StorageDir != tt.wantStorageDir {
				t.Errorf("expected StorageDir to be '%s', got '%s'", tt.wantStorageDir, cfg.StorageDir)
			}
		})
	}
}

// TestWithJetStreamDomain verifies JetStream domain configuration.
func TestWithJetStreamDomain(t *testing.T) {
	tests := []struct {
		name        string
		domain      string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid domain",
			domain:  "production",
			wantErr: false,
		},
		{
			name:        "empty domain returns error",
			domain:      "",
			wantErr:     true,
			errContains: "JetStream domain cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithJetStreamDomain(tt.domain)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.JetStreamDomain != tt.domain {
					t.Errorf("expected JetStreamDomain to be '%s', got '%s'", tt.domain, cfg.JetStreamDomain)
				}
			}
		})
	}
}

// TestWithClustering verifies clustering configuration.
func TestWithClustering(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		clusterHost string
		clusterPort int
		routes      []string
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid clustering config",
			clusterName: "my-cluster",
			clusterHost: "127.0.0.1",
			clusterPort: 6222,
			routes:      []string{"nats://node1:6222", "nats://node2:6222"},
			wantErr:     false,
		},
		{
			name:        "valid seed node (no routes)",
			clusterName: "my-cluster",
			clusterHost: "127.0.0.1",
			clusterPort: 6222,
			routes:      nil,
			wantErr:     false,
		},
		{
			name:        "empty cluster name returns error",
			clusterName: "",
			clusterHost: "127.0.0.1",
			clusterPort: 6222,
			routes:      []string{"nats://node1:6222"},
			wantErr:     true,
			errContains: "cluster name cannot be empty",
		},
		{
			name:        "empty cluster host returns error",
			clusterName: "my-cluster",
			clusterHost: "",
			clusterPort: 6222,
			routes:      []string{"nats://node1:6222"},
			wantErr:     true,
			errContains: "cluster host cannot be empty",
		},
		{
			name:        "cluster port too low returns error",
			clusterName: "my-cluster",
			clusterHost: "127.0.0.1",
			clusterPort: 1023,
			routes:      []string{"nats://node1:6222"},
			wantErr:     true,
			errContains: "cluster port must be between 1024 and 65535",
		},
		{
			name:        "cluster port too high returns error",
			clusterName: "my-cluster",
			clusterHost: "127.0.0.1",
			clusterPort: 65536,
			routes:      []string{"nats://node1:6222"},
			wantErr:     true,
			errContains: "cluster port must be between 1024 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithClustering(tt.clusterName, tt.clusterHost, tt.clusterPort, tt.routes)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !cfg.ClusterEnabled {
					t.Error("expected ClusterEnabled to be true")
				}
				if cfg.ClusterName != tt.clusterName {
					t.Errorf("expected ClusterName to be '%s', got '%s'", tt.clusterName, cfg.ClusterName)
				}
				if cfg.ClusterHost != tt.clusterHost {
					t.Errorf("expected ClusterHost to be '%s', got '%s'", tt.clusterHost, cfg.ClusterHost)
				}
				if cfg.ClusterPort != tt.clusterPort {
					t.Errorf("expected ClusterPort to be %d, got %d", tt.clusterPort, cfg.ClusterPort)
				}
				if len(cfg.ClusterRoutes) != len(tt.routes) {
					t.Errorf("expected %d routes, got %d", len(tt.routes), len(cfg.ClusterRoutes))
				}
			}
		})
	}
}

// TestWithMaxPayload verifies max payload configuration and validation.
func TestWithMaxPayload(t *testing.T) {
	tests := []struct {
		name        string
		bytes       int32
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid minimum payload (1 KB)",
			bytes:   1024,
			wantErr: false,
		},
		{
			name:    "valid medium payload (1 MB)",
			bytes:   1024 * 1024,
			wantErr: false,
		},
		{
			name:    "valid maximum payload (8 MB)",
			bytes:   8388608,
			wantErr: false,
		},
		{
			name:        "below minimum returns error",
			bytes:       1023,
			wantErr:     true,
			errContains: "max payload must be at least 1024 bytes (1 KB)",
		},
		{
			name:        "above maximum returns error",
			bytes:       8388609,
			wantErr:     true,
			errContains: "max payload must be at most 8388608 bytes (8 MB)",
		},
		{
			name:        "zero returns error",
			bytes:       0,
			wantErr:     true,
			errContains: "max payload must be at least 1024 bytes (1 KB)",
		},
		{
			name:        "negative returns error",
			bytes:       -1,
			wantErr:     true,
			errContains: "max payload must be at least 1024 bytes (1 KB)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithMaxPayload(tt.bytes)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.MaxPayload != tt.bytes {
					t.Errorf("expected MaxPayload to be %d, got %d", tt.bytes, cfg.MaxPayload)
				}
			}
		})
	}
}

// TestOptionComposition verifies that multiple options can be composed together.
func TestOptionComposition(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Apply multiple options
	options := []NATSOption{
		WithJetStream("/data/jetstream"),
		WithJetStreamDomain("production"),
		WithClustering("prod-cluster", "127.0.0.1", 6222, []string{"nats://node1:6223"}),
		WithMaxPayload(2 * 1024 * 1024), // 2 MB
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			t.Fatalf("option composition failed: %v", err)
		}
	}

	// Verify all options were applied
	if !cfg.JetStreamEnabled {
		t.Error("expected JetStreamEnabled to be true")
	}

	if cfg.StorageDir != "/data/jetstream" {
		t.Errorf("expected StorageDir to be '/data/jetstream', got '%s'", cfg.StorageDir)
	}

	if cfg.JetStreamDomain != "production" {
		t.Errorf("expected JetStreamDomain to be 'production', got '%s'", cfg.JetStreamDomain)
	}

	if !cfg.ClusterEnabled {
		t.Error("expected ClusterEnabled to be true")
	}

	if cfg.ClusterName != "prod-cluster" {
		t.Errorf("expected ClusterName to be 'prod-cluster', got '%s'", cfg.ClusterName)
	}

	expectedMaxPayload := int32(2 * 1024 * 1024)
	if cfg.MaxPayload != expectedMaxPayload {
		t.Errorf("expected MaxPayload to be %d, got %d", expectedMaxPayload, cfg.MaxPayload)
	}
}

// TestOptionLastWins verifies that when the same option is applied multiple times,
// the last one wins.
func TestOptionLastWins(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Apply the same option type multiple times
	options := []NATSOption{
		WithMaxPayload(1024 * 1024),     // 1 MB
		WithMaxPayload(2 * 1024 * 1024), // 2 MB (should win)
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			t.Fatalf("option application failed: %v", err)
		}
	}

	expectedMaxPayload := int32(2 * 1024 * 1024)
	if cfg.MaxPayload != expectedMaxPayload {
		t.Errorf("expected last option to win with MaxPayload=%d, got %d", expectedMaxPayload, cfg.MaxPayload)
	}
}

// TestWithHost verifies host configuration and validation.
func TestWithHost(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid localhost",
			host:    "localhost",
			wantErr: false,
		},
		{
			name:    "valid IPv4",
			host:    "192.168.1.100",
			wantErr: false,
		},
		{
			name:    "valid IPv6",
			host:    "::1",
			wantErr: false,
		},
		{
			name:    "valid hostname",
			host:    "nats.example.com",
			wantErr: false,
		},
		{
			name:        "empty host returns error",
			host:        "",
			wantErr:     true,
			errContains: "host cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithHost(tt.host)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.Host != tt.host {
					t.Errorf("expected Host to be '%s', got '%s'", tt.host, cfg.Host)
				}
			}
		})
	}
}

// TestWithPort verifies port configuration and validation.
func TestWithPort(t *testing.T) {
	tests := []struct {
		name        string
		port        int
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid minimum port (1024)",
			port:    1024,
			wantErr: false,
		},
		{
			name:    "valid standard NATS port (4222)",
			port:    4222,
			wantErr: false,
		},
		{
			name:    "valid maximum port (65535)",
			port:    65535,
			wantErr: false,
		},
		{
			name:    "valid custom port",
			port:    8080,
			wantErr: false,
		},
		{
			name:        "port below minimum (1023)",
			port:        1023,
			wantErr:     true,
			errContains: "port must be between 1024 and 65535",
		},
		{
			name:        "port above maximum (65536)",
			port:        65536,
			wantErr:     true,
			errContains: "port must be between 1024 and 65535",
		},
		{
			name:        "zero port returns error",
			port:        0,
			wantErr:     true,
			errContains: "port must be between 1024 and 65535",
		},
		{
			name:        "negative port returns error",
			port:        -1,
			wantErr:     true,
			errContains: "port must be between 1024 and 65535",
		},
		{
			name:        "privileged port (80) returns error",
			port:        80,
			wantErr:     true,
			errContains: "port must be between 1024 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()
			opt := WithPort(tt.port)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.Port != tt.port {
					t.Errorf("expected Port to be %d, got %d", tt.port, cfg.Port)
				}
			}
		})
	}
}

// TestWithLogging verifies NATS server logging configuration.
func TestWithLogging(t *testing.T) {
	tests := []struct {
		name         string
		debug        bool
		trace        bool
		sysTrace     bool
		wantDebug    bool
		wantTrace    bool
		wantSysTrace bool
	}{
		{
			name:         "all logging disabled",
			debug:        false,
			trace:        false,
			sysTrace:     false,
			wantDebug:    false,
			wantTrace:    false,
			wantSysTrace: false,
		},
		{
			name:         "debug only",
			debug:        true,
			trace:        false,
			sysTrace:     false,
			wantDebug:    true,
			wantTrace:    false,
			wantSysTrace: false,
		},
		{
			name:         "trace only",
			debug:        false,
			trace:        true,
			sysTrace:     false,
			wantDebug:    false,
			wantTrace:    true,
			wantSysTrace: false,
		},
		{
			name:         "sysTrace only",
			debug:        false,
			trace:        false,
			sysTrace:     true,
			wantDebug:    false,
			wantTrace:    false,
			wantSysTrace: true,
		},
		{
			name:         "debug and trace",
			debug:        true,
			trace:        true,
			sysTrace:     false,
			wantDebug:    true,
			wantTrace:    true,
			wantSysTrace: false,
		},
		{
			name:         "all logging enabled",
			debug:        true,
			trace:        true,
			sysTrace:     true,
			wantDebug:    true,
			wantTrace:    true,
			wantSysTrace: true,
		},
		{
			name:         "trace and sysTrace",
			debug:        false,
			trace:        true,
			sysTrace:     true,
			wantDebug:    false,
			wantTrace:    true,
			wantSysTrace: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()

			// Verify defaults are false
			if cfg.LogDebug || cfg.LogTrace || cfg.LogSysTrace {
				t.Error("expected all logging flags to be false by default")
			}

			opt := WithLogging(tt.debug, tt.trace, tt.sysTrace)

			err := opt(cfg)
			if err != nil {
				t.Fatalf("WithLogging() returned unexpected error: %v", err)
			}

			if cfg.LogDebug != tt.wantDebug {
				t.Errorf("expected LogDebug to be %v, got %v", tt.wantDebug, cfg.LogDebug)
			}
			if cfg.LogTrace != tt.wantTrace {
				t.Errorf("expected LogTrace to be %v, got %v", tt.wantTrace, cfg.LogTrace)
			}
			if cfg.LogSysTrace != tt.wantSysTrace {
				t.Errorf("expected LogSysTrace to be %v, got %v", tt.wantSysTrace, cfg.LogSysTrace)
			}
		})
	}
}

// TestWithDontListen verifies that WithDontListen option sets DontListen flag.
func TestWithDontListen(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Verify default is false
	if cfg.DontListen {
		t.Error("expected DontListen to be false by default")
	}

	// Apply option
	opt := WithDontListen()
	if err := opt(cfg); err != nil {
		t.Fatalf("WithDontListen() returned error: %v", err)
	}

	// Verify option was applied
	if !cfg.DontListen {
		t.Error("expected DontListen to be true after applying WithDontListen")
	}
}

// TestWithInProcessConn verifies that WithInProcessConn option sets UseInProcessConn flag.
func TestWithInProcessConn(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Verify default is false
	if cfg.UseInProcessConn {
		t.Error("expected UseInProcessConn to be false by default")
	}

	// Apply option
	opt := WithInProcessConn()
	if err := opt(cfg); err != nil {
		t.Fatalf("WithInProcessConn() returned error: %v", err)
	}

	// Verify option was applied
	if !cfg.UseInProcessConn {
		t.Error("expected UseInProcessConn to be true after applying WithInProcessConn")
	}
}

// TestDontListenAndInProcessConnTogether verifies both options can be applied together.
func TestDontListenAndInProcessConnTogether(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Apply both options
	options := []NATSOption{
		WithDontListen(),
		WithInProcessConn(),
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			t.Fatalf("option application failed: %v", err)
		}
	}

	// Verify both are set
	if !cfg.DontListen {
		t.Error("expected DontListen to be true")
	}
	if !cfg.UseInProcessConn {
		t.Error("expected UseInProcessConn to be true")
	}
}

// TestWithConfigFile verifies config file path configuration and validation.
func TestWithConfigFile(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid absolute path",
			path:    "/etc/nats/server.conf",
			wantErr: false,
		},
		{
			name:    "valid relative path",
			path:    "./nats.conf",
			wantErr: false,
		},
		{
			name:    "valid path with spaces",
			path:    "/path/to/nats config.conf",
			wantErr: false,
		},
		{
			name:        "empty path returns error",
			path:        "",
			wantErr:     true,
			errContains: "config file path cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultNATSConfig()

			// Verify default is empty
			if cfg.ConfigFile != "" {
				t.Error("expected ConfigFile to be empty by default")
			}

			opt := WithConfigFile(tt.path)

			err := opt(cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if cfg.ConfigFile != tt.path {
					t.Errorf("expected ConfigFile to be '%s', got '%s'", tt.path, cfg.ConfigFile)
				}
			}
		})
	}
}

// TestWithConfigFileComposition verifies config file can be combined with other options.
func TestWithConfigFileComposition(t *testing.T) {
	cfg := DefaultNATSConfig()

	// Apply config file with other options
	// Config file is applied first, then other options can override
	options := []NATSOption{
		WithConfigFile("/etc/nats/server.conf"),
		WithPort(4333),      // Override port from config file
		WithHost("0.0.0.0"), // Override host from config file
	}

	for _, opt := range options {
		if err := opt(cfg); err != nil {
			t.Fatalf("option composition failed: %v", err)
		}
	}

	// Verify all options were applied
	if cfg.ConfigFile != "/etc/nats/server.conf" {
		t.Errorf("expected ConfigFile to be '/etc/nats/server.conf', got '%s'", cfg.ConfigFile)
	}
	if cfg.Port != 4333 {
		t.Errorf("expected Port to be 4333, got %d", cfg.Port)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("expected Host to be '0.0.0.0', got '%s'", cfg.Host)
	}
}
