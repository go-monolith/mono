package types

import (
	"crypto/x509"
	"io"
	"time"
)

// MonoFrameworkConfig holds framework configuration.
// While exported for use with functional options, instances should not be
// created directly. Use NewMonoApplication with functional options instead.
type MonoFrameworkConfig struct {
	// NATSOptions configures the embedded NATS server
	NATSOptions NATSOptions

	// LoggerOptions configures the logger factory
	LoggerOptions LoggerOptions

	// Logger is the logger instance used by the framework and modules
	Logger Logger

	// ShutdownTimeout is the maximum time to wait for graceful shutdown
	ShutdownTimeout time.Duration

	// QueueGroupOptimisticWindow configures the optimistic publish window for queue group services.
	// When > 0, after a successful ACK, subsequent sends within this window use fire-and-forget publish.
	// When 0 (default), always use ACK mode (disabled).
	QueueGroupOptimisticWindow time.Duration
}

// NATSOptions holds NATS server configuration.
type NATSOptions struct {
	Host                string
	Port                int
	DontListen          bool          // If true, server won't listen on TCP (useful for in-process only). Requires UseInProcessConn=true.
	UseInProcessConn    bool          // If true, client uses in-process connection instead of TCP. Can be used independently or with DontListen.
	JetStreamEnabled    bool          // Track if JetStream is requested
	JetStreamDomain     string        // JetStream domain for multi-tenancy
	JetStreamDir        string        // JetStream storage directory
	ClusterName         string        // NATS cluster name
	ClusterHost         string        // NATS cluster host for inter-node communication
	ClusterPort         int           // NATS cluster port for inter-node communication
	ClusterRoutes       []string      // NATS cluster routes (URLs to other cluster nodes)
	MaxPayload          int32         // Maximum NATS message payload size
	StartupReadyTimeout time.Duration // Maximum time to wait for NATS server readiness (default: 10s)
	// NATS server logging flags (passed to SetLoggerV2)
	LogDebug    bool // If true, enables debug-level NATS server logging
	LogTrace    bool // If true, enables trace-level NATS server logging
	LogSysTrace bool // If true, enables system trace logging (internal NATS operations)

	// ConfigFile is the path to a NATS server configuration file.
	// When specified, the file is processed using server.ProcessConfigFile() during Start().
	// Programmatic options (like WithNATSPort) override settings from the config file.
	ConfigFile string

	// AutoTLS enables automatic ACME (Let's Encrypt) certificate management
	// for the NATS client listener. Nil means disabled.
	AutoTLS *AutoTLSConfig
}

// AutoTLSConfig configures automatic TLS certificate provisioning for the
// embedded NATS server's client listener using the ACME protocol (Let's Encrypt
// and compatible certificate authorities), backed by
// golang.org/x/crypto/acme/autocert.
//
// Scope: client-to-server connections only. Route (cluster), gateway, leafnode,
// websocket and MQTT listeners each read their own separate TLS configuration
// and are unaffected, so traffic between cluster nodes stays plaintext unless
// it is configured separately. Routes are peer-to-peer and conventionally use
// mutual TLS, which requires a client certificate autocert does not issue; use
// an internal CA supplied through a cluster{tls{...}} block in a NATS config
// file for that.
//
// When enabled, the framework serves the ACME http-01 challenge from a
// framework-owned HTTP listener and installs a tls.Config with a lazy
// GetCertificate callback on the NATS client listener. Certificates are
// obtained at startup (see StartupIssueTimeout) or on the first TLS handshake,
// and renewed automatically in the background - no restart or reload is needed
// at renewal time.
//
// Only the http-01 challenge is supported: tls-alpn-01 requires the validated
// listener to be on port 443, and dns-01 is not implemented by autocert. See
// docs/spec/foundation.md for the full design.
//
// Enabling AutoTLS makes TLS mandatory for external NATS clients: plaintext
// TCP connections are rejected once a certificate is configured. Route
// connections are not affected either way.
type AutoTLSConfig struct {
	// Domains is the list of fully qualified domain names to obtain
	// certificates for. At least one entry is required.
	//
	// Each name must be a resolvable FQDN that points at this host: the
	// certificate authority connects back to HTTPChallengeAddr over plain
	// HTTP on port 80 to validate ownership. Wildcards are rejected because
	// they require the dns-01 challenge, which autocert does not implement.
	//
	// autocert issues one certificate per name (not a single multi-SAN
	// certificate); the name presented in the TLS SNI extension selects which
	// one is served. Domains doubles as the host allowlist: a handshake for
	// any other name is refused.
	Domains []string

	// Email is the contact address registered with the ACME account.
	// Optional but strongly recommended: certificate authorities use it to
	// send expiry and revocation notices.
	Email string

	// CacheDir is the directory used to persist ACME account keys, issued
	// certificates and their private keys. It is required and is created with
	// mode 0700 if missing.
	//
	// It MUST survive process restarts (a persistent volume, not a
	// container's writable layer): losing it forces reissuance on every boot,
	// which will exhaust the certificate authority's rate limits. There is
	// deliberately no default so that persistence is an explicit decision.
	CacheDir string

	// HTTPChallengeAddr is the listen address for the framework-owned HTTP
	// server that answers ACME http-01 challenges. Defaults to ":80".
	//
	// The certificate authority always connects to port 80 of the domain, so
	// in production this must be reachable there, either directly or through
	// a port forward, load balancer, or a reverse proxy that forwards
	// /.well-known/acme-challenge/ to this address.
	HTTPChallengeAddr string

	// DirectoryURL is the ACME directory endpoint. Empty means the
	// Let's Encrypt production directory.
	//
	// Use "https://acme-staging-v02.api.letsencrypt.org/directory" while
	// developing: the staging environment has far higher rate limits, at the
	// cost of issuing certificates from an untrusted root.
	DirectoryURL string

	// DirectoryCAPool overrides the root certificate pool used for the HTTPS
	// connection to DirectoryURL. Nil uses the system pool.
	//
	// This is only needed for a private ACME CA whose directory endpoint
	// serves a certificate the system pool does not trust, such as the Pebble
	// test server. It does not affect verification of the certificates the CA
	// issues.
	DirectoryCAPool *x509.CertPool

	// RenewBefore is how long before expiry a certificate is renewed.
	// Zero uses the autocert default (30 days).
	RenewBefore time.Duration

	// StartupIssueTimeout bounds a synchronous certificate fetch performed
	// during startup, before the framework reports itself ready. Zero uses
	// the default of 60 seconds; a negative value skips the startup fetch and
	// lets certificates be obtained lazily on the first TLS handshake.
	//
	// The startup fetch upholds the framework's fail-fast principle: a
	// misconfigured domain, an unreachable challenge port or a rejected ACME
	// account surfaces as a startup error rather than as a handshake failure
	// hours later. It is a no-op when a valid certificate is already cached.
	StartupIssueTimeout time.Duration

	// AcceptTOS must be set to true to acknowledge acceptance of the
	// certificate authority's Terms of Service on behalf of the operator.
	// Configuration is rejected if it is false. See the subscriber agreement
	// linked from DirectoryURL.
	AcceptTOS bool
}

// LoggerOptions holds logger configuration.
type LoggerOptions struct {
	Level      LogLevel
	Format     LogFormat
	Output     io.Writer
	AddSource  bool
	UseDefault bool // If true, use default logger and ignore other options
}
