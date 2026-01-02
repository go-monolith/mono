package types

import (
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
	Host             string
	Port             int
	DontListen       bool     // If true, server won't listen on TCP (useful for in-process only). Requires UseInProcessConn=true.
	UseInProcessConn bool     // If true, client uses in-process connection instead of TCP. Can be used independently or with DontListen.
	JetStreamEnabled bool     // Track if JetStream is requested
	JetStreamDomain  string   // JetStream domain for multi-tenancy
	JetStreamDir     string   // JetStream storage directory
	ClusterName      string   // NATS cluster name
	ClusterHost      string   // NATS cluster host for inter-node communication
	ClusterPort      int      // NATS cluster port for inter-node communication
	ClusterRoutes    []string // NATS cluster routes (URLs to other cluster nodes)
	MaxPayload       int32    // Maximum NATS message payload size
	// NATS server logging flags (passed to SetLoggerV2)
	LogDebug    bool // If true, enables debug-level NATS server logging
	LogTrace    bool // If true, enables trace-level NATS server logging
	LogSysTrace bool // If true, enables system trace logging (internal NATS operations)

	// ConfigFile is the path to a NATS server configuration file.
	// When specified, the file is processed using server.ProcessConfigFile() during Start().
	// Programmatic options (like WithNATSPort) override settings from the config file.
	ConfigFile string
}

// LoggerOptions holds logger configuration.
type LoggerOptions struct {
	Level      LogLevel
	Format     LogFormat
	Output     io.Writer
	AddSource  bool
	UseDefault bool // If true, use default logger and ignore other options
}
