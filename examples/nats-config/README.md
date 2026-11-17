# NATS Config File Example

Loads the embedded NATS server's configuration from a standard NATS config file with `WithNATSConfigFile()`, and overrides the port and host with programmatic options.
Use it to keep organization-standard or complex NATS settings (clustering, JetStream, authorization) in a file, separate from code.

## Running the Example

```bash
# From the examples/nats-config directory
cd examples/nats-config
go run .

# Or from the repository root
go run ./examples/nats-config
```

## Code Structure

| File | Description |
|------|-------------|
| `main.go` | Application entry point demonstrating config file + overrides |
| `module.go` | Simple module implementation |
| `server.conf` | Sample NATS server configuration file |
| `README.md` | This documentation |

## Key Patterns

### Config Precedence

When both a config file and programmatic options are specified:

1. **Config file provides base settings** - All settings from the file are applied first
2. **Programmatic options override** - Options like `WithNATSPort()` take precedence

Exceptions: `WithNATSClustering` is merged into the file's `cluster {}` block (its routes are added to the file's), and options left at their defaults do not override the file.

```go
app, err := mono.NewMonoApplication(
    mono.WithNATSConfigFile("server.conf"), // Base: port=4222, host=127.0.0.1
    mono.WithNATSPort(4333),                // Override: port becomes 4333
    mono.WithNATSHost("0.0.0.0"),           // Override: host becomes 0.0.0.0
)
```

This pattern allows you to use a standard config file while customizing specific settings at runtime.

### The Config File

The `server.conf` file includes:

- **Server settings**: `server_name`, `port`, `host`
- **Connection limits**: `max_connections`, `max_payload`
- **JetStream settings**: Memory and file storage limits
- **Commented examples**: Clustering, leaf nodes, authorization

See the [NATS Server Configuration](https://docs.nats.io/running-a-nats-service/configuration) documentation for all available options.

### When to Use Config Files

- **Config file**: complex NATS settings (clustering, TLS, authorization), settings shared with operations teams or reused across environments, sensitive settings kept out of code.
- **Programmatic options**: simple configurations, values computed at runtime, tests with specific ports.
- **Both**: a base configuration from a file with environment-specific overrides.

## Expected Output

Framework log lines are interleaved and omitted here:

```
=== Mono-Framework NATS Config File Example ===
Demonstrates: Loading NATS configuration from a file with programmatic overrides

Config file: server.conf
✓ App created with config file + overrides
  - Base config loaded from: server.conf
  - Port overridden: 4222 -> 4333
  - Host overridden: 127.0.0.1 -> 0.0.0.0

✓ Module registered
  → HelloModule started
✓ App started - NATS server is running with merged configuration

Health Status: Healthy=true, NATSHealthy=true

Press Ctrl+C to shutdown...
```

## Next Steps

- [Framework API - WithNATSConfigFile](../../docs/official/api/framework.md)
- [Quick Start Guide](../../docs/official/getting-started/quickstart.md)
