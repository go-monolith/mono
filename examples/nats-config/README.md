# NATS Config File Example

This example demonstrates how to use `WithNATSConfigFile()` to load NATS server configuration from an external file, and how to combine it with programmatic overrides.

## Overview

The Monolith Framework supports loading NATS server configuration from a standard NATS config file. This is useful for:

- Using organization-standard NATS configurations
- Separating configuration from code
- Managing complex NATS settings (clustering, JetStream, authorization)

## Config Precedence

When both a config file and programmatic options are specified:

1. **Config file provides base settings** - All settings from the file are applied first
2. **Programmatic options override** - Options like `WithNATSPort()` take precedence

```go
app, err := mono.NewMonoApplication(
    mono.WithNATSConfigFile("server.conf"), // Base: port=4222, host=127.0.0.1
    mono.WithNATSPort(4333),                // Override: port becomes 4333
    mono.WithNATSHost("0.0.0.0"),           // Override: host becomes 0.0.0.0
)
```

This pattern allows you to use a standard config file while customizing specific settings at runtime.

## Running the Example

```bash
# From the examples/nats-config directory
cd examples/nats-config
go run .

# Or from the repository root
go run ./examples/nats-config
```

## Expected Output

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

## Files

| File | Description |
|------|-------------|
| `main.go` | Application entry point demonstrating config file + overrides |
| `module.go` | Simple module implementation |
| `server.conf` | Sample NATS server configuration file |
| `README.md` | This documentation |

## Config File Reference

The `server.conf` file includes:

- **Server settings**: `server_name`, `port`, `host`
- **Connection limits**: `max_connections`, `max_payload`
- **JetStream settings**: Memory and file storage limits
- **Commented examples**: Clustering, leaf nodes, authorization

See the [NATS Server Configuration](https://docs.nats.io/running-a-nats-service/configuration) documentation for all available options.

## When to Use Config Files

**Use config files when:**
- You have complex NATS settings (clustering, TLS, authorization)
- You want to reuse configurations across environments
- You need to share configurations with operations teams
- You want to keep sensitive settings separate from code

**Use programmatic options when:**
- You have simple configurations
- Settings need to be computed at runtime
- You're writing tests with specific port requirements
- You want everything self-contained in code

**Use both together when:**
- You have a base configuration but need runtime overrides
- You want sensible defaults from a file with environment-specific tweaks
- You're running the same app in different environments

## Related Documentation

- [Framework API - WithNATSConfigFile](../../docs/official/api/framework.md)
- [Quick Start Guide](../../docs/official/getting-started/quickstart.md)
