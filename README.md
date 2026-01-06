# Mono Framework

[![Go Reference](https://pkg.go.dev/badge/github.com/go-monolith/mono.svg)](https://pkg.go.dev/github.com/go-monolith/mono)
[![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-90.1%25-brightgreen.svg)](docs/official/extra/code-coverage.md)

A Go framework for building **distributed modular monolith** applications powered by NATS.io.

Mono Framework enables building applications as a collection of loosely-coupled modules that communicate via NATS messaging. Start with a single binary monolith for simplicity, then scale horizontally to a distributed cluster when needed—without changing your code. Powered by NATS.io's distributed architecture, your application can seamlessly evolve from a single instance to a highly scalable distributed system.

## Features

- **Distributed Modular Monolith** - Start simple, scale horizontally without code changes
- **NATS.io Powered** - Built on NATS distributed messaging for high scalability and resilience
- **Embedded or External NATS** - Run embedded for development, connect to NATS clusters in production
- **Event-Driven Communication** - Publish/subscribe patterns for inter-module messaging
- **Four Service Patterns** - Channel, Request-Reply, Queue Group, and Stream Consumer
- **JetStream Persistence** - Durable messaging with at-least-once delivery guarantees
- **Lifecycle Management** - Automatic dependency resolution and ordered startup/shutdown
- **Built-in Middleware** - Access logging, audit trails, and request ID injection
- **Plugin System** - Extensible architecture for custom functionality

## Why Distributed Modular Monolith?

| Approach | Development | Deployment | Scaling |
|----------|-------------|------------|---------|
| Traditional Monolith | Simple | Single binary | Vertical only |
| Microservices | Complex | Many services | Horizontal |
| **Distributed Modular Monolith** | **Simple** | **Single binary** | **Horizontal** |

Mono Framework gives you the best of both worlds:
- **Develop** like a monolith: single codebase, simple debugging, no network complexity during development
- **Deploy** like microservices: run multiple instances behind a load balancer, scale horizontally on demand
- **Communicate** through NATS: modules use messaging patterns that work identically whether running in one process or distributed across a cluster

## Quick Start

### Installation

```bash
go get github.com/go-monolith/mono
```

### Basic Example

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/go-monolith/mono"
)

// HelloModule implements a simple module
type HelloModule struct{}

func (m *HelloModule) Name() string              { return "hello" }
func (m *HelloModule) Start(_ context.Context) error { return nil }
func (m *HelloModule) Stop(_ context.Context) error  { return nil }

func main() {
    // Create application with configuration
    app, err := mono.NewMonoApplication(
        mono.WithLogLevel(mono.LogLevelInfo),
        mono.WithShutdownTimeout(10*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register module and start
    app.Register(&HelloModule{})

    if err := app.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    // Application is now running with embedded NATS server
    // Handle shutdown...

    app.Stop(context.Background())
}
```

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                         Application Layer                             │
│            (Your Modules implementing mono.Module)                    │
├───────────────────────────────────────────────────────────────────────┤
│                          Framework Layer                              │
│  ┌──────────────────┐ ┌──────────────────┐ ┌───────────────────────┐  │
│  │ ServiceContainer │ │     EventBus     │ │    EventRegistry      │  │
│  │  (DI & Services) │ │    (Pub/Sub)     │ │  (EDA & Consumers)    │  │
│  └──────────────────┘ └──────────────────┘ └───────────────────────┘  │
├───────────────────────────────────────────────────────────────────────┤
│                       Infrastructure Layer                            │
│              (NATS.io + JetStream Persistence)                        │
└───────────────────────────────────────────────────────────────────────┘
                                  │
       ┌──────────────────────────┼──────────────────────────┐
       ▼                          ▼                          ▼
  ┌──────────┐              ┌──────────┐              ┌──────────┐
  │  Node 1  │◄────────────►│  Node 2  │◄────────────►│  Node 3  │
  │(Instance)│ NATS Cluster │(Instance)│ NATS Cluster │(Instance)│
  └──────────┘              └──────────┘              └──────────┘
```

Modules communicate through NATS messaging patterns rather than direct method calls. This enables loose coupling, clear module boundaries, and **horizontal scaling**—deploy multiple instances that automatically coordinate through the NATS cluster.

## Service Communication Patterns

| Pattern | Use Case | Latency | Durability |
|---------|----------|---------|------------|
| **Channel** | In-process communication | ~microseconds | None |
| **Request-Reply** | Synchronous service calls | ~1ms | None |
| **Queue Group** | Load-balanced async processing | ~1ms | None |
| **Stream Consumer** | Durable message processing | ~1ms | JetStream |

## Examples

| Example | Description |
|---------|-------------|
| [basic](examples/basic/) | Hello World module with lifecycle management |
| [multi-module](examples/multi-module/) | Order system with dependencies and service patterns |
| [analytics](examples/analytics/) | Channel services for high-performance in-process communication |
| [event-emitter](examples/event-emitter/) | Event publishing with EventEmitter and EventConsumer |

## Built-in Components

### Middleware
- `accesslog` - HTTP-style access logging for service calls
- `audit` - Security event auditing
- `requestid` - Request ID injection and propagation

### Plugins
- `fs-jetstream` - File storage using JetStream Object Store
- `kv-jetstream` - Key-value storage using JetStream KV Store

## Security

The framework includes built-in security features:

- **Sensitive Data Redaction** - Automatic redaction of passwords, tokens, API keys, and credentials from logs
- **Audit Logging** - Security event tracking with optional hash chaining for tamper detection
- **Input Validation** - Validation helpers for service handlers

For security best practices and vulnerability reporting, see [SECURITY.md](SECURITY.md).

## Documentation

| Resource | Description |
|----------|-------------|
| [**Official Documentation**](docs/official/README.md) | Complete framework guide (GitBook format) |
| [Quick Start Guide](docs/official/getting-started/quickstart.md) | Get started in 5 minutes |
| [Core Concepts](docs/official/core-concepts/README.md) | Modules, services, and architecture |
| [API Reference](docs/official/api/README.md) | Detailed API documentation |
| [Go Documentation](https://pkg.go.dev/github.com/go-monolith/mono) | Generated godoc reference |
| [Examples](examples/) | Runnable example applications |

## Development

For contributing to the framework, building from source, and running tests, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
