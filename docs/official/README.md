# 👋 Welcome

Welcome to the Mono Framework documentation—a Go framework for building **distributed modular monolith** applications powered by NATS.io.

{% hint style="info" %}
These docs are for the **Mono Framework**. [GitHub Repository](https://github.com/go-monolith/mono)
{% endhint %}

## What is Mono Framework?

Mono Framework enables building applications as a collection of loosely-coupled modules that communicate via NATS messaging. Start with a single binary monolith for simplicity, then scale horizontally to a distributed cluster when needed—**without changing your code**.

Powered by NATS.io's distributed architecture, your application can seamlessly evolve from a single instance to a highly scalable distributed system.

### Why Distributed Modular Monolith?

| Approach | Development | Deployment | Scaling |
|----------|-------------|------------|---------|
| Traditional Monolith | Simple | Single binary | Vertical only |
| Microservices | Complex | Many services | Horizontal |
| **Distributed Modular Monolith** | **Simple** | **Single binary** | **Horizontal** |

- **Develop** like a monolith: single codebase, simple debugging, no network complexity during development
- **Deploy** like microservices: run multiple instances behind a load balancer, scale horizontally on demand
- **Communicate** through NATS: modules use messaging patterns that work identically whether running in one process or distributed across a cluster

### Key Features

- **Distributed Modular Monolith** - Start simple, scale horizontally without code changes
- **NATS.io Powered** - Built on NATS distributed messaging for high scalability and resilience
- **Embedded or External NATS** - Run embedded for development or single instance deployment, connect to production NATS clusters for HA deployments
- **Event-Driven Communication** - Publish/subscribe patterns for loose coupling
- **Four Service Patterns** - Channel, Request-Reply, Queue Group, and Stream Consumer services
- **JetStream Persistence** - Durable messaging with at-least-once delivery guarantees
- **Lifecycle Management** - Automatic dependency resolution and ordered startup/shutdown
- **Built-in Middleware** - Access logging, audit trails, and request ID injection
- **Plugin System** - Extensible architecture for custom functionality

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

type GreetingModule struct{}

func (m *GreetingModule) Name() string              { return "greeting" }
func (m *GreetingModule) Start(context.Context) error  { return nil }
func (m *GreetingModule) Stop(context.Context) error   { return nil }

func main() {
    app, err := mono.NewMonoApplication(
        mono.WithLogLevel(mono.LogLevelInfo),
        mono.WithShutdownTimeout(10*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }

    app.Register(&GreetingModule{})

    if err := app.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    // Application is running with embedded NATS server...

    app.Stop(context.Background())
}
```

## Next Steps

New to Mono Framework? Start here:

| If you want to...                    | Start here                                           |
| ------------------------------------ | ---------------------------------------------------- |
| Set up the framework in your project | [📦 Installation](getting-started/installation.md)   |
| Build your first module quickly      | [⚡ Quick Start](getting-started/quickstart.md)      |
| Understand how to organize your app  | [📁 Project Structure](getting-started/project-structure.md) |
| Learn the core concepts              | [🧠 Core Concepts](core-concepts/README.md)          |
| See working examples                 | [Examples](../examples/)                             |
| Look up API details                  | [📚 API Reference](api/README.md)                    |

## Core Concepts

Understand the architectural foundation:

- [Modules](core-concepts/modules.md) - How modules work and their lifecycle
- [Inter-Module Communication](core-concepts/inter-module-communication.md) - Service and Event communication patterns
- [Architecture](core-concepts/architecture.md) - Framework architecture and design

## Examples

Learn by example:

- [Basic](../../examples/basic/README.md) - Simple "Hello World" module
- [Multi-Module](../../examples/multi-module/README.md) - Order system with dependencies and service patterns
- [Analytics](../../examples/analytics/README.md) - Channel-based services for high-performance communication
- [Event Emitter](../../examples/event-emitter/README.md) - Event publishing and consumption patterns

## Explore More

- [API Reference](https://pkg.go.dev/github.com/go-monolith/mono) - Complete godoc documentation
- [GitHub Repository](https://github.com/go-monolith/mono) - Source code and issue tracking

## Help and Support

- Check our [examples](../../examples/) directory for working code
- Review the [API documentation](https://pkg.go.dev/github.com/go-monolith/mono) for detailed information
- Visit the [GitHub repository](https://github.com/go-monolith/mono) for issues and discussions

---

Ready to get started? Head to [Installation](getting-started/installation.md).
