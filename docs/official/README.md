# 👋 Welcome

Welcome to the Monolith Framework documentation. This framework enables you to build modular applications as a single deployable unit with clear module boundaries and event-driven communication.

{% hint style="info" %}
These docs are for the **Monolith Framework**. [GitHub Repository](https://github.com/go-monolith/mono)
{% endhint %}

## What is Monolith Framework?

Monolith Framework is a Go framework for building modular monolith applications centered around an embedded NATS.io message queue system. It combines the simplicity and deployment benefits of a monolith with the architectural advantages of microservices—modules are independently developable and testable while running in a single process.

### Key Features

- **Modular Monolith Architecture** - Define clear module boundaries with dependency injection
- **Embedded NATS Server** - Built-in messaging with optional JetStream persistence
- **Event-Driven Communication** - Publish/subscribe patterns for loose coupling
- **Four Service Patterns** - Channel, Request-Reply, Queue Group, and Stream Consumer services
- **Lifecycle Management** - Automatic dependency resolution and ordered startup/shutdown
- **Structured Logging** - Module-aware logging with sensitive data redaction
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

New to Monolith Framework? Start here:

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
