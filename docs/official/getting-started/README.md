# Getting Started

Welcome! This section gets you from an empty directory to a running Monolith Framework application with the embedded NATS server, in three steps.

## Your Path

| Step | Page | What you do | Time |
|------|------|-------------|------|
| 1 | [Installation](installation.md) | Set up a Go module, add the framework, run a minimal application | 2 min |
| 2 | [Quick Start](quickstart.md) | Build your first module and application, with logging, health and graceful shutdown | 5 min |
| 3 | [Project Structure](project-structure.md) | Learn how to organize a larger application into modules and contracts | 3 min |

## What is a Module?

In Monolith Framework, a module is the fundamental unit of organization. Every module:

- Implements the `Module` interface (`Name`, `Start`, `Stop` methods)
- Runs in the same process as other modules
- Communicates with other modules through NATS messaging
- Has its own lifecycle and dependencies

The [Quick Start](quickstart.md) builds one step by step.

## Then

Explore [Core Concepts](../core-concepts/README.md) to understand module lifecycle and dependencies, the service communication patterns, and the framework architecture.

## Need Help?

- Check the [examples](../../../examples/basic/README.md) for working code
- Review the [API documentation](https://pkg.go.dev/github.com/go-monolith/mono)
- Visit the [GitHub repository](https://github.com/go-monolith/mono) for issues and discussions
