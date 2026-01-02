# Getting Started

Welcome! This section will help you get up and running with the Monolith Framework.

## What You'll Learn

By following this guide, you will:

1. **Install** the framework in your Go project
2. **Build** your first module and application
3. **Understand** the basic project structure
4. **Run** a working example with the embedded NATS server

## Quick Links

- [Installation](installation.md) - Set up Go module and dependencies
- [Quick Start Tutorial](quickstart.md) - Build your first module in 5 minutes
- [Project Structure](project-structure.md) - Understand how to organize your application

## What is a Module?

In Monolith Framework, a module is the fundamental unit of organization. Every module:

- Implements the `Module` interface (Name, Start, Stop methods)
- Runs in the same process as other modules
- Communicates with other modules through NATS messaging
- Has its own lifecycle and dependencies

Here's a minimal module:

```go
type MyModule struct{}

func (m *MyModule) Name() string                 { return "my-module" }
func (m *MyModule) Start(context.Context) error { return nil }
func (m *MyModule) Stop(context.Context) error  { return nil }
```

## Application Startup

The framework manages a complete startup sequence:

```go
app, _ := mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),
)
app.Register(&MyModule{})
app.Start(context.Background())
// App is now running with embedded NATS server
app.Stop(context.Background())
```

## Next Steps

Ready to start? Follow these steps in order:

1. **[Installation](installation.md)** - Install the framework (2 min)
2. **[Quick Start](quickstart.md)** - Build your first module (5 min)
3. **[Project Structure](project-structure.md)** - Learn the layout (3 min)

Then explore [Core Concepts](../core-concepts/README.md) to understand:

- Module lifecycle and dependencies
- Different service communication patterns
- Framework architecture

## Need Help?

- Check the [examples](../../../examples/) directory for working code
- Review the [API documentation](https://pkg.go.dev/github.com/go-monolith/mono)
- Visit the [GitHub repository](https://github.com/go-monolith/mono) for issues and discussions

---

Let's get started with [Installation](installation.md)!
