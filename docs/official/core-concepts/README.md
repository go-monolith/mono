# 🧠 Core Concepts

Understanding these core concepts will help you effectively design and implement applications using the Monolith Framework.

## The Five Key Concepts

### 1. [Modules](modules.md)

The foundation of your application architecture.

- **What**: Independent components that implement the Module interface
- **Why**: Encapsulation, independent testability, clear boundaries
- **Learn**: Module lifecycle, dependencies, and lifecycle hooks

### 2. [Services](services.md)

The public APIs of your modules.

- **What**: Named endpoints that modules register for other modules to call
- **Why**: Explicit dependencies, managed startup order, type-safe communication
- **Learn**: ServiceContainer, service registration, dependency injection

### 3. [Inter-Module Communication](inter-module-communication.md)

How modules interact with each other.

- **What**: Service patterns and Event patterns for different use cases
- **Why**: Loose coupling, performance, durability based on your needs
- **Learn**: Service Communication (4 patterns) and Event Communication (2 patterns)

### 4. [Architecture](architecture.md)

How the framework components fit together.

- **What**: Embedded NATS, modular monolith design, layered architecture
- **Why**: Scalability, observability, operational simplicity
- **Learn**: Framework components and their responsibilities

### 5. Embedded NATS

The framework's heart.

- **What**: NATS is an embedded message broker that runs in your process
- **Why**: Inter-module communication without external dependencies
- **Learn**: JetStream for persistence, subject naming conventions

## Quick Reference

| Concept | Purpose | Complexity | When to Use |
|---------|---------|-----------|------------|
| **Modules** | Organize code into independent units | Basic | Always |
| **Channel Services** | In-process communication | Low | High throughput, same process |
| **Request-Reply Services** | Synchronous service calls | Medium | When you need immediate response |
| **Queue Groups** | Load-balanced async processing | Medium | Distributed work, fire-and-forget |
| **Stream Consumers** | Durable message processing | High | When message loss is unacceptable |
| **Events** | Loose coupling via pub/sub | Medium | Broadcasting to many consumers |

## Learning Path

Start here:

1. **[Modules](modules.md)** (10 min)
   - Understand how to structure your application
   - Learn the lifecycle of a module
   - Discover optional module interfaces

2. **[Services](services.md)** (10 min)
   - Understand how modules expose public APIs
   - Learn how services establish dependencies
   - See how the framework manages startup order

3. **[Inter-Module Communication](inter-module-communication.md)** (15 min)
   - Choose the right pattern for your use case
   - Understand Service vs Event communication
   - See examples of each pattern

4. **[Architecture](architecture.md)** (10 min)
   - Understand how the framework is organized
   - Learn about the layered architecture
   - Understand subject naming conventions

## Key Principles

### 1. **Modules Are Units of Organization**

Think of modules like packages or components in other frameworks. Each module:
- Has clear responsibilities
- Can be tested independently
- Communicates through well-defined interfaces
- Has its own startup and shutdown sequence

### 2. **Loose Coupling is Central**

Modules don't call each other directly. They communicate through:
- **NATS messaging** for services and events
- **Dependency injection** for configuration
- **Plugins** for cross-cutting concerns

This enables:
- Independent module development
- Easy testing with mocks
- Hot module replacement (future)

### 3. **The Framework Handles Lifecycle**

You define modules, the framework handles:
- Startup order (respecting dependencies)
- Graceful shutdown (reverse order)
- Dependency injection
- Health checks
- Structured logging

### 4. **Choose the Right Pattern**

Don't use complex patterns if simple ones work:

```
Do you need immediate response?
  ├─ YES → Use Request-Reply
  └─ NO → Use Queue Group or Events

Do you need message persistence?
  ├─ YES → Use Stream Consumers
  └─ NO → Use Queue Group or Events

Many consumers?
  ├─ YES → Use Events
  └─ NO → Use Request-Reply or Queue Group
```

## Common Questions

**Q: When should I create a new module?**

A: Create a new module when you have a distinct responsibility that could be:
- Developed independently
- Tested in isolation
- Stopped/started separately
- Reused in other applications

**Q: Can modules call each other directly?**

A: Yes, but avoid it for loose coupling. Use NATS patterns instead:
- Request-Reply for synchronous calls
- Queue Groups for async work
- Events for broadcasting

**Q: How do I handle dependencies between modules?**

A: Use the DependentModule interface to declare them:
```go
func (m *Module) Dependencies() []string {
    return []string{"payment"}
}
```

The framework ensures correct startup order.

**Q: What's the difference between services and events?**

A:
- **Services** are for request-response (caller awaits response)
- **Events** are for notifications (fire-and-forget)

**Q: Can I use external message brokers?**

A: Yes! The embedded NATS is optional. The framework provides interfaces you can implement with any broker.

## Next Steps

Ready to dive deeper?

1. **[Modules Deep Dive](modules.md)** - Understand module lifecycle and optional interfaces
2. **[Services](services.md)** - Learn how modules expose public APIs and establish dependencies
3. **[Inter-Module Communication](inter-module-communication.md)** - Choose the right pattern for your use case
4. **[Framework Architecture](architecture.md)** - See how the pieces fit together
5. **[Examples](../../../examples/)** - Learn from working code

## Resources

- **API Reference**: https://pkg.go.dev/github.com/go-monolith/mono/v1
- **Examples**: ../../../examples/ directory
- **GitHub**: https://github.com/go-monolith/mono

---

Let's start with [Modules](modules.md)!
