# 🤔 FAQ

Common questions developers ask when working with the Monolith Framework. Each answer is short and links to the page that covers the topic in full.

{% hint style="info" %}
**Can't find your answer?** Check the [How-To Guides](../guides/README.md) for step-by-step instructions, or the [Core Concepts](../core-concepts/README.md) for deeper explanations.
{% endhint %}

## Module Development

### Q: How do I add a new module?

**A:** Implement the `Module` interface (`Name()`, `Start(ctx)`, `Stop(ctx)`), optionally the interfaces you need (`DependentModule`, `ServiceProviderModule`, `EventBusAwareModule`, ...), and register it with `app.Register()` before `app.Start()`. The framework calls the lifecycle methods in order.

Nothing injects a logger into a plain module: pass the framework's (`app.Logger()`) when you construct it, or use `log/slog`.

**Learn more:** [Quick Start](../getting-started/quickstart.md) and [Modules - Core Concepts](../core-concepts/modules.md)

---

### Q: How do modules communicate with each other?

**A:** Through services, which a module calls on a declared dependency, and events, which it publishes for any module to consume. There are five service patterns: in-process Channel services, plus four over NATS:

| Pattern | Use it for | Needs JetStream |
|---------|-----------|-----------------|
| Channel | High-frequency in-process communication (single process only, not through NATS) | No |
| Request-Reply | Synchronous operations | No |
| Queue Group | Asynchronous fire-and-forget work | No |
| Stream Consumer | Durable, acknowledged work | Yes |
| Cron | Scheduled work, one fire per occurrence across a cluster | Yes |

A provider registers its services in `RegisterServices(container)`. A consumer receives the provider's container in `SetDependencyServiceContainer` and calls it, preferably through typed helpers (`helper.RegisterTypedRequestReplyService`, `helper.CallRequestReplyService`, `helper.SendQueueGroupService`) or a typed service contract.

**Learn more:** [Inter-Module Communication](../core-concepts/inter-module-communication.md), [Services](../core-concepts/services.md) and [Event Consumers](../core-concepts/consumers.md)

---

## Inter-Module Communication Patterns

### Q: What's the difference between Channel and Request-Reply services?

**A:** They serve different purposes:

| Aspect | Channel | Request-Reply |
|--------|---------|---------------|
| **Type** | Go channel (in-process) | NATS message (distributed) |
| **Latency** | ~1 microsecond | ~1 millisecond |
| **Distribution** | Single process only | Across modules (even remote) |
| **Protocol** | Native Go | NATS protocol |
| **Use Case** | High-frequency updates | Cross-module API calls |

A Channel service is a pair of `chan *mono.Msg` registered with `RegisterChannelService` and obtained by the consumer with `GetChannelService(serviceName, consumerModule)`. A Request-Reply service is registered with `RegisterRequestReplyService` (or the typed helper) and called through `GetRequestReplyService` or `helper.CallRequestReplyService`.

**Learn more:** [Pattern 1: Channel Services](../core-concepts/inter-module-communication.md#pattern-1-channel-services) and [Pattern 2: Request-Reply Services](../core-concepts/inter-module-communication.md#pattern-2-request-reply-services)

---

## Error Handling

### Q: How do I handle errors in handlers?

**A:** Follow Go's error handling conventions:

- Check errors immediately after function calls
- Wrap errors with context using `fmt.Errorf(...%w...)`
- Log errors with debugging information
- Return user-friendly error messages to clients

**Learn more:** [Error Handling - How-To Guide](../guides/error-handling.md)

---

## Testing

### Q: How do I test modules in isolation?

**A:** Keep the domain logic behind ports and test it with lightweight fakes, table-driven, covering both success and error paths. To start a module without NATS and check its boundaries with one call, use `monotest`: `monotest.CheckModule(t, module, monotest.WithEventBus(bus), monotest.WithDependency("inventory", inventory))`, where `bus` is a `monotest.NewEventBus()` and `inventory` a `monotest.NewContainer("inventory", bus)` with stand-in services.

**Learn more:** [Testing - How-To Guide](../guides/testing.md) and the [Module Boundaries Guide](../guides/module-boundaries.md)

---

## Framework Features

### Q: Can I use this framework without JetStream?

**A:** Yes. JetStream is optional and disabled by default; the framework then uses NATS Core (in-memory messaging) without persistence. Request-Reply, Channel and Queue Group services and the Event Bus (pub/sub) work without it.

Only JetStream-specific features are unavailable: Event Stream Consumers (use regular Event Consumers instead), Stream Consumer services, Cron services, the fs-jetstream and kv-jetstream storage plugins, message persistence across restarts, and stream replay.

Enable JetStream (by setting a storage directory with `mono.WithJetStreamStorageDir(dir)`) when you need message persistence, event replay after a restart, guaranteed (at-least-once) delivery, or event sourcing.

**Learn more:** [`WithJetStreamStorageDir`](../api/framework.md#withjetstreamstoragedir) and [Architecture - Core Concepts](../core-concepts/architecture.md)

---

### Q: How do I implement health checks?

**A:** Implement `HealthCheckableModule`: `Health(ctx context.Context) mono.HealthStatus`, returning `mono.HealthStatus{Healthy: ..., Message: ...}` (optionally with `Details`). Keep health checks fast and check critical dependencies only. `app.Health(ctx)` aggregates them: it is healthy only if the framework, NATS and all health-aware modules are healthy.

**Learn more:** [HealthCheckableModule](../core-concepts/modules.md#healthcheckablemodule) and [`Health`](../api/framework.md#health)

---

## Project Structure

### Q: What's the recommended project structure?

**A:** Give each module its own package under `modules/`, with a `contract` sub-package (DTOs, typed service contracts, emitted events) that is the only package other modules import. Group related functionality in modules, keep them independent and focused, and check the import rule with `monotest.CheckImports(t)`.

**Learn more:** [Getting Started - Project Structure](../getting-started/project-structure.md)

---

## Troubleshooting

### Q: My service handler isn't being called. What's wrong?

**A:** Check these common issues:

1. **Module not registered:** call `app.Register(module)` for it.
2. **Service not registered in `RegisterServices`:** implement `ServiceProviderModule` and register there (the framework calls it before `Start`).
3. **Service name mismatch:** a service `get-order` registered by module `order` has the subject `services.order.get-order`; the caller must use the same service name on the `order` dependency container (`GetRequestReplyService("get-order")`).
4. **Application not started:** call `app.Start(ctx)` before using services.

**Learn more:** [Services](../core-concepts/services.md)

---

### Q: Events aren't being received. What's wrong?

**A:** Check these common issues:

1. **Consumer module not registered before `Start()`:** the module must implement `RegisterEventConsumers(registry mono.EventRegistry) error` and be registered with `app.Register` before `app.Start`.
2. **Event handler not registered:** register it in `RegisterEventConsumers`, for example with `helper.RegisterTypedEventConsumer(registry, OrderCreatedV1, m.handleOrderCreated, m)`.
3. **Event subject doesn't match:** publisher and consumer must use the same event definition (`helper.EventDefinition`, normally from the emitter's contract package).

**Learn more:** [Event Consumers](../core-concepts/consumers.md)

---

## Additional Resources

- [How-To Guides](../guides/README.md) - Step-by-step instructions for common tasks
- [Core Concepts](../core-concepts/README.md) - Foundational concepts and architecture
- [API Reference](../api/README.md) - Complete API documentation
- [Examples](../../../examples/basic/README.md) - Runnable example applications
