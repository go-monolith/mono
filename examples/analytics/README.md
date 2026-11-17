# Analytics Example - Channel-Based Services

An analytics module that tracks events through a **channel service** (`track-event`, in-process Go channels) and serves them back through a **request-reply service** (`get-event`) via a typed adapter. Events are persisted in a KV bucket of the kv-jetstream plugin, so JetStream is enabled with `WithJetStreamStorageDir`.
It shows how to register a bidirectional channel service, run its handler goroutine, and clean up channels and goroutines on shutdown with a context and a WaitGroup.

## Running the Example

```bash
cd examples/analytics
go run .

# Or, from the repository root (stops after 5 s):
make run-example-3
```

The program runs five scenarios: tracking events over the channel service (single, multiple, concurrent, invalid) and retrieving them over `get-event`. Then it waits for Ctrl+C.

## Code Structure

```
examples/analytics/
├── main.go                     # Application demonstrating channel services
├── analytics-module/
│   ├── module.go              # Analytics module with channel service
│   ├── adapter.go             # AnalyticsAdapterPort over the get-event request-reply service
│   └── types.go               # Request/response data models
└── README.md                   # This file
```

## Key Patterns

### 1. Service Registration

The module owns two buffered channels (`trackEventIn` receives requests, `trackEventOut` sends responses), created with `make(chan *mono.Msg, 100)`. Buffer size affects backpressure behavior and performance.

```go
func (m *AnalyticModule) RegisterServices(container mono.ServiceContainer) error {
    // Register bidirectional channel service
    // inChan: receives requests, outChan: sends responses
    if err := container.RegisterChannelService("track-event", m.trackEventIn, m.trackEventOut); err != nil {
        return err
    }

    // Register the get-event request-reply service
    return container.RegisterRequestReplyService("get-event", m.getEvent)
}
```

### 2. Handler Goroutine

`Start` fetches the KV bucket, creates a cancellable context for coordinated shutdown, and starts the handler goroutine tracked by a WaitGroup:

```go
func (m *AnalyticModule) Start(_ context.Context) error {
    // Get the KV bucket from the kv-jetstream plugin (injected via SetPlugin("kv", ...))
    if m.kvPlugin == nil {
        return fmt.Errorf("kv plugin not set")
    }
    m.kvStore = m.kvPlugin.Bucket(BucketName)
    if m.kvStore == nil {
        return fmt.Errorf("bucket %q not found in kv plugin", BucketName)
    }

    // Create cancellable context for graceful shutdown
    m.ctx, m.cancel = context.WithCancel(context.Background())

    // Start handler goroutine with WaitGroup tracking
    m.wg.Add(1)
    go m.handleTrackEventChannel()

    return nil
}
```

The handler processes requests until the context is cancelled or the input channel is closed. The inner `select` sends the response with a timeout, so the handler never blocks during shutdown:

```go
func (m *AnalyticModule) handleTrackEventChannel() {
    defer m.wg.Done()

    for {
        select {
        case <-m.ctx.Done():
            // Graceful shutdown requested
            return

        case msg, ok := <-m.trackEventIn:
            if !ok {
                // Channel closed, exit gracefully
                return
            }

            // Process request
            response := m.processTrackEvent(msg)

            // Send response with timeout
            select {
            case m.trackEventOut <- response:
                // Sent successfully
            case <-time.After(1 * time.Second):
                // Timeout sending response
            case <-m.ctx.Done():
                // Shutdown requested
                return
            }
        }
    }
}
```

### 3. Graceful Shutdown

`Stop` cancels the context, closes the input channel, waits for the handler, then closes the output channel:

```go
// Simplified: the real Stop also prints progress and reports the number of
// events stored in the KV bucket.
func (m *AnalyticModule) Stop(_ context.Context) error {
    // Signal shutdown to handler
    m.cancel()

    // Close input channel (no more requests)
    close(m.trackEventIn)

    // Wait for handler to complete
    done := make(chan struct{})
    go func() {
        m.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // Handler exited cleanly
    case <-time.After(5 * time.Second):
        // Timeout waiting for handler
    }

    // Close output channel after handler completes
    close(m.trackEventOut)

    return nil
}
```

### 4. Testing

The module needs a kv-jetstream plugin injected with `SetPlugin("kv", ...)` before `Start`; `monotest.New` runs a module through the real lifecycle.

## Expected Output

Framework and plugin log lines are interleaved and omitted here; event IDs contain timestamps, and the concurrent workers finish in any order.

```
=== Mono-Framework Channel Services Example ===
Demonstrates: Channel-based services with kv-jetstream for persistent storage

✓ App created successfully
✓ KV plugin registered
✓ Modules registered: [analytics]
  → Analytics module started (using kv-jetstream for storage)
✓ App started

App Health: healthy=true, nats_healthy=true

Running example scenarios...

[Scenario 1] Basic Event Tracking
  → Analytics: tracked event evt_user123_... (type=page_view, user=user123)
  ✓ Event tracked successfully: evt_user123_... (total: 1)

[Scenario 2] Multiple Events
  ✓ Event 1 tracked: evt_user456_... (type=button_click, user=user456, total=2)
  ✓ Event 2 tracked: evt_user789_... (type=form_submit, user=user789, total=3)
  ✓ Event 3 tracked: evt_user123_... (type=video_play, user=user123, total=4)

[Scenario 3] Concurrent Event Tracking
  Worker 9: success (evt_user9_..., total=5)
  ...
  Worker 3: success (evt_user3_..., total=14)

[Scenario 4] Error Handling
  → Invalid event rejected gracefully

[Scenario 5] Retrieve Events via Adapter Pattern
  Retrieving 3 events using adapter pattern...
  ✓ Event evt_user123_... retrieved: type=page_view, user=user123, recorded=...
  ...

  Testing retrieval of non-existent event...
  → Analytics: event non-existent-event-id not found
  ✓ Non-existent event correctly returned as not found

Total events tracked: 14

Press Ctrl+C to shutdown...

Shutdown signal received...
  → Analytics module stopping...
  → Analytics: handler goroutine exited cleanly
  → Analytics module stopped (total 14 events in store)
✓ App stopped successfully
Example completed!
```

## Channel Services vs NATS Services

| Feature | Channel Services | RequestReply | QueueGroup |
|---------|-----------------|--------------|------------|
| Transport | Go channels (in-memory) | NATS | NATS |
| Latency | ~microseconds | ~milliseconds | ~milliseconds |
| Distribution | Same process | Multi-process | Multi-process |
| Response | Bidirectional (separate in/out channels) | Synchronous | Fire-and-forget |
| Load Balancing | Manual | Automatic (queue group, one responder per request) | Automatic |
| Ordering | Per channel | None (pooled, concurrent handlers) | Arrival order per instance by default (`MaxConcurrency` 1) |
| Backpressure | Channel buffer | Fail fast with `ErrServiceOverloaded` when the worker pool is full | Blocks in the NATS client buffer |
| Setup and shutdown | Create channels, run and stop the handler goroutine yourself | Register a handler; the framework unsubscribes | Register a handler with a queue group; the framework unsubscribes |
| Use for | High-frequency in-process work (analytics tracking, caching, local aggregation, rate limiting) | Synchronous calls that may be distributed (inventory checks, payments, authentication) | Asynchronous, load-balanced commands (email, background jobs, notifications) |

Channel services cannot span processes, and their goroutines, channels and cleanup are yours to manage. Use NATS services (RequestReply/QueueGroup) when you need distribution or service discovery.

## Next Steps

Ideas for experimenting with the example:

1. **Add Multiple Services**: Register multiple channel services in one module
2. **Implement Backpressure Handling**: Monitor buffer usage and apply backpressure
3. **Add Performance Metrics**: Track latency, throughput, buffer usage
4. **Implement Priority Queues**: Use multiple channels for different priorities
5. **Add Request Batching**: Buffer multiple requests for batch processing

See also:

- [Basic Example](../basic/README.md) - Simple single-module introduction
- [Multi-Module Example](../multi-module/README.md) - RequestReply and QueueGroup patterns
- [Design Document](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md) - Complete framework design
- [Module Interface Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/module.go) - All module interfaces
