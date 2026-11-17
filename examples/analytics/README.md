# Analytics Example - Channel-Based Services

This example demonstrates channel-based services in the Monolith-Framework for high-performance in-process communication.

## Overview

The Analytics module shows how to implement channel-based services for scenarios requiring ultra-low latency and high throughput within a single process.

## What You'll Learn

1. **Channel Service Registration**: How to register bidirectional channel services
2. **Goroutine Handler Pattern**: Setting up background handlers for channel processing
3. **Graceful Shutdown**: Proper cleanup of channels and goroutines using context and WaitGroup
4. **When to Use Channels**: Understanding channel services vs NATS-based services
5. **Plugin storage**: Persisting events with the kv-jetstream plugin (JetStream enabled with `WithJetStreamStorageDir`)
6. **Adapter pattern**: Retrieving events through a typed adapter over a request-reply service

## Channel Services vs NATS Services

### Channel Services (In-Process)

**Characteristics:**
- Transport: Go channels (in-memory)
- Latency: Ultra-low (~microseconds)
- Scope: Same process only
- Response: Bidirectional (separate in/out channels)
- Setup: Create channels, spawn handler goroutine
- Shutdown: Cancel context, close channels, wait for goroutine

**Use When:**
- All modules run in the same process
- Need microsecond-level latency
- High-frequency operations (>10k ops/sec)
- Want synchronous backpressure

**Examples:**
- High-frequency analytics tracking
- In-process caching
- Local event aggregation
- Rate limiting services

### RequestReply Services (NATS-Based)

**Characteristics:**
- Transport: NATS (can be distributed)
- Latency: Low (~milliseconds)
- Scope: Can span processes/machines
- Response: Synchronous reply
- Setup: Register handler function
- Shutdown: Framework handles unsubscribe

**Use When:**
- Need synchronous request/response
- Services may be distributed
- Want service discovery via NATS

**Examples:**
- Inventory checks
- Payment processing
- Authentication services

### QueueGroup Services (NATS-Based)

**Characteristics:**
- Transport: NATS (can be distributed)
- Latency: Low (~milliseconds)
- Scope: Can span processes/machines
- Response: Fire-and-forget
- Setup: Register handler with queue group
- Shutdown: Framework handles unsubscribe

**Use When:**
- Asynchronous commands
- Need load balancing across workers
- Fire-and-forget operations

**Examples:**
- Email sending
- Background jobs
- Notification dispatch

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

## Module Implementation

### Service Registration

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

### Handler Goroutine Setup

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

### Handler Pattern

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

### Graceful Shutdown

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

## Running the Example

```bash
# Navigate to the example directory
cd examples/analytics

# Run the example
go run .

# Expected output shows:
# - App initialization with JetStream and the kv-jetstream plugin
# - Five scenarios: tracking events over the channel service (single,
#   multiple, concurrent, invalid) and retrieving them over get-event
# - Graceful shutdown with proper cleanup

# Or, from the repository root (stops after 5 s):
# make run-example-3
```

### Expected Output

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

## Key Concepts Demonstrated

### 1. Bidirectional Channels

```go
trackEventIn  chan *mono.Msg  // Receives requests
trackEventOut chan *mono.Msg  // Sends responses
```

Separate channels for request and response flow.

### 2. Buffer Sizing

```go
// Create buffered channels for better performance
trackEventIn:  make(chan *mono.Msg, 100)
trackEventOut: make(chan *mono.Msg, 100)
```

Buffer size affects backpressure behavior and performance.

### 3. Context-Based Cancellation

```go
m.ctx, m.cancel = context.WithCancel(context.Background())
```

Enables coordinated shutdown across goroutines.

### 4. WaitGroup for Goroutine Tracking

```go
m.wg.Add(1)
go m.handleTrackEventChannel()
// ... later ...
m.wg.Wait() // Wait for handler to complete
```

Ensures all goroutines complete before shutdown.

### 5. Select for Non-Blocking Operations

```go
select {
case m.trackEventOut <- response:
    // Sent successfully
case <-time.After(1 * time.Second):
    // Timeout
case <-m.ctx.Done():
    // Shutdown
}
```

Prevents blocking during shutdown.

## Comparison Table

| Feature | Channel Services | RequestReply | QueueGroup |
|---------|-----------------|--------------|------------|
| Latency | ~microseconds | ~milliseconds | ~milliseconds |
| Distribution | Same process | Multi-process | Multi-process |
| Response | Bidirectional | Synchronous | Fire-and-forget |
| Load Balancing | Manual | Automatic (queue group, one responder per request) | Automatic |
| Ordering | Per channel | None (pooled, concurrent handlers) | Arrival order per instance by default (`MaxConcurrency` 1) |
| Backpressure | Channel buffer | Fail fast with `ErrServiceOverloaded` when the worker pool is full | Blocks in the NATS client buffer |

## Advanced Topics

### Buffer Sizing Considerations

- **Small buffers** (10-50): Low memory, more backpressure
- **Medium buffers** (100-500): Balanced performance
- **Large buffers** (1000+): High throughput, more memory

### Testing Strategies

A sketch, not part of the example (the module also needs a kv-jetstream plugin injected with `SetPlugin("kv", ...)` before `Start`; `monotest.New` runs a module through the real lifecycle):

```go
// Unit test channel service (sketch)
func TestChannelService(t *testing.T) {
    module := NewModule()

    // Test registration
    container := &mockContainer{}
    err := module.RegisterServices(container)
    assert.NoError(t, err)

    // Test request/response
    module.Start(context.Background())
    module.trackEventIn <- testRequest
    response := <-module.trackEventOut
    assert.Equal(t, expectedResponse, response)

    // Test shutdown
    module.Stop(context.Background())
}
```

## Extending the Example

Ideas for learning:

1. **Add Multiple Services**: Register multiple channel services in one module
2. **Implement Backpressure Handling**: Monitor buffer usage and apply backpressure
3. **Add Performance Metrics**: Track latency, throughput, buffer usage
4. **Implement Priority Queues**: Use multiple channels for different priorities
5. **Add Request Batching**: Buffer multiple requests for batch processing
6. **Create Integration Tests**: Test with real framework lifecycle

## Limitations

Channel services have these limitations:

1. **No Distribution**: Cannot span multiple processes
2. **No Discovery**: Other modules must know the service exists
3. **Manual Management**: Must handle goroutines, channels, cleanup
4. **Memory Overhead**: Channels consume memory for buffering

Use NATS services (RequestReply/QueueGroup) when you need distribution or service discovery.

## See Also

- [Basic Example](../basic/README.md) - Simple single-module introduction
- [Multi-Module Example](../multi-module/README.md) - RequestReply and QueueGroup patterns
- [Design Document](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md) - Complete framework design
- [Module Interface Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/module.go) - All module interfaces
