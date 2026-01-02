# Analytics Example - Channel-Based Services

This example demonstrates channel-based services in the Monolith-Framework for high-performance in-process communication.

## Overview

The Analytics module shows how to implement channel-based services for scenarios requiring ultra-low latency and high throughput within a single process.

## What You'll Learn

1. **Channel Service Registration**: How to register bidirectional channel services
2. **Goroutine Handler Pattern**: Setting up background handlers for channel processing
3. **Graceful Shutdown**: Proper cleanup of channels and goroutines using context and WaitGroup
4. **When to Use Channels**: Understanding channel services vs NATS-based services

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
│   └── types.go               # Request/response data models
└── README.md                   # This file
```

## Module Implementation

### Service Registration

```go
func (m *Module) RegisterServices(container mono.ServiceContainer) error {
    // Register bidirectional channel service
    // inChan: receives requests, outChan: sends responses
    return container.RegisterChannelService("track-event", m.trackEventIn, m.trackEventOut)
}
```

### Handler Goroutine Setup

```go
func (m *Module) Start(_ context.Context) error {
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
func (m *Module) handleTrackEventChannel() {
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
func (m *Module) Stop(_ context.Context) error {
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
# - Framework initialization
# - Channel service registration
# - Handler goroutine starting
# - Graceful shutdown with proper cleanup
```

### Expected Output

```
=== Mono-Framework Channel Services Example ===
Demonstrates: Channel-based services for high-performance in-process communication

✓ Framework created successfully
✓ Modules registered: [analytics]
✓ Framework started

Framework Health: healthy=true, nats_healthy=true

Demonstration: Channel service handler is running in the background
In production, other modules would access this via the ServiceContainer

Press Ctrl+C to shutdown...

Shutdown signal received...
  → Analytics module stopping...
  → Analytics: handler goroutine exited cleanly
  → Analytics module stopped (tracked 0 events)
✓ Framework stopped successfully
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
| Load Balancing | Manual | Single responder | Automatic |
| Ordering | Guaranteed | Best effort | No guarantee |
| Backpressure | Synchronous | Timeout-based | Queue-based |

## Advanced Topics

### Buffer Sizing Considerations

- **Small buffers** (10-50): Low memory, more backpressure
- **Medium buffers** (100-500): Balanced performance
- **Large buffers** (1000+): High throughput, more memory

### Performance Characteristics

Channel services provide:
- **Throughput**: 100k+ ops/sec on modern hardware
- **Latency**: <10 microseconds p99
- **Memory**: ~8KB per 100-buffer channel

### Testing Strategies

```go
// Unit test channel service
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

- [Basic Example](../basic/) - Simple single-module introduction
- [Multi-Module Example](../multi-module/) - RequestReply and QueueGroup patterns
- [Design Document](../../docs/spec/monolith-framework/design.md) - Complete framework design
- [Module Interface Documentation](../../pkg/mono/module.go) - All module interfaces
