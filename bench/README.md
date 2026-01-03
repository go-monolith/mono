# Mono-Framework Performance Benchmarks

This directory contains comprehensive performance benchmarks for the mono-framework, measuring throughput, latency, startup time, and memory footprint across various service types and framework operations.

## Quick Start

```bash
# Run all benchmarks
go test -bench=. -benchmem ./bench/

# Run specific benchmark category
go test -bench='BenchmarkFramework' -benchmem ./bench/
go test -bench='BenchmarkInProcess' -benchmem ./bench/
go test -bench='BenchmarkSocket' -benchmem ./bench/

# Run benchmarks with timeout (some benchmarks take time)
go test -bench=. -benchmem -timeout=10m ./bench/

# Save benchmark results for comparison
go test -bench=. -benchmem ./bench/ > new.txt
benchstat old.txt new.txt
```

## Performance Targets

The framework is designed to meet the following performance targets (defined in `docs/spec/foundation.md`):

| Metric | Target | Actual Performance | Status |
|--------|--------|-------------------|--------|
| **Message Throughput** | > 40,000 msgs/sec | 90,000+ msgs/sec | ✅ PASS |
| **Framework Startup** | < 10ms | 14-60 microseconds | ✅ PASS |
| **Memory Footprint** | < 20MB base overhead | 0.06-0.27 MB | ✅ PASS |
| **Pub/Sub Latency** | < 1ms per message | 456-585 ns | ✅ PASS |
| **Service Call Latency** | Varies by type | See below | ✅ PASS |

## Benchmark Categories

### Framework Benchmarks (`bench_framework_test.go`)

These benchmarks measure framework-level operations:

#### BenchmarkFrameworkStartup
- **Purpose**: Measures framework initialization and startup time
- **Target**: < 10ms for 10 modules
- **Results**:
  - 1 module: ~14 μs
  - 5 modules: ~23 μs
  - 10 modules: ~37 μs
  - 20 modules: ~58 μs
- **Status**: ✅ All well within 10ms target

#### BenchmarkFrameworkStartupWithDependencies
- **Purpose**: Measures startup time with module dependency resolution
- **Results**:
  - Linear chain (10 modules): ~43 μs
  - Tree structure (15 modules): ~61 μs
- **Status**: ✅ Dependency resolution adds minimal overhead (~1-2μs per dependency)

#### BenchmarkFrameworkMemoryFootprint
- **Purpose**: Measures base memory overhead of the framework
- **Target**: < 20MB base overhead
- **Results**:
  - No modules: ~0.06 MB
  - 1 module: ~0.27 MB
  - 10 modules: ~0.23 MB
  - 50 modules: ~0.06 MB
  - 100 modules: ~0.11 MB
- **Status**: ✅ All well within 20MB target

#### BenchmarkFrameworkThroughput
- **Purpose**: End-to-end message throughput test
- **Target**: > 40,000 msgs/sec
- **Results**: ~90,979 msgs/sec
- **Status**: ✅ ~2x above target

### In-Process Service Benchmarks (`bench_in_process_test.go`)

These benchmarks use in-process NATS connections (no TCP overhead) to measure pure service performance:

#### BenchmarkInProcessChannelService
- **Service Type**: Go channel-based communication
- **Latency**: ~540-640 ns/op
- **Throughput**: 400-8,000 MB/s depending on payload size
- **Best for**: Ultra-low latency in-process communication

#### BenchmarkInProcessRequestReplyService
- **Service Type**: NATS request-response pattern
- **Latency**: ~9.6-22.6 μs/op
- **Throughput**: 26-226 MB/s depending on payload size
- **Best for**: Synchronous service calls with responses

#### BenchmarkInProcessQueueGroupService
- **Service Type**: Fire-and-forget with load balancing
- **Latency**: ~9.8-21.5 μs/op
- **Throughput**: 26-238 MB/s depending on payload size
- **Best for**: Background tasks with horizontal scaling

#### BenchmarkInProcessStreamConsumerService
- **Service Type**: JetStream durable messaging with batching
- **Latency**: ~18-74 μs/op
- **Throughput**: 14-69 MB/s depending on payload size
- **Best for**: Reliable message delivery with persistence

#### BenchmarkInProcessEventConsumer
- **Service Type**: NATS Core pub/sub event delivery
- **Latency**: ~585-1,768 ns/op (< 1ms target ✅)
- **Throughput**: 437-2,896 MB/s depending on payload size
- **Best for**: High-throughput event broadcasting

#### BenchmarkInProcessEventStreamConsumer
- **Service Type**: JetStream durable event delivery
- **Latency**: ~15-80 μs/op
- **Throughput**: 16-64 MB/s depending on payload size
- **Best for**: Durable event sourcing with replay

### Socket Service Benchmarks (`bench_socket_test.go`)

These benchmarks use TCP socket connections to measure network I/O overhead:

#### Key Differences from In-Process
- Uses TCP loopback (127.0.0.1) instead of in-memory connections
- Adds network serialization/deserialization overhead
- Simulates realistic distributed deployment scenarios

#### Performance Comparison
- Channel Service: ~10% faster with sockets (optimized TCP stack)
- Request-Reply: ~70% slower (network round-trip)
- Queue Group: ~80% slower (network + ack overhead)
- Event Consumer: ~20-30% faster for small payloads

### Multi-Module Benchmarks (`bench_multi_module_test.go`)

#### BenchmarkMultiModuleOrderOrchestration
- **Purpose**: End-to-end workflow simulation
- **Scenario**: Order → Inventory → Payment → Notification
- **Latency**: ~70-290 μs depending on payload size
- **Demonstrates**: Real-world application performance with multiple service calls

#### BenchmarkMultiModuleOrderOrchestrationJSON
- **Purpose**: Same workflow with JSON serialization
- **Comparison**: ~10-15% slower than raw bytes (JSON overhead)
- **Demonstrates**: Typical API serialization costs

## Benchmark Methodology

### Payload Sizes
All service benchmarks test three payload sizes:
- **256 bytes**: Typical small message (user ID, simple event)
- **1 KB**: Medium message (user profile, order details)
- **5 KB**: Large message (document, image metadata)

### Environment
Benchmarks are designed to run on:
- **CPU**: 11 vCPUs (ARM64 or x86_64)
- **Memory**: 4GB+ recommended
- **OS**: Linux, macOS, or Windows
- **Go**: 1.25+

### Accuracy Considerations
- Each benchmark runs multiple iterations (b.N) for statistical accuracy
- `b.ResetTimer()` used to exclude setup costs
- `runtime.GC()` run twice before memory benchmarks for clean baseline
- Context timeouts prevent hanging benchmarks

## Interpreting Results

### Throughput (MB/s)
- Higher is better
- Calculated as: `(payload_size * b.N) / elapsed_time`
- Set with `b.SetBytes(int64(payloadSize))`

### Latency (ns/op, μs/op, ms/op)
- Lower is better
- Represents time per operation
- For round-trip operations, includes both send and receive

### Allocations (B/op, allocs/op)
- Lower is better
- Shows memory allocation overhead
- Key metric for GC pressure

### Custom Metrics
Some benchmarks report custom metrics:
- `msgs/sec`: Messages per second throughput
- `MB_alloc_delta`: Memory allocated above baseline
- `MB_heap_delta`: Heap growth above baseline

## Comparing Results with benchstat

```bash
# Run baseline
go test -bench=. -benchmem -count=5 ./bench/ > old.txt

# Make changes to code

# Run new benchmarks
go test -bench=. -benchmem -count=5 ./bench/ > new.txt

# Compare (requires: go install golang.org/x/perf/cmd/benchstat@latest)
benchstat old.txt new.txt
```

Expected output:
```
name                                           old time/op    new time/op    delta
FrameworkStartup/10_modules-11                   37.0µs ± 2%    36.5µs ± 3%     ~
InProcessEventConsumer/payload_256B-11            585ns ± 1%     580ns ± 2%   -0.85%
```

## Performance Optimization Tips

### For Service Selection
- **Use ChannelService** for ultra-low latency in-process communication
- **Use RequestReplyService** for synchronous RPC with responses
- **Use QueueGroupService** for background tasks with load balancing
- **Use EventConsumer** for high-throughput pub/sub patterns
- **Use StreamConsumer** when you need message persistence and replay

### For Deployment Mode
- **In-Process NATS** (default): Best performance, single-process deployment
- **TCP Socket NATS**: Necessary for multi-process/distributed deployment
- **Clustered NATS**: Horizontal scaling with minimal overhead

### For Memory Optimization
- Module count has minimal impact (< 10KB per module)
- Service registration is lightweight (< 1KB per service)
- JetStream adds overhead (~10-20MB for storage + buffers)

## Requirements Traceability

| Requirement | Benchmark | Result |
|------------|-----------|--------|
| **NFR1.1**: Message throughput > 40,000 msgs/sec | BenchmarkFrameworkThroughput | 90,979 msgs/sec ✅ |
| **NFR1.1**: Framework startup < 10ms | BenchmarkFrameworkStartup | 14-60 μs ✅ |
| **NFR1.4**: Pub/sub latency < 1ms | BenchmarkInProcessEventConsumer | 585 ns ✅ |
| **NFR1.4**: Memory footprint < 20MB | BenchmarkFrameworkMemoryFootprint | 0.06-0.27 MB ✅ |

## JSON Benchmark Results

The `benchparse` tool converts Go benchmark JSON output to a structured format with targets and pass/fail indicators:

```bash
# Generate JSON benchmark results
go test -bench='BenchmarkFramework' -benchmem -json ./bench/ | go run ./bench/cmd/benchparse

# Output is written to mono_benchmark_result.json
```

### JSON Output Format

The output includes two sections:

1. **`results`**: All benchmark results with standard metrics
2. **`framework_results`**: Framework benchmarks with targets and pass/fail status

```json
{
  "timestamp": "2025-12-31T05:00:00Z",
  "results": [...],
  "framework_results": [
    {
      "name": "BenchmarkFrameworkStartup/10_modules-11",
      "duration": "0.02ms",
      "target_duration": "10ms",
      "duration_passed": true
    },
    {
      "name": "BenchmarkFrameworkMemoryFootprint/10_modules-11",
      "memory": "0.25MB",
      "target_memory": "20MB",
      "memory_passed": true
    },
    {
      "name": "BenchmarkFrameworkThroughput-11",
      "throughput": "90000 msgs/sec",
      "target_throughput": "40000 msgs/sec",
      "throughput_passed": true
    }
  ]
}
```

### Framework Benchmark Targets

| Benchmark Type | Metric | Target | Pass Condition |
|----------------|--------|--------|----------------|
| `BenchmarkFrameworkStartup/*` | duration | 10ms | actual <= target |
| `BenchmarkFrameworkStartupWithDependencies/*` | duration | 10ms | actual <= target |
| `BenchmarkFrameworkMemoryFootprint/*` | memory | 20MB | actual <= target |
| `BenchmarkFrameworkThroughput` | throughput | 40000 msgs/sec | actual >= target |

## Continuous Performance Monitoring

Add these benchmarks to your CI/CD pipeline:

```yaml
# .github/workflows/benchmarks.yml
- name: Run Benchmarks
  run: |
    go test -bench=. -benchmem -timeout=10m ./bench/ | tee bench-results.txt

- name: Run Benchmarks with JSON Output
  run: |
    go test -bench='BenchmarkFramework' -benchmem -json ./bench/ | go run ./bench/cmd/benchparse

- name: Check Performance Regressions
  run: |
    benchstat baseline.txt bench-results.txt > comparison.txt
    # Fail if throughput decreased by >10% or latency increased by >10%
```

## Contributing New Benchmarks

When adding new benchmarks:

1. **Follow naming convention**: `Benchmark<Category><TestName>`
2. **Use standard payload sizes**: 256B, 1KB, 5KB
3. **Include benchmem flag**: Report allocations
4. **Document targets**: Add expected performance in comments
5. **Use b.ResetTimer()**: Exclude setup time
6. **Report custom metrics**: Use `b.ReportMetric()` for domain-specific measures
7. **Add to this README**: Update tables and documentation

## References

- [Go Benchmark Documentation](https://pkg.go.dev/testing#hdr-Benchmarks)
- [NATS Benchmarking Guide](https://docs.nats.io/running-a-nats-service/nats_admin/monitoring#benchmarking)
- [Foundation Specification](../docs/spec/foundation.md)
- [Performance Targets](../docs/spec/foundation.md#performance-targets)
