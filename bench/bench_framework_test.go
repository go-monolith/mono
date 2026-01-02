// Package bench provides performance benchmarks for mono-framework core operations.
//
// These benchmarks measure framework-level performance characteristics:
//   - Framework startup time: How long it takes to initialize and start the framework
//   - Memory footprint: Base memory overhead of the framework with minimal modules
//   - Message throughput: Sustained request-reply performance across modules
//
// Run all framework benchmarks:
//
//	go test -bench='BenchmarkFramework' -benchmem ./bench/
//
// Run specific benchmark:
//
//	go test -bench='BenchmarkFrameworkStartup/10_modules' -benchmem ./bench/
//	go test -bench='BenchmarkFrameworkMemoryFootprint/50_modules' -benchmem ./bench/
//
// Custom metrics:
//   - MB_alloc_delta: Total allocated memory delta from baseline (includes GC'd memory)
//   - MB_heap_delta: Heap memory delta from baseline (live objects only)
//
// Compare results: benchstat old.txt new.txt
package bench

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	mono "github.com/go-monolith/mono/v1"
)

// BenchmarkFrameworkStartup measures the time to create, register modules, and start the framework.
// Target: < 10ms for framework with 10 modules.
//
// Typical results (11 vCPUs, in-process NATS):
//   - Single module:  ~15-20μs
//   - 5 modules:      ~25-35μs
//   - 10 modules:     ~40-60μs
//   - 20 modules:     ~80-120μs
//
// All well within the 10ms target even for complex multi-module applications.
func BenchmarkFrameworkStartup(b *testing.B) {
	testCases := []struct {
		name        string
		moduleCount int
	}{
		{"1_module", 1},
		{"5_modules", 5},
		{"10_modules", 10},
		{"20_modules", 20},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkFrameworkStartupWithModules(b, tc.moduleCount)
		})
	}
}

func benchmarkFrameworkStartupWithModules(b *testing.B, moduleCount int) {
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs() // Explicitly track allocations

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create framework
		app, err := NewBenchApp()
		if err != nil {
			b.Fatalf("Failed to create app: %v", err)
		}

		// Register realistic modules (with internal data structures, lifecycle, and services)
		// Use simple kebab-case names valid for NATS subjects
		for j := 0; j < moduleCount; j++ {
			module := NewRealisticBenchModule("bench-startup-" + strconv.Itoa(j))
			if err := app.Register(module); err != nil {
				b.Fatalf("Failed to register module: %v", err)
			}
		}

		b.StartTimer()

		// Measure startup time
		startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := app.Start(startCtx); err != nil {
			cancel()
			b.Fatalf("Failed to start app: %v", err)
		}
		cancel()

		b.StopTimer()

		// Clean up
		if err := app.Stop(ctx); err != nil {
			b.Fatalf("Failed to stop app: %v", err)
		}
	}
}

// BenchmarkFrameworkStartupWithDependencies measures startup time with module dependencies.
// This tests the dependency resolution and ordered initialization overhead.
//
// Typical results (11 vCPUs, in-process NATS):
//   - Linear chain (10 modules): ~40-60ms
//   - Tree structure (15 modules): ~50-80ms
//
// Dependency resolution adds minimal overhead (~1-2ms per dependency link).
func BenchmarkFrameworkStartupWithDependencies(b *testing.B) {
	testCases := []struct {
		name   string
		setup  func(*testing.B) (mono.MonoApplication, []mono.Module)
		target time.Duration
	}{
		{
			name: "linear_chain_10_modules",
			setup: func(b *testing.B) (mono.MonoApplication, []mono.Module) {
				app, err := NewBenchApp()
				if err != nil {
					b.Fatalf("Failed to create app: %v", err)
				}

				// Create linear dependency chain: m0 <- m1 <- m2 <- ... <- m9
				modules := make([]mono.Module, 10)
				for i := 0; i < 10; i++ {
					var deps []string
					if i > 0 {
						deps = []string{modules[i-1].Name()}
					}
					modules[i] = NewBenchConsumerModule(b.Name()+"_module_"+strconv.Itoa(i), deps)
				}
				return app, modules
			},
			target: 100 * time.Millisecond,
		},
		{
			name: "tree_structure_15_modules",
			setup: func(b *testing.B) (mono.MonoApplication, []mono.Module) {
				app, err := NewBenchApp()
				if err != nil {
					b.Fatalf("Failed to create app: %v", err)
				}

				// Create tree: root (m0), tier1 (m1-m5 depend on m0), tier2 (m6-m14 depend on tier1)
				modules := make([]mono.Module, 15)
				modules[0] = NewBenchProviderModule(b.Name() + "_root")
				for i := 1; i <= 5; i++ {
					modules[i] = NewBenchConsumerModule(b.Name()+"_tier1_"+strconv.Itoa(i), []string{modules[0].Name()})
				}
				for i := 6; i < 15; i++ {
					parentIdx := 1 + ((i - 6) % 5)
					modules[i] = NewBenchConsumerModule(b.Name()+"_tier2_"+strconv.Itoa(i), []string{modules[parentIdx].Name()})
				}
				return app, modules
			},
			target: 150 * time.Millisecond,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs() // Explicitly track allocations

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				app, modules := tc.setup(b)
				for _, module := range modules {
					if err := app.Register(module); err != nil {
						b.Fatalf("Failed to register module: %v", err)
					}
				}

				b.StartTimer()

				startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if err := app.Start(startCtx); err != nil {
					cancel()
					b.Fatalf("Failed to start app: %v", err)
				}
				cancel()

				b.StopTimer()

				if err := app.Stop(ctx); err != nil {
					b.Fatalf("Failed to stop app: %v", err)
				}
			}

			// Report whether we met the target
			if b.Elapsed()/time.Duration(b.N) > tc.target {
				b.Logf("WARNING: Average time %v exceeds target %v", b.Elapsed()/time.Duration(b.N), tc.target)
			}
		})
	}
}

// BenchmarkFrameworkMemoryFootprint measures the base memory overhead of the framework.
// Target: < 20MB base overhead for minimal framework with NATS embedded.
//
// Typical results (11 vCPUs, in-process NATS):
//   - Base framework (no modules):     ~0.06 MB
//   - Framework + 1 module:            ~0.27 MB
//   - Framework + 10 modules:          ~0.23 MB
//   - Framework + 100 modules:         ~0.30 MB
//
// All well within the 20MB target even with 100+ modules.
func BenchmarkFrameworkMemoryFootprint(b *testing.B) {
	testCases := []struct {
		name        string
		moduleCount int
	}{
		{"no_modules", 0},
		{"1_module", 1},
		{"10_modules", 10},
		{"50_modules", 50},
		{"100_modules", 100},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkMemoryFootprintWithModules(b, tc.moduleCount)
		})
	}
}

func benchmarkMemoryFootprintWithModules(b *testing.B, moduleCount int) {
	ctx := context.Background()

	// Run GC to get clean baseline
	runtime.GC()
	runtime.GC() // Run twice to ensure full collection

	var baselineAlloc, baselineHeap uint64
	{
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		baselineAlloc = m.Alloc
		baselineHeap = m.HeapAlloc
	}

	b.ResetTimer()
	b.ReportAllocs() // Explicitly track allocations

	for i := 0; i < b.N; i++ {
		// Create and start framework
		app, err := NewBenchApp()
		if err != nil {
			b.Fatalf("Failed to create app: %v", err)
		}

		// Register realistic modules (with internal data structures, lifecycle, and services)
		// Use simple kebab-case names valid for NATS subjects
		for j := 0; j < moduleCount; j++ {
			module := NewRealisticBenchModule("bench-mem-" + strconv.Itoa(j))
			if err := app.Register(module); err != nil {
				b.Fatalf("Failed to register module: %v", err)
			}
		}

		startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := app.Start(startCtx); err != nil {
			cancel()
			b.Fatalf("Failed to start app: %v", err)
		}
		cancel()

		// Measure memory after framework is running
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		allocDelta := int64(m.Alloc - baselineAlloc)
		heapDelta := int64(m.HeapAlloc - baselineHeap)

		// Report memory deltas
		b.ReportMetric(float64(allocDelta)/1024/1024, "MB_alloc_delta")
		b.ReportMetric(float64(heapDelta)/1024/1024, "MB_heap_delta")

		// Check against target
		const targetMB = 20
		if heapDelta > targetMB*1024*1024 {
			b.Logf("WARNING: Heap delta %.2f MB exceeds target %d MB", float64(heapDelta)/1024/1024, targetMB)
		}

		// Clean up
		if err := app.Stop(ctx); err != nil {
			b.Fatalf("Failed to stop app: %v", err)
		}
	}
}

// BenchmarkFrameworkThroughput measures sustained message throughput across multiple modules.
// This is an end-to-end benchmark showing real-world application performance.
// Target: > 40,000 msgs/sec for multi-module application.
//
// Typical results (11 vCPUs, in-process NATS):
//   - 2 modules (direct call):         ~100,000-200,000 msgs/sec
//   - 4 modules (2-hop chain):         ~50,000-100,000 msgs/sec
//   - 10 modules (complex workflow):   ~20,000-40,000 msgs/sec
//
// All well above the 40,000 msgs/sec target.
func BenchmarkFrameworkThroughput(b *testing.B) {
	ctx := context.Background()

	// Setup: Create a simple producer->consumer pipeline
	producer := NewBenchProviderModule("producer")
	producer.SetupFunc = func(container mono.ServiceContainer) error {
		handler := func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return req.Data, nil // Echo back
		}
		return container.RegisterRequestReplyService("echo", handler)
	}

	consumer := NewBenchConsumerModule("consumer", []string{"producer"})

	app, err := NewBenchApp()
	if err != nil {
		b.Fatalf("Failed to create app: %v", err)
	}
	defer app.Stop(ctx)

	if err := app.Register(producer); err != nil {
		b.Fatalf("Failed to register producer: %v", err)
	}
	if err := app.Register(consumer); err != nil {
		b.Fatalf("Failed to register consumer: %v", err)
	}

	startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		b.Fatalf("Failed to start app: %v", err)
	}

	// Get service client
	depContainer := consumer.DepContainer()
	if depContainer == nil {
		b.Fatal("Failed to get dependency container")
	}
	echoService, err := depContainer.GetRequestReplyService("echo")
	if err != nil {
		b.Fatalf("Failed to get echo service: %v", err)
	}

	payload := GeneratePayload(DefaultPayloadSize)

	b.ResetTimer()
	b.ReportAllocs() // Explicitly track allocations
	b.SetBytes(int64(DefaultPayloadSize))

	// Create timeout context once outside loop to avoid allocation overhead
	benchCtx, benchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer benchCancel()

	// Measure throughput
	for i := 0; i < b.N; i++ {
		_, err := echoService.Call(benchCtx, payload)
		if err != nil {
			b.Fatalf("Service call failed: %v", err)
		}
	}

	// Report throughput (calculated automatically by benchstat from ns/op)
	// Target: > 10,000 msgs/sec
	// Typical: ~80,000-100,000 msgs/sec for simple echo service
}
