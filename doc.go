// Package mono provides a modular monolith framework for building Go applications.
//
// The mono framework enables building applications as a collection of loosely-coupled
// modules that communicate via NATS messaging, while running as a single binary.
// This approach combines the simplicity of a monolith with the architectural
// benefits of microservices.
//
// # Quick Start
//
// Import the framework:
//
//	import "github.com/go-monolith/mono"
//
// Create a simple module and run the framework:
//
//	type MyModule struct {
//	    logger mono.Logger
//	}
//
//	func (m *MyModule) Name() string { return "my-module" }
//
//	func (m *MyModule) Start(ctx context.Context) error { return nil }
//	func (m *MyModule) Stop(ctx context.Context) error { return nil }
//
//	// In main()
//	app, _ := mono.NewMonoApplication(
//	    mono.WithNATSPort(4222),
//	    mono.WithLogLevel(mono.LogLevelInfo),
//	)
//	app.Register(&MyModule{})
//	app.Start(context.Background())
//
// # Core Concepts
//
// The framework is built around several key abstractions:
//
//   - Module: The basic unit of functionality with lifecycle management
//   - EventBus: NATS-backed message bus for inter-module communication
//   - ServiceContainer: Service registration and discovery
//   - Logger: Structured logging with module context
//
// # Architecture
//
// The framework follows a layered architecture with clear separation of concerns:
//
//	┌─────────────────────────────────────────────────────────────────────┐
//	│                       Application Layer                              │
//	│            (Your Modules implementing mono.Module)                   │
//	├─────────────────────────────────────────────────────────────────────┤
//	│                        Framework Layer                               │
//	│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐│
//	│  │ ServiceContainer│ │    EventBus     │ │     EventRegistry       ││
//	│  │  (DI & Services)│ │   (Pub/Sub)     │ │   (EDA & Consumers)     ││
//	│  └─────────────────┘ └─────────────────┘ └─────────────────────────┘│
//	├─────────────────────────────────────────────────────────────────────┤
//	│                     Infrastructure Layer                             │
//	│               (Embedded NATS Server + JetStream)                     │
//	└─────────────────────────────────────────────────────────────────────┘
//
// Modules communicate through NATS messaging patterns rather than direct method
// calls, enabling loose coupling and clear module boundaries.
//
// # Design Patterns
//
// The framework employs several established Go patterns:
//
// # Functional Options Pattern
//
// Configuration uses functional options for clean, composable API:
//
//	app, err := mono.NewMonoApplication(
//	    mono.WithNATSPort(4222),
//	    mono.WithLogLevel(mono.LogLevelInfo),
//	    mono.WithShutdownTimeout(30*time.Second),
//	)
//
// Each option validates its input and returns an error if invalid.
// Options are applied in order, and the first error stops processing.
// This pattern enables backwards-compatible API evolution and clear defaults.
//
// # Dependency Injection Pattern
//
// Modules receive dependencies via interface injection rather than creating
// them directly. The framework injects ServiceContainers before Start() is called:
//
//	func (m *MyModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
//	    if dep == "payment" {
//	        m.paymentServices = container
//	    }
//	}
//
// This enables testability and decouples modules from their dependencies.
//
// # Module Lifecycle Pattern
//
// Modules implement Start/Stop for controlled initialization and cleanup:
//
//	func (m *MyModule) Start(ctx context.Context) error {
//	    // Initialize resources, establish connections
//	    return nil
//	}
//
//	func (m *MyModule) Stop(ctx context.Context) error {
//	    // Release resources, close connections
//	    return nil
//	}
//
// The framework manages lifecycle ordering based on declared dependencies,
// starting modules in dependency order and stopping in reverse order.
//
// # Service Communication Patterns
//
// The framework supports four communication patterns:
//
//   - Channel Services: In-process Go channels for lowest latency
//   - Request-Reply Services: Synchronous NATS request/reply
//   - Queue Group Services: Load-balanced async processing
//   - Stream Consumer Services: Durable JetStream consumption
//
// # Documentation
//
// For detailed information, see:
//
//   - Architecture: docs/spec/foundation.md
//   - Examples: examples/
//   - Types: pkg/types (core interfaces and type definitions)
//   - Helpers: pkg/helper (convenience functions for common patterns)
package mono
