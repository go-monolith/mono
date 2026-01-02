// Package types provides all public interfaces and supporting types for the mono-framework.
//
// This package exists to resolve import cycles between pkg/mono and internal packages.
// All interfaces, structs, constants, and type definitions that need to be shared between
// the public API (pkg/mono) and internal implementations (internal/*) are defined here.
//
// # Package Purpose
//
// The mono-framework follows a modular architecture where:
//   - pkg/mono: Public API entry point for external consumers
//   - pkg/types: Interface and type definitions (this package)
//   - internal/*: Implementation details
//
// By separating interfaces into this package, internal packages can implement
// interfaces defined here without creating import cycles with pkg/mono.
//
// # Import Pattern
//
// Internal packages should import this package for interface definitions:
//
//	import "github.com/go-monolith/mono/v1/pkg/types"
//
// External consumers should continue using pkg/mono, which re-exports
// all types from this package for backward compatibility:
//
//	import "github.com/go-monolith/mono/v1/pkg/mono"
//
// # Contents
//
// This package contains the following type categories:
//
//   - Framework types: MonoFramework, MonoFrameworkState, FrameworkHealth, ModuleHealth
//   - Module types: Module, DependentModule, ServiceProviderModule, and other module interfaces
//   - EventBus types: EventBus, Subscription, EventStream, Msg, Header, MsgHandler
//   - Logger types: Logger, LoggerFactory, LogLevel, LogFormat
//   - Container types: ServiceContainer, handler types, service client interfaces
//   - Middleware types: MiddlewareModule, MiddlewareChainRunner, lifecycle events
//   - JetStream types: StreamConfig, ConsumerConfig, policy enums
//
// # Zero Dependencies
//
// This package has NO dependencies on internal/* packages. It may only import:
//   - Standard library packages
//   - External dependencies (e.g., nats.go/jetstream for JetStream types)
//
// This constraint is critical for preventing import cycles.
package types
