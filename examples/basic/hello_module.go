package main

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono/v1"
)

// HelloModule implements a simple module that demonstrates the basic Module interface
type HelloModule struct {
	name string
}

// Compile-time interface check
var _ mono.HealthCheckableModule = (*HelloModule)(nil)

// NewHelloModule creates a new instance of HelloModule
func NewHelloModule() *HelloModule {
	return &HelloModule{
		name: "hello-world",
	}
}

// Name returns the module identifier (required by Module interface)
func (m *HelloModule) Name() string {
	return m.name
}

// Start initializes the module (required by Module interface)
// The framework will log module start/stop events automatically
func (m *HelloModule) Start(_ context.Context) error {
	// Print directly to demonstrate the module is running
	fmt.Println("  → Hello from HelloModule! Module is now running...")
	fmt.Printf("  → Module name: %s\n", m.Name())
	return nil
}

// Stop gracefully shuts down the module (required by Module interface)
func (m *HelloModule) Stop(_ context.Context) error {
	// Print directly to demonstrate the module is stopping
	fmt.Println("  → Goodbye from HelloModule!")
	return nil
}

// Health returns the current health status (required by HealthCheckableModule interface)
// For this basic example, the module is always healthy since it has no external dependencies.
// In production modules, you should check actual resource states (DB connections, etc.)
func (m *HelloModule) Health(_ context.Context) mono.HealthStatus {
	return mono.HealthStatus{
		Healthy: true,
	}
}
