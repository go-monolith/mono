package main

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono"
)

// HelloModule is a simple module for demonstrating the NATS config file feature.
type HelloModule struct {
	name string
}

// Compile-time interface check
var _ mono.Module = (*HelloModule)(nil)

// NewHelloModule creates a new HelloModule instance.
func NewHelloModule() *HelloModule {
	return &HelloModule{
		name: "hello",
	}
}

// Name returns the module identifier.
func (m *HelloModule) Name() string {
	return m.name
}

// Start initializes the module.
func (m *HelloModule) Start(_ context.Context) error {
	fmt.Println("  → HelloModule started")
	return nil
}

// Stop gracefully shuts down the module.
func (m *HelloModule) Stop(_ context.Context) error {
	fmt.Println("  → HelloModule stopped")
	return nil
}
