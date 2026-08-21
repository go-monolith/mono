package main

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono"
)

// EchoModule is a minimal module that exposes one request-reply service, so the
// example has something for a TLS client to actually call once the certificate
// is in place.
type EchoModule struct{}

// Compile-time check that EchoModule satisfies the framework's module contract.
var (
	_ mono.Module                = (*EchoModule)(nil)
	_ mono.ServiceProviderModule = (*EchoModule)(nil)
)

// NewEchoModule creates the module.
func NewEchoModule() *EchoModule {
	return &EchoModule{}
}

// Name returns the module name, which forms the first segment of its service
// subjects: services.echo.<service>.
func (m *EchoModule) Name() string {
	return "echo"
}

// RegisterServices registers the echo request-reply service.
func (m *EchoModule) RegisterServices(container mono.ServiceContainer) error {
	return container.RegisterRequestReplyService("say", func(_ context.Context, msg *mono.Msg) ([]byte, error) {
		return append([]byte("echo: "), msg.Data...), nil
	})
}

// Start is called after the module's services are registered.
func (m *EchoModule) Start(_ context.Context) error {
	fmt.Println("  → EchoModule started")
	return nil
}

// Stop is called during graceful shutdown.
func (m *EchoModule) Stop(_ context.Context) error {
	fmt.Println("  → EchoModule stopped")
	return nil
}
