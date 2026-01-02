// Package middleware provides middleware chain execution for intercepting
// service registrations and module lifecycle events.
package middleware

import (
	"context"

	"github.com/go-monolith/mono/v1/pkg/types"
)

// Chain executes middleware modules in registration order.
//
// Each middleware receives the event/registration from the previous middleware
// and can modify it before passing to the next. The final result is used by
// the framework.
//
// Example flow:
//
//	Original Event -> Middleware1 -> Middleware2 -> Middleware3 -> Framework
//	                  (wrap handler)  (add logging)   (audit log)
type Chain struct {
	middlewares []types.MiddlewareModule
}

// NewChain creates a new middleware chain with the given modules.
// Modules are executed in the order they appear in the slice.
func NewChain(middlewares []types.MiddlewareModule) *Chain {
	return &Chain{
		middlewares: middlewares,
	}
}

// RunModuleLifecycle runs the module lifecycle event through the middleware chain.
// Each middleware receives the event and returns a (possibly modified) event.
// The final event is returned after all middleware have processed it.
func (c *Chain) RunModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	for _, mw := range c.middlewares {
		event = mw.OnModuleLifecycle(ctx, event)
	}
	return event
}

// RunServiceRegistration runs the service registration through the middleware chain.
// Each middleware receives the registration and returns a (possibly modified) registration.
// Middleware can wrap handlers or modify configuration.
// The final registration is returned after all middleware have processed it.
func (c *Chain) RunServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	for _, mw := range c.middlewares {
		reg = mw.OnServiceRegistration(ctx, reg)
	}
	return reg
}

// RunConfigurationChange runs the configuration event through the middleware chain.
// Each middleware receives the event and returns a (possibly modified) event.
// The final event is returned after all middleware have processed it.
func (c *Chain) RunConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	for _, mw := range c.middlewares {
		event = mw.OnConfigurationChange(ctx, event)
	}
	return event
}

// RunOutgoingMessage runs the outgoing message context through the middleware chain.
// Each middleware receives the context and returns a (possibly modified) context.
// Middleware can modify the message headers, data, or other fields before it's sent.
// The final context is returned after all middleware have processed it.
func (c *Chain) RunOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	for _, mw := range c.middlewares {
		octx = mw.OnOutgoingMessage(octx)
	}
	return octx
}

// RunEventConsumerRegistration runs the event consumer entry through the middleware chain.
// Each middleware receives the entry and returns a (possibly modified) entry.
// Middleware can wrap handlers or modify the entry before it's stored.
// The final entry is returned after all middleware have processed it.
func (c *Chain) RunEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	for _, mw := range c.middlewares {
		entry = mw.OnEventConsumerRegistration(ctx, entry)
	}
	return entry
}

// RunEventStreamConsumerRegistration runs the event stream consumer entry through the middleware chain.
// Each middleware receives the entry and returns a (possibly modified) entry.
// Middleware can wrap handlers or modify the entry before it's stored.
// The final entry is returned after all middleware have processed it.
func (c *Chain) RunEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	for _, mw := range c.middlewares {
		entry = mw.OnEventStreamConsumerRegistration(ctx, entry)
	}
	return entry
}

// Middlewares returns the list of middleware modules in the chain.
// Returned in registration order.
func (c *Chain) Middlewares() []types.MiddlewareModule {
	return c.middlewares
}

// Len returns the number of middleware modules in the chain.
func (c *Chain) Len() int {
	return len(c.middlewares)
}
