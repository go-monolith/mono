package container

import (
	"context"
	"fmt"

	monoerrors "github.com/go-monolith/mono/v1/pkg/errors"
	"github.com/go-monolith/mono/v1/pkg/types"
)

// RegisterChannelService registers a bidirectional Go channel service.
//
// Channel services provide in-process communication using Go channels.
// They do not use NATS and are purely in-memory.
//
// The framework creates per-consumer out channels to prevent race conditions when
// multiple modules consume from the same service. Messages are routed based on the
// msg.Reply field:
//   - If msg.Reply is non-empty: message is sent only to the matching consumer channel
//   - If msg.Reply is empty: message is broadcast to all consumer channels (fan-out)
//
// Parameters:
//   - name: Service name (must be kebab-case)
//   - in: Input channel for sending messages to the service
//   - out: Output channel for receiving responses from the service
//
// Example:
//
//	in := make(chan *types.Msg, 10)
//	out := make(chan *types.Msg, 10)
//	err := container.RegisterChannelService("process-data", in, out)
func (c *serviceContainer) RegisterChannelService(name string, in chan *types.Msg, out chan *types.Msg) error {
	// Validate channels
	if in == nil {
		return fmt.Errorf("input channel cannot be nil for service '%s'", name)
	}
	if out == nil {
		return fmt.Errorf("output channel cannot be nil for service '%s'", name)
	}

	entry := &types.ServiceEntry{
		Type:             types.ServiceTypeChannel,
		InChannel:        in,
		OutChannel:       out,
		ConsumerChannels: make(map[string]chan *types.Msg),
	}

	return c.registerService(name, entry)
}

// routerLoop reads from the provider's out channel and routes messages to consumer channels.
//
// Routing logic:
//   - If msg.Reply is non-empty: send only to the matching consumer channel (targeted routing)
//   - If msg.Reply is empty: send to all consumer channels (fan-out/broadcast)
//
// All sends are non-blocking to prevent slow consumers from blocking the provider.
// Dropped messages are logged as warnings.
//
// The goroutine exits when either:
//   - The out channel is closed (provider stopped)
//   - The context is cancelled (framework shutdown)
func (c *serviceContainer) routerLoop(ctx context.Context, entry *types.ServiceEntry) {
	for {
		select {
		case msg, ok := <-entry.OutChannel:
			if !ok {
				// Provider closed the out channel
				return
			}

			entry.ConsumerMu.RLock()

			if msg.Reply != "" {
				// Targeted routing: send only to the specified consumer
				if ch, ok := entry.ConsumerChannels[msg.Reply]; ok {
					select {
					case ch <- msg:
					default:
						c.logger.Warn("consumer channel full, dropping message",
							"service", entry.Name, "consumer", msg.Reply)
						// TODO: Add metric increment when metrics system is available
						// c.metrics.IncrementDroppedMessages(entry.Name, msg.Reply)
					}
				} else {
					c.logger.Warn("consumer not found for reply, dropping message",
						"service", entry.Name, "reply", msg.Reply)
				}
			} else {
				// Fan-out: broadcast to all consumers
				for consumerModule, ch := range entry.ConsumerChannels {
					select {
					case ch <- msg:
					default:
						c.logger.Warn("consumer channel full, dropping message",
							"service", entry.Name, "consumer", consumerModule)
						// TODO: Add metric increment when metrics system is available
						// c.metrics.IncrementDroppedMessages(entry.Name, consumerModule)
					}
				}
			}

			entry.ConsumerMu.RUnlock()

		case <-ctx.Done():
			// Framework shutdown, stop routing
			return
		}
	}
}

// GetChannelService retrieves a channel service with a per-consumer out channel.
//
// The consumerModule parameter identifies the calling module. Each unique consumer
// receives a dedicated out channel to prevent race conditions when multiple modules
// consume from the same service. Consumer channels are created lazily on first access.
//
// Returns the shared input channel and a consumer-specific output channel.
// Returns ErrServiceNotFound if the service is not registered.
// Returns an error if the service type is not Channel.
//
// Example:
//
//	in, out, err := container.GetChannelService("process-data", "analytics-module")
//	if err != nil {
//	    return err
//	}
//	in <- &types.Msg{Data: []byte("request")}
//	response := <-out
func (c *serviceContainer) GetChannelService(serviceName string, consumerModule string) (in chan *types.Msg, out chan *types.Msg, err error) {
	c.mu.RLock()
	entry, exists := c.services[serviceName]
	c.mu.RUnlock()

	if !exists {
		if c.boundModule != nil {
			return nil, nil, monoerrors.WrapServiceNotFound(serviceName, c.boundModule.Name())
		}
		return nil, nil, monoerrors.WrapServiceNotFound(serviceName, "<unbound>")
	}

	if entry.Type != types.ServiceTypeChannel {
		return nil, nil, monoerrors.WrapServiceError(serviceName, entry.ModuleName, entry.Type,
			fmt.Errorf("service is not a Channel service (type: %s)", types.FormatServiceType(entry.Type)))
	}

	// Get or create consumer-specific out channel
	entry.ConsumerMu.Lock()
	consumerOut, exists := entry.ConsumerChannels[consumerModule]
	if !exists {
		// Create new consumer channel with same buffer size as provider's out channel
		bufferSize := cap(entry.OutChannel)
		consumerOut = make(chan *types.Msg, bufferSize)
		entry.ConsumerChannels[consumerModule] = consumerOut
	}
	entry.ConsumerMu.Unlock()

	return entry.InChannel, consumerOut, nil
}

// MustGetChannelService retrieves a channel service and panics if not found.
//
// The consumerModule parameter identifies the calling module and ensures it receives
// a dedicated out channel.
//
// This is a convenience method for cases where the service must exist.
// Use GetChannelService if you want to handle errors explicitly.
//
// Example:
//
//	in, out := container.MustGetChannelService("process-data", "analytics-module")
//	in <- &types.Msg{Data: []byte("request")}
//	response := <-out
func (c *serviceContainer) MustGetChannelService(serviceName string, consumerModule string) (in chan *types.Msg, out chan *types.Msg) {
	in, out, err := c.GetChannelService(serviceName, consumerModule)
	if err != nil {
		c.mu.RLock()
		moduleName := "<unbound>"
		if c.boundModule != nil {
			moduleName = c.boundModule.Name()
		}
		c.mu.RUnlock()
		panic(fmt.Sprintf("service '%s' not found in module '%s': %v", serviceName, moduleName, err))
	}
	return in, out
}

// StartChannelRouters starts router goroutines for all registered channel services.
//
// This is called by the lifecycle manager after all modules have started (Step 4.5).
// Router goroutines handle message routing from provider out channels to per-consumer
// channels, supporting both targeted routing (via msg.Reply) and fan-out broadcasting.
//
// The router goroutines use the provided context for lifecycle management and will
// exit when the context is cancelled (framework shutdown) or the out channel is closed.
func (c *serviceContainer) StartChannelRouters(ctx context.Context) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, entry := range c.services {
		if entry.Type == types.ServiceTypeChannel {
			go c.routerLoop(ctx, entry)
		}
	}
}
