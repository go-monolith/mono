package monotest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/pkg/types"
)

// Container is the framework's real ServiceContainer bound to a module and
// backed by the in-memory EventBus, plus the dispatcher that subscribes the
// registered request-reply and queue group handlers on that bus the way the
// framework does (reply payload, Mono-Error headers on a handler error, ACK for
// queue group sends). Stream consumer and cron services are registered but not
// dispatched (no JetStream).
//
// Use NewContainer to stand in for a dependency: register stub handlers with
// StubRequestReply / StubQueueGroup (or the dependency's own contracts), hand
// the container to the Harness with WithDependency, and read Calls to assert
// what the module under test called.
type Container struct {
	types.ServiceContainer

	bus    *EventBus
	module string
	logger types.Logger

	mu             sync.Mutex
	dispatched     map[string]bool
	routersStarted bool
	subs           []types.Subscription
	calls          map[string]*atomic.Int64
}

// stubModule is the module a stand-in container is bound to.
type stubModule struct{ name string }

func (m stubModule) Name() string                { return m.name }
func (m stubModule) Start(context.Context) error { return nil }
func (m stubModule) Stop(context.Context) error  { return nil }

// NewContainer creates a container bound to a stand-in module named moduleName
// on bus. The container accepts every Register* call the real one does, so a
// test can register the dependency's services through its contracts.
func NewContainer(moduleName string, bus *EventBus) *Container {
	c, err := newContainer(stubModule{name: moduleName}, bus, newTestLogger(nil))
	if err != nil {
		// Only an empty name can fail here; make it loud at the call site.
		panic(fmt.Sprintf("monotest.NewContainer(%q): %v", moduleName, err))
	}
	return c
}

func newContainer(module types.Module, bus *EventBus, logger types.Logger) (*Container, error) {
	if bus == nil {
		return nil, fmt.Errorf("event bus cannot be nil")
	}
	inner := container.New(logger)
	if err := inner.BindModule(module); err != nil {
		return nil, err
	}
	inner.SetEventBus(bus)
	return &Container{
		ServiceContainer: inner,
		bus:              bus,
		module:           module.Name(),
		logger:           logger,
		dispatched:       make(map[string]bool),
		calls:            make(map[string]*atomic.Int64),
	}, nil
}

// EventBus returns the bus the container dispatches on.
func (c *Container) EventBus() *EventBus { return c.bus }

// ModuleName returns the name of the module the container is bound to.
func (c *Container) ModuleName() string { return c.module }

// Calls returns how many times the handler of service name has run to
// completion (request-reply and queue group services dispatched by this
// container). It is incremented after the handler returns, so once a test
// observes the count it expects, the handler's side effects are visible.
func (c *Container) Calls(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.calls[name]; ok {
		return n.Load()
	}
	return 0
}

func (c *Container) counter(name string) *atomic.Int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.calls[name]
	if !ok {
		n = &atomic.Int64{}
		c.calls[name] = n
	}
	return n
}

// Dispatch subscribes the handlers of every request-reply and queue group
// service registered so far on the bus, so that calls through the clients of
// this container (and raw requests on its subjects) reach them, and starts the
// channel routers once. It is idempotent per service and can be called again
// after more request-reply or queue group registrations (channel services must
// be registered before the first call, as in the framework); the Harness calls
// it for the module under test and for every dependency after the module
// started, which is when the framework creates subscriptions. Handlers receive
// the bus's runtime context; ctx bounds the channel routers.
func (c *Container) Dispatch(ctx context.Context) error {
	for _, entry := range c.Entries() {
		c.mu.Lock()
		done := c.dispatched[entry.Name]
		c.mu.Unlock()
		if done {
			continue
		}
		var subs []types.Subscription
		var err error
		switch entry.Type {
		case types.ServiceTypeRequestReply:
			subs, err = c.dispatchRequestReply(ctx, entry)
		case types.ServiceTypeQueueGroup:
			subs, err = c.dispatchQueueGroup(ctx, entry)
		case types.ServiceTypeChannel:
			c.mu.Lock()
			started := c.routersStarted
			c.mu.Unlock()
			if started {
				c.logger.Error("monotest: channel service registered after the routers started; it will not be routed",
					"service", entry.Name)
			}
		case types.ServiceTypeStreamConsumer, types.ServiceTypeCron:
			// Need JetStream; registered but not dispatched.
		}
		if err != nil {
			return fmt.Errorf("monotest: dispatch %s/%s: %w", c.module, entry.Name, err)
		}
		c.mu.Lock()
		c.dispatched[entry.Name] = true
		c.subs = append(c.subs, subs...)
		c.mu.Unlock()
	}
	c.mu.Lock()
	start := !c.routersStarted
	c.routersStarted = true
	c.mu.Unlock()
	if start {
		c.StartChannelRouters(ctx)
	}
	return nil
}

func (c *Container) dispatchRequestReply(_ context.Context, entry *types.ServiceEntry) ([]types.Subscription, error) {
	handler := entry.RequestHandler
	name := entry.Name
	count := c.counter(name)
	sub, err := c.bus.QueueSubscribe(entry.Subject, c.module, func(ctx context.Context, msg *types.Msg) {
		response, err := invoke(ctx, handler, msg)
		count.Add(1)
		if msg.Reply == "" {
			return
		}
		var pubErr error
		switch {
		case err != nil:
			pubErr = c.bus.PublishMsg(errorReply(msg.Reply, err))
		case response == nil:
			// The framework sends nothing for a nil response, so the caller
			// times out; do the same and say why.
			c.logger.Error("monotest: handler returned a nil response, no reply sent; in production the caller times out",
				"service", name)
		default:
			pubErr = c.bus.Publish(msg.Reply, response)
		}
		if pubErr != nil {
			c.logger.Error("monotest: failed to publish reply", "service", name, "error", pubErr)
		}
	})
	if err != nil {
		return nil, err
	}
	return []types.Subscription{sub}, nil
}

func (c *Container) dispatchQueueGroup(_ context.Context, entry *types.ServiceEntry) ([]types.Subscription, error) {
	var subs []types.Subscription
	count := c.counter(entry.Name)
	for _, pair := range entry.QueueHandlers {
		handler := pair.Handler
		sub, err := c.bus.QueueSubscribe(entry.Subject, pair.QueueGroup, func(ctx context.Context, msg *types.Msg) {
			// ACK first, as the framework does: the ACK means "accepted".
			if msg.Reply != "" {
				if err := c.bus.Publish(msg.Reply, nil); err != nil {
					c.logger.Error("monotest: failed to publish ACK", "service", entry.Name, "error", err)
					return
				}
			}
			if err := invokeQueueGroup(ctx, handler, msg); err != nil {
				c.logger.Error("monotest: queue group handler error", "service", entry.Name, "error", err)
			}
			count.Add(1)
		})
		if err != nil {
			return subs, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

// invokeQueueGroup runs a queue group handler, turning a panic into an error.
func invokeQueueGroup(ctx context.Context, handler types.QueueGroupHandler, msg *types.Msg) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &handlerPanicError{value: r}
		}
	}()
	return handler(ctx, msg)
}

// invoke runs a request-reply handler, turning a panic into an error.
func invoke(ctx context.Context, handler types.RequestReplyHandler, msg *types.Msg) (response []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &handlerPanicError{value: r}
		}
	}()
	return handler(ctx, msg)
}

type handlerPanicError struct{ value any }

func (e *handlerPanicError) Error() string { return fmt.Sprintf("handler panic: %v", e.value) }

// errorReply builds the error response the framework sends for a handler
// error: Mono-Error, Mono-Error-Message and Mono-Error-Type (the error's type
// name, lower-cased and without the Error suffix; a recovered panic maps to
// the framework's handler-panic type).
func errorReply(reply string, err error) *types.Msg {
	header := types.Header{
		types.HeaderError:        []string{"true"},
		types.HeaderErrorMessage: []string{err.Error()},
	}
	if _, isPanic := err.(*handlerPanicError); isPanic {
		header[types.HeaderErrorType] = []string{types.ErrorTypeHandlerPanic}
	} else if name := errorTypeName(err); name != "" {
		header[types.HeaderErrorType] = []string{name}
	}
	return &types.Msg{Subject: reply, Header: header}
}

func errorTypeName(err error) string {
	t := reflect.TypeOf(err)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return strings.ToLower(strings.TrimSuffix(t.Name(), "Error"))
}

// close drains the dispatcher's subscriptions, waiting up to timeout for
// handlers in flight. Services can be dispatched again afterwards.
func (c *Container) close(timeout time.Duration) {
	c.mu.Lock()
	subs := c.subs
	c.subs = nil
	c.dispatched = make(map[string]bool)
	c.mu.Unlock()
	for _, s := range subs {
		if sub, ok := s.(*subscription); ok {
			if err := sub.drain(timeout); err != nil {
				c.logger.Warn("monotest: drain failed", "subject", s.Subject(), "error", err)
			}
			continue
		}
		if err := s.Drain(); err != nil {
			c.logger.Warn("monotest: drain failed", "subject", s.Subject(), "error", err)
		}
	}
}

// StubRequestReply registers fn as the request-reply service name on the
// stand-in container c, with JSON serialization: the request is decoded into
// Req and the response encoded from Resp. The dependency's own contract
// (def.Register) can be used instead when the test has it.
func StubRequestReply[Req any, Resp any](c *Container, name string, fn func(ctx context.Context, req Req) (Resp, error)) error {
	return c.RegisterRequestReplyService(name, func(ctx context.Context, msg *types.Msg) ([]byte, error) {
		var req Req
		if len(msg.Data) > 0 {
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				return nil, fmt.Errorf("stub %s/%s: decode request: %w", c.module, name, err)
			}
		}
		resp, err := fn(ctx, req)
		if err != nil {
			return nil, err
		}
		return json.Marshal(resp)
	})
}

// StubQueueGroup registers fn as the queue group service name on the stand-in
// container c (queue group "<module>-workers"), decoding the payload as JSON.
func StubQueueGroup[T any](c *Container, name string, fn func(ctx context.Context, payload T) error) error {
	return c.RegisterQueueGroupService(name, types.QGHP{
		QueueGroup: c.module + "-workers",
		Handler: func(ctx context.Context, msg *types.Msg) error {
			var payload T
			if len(msg.Data) > 0 {
				if err := json.Unmarshal(msg.Data, &payload); err != nil {
					return fmt.Errorf("stub %s/%s: decode payload: %w", c.module, name, err)
				}
			}
			return fn(ctx, payload)
		},
	})
}
