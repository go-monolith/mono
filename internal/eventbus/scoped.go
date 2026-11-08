package eventbus

import (
	"context"
	"strings"
	"time"

	"github.com/go-monolith/mono/internal/boundary"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// scopedEventBus is the EventBus a module receives through SetEventBus. It
// wraps the shared bus and checks every subject of the form
// services.<module>.<service> against the module's declared boundaries: a
// publish or request to a module that is neither itself, a declared dependency
// nor a plugin bypasses the port (the dependency container) and is a boundary
// violation; a subscription to another module's service subject would
// intercept that module's requests and is one too. Event subjects (events.*)
// and any other subject pass through untouched.
//
// In strict mode a violation fails the call with ErrModuleBoundaryViolation;
// otherwise it is recorded, warned once, and the call goes through.
type scopedEventBus struct {
	inner types.EventBus
	rec   *boundary.Recorder
}

// scopedEventBusWithConn adds Conn() when the wrapped bus exposes its NATS
// connection, so that plugins and modules asserting
// types.EventBusWithConn[*nats.Conn] keep working on the scoped bus.
type scopedEventBusWithConn struct {
	*scopedEventBus
	conn types.EventBusWithConn[*nats.Conn]
}

// Conn returns the NATS connection of the wrapped bus.
func (s *scopedEventBusWithConn) Conn() *nats.Conn { return s.conn.Conn() }

// NewScoped wraps inner for the module whose recorder is rec. The result
// implements types.EventBusWithConn[*nats.Conn] exactly when inner does.
func NewScoped(inner types.EventBus, rec *boundary.Recorder) types.EventBus {
	scoped := &scopedEventBus{inner: inner, rec: rec}
	if withConn, ok := inner.(types.EventBusWithConn[*nats.Conn]); ok {
		return &scopedEventBusWithConn{scopedEventBus: scoped, conn: withConn}
	}
	return scoped
}

var (
	_ types.EventBus                     = (*scopedEventBus)(nil)
	_ types.EventBusWithConn[*nats.Conn] = (*scopedEventBusWithConn)(nil)
)

// target returns the module a service subject addresses, or ok=false for any
// other subject. Wildcard subjects under services.* ("services.>",
// "services.*.x") address every module and are treated as a foreign target.
func target(subject string) (module string, ok bool) {
	if !strings.HasPrefix(subject, "services.") {
		return "", false
	}
	module, _, ok = parseServiceSubject(subject)
	if !ok {
		// services.> or services.*.*.* : not one module, but still the
		// service tree; report it under the wildcard token.
		rest := strings.TrimPrefix(subject, "services.")
		if i := strings.IndexByte(rest, '.'); i >= 0 {
			rest = rest[:i]
		}
		return rest, rest != ""
	}
	return module, true
}

// checkSend validates a publish or request subject.
func (s *scopedEventBus) checkSend(subject string) error {
	module, ok := target(subject)
	if !ok || s.rec.Allowed(module) {
		return nil
	}
	return s.rec.Violation(types.BoundaryViolationUndeclaredServiceSubject, module, subject)
}

// checkSubscribe validates a subscription subject: only the module's own
// service subjects may be subscribed to.
func (s *scopedEventBus) checkSubscribe(subject string) error {
	module, ok := target(subject)
	if !ok || module == s.rec.Module() {
		return nil
	}
	return s.rec.Violation(types.BoundaryViolationForeignServiceSubscription, module, subject)
}

func (s *scopedEventBus) Publish(subject string, data []byte) error {
	if err := s.checkSend(subject); err != nil {
		return err
	}
	return s.inner.Publish(subject, data)
}

func (s *scopedEventBus) PublishMsg(msg *types.Msg) error {
	if msg != nil {
		if err := s.checkSend(msg.Subject); err != nil {
			return err
		}
	}
	return s.inner.PublishMsg(msg)
}

func (s *scopedEventBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	if err := s.checkSend(subject); err != nil {
		return nil, err
	}
	return s.inner.Request(subject, data, timeout)
}

func (s *scopedEventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	if err := s.checkSend(subject); err != nil {
		return nil, err
	}
	return s.inner.RequestWithContext(ctx, subject, data)
}

func (s *scopedEventBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	if msg != nil {
		if err := s.checkSend(msg.Subject); err != nil {
			return nil, err
		}
	}
	return s.inner.RequestMsgWithContext(ctx, msg)
}

func (s *scopedEventBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	if err := s.checkSubscribe(subject); err != nil {
		return nil, err
	}
	return s.inner.Subscribe(subject, handler)
}

func (s *scopedEventBus) SubscribeSync(subject string) (types.Subscription, error) {
	if err := s.checkSubscribe(subject); err != nil {
		return nil, err
	}
	return s.inner.SubscribeSync(subject)
}

func (s *scopedEventBus) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	if err := s.checkSubscribe(subject); err != nil {
		return nil, err
	}
	return s.inner.QueueSubscribe(subject, queue, handler)
}

func (s *scopedEventBus) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	if err := s.checkSubscribe(subject); err != nil {
		return nil, err
	}
	return s.inner.QueueSubscribeSync(subject, queue)
}

func (s *scopedEventBus) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	if err := s.checkSubscribe(subject); err != nil {
		return nil, err
	}
	return s.inner.ChanSubscribe(subject, ch)
}

func (s *scopedEventBus) EventStream() (types.EventStream, error) { return s.inner.EventStream() }

func (s *scopedEventBus) SetRuntimeContext(ctx context.Context) { s.inner.SetRuntimeContext(ctx) }
