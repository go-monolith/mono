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
// intercept that module's requests and is one too. A publish or request to a
// service subject of a declared dependency is allowed, and for a module that
// implements ServiceConsumerModule it is held to ConsumeServices() like a
// lookup on the dependency's container: counted as a use of the contract when
// the service is in the list, an undeclared-service-use violation when it is
// not (see boundary.Recorder.SubjectUse). Event subjects (events.*) and any
// other subject pass through untouched.
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
// other subject. service is the service addressed when the subject is exactly
// services.<module>.<service>, and empty otherwise. Wildcard subjects under
// services.* ("services.>", "services.*.x") address every module and are
// treated as a foreign target.
func target(subject string) (module, service string, ok bool) {
	if !strings.HasPrefix(subject, "services.") {
		return "", "", false
	}
	module, service, ok = parseServiceSubject(subject)
	if !ok {
		// services.> or services.*.*.* : not one module, but still the
		// service tree; report it under the wildcard token.
		rest := strings.TrimPrefix(subject, "services.")
		if i := strings.IndexByte(rest, '.'); i >= 0 {
			rest = rest[:i]
		}
		return rest, "", rest != ""
	}
	return module, service, true
}

// SubscriptionTarget returns the module whose service subjects a subscription
// to subject receives, or ok=false when it receives none. Beside the subjects
// under services. (see target) these are the patterns whose first token is a
// wildcard: ">" and "*.>" receive every service subject, "*.billing.charge"
// receives services.billing.charge. Checking only for the services. prefix
// let such a subscription read another module's requests, and their replies,
// under strict boundaries. The module is the one the pattern names, or "*"
// when its second token is a wildcard too.
//
// A service subject has at least three tokens (services.<module>.<service>),
// so "*" and "*.x" receive none.
func SubscriptionTarget(subject string) (module string, ok bool) {
	if module, _, ok := target(subject); ok {
		return module, true
	}
	tokens := strings.Split(subject, ".")
	switch {
	case tokens[0] == ">":
		return "*", true
	case tokens[0] != "*":
		return "", false
	case len(tokens) == 2 && tokens[1] == ">":
		return "*", true
	case len(tokens) < 3:
		return "", false
	}
	if tokens[1] == "*" || tokens[1] == ">" {
		return "*", true
	}
	return tokens[1], true
}

// checkSend validates a publish or request subject: the module addressed must
// be the module itself, a declared dependency or a plugin, and a service of a
// declared dependency must be one the module consumes (SubjectUse).
func (s *scopedEventBus) checkSend(subject string) error {
	module, service, ok := target(subject)
	if !ok {
		return nil
	}
	if !s.rec.Allowed(module) {
		return s.rec.Violation(types.BoundaryViolationUndeclaredServiceSubject, module, subject)
	}
	if service == "" {
		return nil
	}
	return s.rec.SubjectUse(module, service, subject)
}

// checkSubscribe validates a subscription subject: only the module's own
// service subjects may be subscribed to, by name or through a wildcard.
func (s *scopedEventBus) checkSubscribe(subject string) error {
	module, ok := SubscriptionTarget(subject)
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

// SetRuntimeContext replaces the context of every handler of every module,
// which is the framework's to set (it does so on the shared bus). From a
// module it is a violation: recorded and passed on, or, under strict
// boundaries, ignored, as the method has no error to return.
func (s *scopedEventBus) SetRuntimeContext(ctx context.Context) {
	if err := s.rec.Violation(types.BoundaryViolationRuntimeContextReplaced, "", ""); err != nil {
		// There is nobody to return the error to, and a module whose call
		// does nothing needs to be told why.
		s.rec.WarnIgnored(types.BoundaryViolationRuntimeContextReplaced, err)
		return
	}
	s.inner.SetRuntimeContext(ctx)
}
