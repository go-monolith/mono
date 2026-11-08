package monotest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// ErrJetStreamNotAvailable is returned by EventBus.EventStream: monotest
// emulates core NATS only.
var ErrJetStreamNotAvailable = errors.New("monotest: JetStream is not available on the in-memory event bus")

// ErrEventBusClosed is returned by publishes and subscriptions after Close.
var ErrEventBusClosed = errors.New("monotest: event bus is closed")

// DefaultDrainTimeout bounds how long a subscription's Drain waits for the
// handler of the message being processed to return.
const DefaultDrainTimeout = 5 * time.Second

// EventBus is an in-memory types.EventBus with core NATS semantics: subjects
// are matched token by token with the "*" and ">" wildcards, every matching
// plain subscription receives a message, subscriptions sharing a queue group
// receive it on exactly one member, and Request* deliver the reply published
// to the message's Reply subject. Each subscription gets its own copy of a
// message and receives messages in order on its own goroutine, as nats.go does.
//
// It records the subjects published and subscribed to, which the Harness and
// CheckModule read. It is safe for concurrent use.
type EventBus struct {
	mu          sync.RWMutex
	subs        []*subscription
	runtimeCtx  context.Context
	published   []string
	subscribed  []string
	inboxSeq    atomic.Int64
	roundRobin  map[string]*atomic.Uint64
	closed      bool
	handlerWait sync.WaitGroup
	panics      atomic.Int64

	// PanicHandler is called when a subscription handler panics, with the
	// subject, the panic value and the stack. The default writes them to the
	// standard logger. The subscription keeps running, as the framework's
	// bus does.
	PanicHandler func(subject string, value any, stack []byte)
}

// NewEventBus creates an empty in-memory event bus.
func NewEventBus() *EventBus {
	return &EventBus{
		runtimeCtx: context.Background(),
		roundRobin: make(map[string]*atomic.Uint64),
		PanicHandler: func(subject string, value any, stack []byte) {
			log.Printf("monotest: handler panic on %s: %v\n%s", subject, value, stack)
		},
	}
}

var _ types.EventBus = (*EventBus)(nil)

// Published returns the subjects of every Publish, PublishMsg and Request* call
// so far, in order (reply inboxes excluded).
func (b *EventBus) Published() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]string(nil), b.published...)
}

// Subscribed returns the subjects of every subscription created so far, in
// order (reply inboxes excluded).
func (b *EventBus) Subscribed() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]string(nil), b.subscribed...)
}

// Panics returns how many handler panics the bus recovered.
func (b *EventBus) Panics() int64 { return b.panics.Load() }

// Close stops every subscription, waits for in-flight handlers, and makes
// later publishes and subscriptions fail with ErrEventBusClosed.
func (b *EventBus) Close() {
	b.mu.Lock()
	b.closed = true
	subs := append([]*subscription(nil), b.subs...)
	b.subs = nil
	b.mu.Unlock()
	for _, s := range subs {
		s.stop()
	}
	b.handlerWait.Wait()
}

// --- publishing ------------------------------------------------------------

// Publish delivers data on subject to every matching subscription.
func (b *EventBus) Publish(subject string, data []byte) error {
	return b.PublishMsg(&types.Msg{Subject: subject, Data: data})
}

// PublishMsg delivers msg (subject, reply, headers and data) to every matching
// subscription. Like NATS it does not fail when nobody listens.
func (b *EventBus) PublishMsg(msg *types.Msg) error {
	if msg == nil {
		return fmt.Errorf("monotest: message cannot be nil")
	}
	if msg.Subject == "" {
		return fmt.Errorf("monotest: message subject cannot be empty")
	}
	_, err := b.deliver(msg)
	return err
}

// Request publishes data on subject with a reply inbox and waits up to timeout
// for the reply.
func (b *EventBus) Request(subject string, data []byte, timeout time.Duration) (*types.Msg, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return b.RequestWithContext(ctx, subject, data)
}

// RequestWithContext publishes data on subject with a reply inbox and waits for
// the reply until ctx ends.
func (b *EventBus) RequestWithContext(ctx context.Context, subject string, data []byte) (*types.Msg, error) {
	return b.RequestMsgWithContext(ctx, &types.Msg{Subject: subject, Data: data})
}

// RequestMsgWithContext publishes msg with a reply inbox and waits for the
// reply until ctx ends. Without any matching subscription it returns
// ErrServiceUnavailable for a services.<module>.<service> subject, mirroring
// the framework's "no responders" handling.
func (b *EventBus) RequestMsgWithContext(ctx context.Context, msg *types.Msg) (*types.Msg, error) {
	if msg == nil {
		return nil, fmt.Errorf("monotest: message cannot be nil")
	}
	if msg.Subject == "" {
		return nil, fmt.Errorf("monotest: message subject cannot be empty")
	}
	inbox := fmt.Sprintf("_INBOX.monotest.%d", b.inboxSeq.Add(1))
	sub, err := b.subscribe(inbox, "", nil, true)
	if err != nil {
		return nil, err
	}
	defer sub.stop()

	out := &types.Msg{Subject: msg.Subject, Reply: inbox, Data: msg.Data, Header: cloneHeader(msg.Header)}
	n, err := b.deliver(out)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		noResponders := errors.New("no responders available for request")
		if module, service, ok := parseServiceSubject(msg.Subject); ok {
			return nil, monoerrors.WrapServiceUnavailable(service, module, types.ServiceTypeRequestReply, noResponders)
		}
		return nil, fmt.Errorf("%w: request to subject %q failed: %w", monoerrors.ErrServiceUnavailable, msg.Subject, noResponders)
	}
	return sub.NextMsgWithContext(ctx)
}

// --- subscribing -----------------------------------------------------------

// Subscribe delivers every message on subject (wildcards allowed) to handler.
func (b *EventBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("monotest: handler cannot be nil")
	}
	return b.subscribe(subject, "", handler, false)
}

// SubscribeSync creates a subscription read with NextMsg.
func (b *EventBus) SubscribeSync(subject string) (types.Subscription, error) {
	return b.subscribe(subject, "", nil, false)
}

// QueueSubscribe delivers each message on subject to one member of queue.
func (b *EventBus) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	if handler == nil {
		return nil, fmt.Errorf("monotest: handler cannot be nil")
	}
	return b.subscribe(subject, queue, handler, false)
}

// QueueSubscribeSync creates a queue subscription read with NextMsg.
func (b *EventBus) QueueSubscribeSync(subject, queue string) (types.Subscription, error) {
	return b.subscribe(subject, queue, nil, false)
}

// ChanSubscribe delivers every message on subject to ch (blocking when full).
func (b *EventBus) ChanSubscribe(subject string, ch chan *types.Msg) (types.Subscription, error) {
	if ch == nil {
		return nil, fmt.Errorf("monotest: channel cannot be nil")
	}
	return b.subscribe(subject, "", func(_ context.Context, m *types.Msg) { ch <- m }, false)
}

// EventStream always returns ErrJetStreamNotAvailable.
func (b *EventBus) EventStream() (types.EventStream, error) {
	return nil, ErrJetStreamNotAvailable
}

// SetRuntimeContext sets the context passed to the handlers of subscriptions
// created afterwards.
func (b *EventBus) SetRuntimeContext(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx != nil {
		b.runtimeCtx = ctx
	}
}

func (b *EventBus) subscribe(subject, queue string, handler types.MsgHandler, inbox bool) (*subscription, error) {
	if subject == "" {
		return nil, fmt.Errorf("monotest: subject cannot be empty")
	}
	s := &subscription{
		bus:      b,
		subject:  subject,
		queue:    queue,
		handler:  handler,
		queueCh:  make(chan *types.Msg, 1024),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	s.valid.Store(true)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrEventBusClosed
	}
	b.subs = append(b.subs, s)
	if !inbox {
		b.subscribed = append(b.subscribed, subject)
	}
	ctx := b.runtimeCtx
	if handler != nil {
		b.handlerWait.Add(1)
	}
	b.mu.Unlock()
	if handler != nil {
		go s.run(ctx)
	} else {
		// No goroutine: a sync subscription is finished as soon as it stops.
		go func() { <-s.done; close(s.finished) }()
	}
	return s, nil
}

// deliver hands a copy of msg to every matching subscription (one per queue
// group) and returns how many received it.
func (b *EventBus) deliver(msg *types.Msg) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrEventBusClosed
	}
	if !strings.HasPrefix(msg.Subject, "_INBOX.") {
		b.published = append(b.published, msg.Subject)
	}
	var plain []*subscription
	groups := make(map[string][]*subscription)
	for _, s := range b.subs {
		if !s.valid.Load() || !subjectMatches(s.subject, msg.Subject) {
			continue
		}
		if s.queue == "" {
			plain = append(plain, s)
			continue
		}
		key := s.queue + "|" + s.subject
		groups[key] = append(groups[key], s)
	}
	targets := plain
	for key, members := range groups {
		counter, ok := b.roundRobin[key]
		if !ok {
			counter = &atomic.Uint64{}
			b.roundRobin[key] = counter
		}
		idx := (counter.Add(1) - 1) % uint64(len(members))
		targets = append(targets, members[idx])
	}
	b.mu.Unlock()

	for i, s := range targets {
		copy := msg
		if i > 0 {
			copy = &types.Msg{Subject: msg.Subject, Reply: msg.Reply, Data: msg.Data, Header: cloneHeader(msg.Header)}
		}
		s.enqueue(copy)
	}
	return len(targets), nil
}

func (b *EventBus) remove(s *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, x := range b.subs {
		if x == s {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			return
		}
	}
}

// subjectMatches reports whether a NATS subject pattern matches a concrete
// subject: "*" matches one token, ">" one or more trailing tokens.
func subjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return i < len(s)
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

func parseServiceSubject(subject string) (module, service string, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) == 3 && parts[0] == "services" {
		return parts[1], parts[2], true
	}
	return "", "", false
}

func cloneHeader(h types.Header) types.Header {
	if h == nil {
		return nil
	}
	c := make(types.Header, len(h))
	for k, v := range h {
		c[k] = append([]string(nil), v...)
	}
	return c
}

// --- subscription ----------------------------------------------------------

// subscription implements types.Subscription for the in-memory bus.
type subscription struct {
	bus       *EventBus
	subject   string
	queue     string
	handler   types.MsgHandler // nil for sync subscriptions
	queueCh   chan *types.Msg
	done      chan struct{} // closed by stop
	finished  chan struct{} // closed when the delivery goroutine has exited
	valid     atomic.Bool
	delivered atomic.Int64
	dropped   atomic.Int64
	stopOnce  sync.Once
}

var _ types.Subscription = (*subscription)(nil)

func (s *subscription) enqueue(msg *types.Msg) {
	select {
	case s.queueCh <- msg:
		s.delivered.Add(1)
	default:
		s.dropped.Add(1)
	}
}

// run delivers queued messages to the handler, in order, recovering panics
// like the framework's bus does. On stop it finishes what is queued, then exits.
func (s *subscription) run(ctx context.Context) {
	defer s.bus.handlerWait.Done()
	defer close(s.finished)
	for {
		select {
		case msg := <-s.queueCh:
			s.call(ctx, msg)
		case <-s.done:
			for {
				select {
				case msg := <-s.queueCh:
					s.call(ctx, msg)
				default:
					return
				}
			}
		}
	}
}

func (s *subscription) call(ctx context.Context, msg *types.Msg) {
	defer func() {
		if r := recover(); r != nil {
			s.bus.panics.Add(1)
			if s.bus.PanicHandler != nil {
				s.bus.PanicHandler(s.subject, r, debug.Stack())
			}
		}
	}()
	s.handler(ctx, msg)
}

func (s *subscription) stop() {
	s.stopOnce.Do(func() {
		s.valid.Store(false)
		close(s.done)
	})
}

// Unsubscribe removes the subscription; queued messages are still delivered.
func (s *subscription) Unsubscribe() error {
	s.bus.remove(s)
	s.stop()
	return nil
}

// Drain unsubscribes and waits, up to DefaultDrainTimeout, for the queued
// messages and the handler in flight to finish (see drain for a custom bound).
func (s *subscription) Drain() error { return s.drain(DefaultDrainTimeout) }

func (s *subscription) drain(timeout time.Duration) error {
	if err := s.Unsubscribe(); err != nil {
		return err
	}
	select {
	case <-s.finished:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("monotest: subscription %q still running a handler after %s", s.subject, timeout)
	}
}

func (s *subscription) IsValid() bool { return s.valid.Load() }

func (s *subscription) Subject() string { return s.subject }

func (s *subscription) Queue() string { return s.queue }

func (s *subscription) NextMsg(timeout time.Duration) (*types.Msg, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.NextMsgWithContext(ctx)
}

func (s *subscription) NextMsgWithContext(ctx context.Context) (*types.Msg, error) {
	if s.handler != nil {
		return nil, fmt.Errorf("monotest: NextMsg is only available on sync subscriptions")
	}
	select {
	case msg := <-s.queueCh:
		return msg, nil
	default:
	}
	select {
	case msg := <-s.queueCh:
		return msg, nil
	case <-s.done:
		return nil, fmt.Errorf("monotest: subscription closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SetPendingLimits implements types.PendingLimiter as a no-op.
func (s *subscription) SetPendingLimits(_, _ int) error { return nil }

// Pending implements types.SubscriptionStatsReader.
func (s *subscription) Pending() (int, int, error) { return len(s.queueCh), 0, nil }

// Dropped implements types.SubscriptionStatsReader (messages dropped because
// the subscription's buffer was full).
func (s *subscription) Dropped() (int, error) { return int(s.dropped.Load()), nil }

// Delivered implements types.SubscriptionStatsReader.
func (s *subscription) Delivered() (int64, error) { return s.delivered.Load(), nil }
