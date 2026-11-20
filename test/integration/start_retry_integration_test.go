//go:build integration
// +build integration

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/middleware/accesslog"
	"github.com/go-monolith/mono/middleware/audit"
	"github.com/go-monolith/mono/pkg/helper"
)

// retryProductAdded is the event the retried-start test emits and consumes.
var retryProductAdded = helper.EventDefinition[OrderEventPayload]("catalog", "ProductAdded", "v1")

// retryCatalog emits an event and provides a channel service that echoes.
type retryCatalog struct {
	bus     mono.EventBus
	in, out chan *mono.Msg
	stop    chan struct{}
}

func (m *retryCatalog) Name() string                  { return "catalog" }
func (m *retryCatalog) SetEventBus(bus mono.EventBus) { m.bus = bus }
func (m *retryCatalog) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{retryProductAdded.ToBase()}
}
func (m *retryCatalog) RegisterServices(c mono.ServiceContainer) error {
	return c.RegisterChannelService("echo", m.in, m.out)
}
func (m *retryCatalog) Start(context.Context) error {
	m.stop = make(chan struct{})
	go func(stop chan struct{}) {
		for {
			select {
			case <-stop:
				return
			case msg := <-m.in:
				select {
				case m.out <- &mono.Msg{Reply: msg.Reply, Data: msg.Data}:
				case <-stop:
					return
				}
			}
		}
	}(m.stop)
	return nil
}
func (m *retryCatalog) Stop(context.Context) error { close(m.stop); return nil }

// retryOrders consumes the event (core NATS and JetStream), the echo service
// and the audit trail, and provides a request-reply service and a stream
// consumer service whose stream configuration is rejected at subscription
// setup while failSetup is set.
type retryOrders struct {
	failSetup atomic.Bool

	events, streamEvents, starts, stops atomic.Int64
	echoIn, echoOut                     chan *mono.Msg
	adapter                             audit.AuditAdapterPort
	depErr                              error
}

func (m *retryOrders) Name() string           { return "orders" }
func (m *retryOrders) Dependencies() []string { return []string{"catalog", "audit"} }
func (m *retryOrders) Start(context.Context) error {
	m.starts.Add(1)
	return m.depErr
}
func (m *retryOrders) Stop(context.Context) error { m.stops.Add(1); return nil }
func (m *retryOrders) SetDependencyServiceContainer(dep string, c mono.ServiceContainer) {
	switch dep {
	case "catalog":
		m.echoIn, m.echoOut, m.depErr = c.GetChannelService("echo", m.Name())
	case "audit":
		m.adapter, m.depErr = audit.NewAuditAdapter(c, m.Name())
	}
}
func (m *retryOrders) RegisterServices(c mono.ServiceContainer) error {
	if err := c.RegisterRequestReplyService("ping", func(context.Context, *mono.Msg) ([]byte, error) {
		return []byte("pong"), nil
	}); err != nil {
		return err
	}
	// A stream that does not exist cannot be created sealed: the server
	// rejects the write, at subscription setup, after every module started.
	return c.RegisterStreamConsumerService("ingest", mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{Name: "RETRY_INGEST", Sealed: m.failSetup.Load()},
		Fetch:  mono.FetchConfig{BatchSize: 1, Timeout: time.Second},
	}, func(_ context.Context, msgs []*mono.Msg) error {
		for _, msg := range msgs {
			_ = msg.Ack()
		}
		return nil
	})
}
func (m *retryOrders) RegisterEventConsumers(registry mono.EventRegistry) error {
	if err := registry.RegisterEventConsumer(retryProductAdded.ToBase(), func(context.Context, *mono.Msg) error {
		m.events.Add(1)
		return nil
	}, m); err != nil {
		return err
	}
	return registry.RegisterEventStreamConsumer(retryProductAdded.ToBase(), mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{Name: "RETRY_EVENTS"},
		Fetch:  mono.FetchConfig{BatchSize: 1, Timeout: time.Second},
	}, func(_ context.Context, msgs []*mono.Msg) error {
		for _, msg := range msgs {
			m.streamEvents.Add(1)
			_ = msg.Ack()
		}
		return nil
	}, m)
}

// TestIntegration_RetriedStart: after a Start that failed at subscription
// setup (every module, middleware and channel router was started and is
// rolled back), a second Start of the same application works as a first one:
// events are registered once and delivered once, channel services answer,
// and the audit and access log, on files, are written again.
func TestIntegration_RetriedStart(t *testing.T) {
	dir := t.TempDir()
	auditPath, accessPath := filepath.Join(dir, "audit.log"), filepath.Join(dir, "access.log")
	open := func(path string) *os.File {
		t.Helper()
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	auditModule, err := audit.New(audit.WithOutput(open(auditPath)), audit.WithHashChaining(""))
	if err != nil {
		t.Fatal(err)
	}
	accessModule, err := accesslog.New(accesslog.WithOutput(open(accessPath)))
	if err != nil {
		t.Fatal(err)
	}
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(filepath.Join(dir, "js")),
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	catalog := &retryCatalog{in: make(chan *mono.Msg, 8), out: make(chan *mono.Msg, 8)}
	orders := &retryOrders{}
	orders.failSetup.Store(true)
	for _, m := range []mono.Module{auditModule, accessModule, catalog, orders} {
		if err := app.Register(m); err != nil {
			t.Fatalf("Register %s: %v", m.Name(), err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two failed starts in a row, then one that works.
	for attempt := 1; attempt <= 2; attempt++ {
		err := app.Start(ctx)
		if err == nil {
			t.Fatalf("Start #%d succeeded with a stream configuration the server rejects", attempt)
		}
		if strings.Contains(err.Error(), "already") {
			t.Fatalf("Start #%d failed for what an earlier start left behind: %v", attempt, err)
		}
		if got := orders.stops.Load(); got != int64(attempt) {
			t.Fatalf("after failed Start #%d the module was stopped %d times", attempt, got)
		}
	}
	orders.failSetup.Store(false)
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start after two failed ones: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	// One published event is handled once by each consumer (the stream
	// stores what is published on its subject).
	if err := retryProductAdded.Publish(catalog.bus, OrderEventPayload{OrderID: "p1"}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitWithin(t, 10*time.Second, func() bool { return orders.events.Load() >= 1 }, "the event consumer got no event")
	waitWithin(t, 10*time.Second, func() bool { return orders.streamEvents.Load() >= 1 }, "the event stream consumer got no event")
	time.Sleep(time.Second)
	if got := orders.events.Load(); got != 1 {
		t.Errorf("the event consumer handled one event %d times (a subscription per start?)", got)
	}
	if got := orders.streamEvents.Load(); got != 1 {
		t.Errorf("the event stream consumer handled one event %d times", got)
	}
	es, err := app.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	stream, err := es.Stream(ctx, "RETRY_EVENTS")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var durables []string
	names := stream.ConsumerNames(ctx)
	for name := range names.Name() {
		durables = append(durables, name)
	}
	if len(durables) != 1 || durables[0] != "orders-catalog-ProductAdded-v1" {
		t.Errorf("durables of the event stream = %v, want the one of the registration", durables)
	}

	// The channel service answers: one router, on the containers of this start.
	for i := 0; i < 5; i++ {
		select {
		case orders.echoIn <- &mono.Msg{Reply: "orders", Data: []byte("hello")}:
		case <-time.After(2 * time.Second):
			t.Fatalf("echo request #%d not accepted", i)
		}
		select {
		case reply := <-orders.echoOut:
			if string(reply.Data) != "hello" {
				t.Fatalf("echo reply = %q", reply.Data)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("echo reply #%d did not arrive (a router of the failed start took it?)", i)
		}
	}

	// The audit trail and the access log are written.
	if n, err := orders.adapter.SaveAuditTrail(ctx, []audit.Entry{{ModuleName: "orders", Details: map[string]any{"after": "retry"}}}); n != 1 || err != nil {
		t.Fatalf("SaveAuditTrail = %d, %v", n, err)
	}
	client, err := app.Services("orders").GetRequestReplyService("ping")
	if err != nil {
		t.Fatalf("GetRequestReplyService: %v", err)
	}
	if _, err := client.Call(ctx, []byte("x")); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	access, err := os.ReadFile(accessPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(access), "service=ping") {
		t.Errorf("the access log has no line of the call after the retry:\n%s", access)
	}
	af, err := os.Open(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = af.Close() }()
	var entries []audit.Entry
	custom, started := 0, 0
	sc := bufio.NewScanner(af)
	for sc.Scan() {
		var e audit.Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("decode %q: %v", sc.Text(), err)
		}
		entries = append(entries, e)
		switch {
		case e.EventType == audit.EventCustomAuditTrail:
			custom++
		case e.EventType == audit.EventModuleStarted && e.ModuleName == "orders":
			started++
		}
	}
	if custom != 1 || started != 3 {
		t.Errorf("audit log: %d custom entries and %d starts of the module, want 1 and 3", custom, started)
	}
	if err := audit.VerifyChain(entries); err != nil {
		t.Errorf("audit chain across the three starts: %v", err)
	}
}

// TestIntegration_FailedStartThenStop: the rollback of a failed Start leaves
// the audit and access log open for a retry. When the application is stopped
// instead, both are closed, as they are after a run.
func TestIntegration_FailedStartThenStop(t *testing.T) {
	dir := t.TempDir()
	auditFile, err := os.OpenFile(filepath.Join(dir, "audit.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	accessFile, err := os.OpenFile(filepath.Join(dir, "access.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	auditModule, err := audit.New(audit.WithOutput(auditFile))
	if err != nil {
		t.Fatal(err)
	}
	accessModule, err := accesslog.New(accesslog.WithOutput(accessFile))
	if err != nil {
		t.Fatal(err)
	}
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
		mono.WithJetStreamStorageDir(filepath.Join(dir, "js")),
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	orders := &retryOrders{}
	orders.failSetup.Store(true)
	for _, m := range []mono.Module{auditModule, accessModule, &retryCatalog{in: make(chan *mono.Msg, 8), out: make(chan *mono.Msg, 8)}, orders} {
		if err := app.Register(m); err != nil {
			t.Fatalf("Register %s: %v", m.Name(), err)
		}
	}
	ctx := context.Background()
	if err := app.Start(ctx); err == nil {
		t.Fatal("Start succeeded with a stream configuration the server rejects")
	}
	// Still open: a retry could write.
	for name, f := range map[string]*os.File{"audit": auditFile, "access": accessFile} {
		if _, err := f.Write(nil); err != nil {
			t.Fatalf("the %s log is closed after the rollback: %v", name, err)
		}
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	for name, f := range map[string]*os.File{"audit": auditFile, "access": accessFile} {
		if _, err := f.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
			t.Errorf("the %s log is still open after Stop: write = %v", name, err)
		}
	}
}
