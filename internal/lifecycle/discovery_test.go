package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/internal/workerpool"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/micro"
)

func TestEndpointMetrics_RecordAndSnapshot(t *testing.T) {
	var m endpointMetrics
	m.record(10*time.Millisecond, 100*time.Millisecond, nil)
	m.record(30*time.Millisecond, 200*time.Millisecond, errors.New("boom"))
	m.record(20*time.Millisecond, 300*time.Millisecond, nil)

	requests, errs, lastError, processing, avgProcessing, queueWait, avgQueueWait, queueWaitMax := m.snapshot()
	if requests != 3 || errs != 1 || lastError != "boom" {
		t.Fatalf("requests=%d errors=%d last=%q", requests, errs, lastError)
	}
	if processing != 600*time.Millisecond || avgProcessing != 200*time.Millisecond {
		t.Fatalf("processing=%s avg=%s", processing, avgProcessing)
	}
	if queueWait != 60*time.Millisecond || avgQueueWait != 20*time.Millisecond || queueWaitMax != 30*time.Millisecond {
		t.Fatalf("queue wait=%s avg=%s max=%s", queueWait, avgQueueWait, queueWaitMax)
	}

	// last_error is bounded and never cut in the middle of a character.
	m.record(0, 0, errors.New(strings.Repeat("x", 2*maxLastErrorLen)))
	if _, _, last, _, _, _, _, _ := m.snapshot(); len(last) != maxLastErrorLen {
		t.Fatalf("last_error length = %d, want %d", len(last), maxLastErrorLen)
	}
	m.record(0, 0, errors.New(strings.Repeat("x", maxLastErrorLen-1)+"é"))
	if _, _, last, _, _, _, _, _ := m.snapshot(); !utf8.ValidString(last) || strings.HasSuffix(last, "é") {
		t.Fatalf("last_error should drop the split character, got %q", last[len(last)-4:])
	}

	// Expired requests are tracked apart from handler runs.
	m.recordExpired(2 * time.Second)
	m.recordExpired(time.Second)
	if requests, _, _, _, _, _, _, _ := m.snapshot(); requests != 5 {
		t.Fatalf("expired requests must not count as requests, got %d", requests)
	}
	if got := time.Duration(m.expiredQueueWaitMaxNs.Load()); got != 2*time.Second {
		t.Fatalf("expired queue wait max = %s, want 2s", got)
	}

	var empty endpointMetrics
	if _, _, _, _, avg, _, avgQW, _ := empty.snapshot(); avg != 0 || avgQW != 0 {
		t.Fatal("averages of no requests must be zero, not a division by zero")
	}
}

func TestNewDiscoveryService(t *testing.T) {
	t.Run("defaults and sorted endpoints", func(t *testing.T) {
		svc, err := newDiscoveryService("orders", types.ServiceDiscoveryConfig{}, []*discoveryEndpoint{
			{name: "zeta", subject: "services.orders.zeta", queueGroup: "zeta"},
			{name: "alpha", subject: "services.orders.alpha", queueGroup: "alpha"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if svc.identity.Name != "orders" || svc.identity.Version != types.DefaultServiceDiscoveryVersion || svc.identity.ID == "" {
			t.Fatalf("identity = %+v", svc.identity)
		}
		if raw, _ := json.Marshal(svc.ping()); !strings.Contains(string(raw), `"metadata":{}`) {
			t.Fatalf("metadata must be {} on the wire like micro, got %s", raw)
		}
		if svc.endpoints[0].name != "alpha" || svc.endpoints[1].name != "zeta" {
			t.Fatal("endpoints should be sorted by name")
		}
	})
	t.Run("invalid module name", func(t *testing.T) {
		if _, err := newDiscoveryService("bad.name", types.ServiceDiscoveryConfig{}, nil); err == nil {
			t.Fatal("a module name with a dot cannot be a protocol service name")
		}
	})
	t.Run("control subjects", func(t *testing.T) {
		svc, _ := newDiscoveryService("orders", types.ServiceDiscoveryConfig{Version: "1.2.3"}, nil)
		subjects, err := svc.controlSubjects()
		if err != nil {
			t.Fatal(err)
		}
		if len(subjects) != 9 {
			t.Fatalf("got %d control subjects, want 9", len(subjects))
		}
		for _, want := range []string{"$SRV.PING", "$SRV.PING.orders", "$SRV.PING.orders." + svc.identity.ID,
			"$SRV.INFO", "$SRV.INFO.orders", "$SRV.STATS", "$SRV.STATS.orders", "$SRV.STATS.orders." + svc.identity.ID} {
			if _, ok := subjects[want]; !ok {
				t.Fatalf("missing control subject %s", want)
			}
		}
	})
}

func TestDiscoveryService_Responses(t *testing.T) {
	pool := workerpool.New(2, 4)
	reporter := newOverloadReporter(&mockLogger{}, "service", "check")
	reporter.reject(pool)
	reporter.expire(pool)
	metrics := &endpointMetrics{}
	metrics.record(5*time.Millisecond, 50*time.Millisecond, nil)
	metrics.record(15*time.Millisecond, 70*time.Millisecond, errors.New("failed"))
	record := &subscriptionRecord{
		kind: "request_reply", module: "orders", name: "check", subject: "services.orders.check", queueGroup: "check",
		sub: &limitedSubscription{pendingMsgs: 3}, pool: pool, reporter: reporter, metrics: metrics,
	}
	svc, err := newDiscoveryService("orders", types.ServiceDiscoveryConfig{
		Version: "2.0.1", Description: "orders API", Metadata: map[string]string{"team": "payments"},
	}, []*discoveryEndpoint{{name: "check", subject: "services.orders.check", queueGroup: "check", record: record}})
	if err != nil {
		t.Fatal(err)
	}

	ping := svc.ping()
	if ping.Type != micro.PingResponseType || ping.Name != "orders" || ping.Version != "2.0.1" || ping.Metadata["team"] != "payments" {
		t.Fatalf("ping = %+v", ping)
	}

	info := svc.info()
	if info.Type != micro.InfoResponseType || info.Description != "orders API" || len(info.Endpoints) != 1 ||
		info.Endpoints[0].Subject != "services.orders.check" || info.Endpoints[0].QueueGroup != "check" {
		t.Fatalf("info = %+v", info)
	}

	stats := svc.stats()
	if stats.Type != micro.StatsResponseType || stats.Started.IsZero() || len(stats.Endpoints) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	ep := stats.Endpoints[0]
	if ep.NumRequests != 2 || ep.NumErrors != 1 || ep.LastError != "failed" ||
		ep.ProcessingTime != 120*time.Millisecond || ep.AverageProcessingTime != 60*time.Millisecond {
		t.Fatalf("endpoint stats = %+v", ep)
	}
	var data EndpointData
	if err := json.Unmarshal(ep.Data, &data); err != nil {
		t.Fatalf("data: %v", err)
	}
	if data.QueueWaitTime != 20*time.Millisecond || data.AverageQueueWaitTime != 10*time.Millisecond || data.MaxQueueWaitTime != 15*time.Millisecond {
		t.Fatalf("queue wait data = %+v", data)
	}
	if data.Expired != 1 || data.Rejected != 1 || data.PendingMsgs != 3 {
		t.Fatalf("counters = %+v", data)
	}

	// The JSON wire form round-trips through the micro types a CLI would use.
	raw, _ := json.Marshal(stats)
	var decoded micro.Stats
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.Endpoints[0].NumRequests != 2 {
		t.Fatalf("round trip: %v %+v", err, decoded)
	}
}

// TestServiceDiscovery_SubscribesAndAnswers starts a manager with discovery
// enabled on the mock event bus and checks the control subjects are
// subscribed per module and answered with protocol responses.
func TestServiceDiscovery_SubscribesAndAnswers(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)
	lm.SetServiceDiscovery(&types.ServiceDiscoveryConfig{Version: "3.1.0"})

	module := &mockModuleWithServices{
		mockModule: mockModule{name: "orders"},
		serviceEntries: []*types.ServiceEntry{
			{Name: "check", Type: types.ServiceTypeRequestReply, Subject: "services.orders.check", QueueGroup: "check",
				RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { return msg.Data, nil }},
			{Name: "jobs", Type: types.ServiceTypeQueueGroup, Subject: "services.orders.jobs",
				QueueHandlers: []types.QGHP{{QueueGroup: "workers", Handler: func(context.Context, *types.Msg) error { return nil }}}},
		},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	for _, subject := range []string{"$SRV.PING", "$SRV.PING.orders", "$SRV.INFO.orders", "$SRV.STATS.orders"} {
		if len(eventBus.getHandlers(subject)) != 1 {
			t.Fatalf("expected a responder on %s", subject)
		}
	}

	// A request to the module's STATS subject is answered on the reply subject
	// with a stats response listing only the request-reply endpoint.
	handler := eventBus.getHandlers("$SRV.STATS.orders")[0]
	handler(context.Background(), &types.Msg{Subject: "$SRV.STATS.orders", Reply: "stats-reply"})
	replies := eventBus.getPublishedMessages("stats-reply")
	if len(replies) != 1 {
		t.Fatalf("got %d stats replies, want 1", len(replies))
	}
	var stats micro.Stats
	if err := json.Unmarshal(replies[0], &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Name != "orders" || stats.Version != "3.1.0" || len(stats.Endpoints) != 1 || stats.Endpoints[0].Name != "check" {
		t.Fatalf("stats = %+v", stats)
	}

	// A request without a reply subject is ignored.
	handler(context.Background(), &types.Msg{Subject: "$SRV.STATS.orders"})
	if len(eventBus.getPublishedMessages("stats-reply")) != 1 {
		t.Fatal("a request without reply subject must not be answered")
	}

	// Discovery subscriptions are tracked for drain with the module's own.
	impl := lm.(*lifecycleManager)
	impl.mu.RLock()
	n := len(impl.subscriptions["orders"])
	impl.mu.RUnlock()
	if n != 2+9 { // request-reply + queue group + nine control subjects
		t.Fatalf("tracked subscriptions for orders = %d, want 11", n)
	}
}

func TestServiceDiscovery_DisabledByDefault(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "orders"},
		serviceEntries: []*types.ServiceEntry{{Name: "check", Type: types.ServiceTypeRequestReply, Subject: "services.orders.check", QueueGroup: "check",
			RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { return msg.Data, nil }}},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()
	if len(eventBus.getHandlers("$SRV.PING")) != 0 {
		t.Fatal("no discovery responders expected when discovery is off")
	}
}

func TestServiceDiscovery_InvalidModuleNameFailsSetup(t *testing.T) {
	// Module names are kebab-case by registration, so a name the protocol
	// cannot represent cannot normally reach setup; the guard is exercised on
	// a hand-made record.
	logger := &mockLogger{}
	lm := &lifecycleManager{logger: logger, eventBus: newMockEventBus(), subscriptions: make(map[string][]types.Subscription)}
	lm.SetServiceDiscovery(&types.ServiceDiscoveryConfig{})
	lm.subRecords = []*subscriptionRecord{{kind: "request_reply", module: "orders.v2", name: "check", subject: "services.orders.v2.check", queueGroup: "check"}}
	err := lm.setupServiceDiscovery()
	if err == nil || !strings.Contains(err.Error(), "cannot be a service name") {
		t.Fatalf("expected an error about the module name, got %v", err)
	}
}
