//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// These tests cover issue #24: the NATS services protocol ($SRV.PING / INFO /
// STATS) for request-reply services, including queue wait time. They talk the
// protocol exactly as `nats micro ls / info / stats` does, over a raw
// connection, so the CLI path is proven without the binary.

func rawConn(t *testing.T, fw mono.MonoApplication, module string) *nats.Conn {
	t.Helper()
	withConn, ok := fw.EventBus(module).(types.EventBusWithConn[*nats.Conn])
	if !ok {
		t.Fatal("event bus does not expose its connection")
	}
	return withConn.Conn()
}

func TestServiceDiscovery_StatsOverTheProtocol(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithServiceDiscovery(mono.ServiceDiscoveryConfig{
		Version: "1.4.2", Description: "orders API", Metadata: map[string]string{"team": "payments"},
	}))

	gate := make(chan struct{}) // the first handler waits here until the other calls are queued
	var gateOnce sync.Once
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			<-gate
			time.Sleep(20 * time.Millisecond)
			if string(req.Data) == "fail" {
				return nil, errors.New("inventory unavailable")
			}
			return req.Data, nil
		}, mono.WithMaxConcurrency(1))
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	// Three calls, one failing. With one worker, concurrent calls queue, so
	// queue wait is measurable.
	client, err := module.container.GetRequestReplyService("check")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{}, 3)
	for _, data := range []string{"a", "fail", "c"} {
		go func(d string) {
			defer func() { done <- struct{}{} }()
			_, _ = client.Call(ctx, []byte(d))
		}(data)
	}
	// Open the gate only once both other calls are queued behind the running
	// one, and after they have waited a known minimum, so the queue wait is
	// measurable regardless of goroutine scheduling.
	waitForCount(t, func() int {
		for _, s := range fw.SubscriptionStats() {
			if s.Name == "check" {
				return s.PoolQueued
			}
		}
		return 0
	}, 2, 5*time.Second)
	time.Sleep(50 * time.Millisecond)
	gateOnce.Do(func() { close(gate) })
	for i := 0; i < 3; i++ {
		<-done
	}

	conn := rawConn(t, fw, "orders")
	reply, err := conn.Request("$SRV.STATS.orders", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("$SRV.STATS.orders: %v", err)
	}
	var stats micro.Stats
	if err := json.Unmarshal(reply.Data, &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Type != micro.StatsResponseType || stats.Name != "orders" || stats.Version != "1.4.2" || stats.Metadata["team"] != "payments" {
		t.Fatalf("stats identity = %+v", stats.ServiceIdentity)
	}
	if len(stats.Endpoints) != 1 || stats.Endpoints[0].Name != "check" || stats.Endpoints[0].Subject != "services.orders.check" {
		t.Fatalf("endpoints = %+v", stats.Endpoints)
	}
	ep := stats.Endpoints[0]
	if ep.NumRequests != 3 || ep.NumErrors != 1 || ep.LastError != "inventory unavailable" {
		t.Fatalf("endpoint stats = %+v", ep)
	}
	if ep.ProcessingTime < 60*time.Millisecond || ep.AverageProcessingTime < 20*time.Millisecond {
		t.Fatalf("processing time = %s avg %s, want at least 3x20ms", ep.ProcessingTime, ep.AverageProcessingTime)
	}
	var data struct {
		QueueWaitTime    time.Duration `json:"queue_wait_time"`
		MaxQueueWaitTime time.Duration `json:"max_queue_wait_time"`
		Expired          int64         `json:"expired"`
		Rejected         int64         `json:"rejected"`
	}
	if err := json.Unmarshal(ep.Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	// The two queued calls waited at least the 50ms gate delay.
	if data.MaxQueueWaitTime < 50*time.Millisecond || data.QueueWaitTime < data.MaxQueueWaitTime {
		t.Fatalf("queue wait = %s max %s, want at least 50ms", data.QueueWaitTime, data.MaxQueueWaitTime)
	}
	if data.Expired != 0 || data.Rejected != 0 {
		t.Fatalf("expired/rejected = %d/%d, want 0/0", data.Expired, data.Rejected)
	}

	// INFO lists the endpoint; PING identifies the service.
	reply, err = conn.Request("$SRV.INFO.orders", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO.orders: %v", err)
	}
	var info micro.Info
	if err := json.Unmarshal(reply.Data, &info); err != nil || info.Type != micro.InfoResponseType ||
		info.Description != "orders API" || len(info.Endpoints) != 1 || info.Endpoints[0].QueueGroup != "check" {
		t.Fatalf("info = %+v (%v)", info, err)
	}
	reply, err = conn.Request("$SRV.PING.orders."+stats.ID, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("$SRV.PING.orders.<id>: %v", err)
	}
	var ping micro.Ping
	if err := json.Unmarshal(reply.Data, &ping); err != nil || ping.Type != micro.PingResponseType || ping.ID != stats.ID {
		t.Fatalf("ping = %+v (%v)", ping, err)
	}
}

// TestServiceDiscovery_ListsAllModulesOnBarePing does what `nats micro ls`
// does: requests $SRV.PING without a name and collects every reply.
func TestServiceDiscovery_ListsAllModulesOnBarePing(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithServiceDiscovery())

	echo := func(_ context.Context, req *mono.Msg) ([]byte, error) { return req.Data, nil }
	orders := &testModule{name: "orders"}
	orders.registerServicesFunc = func(c mono.ServiceContainer) error { return c.RegisterRequestReplyService("check", echo) }
	billing := &testModule{name: "billing"}
	billing.registerServicesFunc = func(c mono.ServiceContainer) error { return c.RegisterRequestReplyService("charge", echo) }
	events := &testModule{name: "no-services"} // no request-reply: not exposed
	for _, m := range []mono.Module{orders, billing, events} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	conn := rawConn(t, fw, "orders")
	inbox := nats.NewInbox()
	sub, err := conn.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := conn.PublishRequest("$SRV.PING", inbox, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	found := map[string]string{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(found) < 2 {
		msg, err := sub.NextMsg(300 * time.Millisecond)
		if err != nil {
			continue
		}
		var ping micro.Ping
		if err := json.Unmarshal(msg.Data, &ping); err == nil && ping.Type == micro.PingResponseType {
			found[ping.Name] = ping.Version
		}
	}
	if found["orders"] != types.DefaultServiceDiscoveryVersion || found["billing"] != types.DefaultServiceDiscoveryVersion {
		t.Fatalf("bare PING replies = %v, want orders and billing with the default version", found)
	}
	if _, ok := found["no-services"]; ok {
		t.Fatal("a module without request-reply services must not be exposed")
	}
}

// TestServiceDiscovery_OffByDefault checks that no responder answers when
// discovery is not enabled.
func TestServiceDiscovery_OffByDefault(t *testing.T) {
	fw := newWorkerPoolApp(t)
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(c mono.ServiceContainer) error {
		return c.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) { return req.Data, nil })
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	conn := rawConn(t, fw, "orders")
	if _, err := conn.Request("$SRV.STATS.orders", nil, 500*time.Millisecond); !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("expected no responder, got %v", err)
	}
}
