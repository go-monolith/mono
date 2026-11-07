//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// TestServiceDiscovery_StatsCountErrorsRejectedAndExpired drives the three
// non-happy outcomes of a request-reply service through the protocol stats
// (issue #24): a recovered panic counts as an error with its text in
// last_error, an overloaded request is counted under rejected (it never ran),
// and an expired request under expired. The bare $SRV.INFO and $SRV.STATS
// subjects are answered as well.
func TestServiceDiscovery_StatsCountErrorsRejectedAndExpired(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithServiceDiscovery())
	gate := make(chan struct{})
	var gateOnce sync.Once
	closeGate := func() { gateOnce.Do(func() { close(gate) }) }
	defer closeGate() // a failing assertion must not leave handlers blocked into Stop
	started := make(chan struct{}, 8)
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			switch string(req.Data) {
			case "panic":
				panic("inventory exploded")
			case "block":
				started <- struct{}{}
				<-gate
			}
			return req.Data, nil
		}, mono.WithMaxConcurrency(1), mono.WithMaxPending(1))
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetRequestReplyService("check")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. A panic: one run, one error.
	if _, err := client.Call(ctx, []byte("panic")); !errors.Is(err, monoerrors.ErrHandlerPanic) {
		t.Fatalf("expected ErrHandlerPanic, got %v", err)
	}

	// 2. Overload: one call runs, one waits, the third is rejected.
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := client.Call(ctx, []byte("block"))
			results <- err
		}()
	}
	<-started
	waitForCount(t, func() int {
		for _, s := range fw.SubscriptionStats() {
			if s.Name == "check" {
				return s.PoolQueued
			}
		}
		return 0
	}, 1, 5*time.Second)
	if _, err := client.Call(ctx, []byte("rejected")); !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("expected ErrServiceOverloaded, got %v", err)
	}
	closeGate()
	<-started
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("blocked call: %v", err)
		}
	}

	// 3. Expired: a plausible Mono-Deadline already in the past.
	conn := rawConn(t, fw, "orders")
	_, err = conn.RequestMsg(&nats.Msg{
		Subject: "services.orders.check",
		Data:    []byte("late"),
		Header:  nats.Header{types.HeaderDeadline: []string{strconv.FormatInt(time.Now().Add(-2*time.Second).UnixMilli(), 10)}},
	}, 500*time.Millisecond)
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("expected no reply for an expired request, got %v", err)
	}

	stats := requestStats(t, conn, "$SRV.STATS") // the bare subject answers too
	if stats.Name != "orders" || len(stats.Endpoints) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	ep := stats.Endpoints[0]
	if ep.NumRequests != 3 || ep.NumErrors != 1 || !strings.Contains(ep.LastError, "inventory exploded") {
		t.Fatalf("endpoint = requests %d errors %d last_error %q, want 3/1/the panic text", ep.NumRequests, ep.NumErrors, ep.LastError)
	}
	var data struct {
		Expired    int64 `json:"expired"`
		Rejected   int64 `json:"rejected"`
		PoolActive int   `json:"pool_active"`
		PoolQueued int   `json:"pool_queued"`
	}
	if err := json.Unmarshal(ep.Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if data.Expired != 1 || data.Rejected != 1 {
		t.Fatalf("data = %+v, want expired 1 and rejected 1", data)
	}
	if data.PoolActive != 0 || data.PoolQueued != 0 {
		t.Fatalf("pool should be idle, got active %d queued %d", data.PoolActive, data.PoolQueued)
	}

	reply, err := conn.Request("$SRV.INFO", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("$SRV.INFO: %v", err)
	}
	var info micro.Info
	if err := json.Unmarshal(reply.Data, &info); err != nil || info.Type != micro.InfoResponseType || info.Name != "orders" {
		t.Fatalf("bare info = %+v (%v)", info, err)
	}
}

// requestStats requests subject and decodes the micro stats response.
func requestStats(t *testing.T, conn *nats.Conn, subject string) micro.Stats {
	t.Helper()
	reply, err := conn.Request(subject, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	var stats micro.Stats
	if err := json.Unmarshal(reply.Data, &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Type != micro.StatsResponseType {
		t.Fatalf("type = %q", stats.Type)
	}
	return stats
}
