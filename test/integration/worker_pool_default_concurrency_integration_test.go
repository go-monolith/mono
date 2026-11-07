//go:build integration
// +build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// TestWorkerPool_DefaultServiceConcurrencyAppliesToRequestReplyOnly checks
// the rule of issue #19 that WithDefaultServiceConcurrency raises the pool of
// request-reply services only: a queue group registered without options stays
// serial, so its handler keeps seeing messages one at a time.
func TestWorkerPool_DefaultServiceConcurrencyAppliesToRequestReplyOnly(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithDefaultServiceConcurrency(4))

	rr := &concurrencyTracker{}
	qg := &concurrencyTracker{}
	gate := make(chan struct{})
	var gateOnce sync.Once
	closeGate := func() { gateOnce.Do(func() { close(gate) }) }
	defer closeGate()
	const calls = 8
	module := &testModule{name: "defaults"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		if err := container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			rr.enter()
			<-gate // hold the handler so that overlapping calls are observed, not raced
			rr.exit(0)
			return req.Data, nil
		}); err != nil {
			return err
		}
		return container.RegisterQueueGroupService("jobs", mono.QGHP{QueueGroup: "workers", Handler: func(context.Context, *mono.Msg) error {
			qg.enter()
			time.Sleep(20 * time.Millisecond)
			qg.exit(0)
			return nil
		}})
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
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.Call(ctx, []byte("x")); err != nil {
				t.Errorf("Call: %v", err)
			}
		}()
	}
	for i := 0; i < calls; i++ {
		if err := module.eventBus.Publish("services.defaults.jobs", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	// With a default of 4 workers, at least two calls must be running at once
	// before any of them is released.
	waitForCount(t, func() int { return int(rr.running.Load()) }, 2, 10*time.Second)
	closeGate()
	wg.Wait()
	waitForCount(t, func() int { return len(qg.finished()) }, calls, 10*time.Second)

	if got := rr.max.Load(); got < 2 || got > 4 {
		t.Fatalf("request-reply peak concurrency = %d, want between 2 and the default of 4", got)
	}
	if got := qg.max.Load(); got != 1 {
		t.Fatalf("queue group peak concurrency = %d, want 1 (serial by default, the framework default applies to request-reply only)", got)
	}
}
