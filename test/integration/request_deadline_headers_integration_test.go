//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// These tests cover the wire-level rules of issue #20 that a polyglot client
// relies on: a plausible Mono-Deadline alone is honoured, a Mono-Deadline that
// disagrees with Mono-Timeout loses to the clock-independent timeout, and
// queue group handlers are never dropped.

// TestRequestDeadline_PastDeadlineHeaderIsDropped sends a request whose
// Mono-Deadline already passed by more than the skew allowance: the handler
// must not run and no reply is sent (the caller is gone).
func TestRequestDeadline_PastDeadlineHeaderIsDropped(t *testing.T) {
	fw := newWorkerPoolApp(t)
	var runs atomic.Int32
	module := &testModule{name: "deadline-drop"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			runs.Add(1)
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := module.eventBus.RequestMsgWithContext(ctx, &mono.Msg{
		Subject: "services.deadline-drop.echo",
		Data:    []byte("late"),
		Header:  mono.Header{types.HeaderDeadline: []string{strconv.FormatInt(time.Now().Add(-2*time.Second).UnixMilli(), 10)}},
	})
	if err == nil {
		t.Fatal("an expired request must get no reply")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("expected a timeout on the caller side, got %v", err)
	}
	if runs.Load() != 0 {
		t.Fatalf("handler ran %d times for an expired request, want 0", runs.Load())
	}

	// The service itself is fine.
	resp, err := module.eventBus.Request("services.deadline-drop.echo", []byte("fresh"), 5*time.Second)
	if err != nil {
		t.Fatalf("fresh request: %v", err)
	}
	if string(resp.Data) != "fresh" {
		t.Fatalf("fresh request = %q, want fresh", resp.Data)
	}
	if runs.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", runs.Load())
	}
}

// TestRequestDeadline_SkewedDeadlineFallsBackToTimeout sends a Mono-Deadline
// an hour ahead together with a Mono-Timeout of two seconds: the two disagree
// by more than the skew allowance, so the handler must get arrival+timeout,
// not the far-off absolute deadline.
func TestRequestDeadline_SkewedDeadlineFallsBackToTimeout(t *testing.T) {
	fw := newWorkerPoolApp(t)
	var seen atomic.Int64 // handler deadline, Unix milliseconds
	module := &testModule{name: "deadline-skew"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
			if dl, ok := ctx.Deadline(); ok {
				seen.Store(dl.UnixMilli())
			}
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := module.eventBus.RequestMsgWithContext(ctx, &mono.Msg{
		Subject: "services.deadline-skew.echo",
		Data:    []byte("x"),
		Header: mono.Header{
			types.HeaderDeadline: []string{strconv.FormatInt(sent.Add(time.Hour).UnixMilli(), 10)},
			types.HeaderTimeout:  []string{"2000"},
		},
	}); err != nil {
		t.Fatalf("request: %v", err)
	}
	got := time.UnixMilli(seen.Load())
	if seen.Load() == 0 {
		t.Fatal("handler context had no deadline")
	}
	if remaining := got.Sub(sent); remaining < time.Second || remaining > 3*time.Second {
		t.Fatalf("handler deadline is %s after send, want about 2s (Mono-Timeout), not the skewed Mono-Deadline", remaining)
	}
}

// TestRequestDeadline_QueueGroupHandlerIsNotDropped checks that a queue group
// message whose deadline headers already expired is still handled: the ACK
// (or the optimistic publish) promised processing, and no caller waits.
func TestRequestDeadline_QueueGroupHandlerIsNotDropped(t *testing.T) {
	fw := newWorkerPoolApp(t)
	handled := make(chan string, 1)
	module := &testModule{name: "deadline-queue"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("jobs", mono.QGHP{QueueGroup: "workers", Handler: func(ctx context.Context, msg *mono.Msg) error {
			if _, has := ctx.Deadline(); has {
				handled <- "handler got a deadline"
				return nil
			}
			handled <- string(msg.Data)
			return nil
		}})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	if err := module.eventBus.PublishMsg(&mono.Msg{
		Subject: "services.deadline-queue.jobs",
		Data:    []byte("late-job"),
		Header: mono.Header{
			types.HeaderDeadline: []string{strconv.FormatInt(time.Now().Add(-2*time.Second).UnixMilli(), 10)},
			types.HeaderTimeout:  []string{"0"},
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case got := <-handled:
		if got != "late-job" {
			t.Fatalf("queue group handler saw %q, want the message with the runtime context", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queue group message with expired deadline headers was not handled")
	}
}
