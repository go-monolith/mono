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
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// These tests cover issue #20: the caller's deadline reaches the request-reply
// handler, and a request whose deadline passed while it waited is dropped.

// TestRequestDeadline_HandlerSeesCallerDeadline checks that ctx.Deadline() in
// the handler matches the caller's deadline within the skew allowance, and
// that a nested call from the handler inherits it.
func TestRequestDeadline_HandlerSeesCallerDeadline(t *testing.T) {
	fw := newWorkerPoolApp(t)

	var handlerDeadline atomic.Value // time.Time
	var nestedDeadline atomic.Value  // time.Time seen by the nested service
	module := &testModule{name: "deadline-responder"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		if err := container.RegisterRequestReplyService("inner", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
			d, _ := ctx.Deadline()
			nestedDeadline.Store(d)
			return req.Data, nil
		}); err != nil {
			return err
		}
		return container.RegisterRequestReplyService("outer", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
			d, _ := ctx.Deadline()
			handlerDeadline.Store(d)
			// The nested call reuses the handler context, so its deadline flows down.
			inner, err := container.GetRequestReplyService("inner")
			if err != nil {
				return nil, err
			}
			resp, err := inner.Call(ctx, req.Data)
			if err != nil {
				return nil, err
			}
			return resp.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetRequestReplyService("outer")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	callerDeadline := time.Now().Add(4 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	if _, err := client.Call(ctx, []byte("x")); err != nil {
		t.Fatalf("Call: %v", err)
	}

	got, _ := handlerDeadline.Load().(time.Time)
	if got.IsZero() {
		t.Fatal("handler context had no deadline")
	}
	// The handler deadline is the caller's (the header has ms precision).
	if diff := got.Sub(callerDeadline); diff.Abs() > 20*time.Millisecond {
		t.Fatalf("handler deadline differs from the caller's by %s", diff)
	}
	nested, _ := nestedDeadline.Load().(time.Time)
	if nested.IsZero() {
		t.Fatal("nested handler context had no deadline")
	}
	// The nested call carried the same deadline: nothing accumulates per hop.
	if diff := nested.Sub(callerDeadline); diff.Abs() > 20*time.Millisecond {
		t.Fatalf("nested deadline differs from the caller's by %s", diff)
	}
}

// TestRequestDeadline_NoHeadersBehaveAsBefore checks that a request sent
// without the deadline headers (an older client, a raw NATS request) gives the
// handler a context without deadline and is answered normally.
func TestRequestDeadline_NoHeadersBehaveAsBefore(t *testing.T) {
	fw := newWorkerPoolApp(t)

	var hadDeadline atomic.Bool
	module := &testModule{name: "deadline-responder"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
			_, has := ctx.Deadline()
			hadDeadline.Store(has)
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	// Raw request on the event bus: no framework client, no headers.
	resp, err := module.eventBus.Request("services.deadline-responder.echo", []byte("raw"), 5*time.Second)
	if err != nil {
		t.Fatalf("raw request: %v", err)
	}
	if string(resp.Data) != "raw" {
		t.Fatalf("got %q", resp.Data)
	}
	if hadDeadline.Load() {
		t.Fatal("a request without deadline headers must not give the handler a deadline")
	}
}

// TestRequestDeadline_ExpiredWhileQueuedIsNotHandled is the acceptance test of
// #20: with one worker busy, a request whose caller gives up while it waits in
// the queue is never handled once the worker is free.
func TestRequestDeadline_ExpiredWhileQueuedIsNotHandled(t *testing.T) {
	fw := newWorkerPoolApp(t)

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	var handled atomic.Int32
	module := &testModule{name: "deadline-responder"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("slow", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			handled.Add(1)
			started <- struct{}{}
			if string(req.Data) == "block" {
				<-release
			}
			return req.Data, nil
		}, mono.WithMaxConcurrency(1))
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetRequestReplyService("slow")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// Occupy the single worker.
	blockerDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.Call(ctx, []byte("block"))
		blockerDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking handler did not start")
	}

	// A request with 300ms left waits in the queue until the caller gives up.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err = client.Call(ctx, []byte("impatient"))
	cancel()
	if err == nil {
		t.Fatal("the impatient call should have timed out on the caller side")
	}
	var timeoutErr *monoerrors.TimeoutError
	if !errors.Is(err, context.DeadlineExceeded) && !errors.As(err, &timeoutErr) {
		t.Fatalf("expected a timeout, got %v", err)
	}

	// Let the skew allowance (250ms) pass as well before freeing the worker.
	time.Sleep(400 * time.Millisecond)
	close(release)
	if err := <-blockerDone; err != nil {
		t.Fatalf("blocking call failed: %v", err)
	}

	// A fresh request is handled normally, proving the service is healthy.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := client.Call(ctx2, []byte("fresh")); err != nil {
		t.Fatalf("fresh call: %v", err)
	}

	// Handled: the blocker and the fresh one. The expired request never ran.
	if got := handled.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2 (the expired request must be skipped)", got)
	}
}

// TestRequestDeadline_HeadersOnTheWire checks the header names and values a
// polyglot client needs: Mono-Deadline is Unix milliseconds, Mono-Timeout is
// the remaining milliseconds.
func TestRequestDeadline_HeadersOnTheWire(t *testing.T) {
	fw := newWorkerPoolApp(t)

	var seenDeadline, seenTimeout atomic.Value
	module := &testModule{name: "deadline-responder"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("echo", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			seenDeadline.Store(req.Header.Get("Mono-Deadline"))
			seenTimeout.Store(req.Header.Get("Mono-Timeout"))
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetRequestReplyService("echo")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if _, err := client.Call(ctx, []byte("x")); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got, _ := seenDeadline.Load().(string); got != strconv.FormatInt(deadline.UnixMilli(), 10) {
		t.Fatalf("Mono-Deadline = %q, want %d", got, deadline.UnixMilli())
	}
	ms, err := strconv.Atoi(seenTimeout.Load().(string))
	if err != nil || ms <= 1000 || ms > 2000 {
		t.Fatalf("Mono-Timeout = %v, want roughly 2000", seenTimeout.Load())
	}
}
