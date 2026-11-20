package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// HangingStop is a Stop that ignores its context until Release is closed.
type HangingStop struct {
	Release chan struct{}
	Calls   atomic.Int32
}

// NewHangingStop returns a HangingStop that is released when the test ends.
func NewHangingStop(t *testing.T) *HangingStop {
	t.Helper()
	h := &HangingStop{Release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-h.Release:
		default:
			close(h.Release)
		}
	})
	return h
}

// Stop blocks until released, whatever happens to ctx.
func (h *HangingStop) Stop(context.Context) error {
	h.Calls.Add(1)
	<-h.Release
	return nil
}

// HangingModule is a module whose Stop hangs.
type HangingModule struct {
	mockModule
	*HangingStop
}

// Stop hangs.
func (m *HangingModule) Stop(ctx context.Context) error { return m.HangingStop.Stop(ctx) }

// HangingMiddleware is a middleware module whose Stop hangs.
type HangingMiddleware struct {
	mockMiddlewareModule
	*HangingStop
}

// Stop hangs.
func (m *HangingMiddleware) Stop(ctx context.Context) error { return m.HangingStop.Stop(ctx) }

// HangingPlugin is a plugin whose Stop hangs.
type HangingPlugin struct {
	mockPluginModule
	*HangingStop
}

// Stop hangs.
func (m *HangingPlugin) Stop(ctx context.Context) error { return m.HangingStop.Stop(ctx) }

// stopWithin calls lm.Stop with a context of timeout and fails the test when
// it has not returned after limit.
func stopWithin(t *testing.T, lm *lifecycleManager, timeout, limit time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- lm.Stop(ctx) }()
	select {
	case err := <-result:
		return err
	case <-time.After(limit):
		t.Fatalf("Stop with a %v context still blocked after %v", timeout, limit)
		return nil
	}
}

// TestStop_AbandonsAStopThatIgnoresItsContext: one Stop that never returns
// used to hold the shutdown for good, past ShutdownTimeout, with the modules
// after it never stopped and the NATS server never stopped by the caller.
func TestStop_AbandonsAStopThatIgnoresItsContext(t *testing.T) {
	t.Run("module", func(t *testing.T) {
		lm, logger, reg := newBareManager(t, newMockEventBus())
		hanging := &HangingModule{mockModule: mockModule{name: "stuck"}, HangingStop: NewHangingStop(t)}
		// "first" depends on "stuck", so it stops before it; "last" stops after.
		first := &mockModule{name: "first", dependencies: []string{"stuck"}}
		last := &mockModule{name: "last"}
		hanging.dependencies = []string{"last"}
		for _, m := range []types.Module{last, hanging, first} {
			if err := reg.Register(m); err != nil {
				t.Fatal(err)
			}
		}
		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		err := stopWithin(t, lm, 200*time.Millisecond, 3*time.Second)
		if !errors.Is(err, monoerrors.ErrStopAbandoned) || !errors.Is(err, monoerrors.ErrModuleStopFailed) {
			t.Fatalf("Stop = %v, want ErrStopAbandoned and ErrModuleStopFailed", err)
		}
		if !strings.Contains(err.Error(), "module stuck") {
			t.Errorf("error does not name the module: %v", err)
		}
		if !first.wasStopped() || !last.wasStopped() {
			t.Errorf("modules around the stuck one stopped: first=%v last=%v, want both", first.wasStopped(), last.wasStopped())
		}
		select {
		case <-lm.shutdownCh:
		default:
			t.Error("shutdown was not signalled")
		}
		if got := lm.AbandonedStops(); len(got) != 1 || got[0] != "module stuck" {
			t.Fatalf("AbandonedStops = %v", got)
		}

		// The module's Start must not run beside its Stop.
		if err := lm.Start(context.Background()); !errors.Is(err, monoerrors.ErrStopAbandoned) || !strings.Contains(err.Error(), "module stuck") {
			t.Fatalf("Start while the Stop is still running = %v, want ErrStopAbandoned naming the module", err)
		}

		close(hanging.Release)
		eventually(t, func() bool { return len(lm.AbandonedStops()) == 0 }, "the returned Stop is still counted as abandoned")
		eventually(t, func() bool { return logger.hasWarnContaining("A Stop that was abandoned has returned") }, "the return of the abandoned Stop was not logged")
	})

	t.Run("middleware module", func(t *testing.T) {
		lm, _, reg := newBareManager(t, newMockEventBus())
		mw := &HangingMiddleware{mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "mw"}}, HangingStop: NewHangingStop(t)}
		other := &mockModule{name: "orders"}
		for _, m := range []types.Module{mw, other} {
			if err := reg.Register(m); err != nil {
				t.Fatal(err)
			}
		}
		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		err := stopWithin(t, lm, 200*time.Millisecond, 3*time.Second)
		if !errors.Is(err, monoerrors.ErrStopAbandoned) || !strings.Contains(err.Error(), "middleware module mw") {
			t.Fatalf("Stop = %v, want ErrStopAbandoned naming the middleware", err)
		}
		if !other.wasStopped() {
			t.Error("the module was not stopped")
		}
	})

	t.Run("plugin", func(t *testing.T) {
		lm, _, reg := newBareManager(t, newMockEventBus())
		plugin := &HangingPlugin{mockPluginModule: mockPluginModule{name: "store"}, HangingStop: NewHangingStop(t)}
		if err := lm.pluginRegistry.Register(plugin, "store"); err != nil {
			t.Fatal(err)
		}
		other := &mockModule{name: "orders"}
		if err := reg.Register(other); err != nil {
			t.Fatal(err)
		}
		if err := lm.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		err := stopWithin(t, lm, 200*time.Millisecond, 3*time.Second)
		if !errors.Is(err, monoerrors.ErrStopAbandoned) || !strings.Contains(err.Error(), "plugin store") {
			t.Fatalf("Stop = %v, want ErrStopAbandoned naming the plugin", err)
		}
		if !other.wasStopped() {
			t.Error("the module was not stopped")
		}
	})
}

// TestRunStop covers the outcomes of one Stop.
func TestRunStop(t *testing.T) {
	lm, _, _ := newBareManager(t, newMockEventBus())

	t.Run("a Stop that returns is reported as it is", func(t *testing.T) {
		want := errors.New("flush failed")
		panicValue, err := lm.RunStop(context.Background(), "module", "a", func(context.Context) error { return want })
		if panicValue != nil || !errors.Is(err, want) {
			t.Fatalf("RunStop = %v, %v", panicValue, err)
		}
	})

	t.Run("a panic is returned, not propagated", func(t *testing.T) {
		panicValue, err := lm.RunStop(context.Background(), "module", "a", func(context.Context) error { panic("boom") })
		if panicValue != "boom" || err != nil {
			t.Fatalf("RunStop = %v, %v", panicValue, err)
		}
	})

	t.Run("a Stop that honours its context is not abandoned", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := lm.RunStop(ctx, "module", "a", func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond) // its own cleanup after the deadline
			return ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, monoerrors.ErrStopAbandoned) {
			t.Fatalf("RunStop = %v, want the Stop's own error", err)
		}
		if got := lm.AbandonedStops(); len(got) != 0 {
			t.Fatalf("AbandonedStops = %v", got)
		}
	})

	t.Run("a context that has already ended still calls Stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		_, err := lm.RunStop(ctx, "module", "a", func(context.Context) error { called = true; return nil })
		if err != nil || !called {
			t.Fatalf("RunStop = %v, called=%v", err, called)
		}
	})

	t.Run("a Stop called after the context ended has time to finish", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		finished := false
		started := time.Now()
		_, err := lm.RunStop(ctx, "module", "slow", func(context.Context) error {
			time.Sleep(StopAbandonGrace + 200*time.Millisecond) // ignores its context, but finishes
			finished = true
			return nil
		})
		if err != nil || !finished {
			t.Fatalf("RunStop = %v, finished=%v, want the Stop run to its end", err, finished)
		}
		if waited := time.Since(started); waited >= StopMinBudget {
			t.Fatalf("waited %v for a Stop that took less", waited)
		}
		if lm.stopWaitAfterContext(true) != StopMinBudget || lm.stopWaitAfterContext(false) != StopAbandonGrace {
			t.Fatal("unexpected default waits")
		}
	})

	t.Run("the same name abandoned twice is counted until both return", func(t *testing.T) {
		lm.stopMinBudget = 50 * time.Millisecond
		defer func() { lm.stopMinBudget = 0 }()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		release := make(chan struct{})
		for i := 0; i < 2; i++ {
			if _, err := lm.RunStop(ctx, "plugin", "p", func(context.Context) error { <-release; return nil }); !errors.Is(err, monoerrors.ErrStopAbandoned) {
				t.Fatalf("RunStop = %v", err)
			}
		}
		if got := lm.AbandonedStops(); len(got) != 1 || got[0] != "plugin p" {
			t.Fatalf("AbandonedStops = %v", got)
		}
		close(release)
		eventually(t, func() bool { return len(lm.AbandonedStops()) == 0 }, "still counted after both returned")
	})
}
