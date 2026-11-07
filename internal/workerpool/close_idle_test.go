package workerpool

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestClose_IdlePoolWithDoneContextIsNotBusy checks that closing an idle pool
// with a context that is already done returns nil: the pool has nothing to
// wait for, so it must not be reported as still busy at shutdown.
func TestClose_IdlePoolWithDoneContextIsNotBusy(t *testing.T) {
	p := New(2, 2)
	done := make(chan struct{})
	if err := p.Submit(func() { close(done) }, true); err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-done
	// The job has returned, but its worker may still be about to release the
	// pool mutex; wait until the pool itself reports idle.
	for deadline := time.Now().Add(5 * time.Second); ; {
		if active, pending := p.Stats(); active == 0 && pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool did not become idle")
		}
		runtime.Gosched()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 50; i++ { // the old select-based Close failed this about half of the time
		if err := p.Close(ctx); err != nil {
			t.Fatalf("Close of an idle pool with a done context returned %v, want nil", err)
		}
	}
	if err := p.Submit(func() {}, false); err != ErrClosed {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
}
