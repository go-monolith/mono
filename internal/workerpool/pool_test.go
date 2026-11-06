package workerpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/workerpool"
)

func TestNew_ClampsBounds(t *testing.T) {
	p := workerpool.New(0, -5)
	if p.MaxWorkers() != 1 || p.MaxPending() != 1 {
		t.Fatalf("got workers=%d pending=%d, want 1/1", p.MaxWorkers(), p.MaxPending())
	}
	p = workerpool.New(8, 32)
	if p.MaxWorkers() != 8 || p.MaxPending() != 32 {
		t.Fatalf("got workers=%d pending=%d, want 8/32", p.MaxWorkers(), p.MaxPending())
	}
}

// TestSubmit_RunsConcurrently checks that one blocked job does not delay the
// others when there is more than one worker.
func TestSubmit_RunsConcurrently(t *testing.T) {
	p := workerpool.New(4, 16)
	release := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	if err := p.Submit(func() { defer wg.Done(); <-release }, false); err != nil {
		t.Fatalf("submit blocking job: %v", err)
	}

	var done atomic.Int32
	for i := 0; i < 3; i++ {
		wg.Add(1)
		if err := p.Submit(func() { defer wg.Done(); done.Add(1) }, false); err != nil {
			t.Fatalf("submit job %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for done.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if done.Load() != 3 {
		t.Fatalf("only %d of 3 fast jobs finished while one job blocks", done.Load())
	}

	close(release)
	wg.Wait()
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if active, pending := p.Stats(); active != 0 || pending != 0 {
		t.Fatalf("after close: active=%d pending=%d, want 0/0", active, pending)
	}
}

// TestSubmit_SingleWorkerKeepsOrder checks that with MaxWorkers 1 jobs run
// one at a time, in submission order.
func TestSubmit_SingleWorkerKeepsOrder(t *testing.T) {
	p := workerpool.New(1, 100)
	var (
		mu    sync.Mutex
		order []int
		run   atomic.Int32
		maxC  atomic.Int32
		wg    sync.WaitGroup
	)
	for i := 0; i < 50; i++ {
		i := i
		wg.Add(1)
		err := p.Submit(func() {
			defer wg.Done()
			c := run.Add(1)
			for {
				m := maxC.Load()
				if c <= m || maxC.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(200 * time.Microsecond)
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			run.Add(-1)
		}, false)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	wg.Wait()
	if maxC.Load() != 1 {
		t.Fatalf("max concurrency %d, want 1", maxC.Load())
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("order[%d] = %d, jobs ran out of order: %v", i, v, order)
		}
	}
}

func TestSubmit_OverloadedWhenFull(t *testing.T) {
	p := workerpool.New(1, 1)
	release := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(2)
	if err := p.Submit(func() { defer wg.Done(); <-release }, false); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := p.Submit(func() { defer wg.Done() }, false); err != nil {
		t.Fatalf("second submit (queued): %v", err)
	}
	if active, pending := p.Stats(); active != 1 || pending != 1 {
		t.Fatalf("active=%d pending=%d, want 1/1", active, pending)
	}

	err := p.Submit(func() { t.Error("overloaded job must not run") }, false)
	if !errors.Is(err, workerpool.ErrOverloaded) {
		t.Fatalf("third submit: got %v, want ErrOverloaded", err)
	}

	close(release)
	wg.Wait()

	// Once the pool drained, a new job is accepted again.
	wg.Add(1)
	if err := p.Submit(func() { wg.Done() }, false); err != nil {
		t.Fatalf("submit after drain: %v", err)
	}
	wg.Wait()
}

// TestSubmit_WaitBlocksUntilSlotFrees checks the back-pressure mode: a
// waiting Submit returns once a queued job is taken by a worker.
func TestSubmit_WaitBlocksUntilSlotFrees(t *testing.T) {
	p := workerpool.New(1, 1)
	release := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(3)
	if err := p.Submit(func() { defer wg.Done(); <-release }, true); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := p.Submit(func() { defer wg.Done() }, true); err != nil {
		t.Fatalf("second submit: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		accepted <- p.Submit(func() { defer wg.Done() }, true)
	}()

	select {
	case err := <-accepted:
		t.Fatalf("waiting submit returned %v before a slot freed", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("waiting submit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting submit did not return after a slot freed")
	}
	wg.Wait()
}

// TestClose_WakesWaitingSubmit checks that a Submit blocked in wait mode
// returns ErrClosed when the pool closes, so a shutdown cannot hang on it.
func TestClose_WakesWaitingSubmit(t *testing.T) {
	p := workerpool.New(1, 1)
	release := make(chan struct{})

	if err := p.Submit(func() { <-release }, true); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := p.Submit(func() {}, true); err != nil {
		t.Fatalf("second submit: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		result <- p.Submit(func() { t.Error("job submitted during close must not run") }, true)
	}()
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close(context.Background()) }()

	select {
	case err := <-result:
		if !errors.Is(err, workerpool.ErrClosed) {
			t.Fatalf("waiting submit: got %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting submit was not woken by Close")
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.Submit(func() {}, false); !errors.Is(err, workerpool.ErrClosed) {
		t.Fatalf("submit after close: got %v, want ErrClosed", err)
	}
}

// TestClose_WaitsForInFlightAndQueued checks that Close returns only after
// running and queued jobs finish.
func TestClose_WaitsForInFlightAndQueued(t *testing.T) {
	p := workerpool.New(1, 4)
	release := make(chan struct{})
	var finished atomic.Int32

	if err := p.Submit(func() { <-release; finished.Add(1) }, false); err != nil {
		t.Fatalf("submit: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := p.Submit(func() { finished.Add(1) }, false); err != nil {
			t.Fatalf("submit queued %d: %v", i, err)
		}
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close(context.Background()) }()

	select {
	case <-closeDone:
		t.Fatal("Close returned while a job was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if finished.Load() != 4 {
		t.Fatalf("%d jobs finished, want 4 (queued jobs must still run)", finished.Load())
	}
	// Idempotent.
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestClose_ReturnsContextErrorWhenJobStuck(t *testing.T) {
	p := workerpool.New(1, 1)
	release := make(chan struct{})
	if err := p.Submit(func() { <-release }, false); err != nil {
		t.Fatalf("submit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close with stuck job: got %v, want DeadlineExceeded", err)
	}
	close(release)
}

// TestSubmit_ConcurrentSubmitAndClose exercises the pool under the race
// detector: many goroutines submit while another closes.
func TestSubmit_ConcurrentSubmitAndClose(t *testing.T) {
	p := workerpool.New(4, 8)
	var ran atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				err := p.Submit(func() { ran.Add(1) }, i%2 == 0)
				if err != nil && !errors.Is(err, workerpool.ErrOverloaded) && !errors.Is(err, workerpool.ErrClosed) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()
	}
	time.Sleep(time.Millisecond)
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	wg.Wait()
	// Every accepted job has run by the time Close returned, and the pool is empty.
	if active, pending := p.Stats(); active != 0 || pending != 0 {
		t.Fatalf("after close: active=%d pending=%d", active, pending)
	}
}

// TestWorker_ExitsWhenIdle checks that workers do not linger after the work
// is done, so an idle pool holds no goroutines.
func TestWorker_ExitsWhenIdle(t *testing.T) {
	p := workerpool.New(8, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		if err := p.Submit(func() { wg.Done() }, false); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	wg.Wait()
	deadline := time.Now().Add(time.Second)
	for {
		active, pending := p.Stats()
		if active == 0 && pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers still active after idle: active=%d pending=%d", active, pending)
		}
		time.Sleep(time.Millisecond)
	}
}
