// Package workerpool runs the message handlers of one NATS subscription
// concurrently, bounded by a maximum number of workers and a maximum number of
// messages waiting for a worker.
//
// nats.go dispatches an asynchronous subscription's messages to its callback
// one at a time, so a slow handler delays every later message on the same
// subscription. The lifecycle manager wraps each handler in a Pool: the
// subscription callback only hands the message over and returns, and a worker
// goroutine runs the handler.
package workerpool

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
)

// ErrOverloaded is returned by Submit when every worker is busy, the pending
// queue is full and the caller asked not to wait.
var ErrOverloaded = errors.New("worker pool overloaded")

// ErrClosed is returned by Submit once the pool has been closed.
var ErrClosed = errors.New("worker pool closed")

// Job is a unit of work run by a pool worker.
type Job func()

// Pool is a bounded worker pool. Workers are started on demand, up to the
// configured maximum, and exit when no work is pending, so an idle pool holds
// no goroutines. Pending jobs are run in submission order; with a single
// worker, jobs therefore run strictly in order.
//
// All state is guarded by one mutex, which makes the "is a worker free, is the
// queue full" decision and the worker's "is there more work or do I exit"
// decision mutually exclusive, so a job is never left in the queue with no
// worker to run it.
type Pool struct {
	mu         sync.Mutex
	cond       *sync.Cond // signalled when a queued job is taken or the pool closes
	maxWorkers int
	maxPending int
	active     int   // running workers
	pending    []Job // waiting jobs, oldest first
	closed     bool
	wg         sync.WaitGroup // tracks worker goroutines

	// PanicHandler, when set, receives the value and stack of a panic that
	// escapes a job, and the worker carries on. When nil, the panic is
	// re-raised and crashes the process, as an unrecovered panic in any
	// goroutine does. Set it before the first Submit; it is read without
	// synchronisation.
	PanicHandler func(recovered any, stack []byte)
}

// New creates a pool that runs at most maxWorkers jobs at once and queues at
// most maxPending further jobs. Values below 1 are raised to 1.
func New(maxWorkers, maxPending int) *Pool {
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if maxPending < 1 {
		maxPending = 1
	}
	p := &Pool{maxWorkers: maxWorkers, maxPending: maxPending}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// MaxWorkers returns the configured maximum number of concurrent workers.
func (p *Pool) MaxWorkers() int { return p.maxWorkers }

// MaxPending returns the configured maximum number of queued jobs.
func (p *Pool) MaxPending() int { return p.maxPending }

// Submit hands a job to the pool.
//
// If a worker is free, one is started for the job. Otherwise the job is
// queued when the queue has room. When the queue is full too, Submit returns
// ErrOverloaded if wait is false; if wait is true, it blocks until a queue
// slot frees up or the pool is closed. Submit returns ErrClosed once Close has
// been called; the job is then not run.
func (p *Pool) Submit(job Job, wait bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return ErrClosed
		}
		if p.active < p.maxWorkers {
			p.active++
			p.wg.Add(1)
			go p.worker(job)
			return nil
		}
		if len(p.pending) < p.maxPending {
			p.pending = append(p.pending, job)
			return nil
		}
		if !wait {
			return ErrOverloaded
		}
		p.cond.Wait()
	}
}

// worker runs job, then keeps taking queued jobs until the queue is empty.
func (p *Pool) worker(job Job) {
	defer p.wg.Done()
	for {
		p.run(job)

		p.mu.Lock()
		if len(p.pending) == 0 {
			p.active--
			p.mu.Unlock()
			return
		}
		job = p.pending[0]
		p.pending[0] = nil // release the reference held by the backing array
		p.pending = p.pending[1:]
		if len(p.pending) == 0 {
			p.pending = nil // let the backing array be collected
		}
		p.cond.Signal() // a queue slot freed up
		p.mu.Unlock()
	}
}

// run executes one job. A panic that escapes the job is handed to
// PanicHandler so the worker, and the pool's accounting, survive it; without a
// handler it propagates.
func (p *Pool) run(job Job) {
	defer func() {
		if r := recover(); r != nil {
			if p.PanicHandler == nil {
				panic(r)
			}
			p.PanicHandler(r, debug.Stack())
		}
	}()
	job()
}

// Stats reports the number of running workers and of queued jobs.
func (p *Pool) Stats() (active, pending int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active, len(p.pending)
}

// Close stops the pool from accepting jobs, wakes up blocked Submit calls,
// and waits for running and queued jobs to finish or for ctx to be done,
// whichever comes first. Queued jobs are still run: they were accepted. It
// returns ctx.Err() when it stops waiting early, nil otherwise. Close is
// idempotent.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	idle := p.active == 0 && len(p.pending) == 0
	p.mu.Unlock()
	// An idle pool has nothing to wait for. This is decided before the select
	// below so that a context that is already done never reports an idle
	// pool as still busy (with both channels ready, select picks at random).
	if idle {
		return nil
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
