package app

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/lifecycle"
	"github.com/go-monolith/mono/pkg/types"
)

// GuardLifecycle is a lifecycle manager whose Start and Stop run the given
// functions and are counted.
type GuardLifecycle struct {
	*mockLifecycleManager
	StartFunc func(ctx context.Context) error
	StopFunc  func(ctx context.Context) error
	Starts    atomic.Int32
	Stops     atomic.Int32
}

func (g *GuardLifecycle) Start(ctx context.Context) error {
	g.Starts.Add(1)
	if g.StartFunc != nil {
		return g.StartFunc(ctx)
	}
	return nil
}

func (g *GuardLifecycle) Stop(ctx context.Context) error {
	g.Stops.Add(1)
	if g.StopFunc != nil {
		return g.StopFunc(ctx)
	}
	return nil
}

// NewGuardApp returns an application in the created state on lm.
func NewGuardApp(lm *GuardLifecycle, timeout time.Duration) *frameworkApplication {
	lm.mockLifecycleManager = newMockLifecycleManager()
	return &frameworkApplication{
		logger:           &mockLogger{},
		natsManager:      &mockNATSManager{},
		lifecycleManager: lm,
		state:            types.StateCreated,
		settings:         Settings{ShutdownTimeout: timeout},
	}
}

// State returns the state of the application.
func State(fw *frameworkApplication) types.MonoFrameworkState {
	fw.mu.RLock()
	defer fw.mu.RUnlock()
	return fw.state
}

// WaitForStopBegun fails the test unless a Stop takes the application over
// (it is stopping, or has stopped already).
func WaitForStopBegun(t *testing.T, fw *frameworkApplication) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if state := State(fw); state == types.StateStopping || state == types.StateStopped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state %v, want stopping or stopped", State(fw))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStart_RejectedWhileStartingOrStopping(t *testing.T) {
	inStart := make(chan struct{})
	release := make(chan struct{})
	lm := &GuardLifecycle{StartFunc: func(context.Context) error {
		close(inStart)
		<-release
		return nil
	}}
	fw := NewGuardApp(lm, time.Second)

	first := make(chan error, 1)
	go func() { first <- fw.Start(context.Background()) }()
	<-inStart
	if err := fw.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already starting") {
		t.Errorf("Start during Start = %v, want already starting", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := lm.Starts.Load(); got != 1 {
		t.Errorf("lifecycle started %d times, want 1", got)
	}

	inStop := make(chan struct{})
	releaseStop := make(chan struct{})
	lm.StopFunc = func(context.Context) error {
		close(inStop)
		<-releaseStop
		return nil
	}
	stopped := make(chan error, 1)
	go func() { stopped <- fw.Stop(context.Background()) }()
	<-inStop
	if err := fw.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "is stopping") {
		t.Errorf("Start during Stop = %v, want is stopping", err)
	}
	close(releaseStop)
	if err := <-stopped; err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// A failed Start returns to the created state and can be retried; its context
// is released, a successful one's is kept until Stop has finished.
func TestStart_ContextLifetime(t *testing.T) {
	startErr := errors.New("database not reachable")
	var contexts []context.Context
	fail := true
	lm := &GuardLifecycle{}
	lm.StartFunc = func(ctx context.Context) error {
		contexts = append(contexts, ctx)
		if fail {
			return startErr
		}
		return nil
	}
	fw := NewGuardApp(lm, time.Second)

	if err := fw.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("Start = %v, want the lifecycle's error", err)
	}
	if State(fw) != types.StateCreated {
		t.Errorf("state %v after a failed Start, want created", State(fw))
	}
	if contexts[0].Err() == nil {
		t.Error("context of the failed Start was not released")
	}

	fail = false
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	if err := contexts[1].Err(); err != nil {
		t.Errorf("context of the successful Start ended when it returned: %v", err)
	}
	lm.StopFunc = func(context.Context) error {
		if err := contexts[1].Err(); err != nil {
			t.Errorf("start context ended before the lifecycle was stopped: %v", err)
		}
		return nil
	}
	if err := fw.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if contexts[1].Err() == nil {
		t.Error("start context still live after Stop")
	}
}

// Stop during Start aborts it, and exactly one party stops the lifecycle:
// the lifecycle's own rollback when the start failed, Start when it completed.
func TestStop_DuringStart(t *testing.T) {
	moduleErr := errors.New("module gave up")
	stopFailed := errors.New("flush failed")
	tests := []struct {
		name string
		// start is the lifecycle's Start; release is closed once Stop has
		// taken over.
		start         func(ctx context.Context, release <-chan struct{}) error
		stopErr       error
		wantStops     int32
		wantStartErrs []error
		wantStopErr   error
	}{
		{
			name: "the start rolls back on the cancelled context",
			start: func(ctx context.Context, _ <-chan struct{}) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantStops:     0,
			wantStartErrs: []error{context.Canceled},
		},
		{
			name: "the start fails with an error of its own",
			start: func(ctx context.Context, _ <-chan struct{}) error {
				<-ctx.Done()
				return moduleErr
			},
			wantStops:     0,
			wantStartErrs: []error{context.Canceled, moduleErr},
		},
		{
			name: "the start completes regardless",
			start: func(_ context.Context, release <-chan struct{}) error {
				<-release
				return nil
			},
			wantStops:     1,
			wantStartErrs: []error{context.Canceled},
		},
		{
			name: "the start completes and stopping it fails",
			start: func(_ context.Context, release <-chan struct{}) error {
				<-release
				return nil
			},
			stopErr:       stopFailed,
			wantStops:     1,
			wantStartErrs: []error{context.Canceled, stopFailed},
			wantStopErr:   stopFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inStart := make(chan struct{})
			release := make(chan struct{})
			var stopDeadline atomic.Bool
			lm := &GuardLifecycle{}
			lm.StartFunc = func(ctx context.Context) error {
				close(inStart)
				return tt.start(ctx, release)
			}
			lm.StopFunc = func(ctx context.Context) error {
				// The stop Start runs has the budget of the Stop that aborted it.
				_, has := ctx.Deadline()
				stopDeadline.Store(has && ctx.Err() == nil)
				return tt.stopErr
			}
			fw := NewGuardApp(lm, 5*time.Second)

			startErr := make(chan error, 1)
			go func() { startErr <- fw.Start(context.Background()) }()
			<-inStart
			stopErr := make(chan error, 1)
			go func() { stopErr <- fw.Stop(context.Background()) }()
			WaitForStopBegun(t, fw)
			close(release)

			gotStart := <-startErr
			for _, want := range tt.wantStartErrs {
				if !errors.Is(gotStart, want) {
					t.Errorf("Start = %v, want it to wrap %v", gotStart, want)
				}
			}
			if got := <-stopErr; !errors.Is(got, tt.wantStopErr) || (tt.wantStopErr == nil && got != nil) {
				t.Errorf("Stop = %v, want %v", got, tt.wantStopErr)
			}
			if got := lm.Stops.Load(); got != tt.wantStops {
				t.Errorf("lifecycle stopped %d times, want %d", got, tt.wantStops)
			}
			if tt.wantStops > 0 && !stopDeadline.Load() {
				t.Error("the lifecycle was stopped without a live, bounded context")
			}
			if State(fw) != types.StateStopped {
				t.Errorf("state %v, want stopped", State(fw))
			}
		})
	}
}

// The rollback budget of a Start carries the shutdown timeout, and the
// deadline of a Stop that aborts it.
func TestStop_DuringStart_HandsOverItsDeadline(t *testing.T) {
	inStart := make(chan struct{})
	rollbackDeadline := make(chan time.Time, 1)
	lm := &GuardLifecycle{}
	lm.StartFunc = func(ctx context.Context) error {
		close(inStart)
		<-ctx.Done()
		rollback, cancel := lifecycle.RollbackContext(ctx)
		defer cancel()
		deadline, _ := rollback.Deadline()
		rollbackDeadline <- deadline
		return ctx.Err()
	}
	fw := NewGuardApp(lm, time.Hour)

	go func() { _ = fw.Start(context.Background()) }()
	<-inStart
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	want, _ := stopCtx.Deadline()
	if err := fw.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := <-rollbackDeadline; !got.Equal(want) {
		t.Errorf("rollback deadline %v, want the Stop's %v", got, want)
	}
}

// A Start that does not return does not hold Stop for ever.
func TestStop_DuringStart_StartDoesNotReturn(t *testing.T) {
	inStart := make(chan struct{})
	release := make(chan struct{})
	lm := &GuardLifecycle{StartFunc: func(context.Context) error {
		close(inStart)
		<-release
		return nil
	}}
	fw := NewGuardApp(lm, time.Second)

	startErr := make(chan error, 1)
	go func() { startErr <- fw.Start(context.Background()) }()
	<-inStart

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := fw.Stop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "did not return") {
		t.Errorf("Stop = %v, want a deadline error saying the start did not return", err)
	}
	if State(fw) != types.StateStopped {
		t.Errorf("state %v, want stopped", State(fw))
	}

	close(release)
	if err := <-startErr; !errors.Is(err, context.Canceled) {
		t.Errorf("late Start = %v, want an error wrapping context.Canceled", err)
	}
	if State(fw) != types.StateStopped {
		t.Errorf("state %v after the late Start, want stopped", State(fw))
	}
	if got := lm.Stops.Load(); got != 1 {
		t.Errorf("lifecycle stopped %d times by the late Start, want 1", got)
	}
}

// A second Stop waits for the first and returns its result, or gives up with
// its own context; neither stops the lifecycle again.
func TestStop_DuringStop(t *testing.T) {
	stopFailed := errors.New("module stop failed")
	inStop := make(chan struct{})
	release := make(chan struct{})
	lm := &GuardLifecycle{StopFunc: func(context.Context) error {
		close(inStop)
		<-release
		return stopFailed
	}}
	fw := NewGuardApp(lm, 5*time.Second)
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- fw.Stop(context.Background()) }()
	<-inStop

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := fw.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "already in progress") {
		t.Errorf("impatient Stop = %v, want a deadline error saying a stop is in progress", err)
	}

	// The second Stop must be waiting before the first is let go; one that
	// came after it had finished would be told "already stopped".
	second := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		close(entered)
		second <- fw.Stop(context.Background())
	}()
	<-entered
	time.Sleep(100 * time.Millisecond)
	close(release)
	if err := <-first; !errors.Is(err, stopFailed) {
		t.Errorf("first Stop = %v, want the lifecycle's error", err)
	}
	if err := <-second; !errors.Is(err, stopFailed) {
		t.Errorf("second Stop = %v, want the result of the first", err)
	}
	if got := lm.Stops.Load(); got != 1 {
		t.Errorf("lifecycle stopped %d times, want 1", got)
	}
	if err := fw.Stop(context.Background()); err == nil || !strings.Contains(err.Error(), "already stopped") {
		t.Errorf("Stop after Stop = %v, want already stopped", err)
	}
}
