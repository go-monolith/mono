package kvjetstream

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// blockingKeyWatcher simulates nats.go's jetstream.KeyWatcher where a delivery
// goroutine sends entries unconditionally into a 256-buffered channel while
// subscribed, and closes Updates() once unsubscribed (Stop() called) and the
// in-flight send completes.
type blockingKeyWatcher struct {
	updates chan jetstream.KeyValueEntry
	stopCh  chan struct{}
	once    sync.Once
	done    chan struct{}
}

func newBlockingKeyWatcher() *blockingKeyWatcher {
	w := &blockingKeyWatcher{
		updates: make(chan jetstream.KeyValueEntry, 256),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	// Pre-fill the 256-entry buffer so the producer immediately blocks on an
	// unconditional send until Updates() is drained.
	for i := 0; i < cap(w.updates); i++ {
		w.updates <- nil
	}
	go func() {
		defer close(w.updates)
		defer close(w.done)
		for {
			select {
			case <-w.stopCh:
				return
			default:
			}
			// Mirror nats.go's watch callback: unconditional send blocks when
			// updates is full until Stop() drains it, then exits on stopCh.
			w.updates <- nil
		}
	}()
	return w
}

func (w *blockingKeyWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return w.updates
}

func (w *blockingKeyWatcher) Stop() error {
	w.once.Do(func() {
		close(w.stopCh)
	})
	return nil
}

type nonClosingKeyWatcher struct {
	updates chan jetstream.KeyValueEntry
}

func (w *nonClosingKeyWatcher) Updates() <-chan jetstream.KeyValueEntry {
	return w.updates
}

func (w *nonClosingKeyWatcher) Stop() error {
	return nil
}

func TestWatcherStopDrainsUpdates_JetStreamKeyWatcher(t *testing.T) {
	mock := newBlockingKeyWatcher()
	w := newJetStreamKeyWatcher(mock)
	// Give processUpdates a moment to fill w.updates so the producer blocks on mock.updates.
	time.Sleep(20 * time.Millisecond)

	if err := w.Stop(); err != nil {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}

	select {
	case <-mock.done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery goroutine leaked: Updates() was not drained on Stop()")
	}
}

func TestWatcherStopDrainsUpdates_StorageKeyWatcher(t *testing.T) {
	mock := newBlockingKeyWatcher()
	w := newStorageKeyWatcher(mock)
	// Give processUpdates a moment to fill w.updates so the producer blocks on mock.updates.
	time.Sleep(20 * time.Millisecond)

	if err := w.Stop(); err != nil {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}

	select {
	case <-mock.done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery goroutine leaked: Updates() was not drained on Stop()")
	}
}

func TestWatcherStopDoesNotBlockWhenUpdatesUnclosed(t *testing.T) {
	tests := []struct {
		name    string
		newStop func(jetstream.KeyWatcher) func() error
	}{
		{
			name: "jetStreamKeyWatcher",
			newStop: func(kw jetstream.KeyWatcher) func() error {
				return newJetStreamKeyWatcher(kw).Stop
			},
		},
		{
			name: "storageKeyWatcher",
			newStop: func(kw jetstream.KeyWatcher) func() error {
				return newStorageKeyWatcher(kw).Stop
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan jetstream.KeyValueEntry)
			t.Cleanup(func() { close(ch) })
			stop := tt.newStop(&nonClosingKeyWatcher{updates: ch})

			done := make(chan error, 1)
			go func() {
				done <- stop()
			}()

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Stop() returned unexpected error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Stop() blocked waiting for unclosed Updates() channel")
			}
		})
	}
}
