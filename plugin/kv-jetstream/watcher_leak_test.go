package kvjetstream

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// blockingKeyWatcher simulates nats.go's jetstream.KeyWatcher where a delivery
// goroutine sends entries into a 256-buffered channel and only closes it after
// all sends complete (even after Stop() is called).
type blockingKeyWatcher struct {
	updates chan jetstream.KeyValueEntry
	stopCh  chan struct{}
	once    sync.Once
	done    chan struct{}
}

func newBlockingKeyWatcher(totalEntries int) *blockingKeyWatcher {
	w := &blockingKeyWatcher{
		updates: make(chan jetstream.KeyValueEntry, 256),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(w.updates)
		defer close(w.done)
		for i := 0; i < totalEntries; i++ {
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

func TestWatcherStopDrainsUpdates_JetStreamKeyWatcher(t *testing.T) {
	mock := newBlockingKeyWatcher(1000)
	// Give the producer goroutine a moment to fill the 256-entry buffer and block.
	time.Sleep(20 * time.Millisecond)

	w := newJetStreamKeyWatcher(mock)
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
	mock := newBlockingKeyWatcher(1000)
	time.Sleep(20 * time.Millisecond)

	w := newStorageKeyWatcher(mock)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}

	select {
	case <-mock.done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery goroutine leaked: Updates() was not drained on Stop()")
	}
}
