package kvjetstream

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// watcherBufferSize is the channel buffer size for watcher updates.
// This provides headroom for burst traffic while preventing unbounded memory growth.
// A buffer of 256 allows ~256 updates to queue before blocking the publisher.
const watcherBufferSize = 256

// jetStreamKeyWatcher wraps jetstream.KeyWatcher to implement KeyWatcher.
type jetStreamKeyWatcher struct {
	watcher jetstream.KeyWatcher
	updates chan *KVEntry
	ctx     context.Context
	cancel  context.CancelFunc
}

// Compile-time interface check.
var _ KeyWatcher = (*jetStreamKeyWatcher)(nil)

// newJetStreamKeyWatcher creates a new wrapper for JetStream KeyWatcher.
// The goroutine is guaranteed to be running before this function returns,
// ensuring that Stop() can be safely called immediately after creation.
func newJetStreamKeyWatcher(watcher jetstream.KeyWatcher) *jetStreamKeyWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &jetStreamKeyWatcher{
		watcher: watcher,
		updates: make(chan *KVEntry, watcherBufferSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Start goroutine to convert JetStream entries to KVEntry.
	// Use a started channel to ensure the goroutine is running before returning.
	// This prevents a theoretical race condition where Stop() called immediately
	// after creation might not properly signal the goroutine.
	started := make(chan struct{})
	go func() {
		close(started) // Signal that goroutine has started
		w.processUpdates()
	}()
	<-started // Wait for goroutine to start

	return w
}

// processUpdates reads from JetStream watcher and converts entries.
// The goroutine exits when Stop() is called (via context cancellation)
// or when the underlying watcher closes its channel.
func (w *jetStreamKeyWatcher) processUpdates() {
	defer close(w.updates)

	for {
		select {
		case <-w.ctx.Done():
			// Stop() was called, exit goroutine
			return
		case entry, ok := <-w.watcher.Updates():
			if !ok {
				// Underlying channel closed
				return
			}
			if entry == nil {
				// nil signals end of initial values
				select {
				case w.updates <- nil:
				case <-w.ctx.Done():
					return
				}
				continue
			}
			// Convert and send entry
			select {
			case w.updates <- convertKVEntry(entry):
			case <-w.ctx.Done():
				return
			}
		}
	}
}

// Updates returns a channel that receives key-value updates.
func (w *jetStreamKeyWatcher) Updates() <-chan *KVEntry {
	return w.updates
}

// Stop stops the watcher and releases resources.
// It signals the processing goroutine to exit and stops the underlying watcher.
func (w *jetStreamKeyWatcher) Stop() error {
	w.cancel() // Signal goroutine to stop
	return w.watcher.Stop()
}

// convertKVEntry converts JetStream KeyValueEntry to plugin KVEntry.
func convertKVEntry(entry jetstream.KeyValueEntry) *KVEntry {
	var op KeyOperation
	switch entry.Operation() {
	case jetstream.KeyValuePut:
		op = KeyOperationPut
	case jetstream.KeyValueDelete:
		op = KeyOperationDelete
	case jetstream.KeyValuePurge:
		op = KeyOperationPurge
	}

	return &KVEntry{
		Bucket:    entry.Bucket(),
		Key:       entry.Key(),
		Value:     entry.Value(),
		Revision:  entry.Revision(),
		Timestamp: entry.Created(),
		Operation: op,
	}
}
