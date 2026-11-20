package kvjetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamKVBackend implements storage.Storage and extended interfaces
// using JetStream KeyValue Store as the backend.
//
// This is the internal backend implementation. It implements:
//   - storage.Storage (base interface)
//   - storage.StorageWithBucket
//   - storage.StorageWithWatch
//   - storage.StorageWithRevision
//   - storage.StorageWithKeys
//   - storage.StorageWithStatus
type JetStreamKVBackend struct {
	bucketName string
	bucketTTL  time.Duration
	kv         jetstream.KeyValue
	logger     *slog.Logger
	// replicas is the bucket's replica count once it was read from its
	// status (0: not read yet); see SingleReplica.
	replicas atomic.Int32
}

// Compile-time interface checks.
var (
	_ storage.Storage             = (*JetStreamKVBackend)(nil)
	_ storage.StorageWithBucket   = (*JetStreamKVBackend)(nil)
	_ storage.StorageWithWatch    = (*JetStreamKVBackend)(nil)
	_ storage.StorageWithRevision = (*JetStreamKVBackend)(nil)
	_ storage.StorageWithKeys     = (*JetStreamKVBackend)(nil)
	_ storage.StorageWithStatus   = (*JetStreamKVBackend)(nil)
)

// NewJetStreamKVBackend creates a new JetStream KV backend for the given bucket.
func NewJetStreamKVBackend(bucketName string, kv jetstream.KeyValue, bucketTTL time.Duration, logger *slog.Logger) *JetStreamKVBackend {
	return &JetStreamKVBackend{
		bucketName: bucketName,
		bucketTTL:  bucketTTL,
		kv:         kv,
		logger:     logger,
	}
}

// =============================================================================
// storage.StorageWithBucket Implementation
// =============================================================================

// BucketName returns the name of the bucket.
func (b *JetStreamKVBackend) BucketName() string {
	return b.bucketName
}

// =============================================================================
// storage.Storage Implementation
// =============================================================================

// GetWithContext gets the value for the given key with a context.
// Returns nil, nil when the key does not exist.
func (b *JetStreamKVBackend) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	entry, err := b.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil // Key not found returns nil, nil per storage.Storage contract
		}
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return entry.Value(), nil
}

// Get gets the value for the given key.
// Returns nil, nil when the key does not exist.
func (b *JetStreamKVBackend) Get(key string) ([]byte, error) {
	return b.GetWithContext(context.Background(), key)
}

// SetWithContext stores the given value for the given key with an expiration value.
// Note: JetStream KV Store only supports bucket-level TTL, not per-key TTL.
// If exp differs from bucket TTL and is non-zero, a warning is logged.
func (b *JetStreamKVBackend) SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error {
	// Log warning if TTL differs from bucket TTL
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-key TTL not supported by JetStream KV Store; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	_, err := b.kv.Put(ctx, key, val)
	if err != nil {
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}
	return nil
}

// Set stores the given value for the given key with an expiration value.
// Empty key or value will be ignored without an error.
func (b *JetStreamKVBackend) Set(key string, val []byte, exp time.Duration) error {
	if key == "" || val == nil {
		return nil
	}
	return b.SetWithContext(context.Background(), key, val, exp)
}

// DeleteWithContext deletes the value for the given key with a context.
// It returns no error if the storage does not contain the key.
//
// A key that does not exist is not deleted: the bucket's delete writes a
// marker whether or not there is anything to delete, so every delete of a
// missing key added a message to the bucket that Reset does not reclaim. The
// key is therefore looked up first, which costs one read per delete.
//
// The lookup is made only for a bucket with one replica. A read of a
// replicated bucket may be answered by a replica that has not caught up, so
// a key written a moment ago could be reported missing and then not be
// deleted; there the marker is written as before. The same could happen to
// a bucket that another stream mirrors with direct access (mirror_direct),
// which also answers reads for it: the plugin creates no such mirror, and a
// bucket that has one should not rely on the lookup being exact.
func (b *JetStreamKVBackend) DeleteWithContext(ctx context.Context, key string) error {
	if b.SingleReplica(ctx) {
		if _, err := b.kv.Get(ctx, key); errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		// Any other lookup error is left to the delete to report.
	}
	if err := b.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil // Key not found is not an error per storage.Storage contract
		}
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// SingleReplica reports whether the bucket is known to have one replica,
// which is read from the bucket's status once. While the status cannot be
// read the answer is no, and it is asked again.
func (b *JetStreamKVBackend) SingleReplica(ctx context.Context) bool {
	switch b.replicas.Load() {
	case 0:
	case 1:
		return true
	default:
		return false
	}
	status, err := b.kv.Status(ctx)
	if err != nil {
		return false
	}
	bucket, ok := status.(*jetstream.KeyValueBucketStatus)
	if !ok || bucket.StreamInfo() == nil {
		return false
	}
	replicas := bucket.StreamInfo().Config.Replicas
	if replicas < 1 {
		replicas = 1
	}
	b.replicas.Store(int32(replicas)) //nolint:gosec // G115: a stream has at most 5 replicas
	return replicas == 1
}

// Delete deletes the value for the given key.
// It returns no error if the storage does not contain the key.
func (b *JetStreamKVBackend) Delete(key string) error {
	return b.DeleteWithContext(context.Background(), key)
}

// ResetWithContext resets the storage and deletes all keys with a context.
func (b *JetStreamKVBackend) ResetWithContext(ctx context.Context) error {
	// The key lister runs until every key was read or its context ends, and
	// it blocks handing over a key nobody takes. A reset that returns early
	// (a purge failed) therefore ends the listing and takes what is left of
	// it; without that its goroutines stayed for the life of the process.
	listCtx, stopListing := context.WithCancel(ctx)
	// List all keys and purge them
	lister, err := b.kv.ListKeys(listCtx)
	if err != nil {
		stopListing()
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil // No keys to delete
		}
		return fmt.Errorf("failed to list keys for reset: %w", err)
	}
	defer func() {
		stopListing()
		for range lister.Keys() { //nolint:revive // drained so that the lister can exit
		}
	}()

	for key := range lister.Keys() {
		if err := b.kv.Purge(ctx, key); err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				return fmt.Errorf("failed to purge key %s during reset: %w", key, err)
			}
		}
	}
	// The lister ends without a word when its context does: a reset cut
	// short must not be reported as done.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reset stopped before every key was purged: %w", err)
	}
	return nil
}

// Reset resets the storage and deletes all keys.
func (b *JetStreamKVBackend) Reset() error {
	return b.ResetWithContext(context.Background())
}

// Close closes the storage. For JetStream backend, this is a no-op
// as the connection is managed by the framework.
func (b *JetStreamKVBackend) Close() error {
	return nil
}

// =============================================================================
// storage.StorageWithRevision Implementation
// =============================================================================

// CreateWithContext stores a value only if the key does not exist.
// Returns ErrKeyExists if key exists.
func (b *JetStreamKVBackend) CreateWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error) {
	// Log warning if TTL differs from bucket TTL
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-key TTL not supported by JetStream KV Store; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	revision, err := b.kv.Create(ctx, key, val)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, fmt.Errorf("%w: %s", storage.ErrKeyExists, key)
		}
		return 0, fmt.Errorf("failed to create key %s: %w", key, err)
	}
	return revision, nil
}

// Create stores a value only if the key does not exist.
func (b *JetStreamKVBackend) Create(key string, val []byte, exp time.Duration) (uint64, error) {
	return b.CreateWithContext(context.Background(), key, val, exp)
}

// UpdateWithContext stores a value only if the current revision matches.
// Returns ErrRevisionMismatch if revision doesn't match.
func (b *JetStreamKVBackend) UpdateWithContext(ctx context.Context, key string, val []byte, exp time.Duration, revision uint64) (uint64, error) {
	// Log warning if TTL differs from bucket TTL
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-key TTL not supported by JetStream KV Store; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	newRevision, err := b.kv.Update(ctx, key, val, revision)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, fmt.Errorf("%w: %s", storage.ErrKeyNotFound, key)
		}
		if isRevisionMismatchError(err) {
			return 0, fmt.Errorf("%w: expected revision %d for key %s", storage.ErrRevisionMismatch, revision, key)
		}
		return 0, fmt.Errorf("failed to update key %s: %w", key, err)
	}
	return newRevision, nil
}

// Update stores a value only if the current revision matches.
func (b *JetStreamKVBackend) Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error) {
	return b.UpdateWithContext(context.Background(), key, val, exp, revision)
}

// PurgeWithContext hard-deletes a key and all its history.
func (b *JetStreamKVBackend) PurgeWithContext(ctx context.Context, key string) error {
	err := b.kv.Purge(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("%w: %s", storage.ErrKeyNotFound, key)
		}
		return fmt.Errorf("failed to purge key %s: %w", key, err)
	}
	return nil
}

// Purge hard-deletes a key and all its history.
func (b *JetStreamKVBackend) Purge(key string) error {
	return b.PurgeWithContext(context.Background(), key)
}

// GetEntryWithContext retrieves the value with revision metadata.
func (b *JetStreamKVBackend) GetEntryWithContext(ctx context.Context, key string) (*storage.Entry, error) {
	entry, err := b.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %s", storage.ErrKeyNotFound, key)
		}
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return convertToStorageEntry(entry), nil
}

// GetEntry retrieves the value with revision metadata.
func (b *JetStreamKVBackend) GetEntry(key string) (*storage.Entry, error) {
	return b.GetEntryWithContext(context.Background(), key)
}

// PutWithRevisionWithContext stores a value and returns the new revision.
func (b *JetStreamKVBackend) PutWithRevisionWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error) {
	// Log warning if TTL differs from bucket TTL
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-key TTL not supported by JetStream KV Store; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	revision, err := b.kv.Put(ctx, key, val)
	if err != nil {
		return 0, fmt.Errorf("failed to put key %s: %w", key, err)
	}
	return revision, nil
}

// PutWithRevision stores a value and returns the new revision.
func (b *JetStreamKVBackend) PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error) {
	return b.PutWithRevisionWithContext(context.Background(), key, val, exp)
}

// =============================================================================
// storage.StorageWithKeys Implementation
// =============================================================================

// KeysWithContext returns all keys in the storage. When ctx ends before the
// listing is complete it returns an error wrapping the context's and no keys.
func (b *JetStreamKVBackend) KeysWithContext(ctx context.Context) ([]string, error) {
	lister, err := b.kv.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	var keys []string
	for key := range lister.Keys() {
		keys = append(keys, key)
	}
	// The lister closes its channel when the context ends as it does when
	// every key was sent, so a listing cut short is told apart here: a
	// shorter list returned as complete would make a caller treat the
	// missing keys as deleted.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("failed to list keys: listing stopped after %d keys: %w", len(keys), err)
	}
	return keys, nil
}

// Keys returns all keys in the storage.
func (b *JetStreamKVBackend) Keys() ([]string, error) {
	return b.KeysWithContext(context.Background())
}

// =============================================================================
// storage.StorageWithWatch Implementation
// =============================================================================

// WatchWithContext creates a watcher for changes to keys matching the pattern.
func (b *JetStreamKVBackend) WatchWithContext(ctx context.Context, pattern string, opts ...storage.WatchOption) (storage.KeyWatcher, error) {
	options := storage.ApplyWatchOptions(opts...)

	var jsOpts []jetstream.WatchOpt
	if options.UpdatesOnly {
		jsOpts = append(jsOpts, jetstream.UpdatesOnly())
	}
	if options.IgnoreDeletes {
		jsOpts = append(jsOpts, jetstream.IgnoreDeletes())
	}
	if options.MetaOnly {
		jsOpts = append(jsOpts, jetstream.MetaOnly())
	}
	if options.ResumeFromRevision > 0 {
		jsOpts = append(jsOpts, jetstream.ResumeFromRevision(options.ResumeFromRevision))
	}

	watcher, err := b.kv.Watch(ctx, pattern, jsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher for pattern %s: %w", pattern, err)
	}

	return newStorageKeyWatcher(watcher), nil
}

// Watch creates a watcher for changes to keys matching the pattern.
func (b *JetStreamKVBackend) Watch(pattern string, opts ...storage.WatchOption) (storage.KeyWatcher, error) {
	return b.WatchWithContext(context.Background(), pattern, opts...)
}

// =============================================================================
// storage.StorageWithStatus Implementation
// =============================================================================

// StatusWithContext returns status information about the storage.
func (b *JetStreamKVBackend) StatusWithContext(ctx context.Context) (*storage.BucketStatus, error) {
	status, err := b.kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket status: %w", err)
	}

	return &storage.BucketStatus{
		Bucket:       status.Bucket(),
		Values:       status.Values(),
		TTL:          status.TTL(),
		BackingStore: status.BackingStore(),
		Bytes:        status.Bytes(),
	}, nil
}

// Status returns status information about the storage.
func (b *JetStreamKVBackend) Status() (*storage.BucketStatus, error) {
	return b.StatusWithContext(context.Background())
}

// =============================================================================
// Helper Functions
// =============================================================================

// convertToStorageEntry converts JetStream KeyValueEntry to storage.Entry.
func convertToStorageEntry(entry jetstream.KeyValueEntry) *storage.Entry {
	var op storage.KeyOperation
	switch entry.Operation() {
	case jetstream.KeyValuePut:
		op = storage.KeyOperationPut
	case jetstream.KeyValueDelete:
		op = storage.KeyOperationDelete
	case jetstream.KeyValuePurge:
		op = storage.KeyOperationPurge
	}

	return &storage.Entry{
		Bucket:    entry.Bucket(),
		Key:       entry.Key(),
		Value:     entry.Value(),
		Revision:  entry.Revision(),
		Timestamp: entry.Created(),
		Operation: op,
	}
}

// jetStreamErrCodeWrongLastSequence is the NATS JetStream API error code
// for "wrong last sequence" errors, indicating a revision mismatch.
const jetStreamErrCodeWrongLastSequence = 10071

// isRevisionMismatchError checks if the error is a revision mismatch error.
func isRevisionMismatchError(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jetStreamErrCodeWrongLastSequence
	}
	return false
}

// =============================================================================
// Storage KeyWatcher Implementation
// =============================================================================

// storageKeyWatcher wraps jetstream.KeyWatcher to implement storage.KeyWatcher.
type storageKeyWatcher struct {
	watcher jetstream.KeyWatcher
	updates chan *storage.Entry
	ctx     context.Context
	cancel  context.CancelFunc
}

// Compile-time interface check.
var _ storage.KeyWatcher = (*storageKeyWatcher)(nil)

// newStorageKeyWatcher creates a new wrapper for JetStream KeyWatcher.
func newStorageKeyWatcher(watcher jetstream.KeyWatcher) *storageKeyWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &storageKeyWatcher{
		watcher: watcher,
		updates: make(chan *storage.Entry, watcherBufferSize),
		ctx:     ctx,
		cancel:  cancel,
	}

	started := make(chan struct{})
	go func() {
		close(started)
		w.processUpdates()
	}()
	<-started

	return w
}

// processUpdates reads from JetStream watcher and converts entries.
func (w *storageKeyWatcher) processUpdates() {
	defer close(w.updates)

	for {
		select {
		case <-w.ctx.Done():
			return
		case entry, ok := <-w.watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				select {
				case w.updates <- nil:
				case <-w.ctx.Done():
					return
				}
				continue
			}
			select {
			case w.updates <- convertToStorageEntry(entry):
			case <-w.ctx.Done():
				return
			}
		}
	}
}

// Updates returns a channel that receives key-value updates.
func (w *storageKeyWatcher) Updates() <-chan *storage.Entry {
	return w.updates
}

// Stop stops the watcher and releases resources.
func (w *storageKeyWatcher) Stop() error {
	w.cancel()
	return w.watcher.Stop()
}
