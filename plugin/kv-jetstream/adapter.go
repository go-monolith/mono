package kvjetstream

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
)

// KVStorageAdapter implements KVStoragePort by wrapping a storage.Storage backend.
// It provides the consumer-facing interface and adds cross-cutting concerns
// like debug logging without modifying backend implementations.
//
// The adapter uses type assertions to access extended capabilities (Watch, Revision,
// Keys, Status) from the underlying storage backend.
type KVStorageAdapter struct {
	storage  storage.Storage
	bucket   storage.StorageWithBucket
	watch    storage.StorageWithWatch
	revision storage.StorageWithRevision
	keys     storage.StorageWithKeys
	status   storage.StorageWithStatus
	logger   *slog.Logger
}

// Compile-time interface check.
var _ KVStoragePort = (*KVStorageAdapter)(nil)

// NewKVAdapter creates a new KVStorageAdapter wrapping the given storage backend.
// The adapter type-asserts for extended interfaces (StorageWithBucket, StorageWithWatch,
// StorageWithRevision, StorageWithKeys, StorageWithStatus) to provide full KVStoragePort functionality.
func NewKVAdapter(s storage.Storage, logger *slog.Logger) *KVStorageAdapter {
	adapter := &KVStorageAdapter{
		storage: s,
		logger:  logger,
	}
	// Type assert for extended interfaces
	if bucket, ok := s.(storage.StorageWithBucket); ok {
		adapter.bucket = bucket
		// Add bucket name to logger
		adapter.logger = logger.With("bucket", bucket.BucketName())
	}
	if watch, ok := s.(storage.StorageWithWatch); ok {
		adapter.watch = watch
	}
	if revision, ok := s.(storage.StorageWithRevision); ok {
		adapter.revision = revision
	}
	if keys, ok := s.(storage.StorageWithKeys); ok {
		adapter.keys = keys
	}
	if status, ok := s.(storage.StorageWithStatus); ok {
		adapter.status = status
	}
	return adapter
}

// =============================================================================
// storage.Storage Implementation
// =============================================================================

// Get gets the value for the given key.
// Returns nil, nil when the key does not exist.
func (a *KVStorageAdapter) Get(key string) ([]byte, error) {
	return a.GetWithContext(context.Background(), key)
}

// GetWithContext gets the value for the given key with a context.
// Returns nil, nil when the key does not exist.
func (a *KVStorageAdapter) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	a.logger.Debug("get operation", "key", key)
	data, err := a.storage.GetWithContext(ctx, key)
	if err != nil {
		a.logger.Debug("get failed", "key", key, "error", err)
		return nil, err
	}
	if data == nil {
		a.logger.Debug("get key not found", "key", key)
	} else {
		a.logger.Debug("get succeeded", "key", key, "size", len(data))
	}
	return data, nil
}

// Set stores the given value for the given key with an expiration value.
// 0 means no expiration. Empty key or value will be ignored without an error.
func (a *KVStorageAdapter) Set(key string, val []byte, exp time.Duration) error {
	return a.SetWithContext(context.Background(), key, val, exp)
}

// SetWithContext stores the given value for the given key with a context.
func (a *KVStorageAdapter) SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error {
	if key == "" || val == nil {
		return nil
	}
	a.logger.Debug("set operation", "key", key, "size", len(val))
	err := a.storage.SetWithContext(ctx, key, val, exp)
	if err != nil {
		a.logger.Debug("set failed", "key", key, "error", err)
		return err
	}
	a.logger.Debug("set succeeded", "key", key)
	return nil
}

// Delete deletes the value for the given key.
// It returns no error if the storage does not contain the key.
func (a *KVStorageAdapter) Delete(key string) error {
	return a.DeleteWithContext(context.Background(), key)
}

// DeleteWithContext deletes the value for the given key with a context.
// It returns no error if the storage does not contain the key.
func (a *KVStorageAdapter) DeleteWithContext(ctx context.Context, key string) error {
	a.logger.Debug("delete operation", "key", key)
	err := a.storage.DeleteWithContext(ctx, key)
	if err != nil {
		a.logger.Debug("delete failed", "key", key, "error", err)
		return err
	}
	a.logger.Debug("delete succeeded", "key", key)
	return nil
}

// Reset resets the storage and deletes all keys.
func (a *KVStorageAdapter) Reset() error {
	return a.ResetWithContext(context.Background())
}

// ResetWithContext resets the storage and deletes all keys with a context.
func (a *KVStorageAdapter) ResetWithContext(ctx context.Context) error {
	a.logger.Debug("reset operation")
	err := a.storage.ResetWithContext(ctx)
	if err != nil {
		a.logger.Debug("reset failed", "error", err)
		return err
	}
	a.logger.Debug("reset succeeded")
	return nil
}

// Close closes the storage.
func (a *KVStorageAdapter) Close() error {
	a.logger.Debug("close operation")
	return a.storage.Close()
}

// =============================================================================
// storage.StorageWithBucket Implementation
// =============================================================================

// BucketName returns the name of the bucket.
func (a *KVStorageAdapter) BucketName() string {
	if a.bucket != nil {
		return a.bucket.BucketName()
	}
	return ""
}

// =============================================================================
// storage.StorageWithWatch Implementation
// =============================================================================

// Watch creates a watcher for changes to keys matching the pattern.
func (a *KVStorageAdapter) Watch(pattern string, opts ...storage.WatchOption) (storage.KeyWatcher, error) {
	return a.WatchWithContext(context.Background(), pattern, opts...)
}

// WatchWithContext creates a watcher for changes to keys matching the pattern.
func (a *KVStorageAdapter) WatchWithContext(ctx context.Context, pattern string, opts ...storage.WatchOption) (storage.KeyWatcher, error) {
	a.logger.Debug("watch operation", "pattern", pattern)

	if a.watch != nil {
		watcher, err := a.watch.WatchWithContext(ctx, pattern, opts...)
		if err != nil {
			a.logger.Debug("watch failed", "pattern", pattern, "error", err)
			return nil, err
		}
		a.logger.Debug("watch created", "pattern", pattern)
		return watcher, nil
	}

	// No watch support in base storage
	return nil, nil
}

// =============================================================================
// storage.StorageWithRevision Implementation
// =============================================================================

// Create stores a value only if the key does not exist.
func (a *KVStorageAdapter) Create(key string, val []byte, exp time.Duration) (uint64, error) {
	return a.CreateWithContext(context.Background(), key, val, exp)
}

// CreateWithContext stores a value only if the key does not exist.
func (a *KVStorageAdapter) CreateWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error) {
	a.logger.Debug("create operation", "key", key, "size", len(val))

	if a.revision != nil {
		rev, err := a.revision.CreateWithContext(ctx, key, val, exp)
		if err != nil {
			a.logger.Debug("create failed", "key", key, "error", err)
			return 0, err
		}
		a.logger.Debug("create succeeded", "key", key, "revision", rev)
		return rev, nil
	}

	// Fallback: check if key exists, then set
	data, err := a.storage.GetWithContext(ctx, key)
	if err != nil {
		a.logger.Debug("create failed", "key", key, "error", err)
		return 0, err
	}
	if data != nil {
		return 0, storage.ErrKeyExists
	}
	err = a.storage.SetWithContext(ctx, key, val, exp)
	if err != nil {
		a.logger.Debug("create failed", "key", key, "error", err)
		return 0, err
	}
	a.logger.Debug("create succeeded", "key", key, "revision", 0)
	return 0, nil
}

// Update stores a value only if the current revision matches.
func (a *KVStorageAdapter) Update(key string, val []byte, exp time.Duration, revision uint64) (uint64, error) {
	return a.UpdateWithContext(context.Background(), key, val, exp, revision)
}

// UpdateWithContext stores a value only if the current revision matches.
func (a *KVStorageAdapter) UpdateWithContext(ctx context.Context, key string, val []byte, exp time.Duration, revision uint64) (uint64, error) {
	a.logger.Debug("update operation", "key", key, "size", len(val), "expected_revision", revision)

	if a.revision != nil {
		rev, err := a.revision.UpdateWithContext(ctx, key, val, exp, revision)
		if err != nil {
			a.logger.Debug("update failed", "key", key, "expected_revision", revision, "error", err)
			return 0, err
		}
		a.logger.Debug("update succeeded", "key", key, "new_revision", rev)
		return rev, nil
	}

	// No revision support in base storage - just set
	err := a.storage.SetWithContext(ctx, key, val, exp)
	if err != nil {
		a.logger.Debug("update failed", "key", key, "expected_revision", revision, "error", err)
		return 0, err
	}
	a.logger.Debug("update succeeded", "key", key, "new_revision", 0)
	return 0, nil
}

// Purge hard-deletes a key and all its history.
func (a *KVStorageAdapter) Purge(key string) error {
	return a.PurgeWithContext(context.Background(), key)
}

// PurgeWithContext hard-deletes a key and all its history.
func (a *KVStorageAdapter) PurgeWithContext(ctx context.Context, key string) error {
	a.logger.Debug("purge operation", "key", key)

	if a.revision != nil {
		err := a.revision.PurgeWithContext(ctx, key)
		if err != nil {
			a.logger.Debug("purge failed", "key", key, "error", err)
			return err
		}
		a.logger.Debug("purge succeeded", "key", key)
		return nil
	}

	// Fallback: use delete
	return a.DeleteWithContext(ctx, key)
}

// GetEntry retrieves the value with revision metadata.
func (a *KVStorageAdapter) GetEntry(key string) (*storage.Entry, error) {
	return a.GetEntryWithContext(context.Background(), key)
}

// GetEntryWithContext retrieves the value with revision metadata.
func (a *KVStorageAdapter) GetEntryWithContext(ctx context.Context, key string) (*storage.Entry, error) {
	a.logger.Debug("get-entry operation", "key", key)

	if a.revision != nil {
		entry, err := a.revision.GetEntryWithContext(ctx, key)
		if err != nil {
			a.logger.Debug("get-entry failed", "key", key, "error", err)
			return nil, err
		}
		if entry != nil {
			a.logger.Debug("get-entry succeeded", "key", key, "revision", entry.Revision)
		}
		return entry, nil
	}

	// Fallback to base storage (no revision info)
	data, err := a.storage.GetWithContext(ctx, key)
	if err != nil {
		a.logger.Debug("get-entry failed", "key", key, "error", err)
		return nil, err
	}
	if data == nil {
		return nil, storage.ErrKeyNotFound
	}
	a.logger.Debug("get-entry succeeded", "key", key, "revision", 0)
	return &storage.Entry{
		Bucket: a.BucketName(),
		Key:    key,
		Value:  data,
	}, nil
}

// PutWithRevision stores a value and returns the new revision.
func (a *KVStorageAdapter) PutWithRevision(key string, val []byte, exp time.Duration) (uint64, error) {
	return a.PutWithRevisionWithContext(context.Background(), key, val, exp)
}

// PutWithRevisionWithContext stores a value and returns the new revision.
func (a *KVStorageAdapter) PutWithRevisionWithContext(ctx context.Context, key string, val []byte, exp time.Duration) (uint64, error) {
	a.logger.Debug("put-with-revision operation", "key", key, "size", len(val))

	if a.revision != nil {
		rev, err := a.revision.PutWithRevisionWithContext(ctx, key, val, exp)
		if err != nil {
			a.logger.Debug("put-with-revision failed", "key", key, "error", err)
			return 0, err
		}
		a.logger.Debug("put-with-revision succeeded", "key", key, "revision", rev)
		return rev, nil
	}

	// Fallback to base storage (no revision return)
	err := a.storage.SetWithContext(ctx, key, val, exp)
	if err != nil {
		a.logger.Debug("put-with-revision failed", "key", key, "error", err)
		return 0, err
	}
	a.logger.Debug("put-with-revision succeeded", "key", key, "revision", 0)
	return 0, nil
}

// =============================================================================
// storage.StorageWithKeys Implementation
// =============================================================================

// Keys returns all keys in the storage.
func (a *KVStorageAdapter) Keys() ([]string, error) {
	return a.KeysWithContext(context.Background())
}

// KeysWithContext returns all keys in the storage.
func (a *KVStorageAdapter) KeysWithContext(ctx context.Context) ([]string, error) {
	a.logger.Debug("keys operation")

	if a.keys != nil {
		keys, err := a.keys.KeysWithContext(ctx)
		if err != nil {
			a.logger.Debug("keys failed", "error", err)
			return nil, err
		}
		a.logger.Debug("keys succeeded", "count", len(keys))
		return keys, nil
	}

	// No keys support in base storage - return empty slice
	return []string{}, nil
}

// =============================================================================
// storage.StorageWithStatus Implementation
// =============================================================================

// Status returns status information about the storage.
func (a *KVStorageAdapter) Status() (*storage.BucketStatus, error) {
	return a.StatusWithContext(context.Background())
}

// StatusWithContext returns status information about the storage.
func (a *KVStorageAdapter) StatusWithContext(ctx context.Context) (*storage.BucketStatus, error) {
	a.logger.Debug("status operation")

	if a.status != nil {
		status, err := a.status.StatusWithContext(ctx)
		if err != nil {
			a.logger.Debug("status failed", "error", err)
			return nil, err
		}
		if status != nil {
			a.logger.Debug("status succeeded", "values", status.Values, "bytes", status.Bytes)
		}
		return status, nil
	}

	// No status support in base storage
	return nil, nil
}

// =============================================================================
// KV-Specific Convenience Methods
// =============================================================================

// WatchAll creates a watcher for all keys in the bucket.
// This is a convenience method equivalent to Watch(">", opts...).
func (a *KVStorageAdapter) WatchAll(ctx context.Context, opts ...WatchOption) (KeyWatcher, error) {
	a.logger.Debug("watch-all operation")
	return a.WatchWithContext(ctx, ">", opts...)
}
