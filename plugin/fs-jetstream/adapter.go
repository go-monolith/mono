package fsjetstream

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/go-monolith/mono/v1/pkg/storage"
)

// FileStorageAdapter implements FileStoragePort by wrapping a storage.Storage backend.
// It provides the consumer-facing interface and can add cross-cutting concerns
// like logging, metrics, or validation without modifying backend implementations.
//
// The adapter uses type assertions to access extended capabilities (List, Stat, Reader)
// from the underlying storage backend.
type FileStorageAdapter struct {
	storage storage.Storage
	bucket  storage.StorageWithBucket
	list    storage.StorageWithList
	stat    storage.StorageWithStat
	reader  storage.StorageWithReader
	logger  *slog.Logger
}

// Compile-time interface check.
var _ FileStoragePort = (*FileStorageAdapter)(nil)

// NewAdapter creates a new FileStorageAdapter wrapping the given storage backend.
// The adapter type-asserts for extended interfaces (StorageWithBucket, StorageWithList,
// StorageWithStat, StorageWithReader) to provide full FileStoragePort functionality.
func NewAdapter(s storage.Storage, logger *slog.Logger) *FileStorageAdapter {
	adapter := &FileStorageAdapter{
		storage: s,
		logger:  logger,
	}
	// Type assert for extended interfaces
	if bucket, ok := s.(storage.StorageWithBucket); ok {
		adapter.bucket = bucket
	}
	if list, ok := s.(storage.StorageWithList); ok {
		adapter.list = list
	}
	if stat, ok := s.(storage.StorageWithStat); ok {
		adapter.stat = stat
	}
	if reader, ok := s.(storage.StorageWithReader); ok {
		adapter.reader = reader
	}
	return adapter
}

// =============================================================================
// storage.Storage Implementation
// =============================================================================

// Get gets the value for the given key.
// Returns nil, nil when the key does not exist.
func (a *FileStorageAdapter) Get(key string) ([]byte, error) {
	return a.GetWithContext(context.Background(), key)
}

// GetWithContext gets the value for the given key with a context.
// Returns nil, nil when the key does not exist.
func (a *FileStorageAdapter) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	return a.storage.GetWithContext(ctx, key)
}

// Set stores the given value for the given key with an expiration value.
// 0 means no expiration. Empty key or value will be ignored without an error.
func (a *FileStorageAdapter) Set(key string, val []byte, exp time.Duration) error {
	return a.SetWithContext(context.Background(), key, val, exp)
}

// SetWithContext stores the given value for the given key with a context.
func (a *FileStorageAdapter) SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error {
	if key == "" || val == nil {
		return nil
	}
	_, err := a.Put(ctx, key, val)
	return err
}

// Delete deletes the value for the given key.
// It returns no error if the storage does not contain the key.
func (a *FileStorageAdapter) Delete(key string) error {
	return a.DeleteWithContext(context.Background(), key)
}

// DeleteWithContext deletes the value for the given key with a context.
// It returns no error if the storage does not contain the key.
func (a *FileStorageAdapter) DeleteWithContext(ctx context.Context, key string) error {
	return a.storage.DeleteWithContext(ctx, key)
}

// Reset resets the storage and deletes all keys.
func (a *FileStorageAdapter) Reset() error {
	return a.ResetWithContext(context.Background())
}

// ResetWithContext resets the storage and deletes all keys with a context.
func (a *FileStorageAdapter) ResetWithContext(ctx context.Context) error {
	return a.storage.ResetWithContext(ctx)
}

// Close closes the storage.
func (a *FileStorageAdapter) Close() error {
	return a.storage.Close()
}

// =============================================================================
// storage.StorageWithBucket Implementation
// =============================================================================

// BucketName returns the name of the bucket.
func (a *FileStorageAdapter) BucketName() string {
	if a.bucket != nil {
		return a.bucket.BucketName()
	}
	return ""
}

// =============================================================================
// storage.StorageWithList Implementation
// =============================================================================

// List returns all objects matching the optional filter options.
func (a *FileStorageAdapter) List(opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	return a.ListWithContext(context.Background(), opts...)
}

// ListWithContext returns all objects matching the optional filter options.
// Returns empty slice if the storage backend does not support listing.
func (a *FileStorageAdapter) ListWithContext(ctx context.Context, opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	if a.list != nil {
		return a.list.ListWithContext(ctx, opts...)
	}
	// No list support in base storage - return empty slice
	return []storage.ObjectInfo{}, nil
}

// =============================================================================
// storage.StorageWithStat Implementation
// =============================================================================

// Stat returns metadata for a key without retrieving the value.
func (a *FileStorageAdapter) Stat(key string) (*storage.ObjectInfo, error) {
	return a.StatWithContext(context.Background(), key)
}

// StatWithContext returns metadata for a key without retrieving the value.
func (a *FileStorageAdapter) StatWithContext(ctx context.Context, key string) (*storage.ObjectInfo, error) {
	if a.stat != nil {
		return a.stat.StatWithContext(ctx, key)
	}

	// Fallback: use Get and return minimal info
	data, err := a.storage.GetWithContext(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, storage.ErrKeyNotFound
	}
	return &storage.ObjectInfo{
		Bucket: a.BucketName(),
		Name:   key,
		Size:   int64(len(data)),
	}, nil
}

// =============================================================================
// storage.StorageWithReader Implementation
// =============================================================================

// GetReader retrieves an object as a reader (for large files).
func (a *FileStorageAdapter) GetReader(key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return a.GetReaderWithContext(context.Background(), key)
}

// GetReaderWithContext retrieves an object as a reader (for large files).
func (a *FileStorageAdapter) GetReaderWithContext(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	if a.reader != nil {
		return a.reader.GetReaderWithContext(ctx, key)
	}

	// Fallback: use base storage and wrap in a reader
	data, err := a.storage.GetWithContext(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, storage.ErrKeyNotFound
	}
	return io.NopCloser(newBytesReader(data)), &storage.ObjectInfo{
		Bucket: a.BucketName(),
		Name:   key,
		Size:   int64(len(data)),
	}, nil
}

// PutReader stores an object from a reader.
func (a *FileStorageAdapter) PutReader(key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	return a.PutReaderWithContext(context.Background(), key, reader, exp, opts...)
}

// PutReaderWithContext stores an object from a reader.
func (a *FileStorageAdapter) PutReaderWithContext(ctx context.Context, key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	if a.reader != nil {
		return a.reader.PutReaderWithContext(ctx, key, reader, exp, opts...)
	}

	// Fallback: read all data and use base storage
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	err = a.storage.SetWithContext(ctx, key, data, exp)
	if err != nil {
		return nil, err
	}
	return &storage.ObjectInfo{
		Bucket: a.BucketName(),
		Name:   key,
		Size:   int64(len(data)),
	}, nil
}

// =============================================================================
// File-Specific Methods (FileStoragePort)
// =============================================================================

// Put stores an object from bytes and returns metadata.
// This is the preferred method over Set() when you need ObjectInfo.
func (a *FileStorageAdapter) Put(ctx context.Context, key string, data []byte, opts ...PutOption) (*ObjectInfo, error) {
	if a.reader != nil {
		// Use the reader interface for full ObjectInfo return
		info, err := a.reader.PutReaderWithContext(ctx, key, newBytesReader(data), 0, opts...)
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	// Fallback to base storage interface (no ObjectInfo return)
	err := a.storage.SetWithContext(ctx, key, data, 0)
	if err != nil {
		return nil, err
	}
	// Return minimal ObjectInfo since base Storage doesn't provide metadata
	return &ObjectInfo{
		Bucket: a.BucketName(),
		Name:   key,
		Size:   int64(len(data)),
	}, nil
}
