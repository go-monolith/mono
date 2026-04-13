package fsjetstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/go-monolith/mono/pkg/storage"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamBackend implements storage.Storage and extended interfaces
// using JetStream ObjectStore as the backend.
//
// This is the internal backend implementation. It implements:
//   - storage.Storage (base interface)
//   - storage.StorageWithBucket
//   - storage.StorageWithList
//   - storage.StorageWithStat
//   - storage.StorageWithReader
type JetStreamBackend struct {
	bucketName string
	bucketTTL  time.Duration
	store      jetstream.ObjectStore
	logger     *slog.Logger
}

// Compile-time interface checks.
var (
	_ storage.Storage           = (*JetStreamBackend)(nil)
	_ storage.StorageWithBucket = (*JetStreamBackend)(nil)
	_ storage.StorageWithList   = (*JetStreamBackend)(nil)
	_ storage.StorageWithStat   = (*JetStreamBackend)(nil)
	_ storage.StorageWithReader = (*JetStreamBackend)(nil)
)

// NewJetStreamBackend creates a new JetStream backend for the given bucket.
func NewJetStreamBackend(bucketName string, store jetstream.ObjectStore, bucketTTL time.Duration, logger *slog.Logger) *JetStreamBackend {
	return &JetStreamBackend{
		bucketName: bucketName,
		bucketTTL:  bucketTTL,
		store:      store,
		logger:     logger,
	}
}

// =============================================================================
// storage.StorageWithBucket Implementation
// =============================================================================

// BucketName returns the name of the bucket.
func (b *JetStreamBackend) BucketName() string {
	return b.bucketName
}

// =============================================================================
// storage.Storage Implementation
// =============================================================================

// GetWithContext gets the value for the given key with a context.
// Returns nil, nil when the key does not exist.
func (b *JetStreamBackend) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	result, err := b.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil // Key not found returns nil, nil per storage.Storage contract
		}
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	defer result.Close() //nolint:errcheck // Close error on read cleanup is non-actionable

	data, err := io.ReadAll(result)
	if err != nil {
		return nil, fmt.Errorf("failed to read object %s: %w", key, err)
	}
	return data, nil
}

// Get gets the value for the given key.
// Returns nil, nil when the key does not exist.
func (b *JetStreamBackend) Get(key string) ([]byte, error) {
	return b.GetWithContext(context.Background(), key)
}

// SetWithContext stores the given value for the given key with an expiration value.
// Note: JetStream ObjectStore only supports bucket-level TTL, not per-object TTL.
// If exp differs from bucket TTL and is non-zero, a warning is logged.
func (b *JetStreamBackend) SetWithContext(ctx context.Context, key string, val []byte, exp time.Duration) error {
	// Log warning if TTL differs from bucket TTL (only when exp > 0 to avoid false warnings)
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-object TTL not supported by JetStream ObjectStore; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	meta := jetstream.ObjectMeta{
		Name: key,
	}

	_, err := b.store.Put(ctx, meta, bytes.NewReader(val))
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", key, err)
	}

	return nil
}

// Set stores the given value for the given key with an expiration value.
// Empty key or value will be ignored without an error.
func (b *JetStreamBackend) Set(key string, val []byte, exp time.Duration) error {
	if key == "" || val == nil {
		return nil
	}
	return b.SetWithContext(context.Background(), key, val, exp)
}

// DeleteWithContext deletes the value for the given key with a context.
// It returns no error if the storage does not contain the key.
func (b *JetStreamBackend) DeleteWithContext(ctx context.Context, key string) error {
	if err := b.store.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil // Key not found is not an error per storage.Storage contract
		}
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}
	return nil
}

// Delete deletes the value for the given key.
// It returns no error if the storage does not contain the key.
func (b *JetStreamBackend) Delete(key string) error {
	return b.DeleteWithContext(context.Background(), key)
}

// ResetWithContext resets the storage and deletes all keys with a context.
func (b *JetStreamBackend) ResetWithContext(ctx context.Context) error {
	// List all objects and delete them
	infos, err := b.store.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil // No objects to delete
		}
		return fmt.Errorf("failed to list objects for reset: %w", err)
	}

	for _, info := range infos {
		if err := b.store.Delete(ctx, info.Name); err != nil {
			if !errors.Is(err, jetstream.ErrObjectNotFound) {
				return fmt.Errorf("failed to delete object %s during reset: %w", info.Name, err)
			}
		}
	}
	return nil
}

// Reset resets the storage and deletes all keys.
func (b *JetStreamBackend) Reset() error {
	return b.ResetWithContext(context.Background())
}

// Close closes the storage. For JetStream backend, this is a no-op
// as the connection is managed by the framework.
func (b *JetStreamBackend) Close() error {
	return nil
}

// =============================================================================
// storage.StorageWithList Implementation
// =============================================================================

// ListWithContext returns all objects matching the optional filter options.
func (b *JetStreamBackend) ListWithContext(ctx context.Context, opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	options := storage.ApplyListOptions(opts...)

	infos, err := b.store.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return []storage.ObjectInfo{}, nil
		}
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	var result []storage.ObjectInfo
	for _, info := range infos {
		if options.Prefix != "" && !strings.HasPrefix(info.Name, options.Prefix) {
			continue
		}
		result = append(result, *convertToStorageObjectInfo(info))
	}

	return result, nil
}

// List returns all objects matching the optional filter options.
func (b *JetStreamBackend) List(opts ...storage.ListOption) ([]storage.ObjectInfo, error) {
	return b.ListWithContext(context.Background(), opts...)
}

// =============================================================================
// storage.StorageWithStat Implementation
// =============================================================================

// StatWithContext returns metadata for an object without retrieving data.
func (b *JetStreamBackend) StatWithContext(ctx context.Context, key string) (*storage.ObjectInfo, error) {
	info, err := b.store.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, fmt.Errorf("%w: %s", storage.ErrKeyNotFound, key)
		}
		return nil, fmt.Errorf("failed to stat object %s: %w", key, err)
	}

	return convertToStorageObjectInfo(info), nil
}

// Stat returns metadata for an object without retrieving data.
func (b *JetStreamBackend) Stat(key string) (*storage.ObjectInfo, error) {
	return b.StatWithContext(context.Background(), key)
}

// =============================================================================
// storage.StorageWithReader Implementation
// =============================================================================

// GetReaderWithContext retrieves an object as a reader (for large files).
func (b *JetStreamBackend) GetReaderWithContext(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	result, err := b.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, nil, fmt.Errorf("%w: %s", storage.ErrKeyNotFound, key)
		}
		return nil, nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}

	info, err := result.Info()
	if err != nil {
		result.Close() //nolint:errcheck // Close error on info failure is secondary
		return nil, nil, fmt.Errorf("failed to get object info %s: %w", key, err)
	}

	return result, convertToStorageObjectInfo(info), nil
}

// GetReader retrieves an object as a reader (for large files).
func (b *JetStreamBackend) GetReader(key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return b.GetReaderWithContext(context.Background(), key)
}

// PutReaderWithContext stores an object from a reader.
// Note: JetStream ObjectStore only supports bucket-level TTL, not per-object TTL.
func (b *JetStreamBackend) PutReaderWithContext(ctx context.Context, key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	// Log warning if TTL differs from bucket TTL (only when exp > 0 to avoid false warnings)
	if exp > 0 && b.bucketTTL > 0 && exp != b.bucketTTL && b.logger != nil {
		b.logger.Warn("per-object TTL not supported by JetStream ObjectStore; using bucket TTL",
			"bucket", b.bucketName,
			"key", key,
			"requested_ttl", exp,
			"bucket_ttl", b.bucketTTL,
		)
	}

	options := storage.ApplyPutOptions(opts...)

	meta := jetstream.ObjectMeta{
		Name:        key,
		Description: options.Description,
	}

	if len(options.Headers) > 0 {
		meta.Headers = make(map[string][]string)
		for k, v := range options.Headers {
			meta.Headers[k] = []string{v}
		}
	}

	info, err := b.store.Put(ctx, meta, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to put object %s: %w", key, err)
	}

	return convertToStorageObjectInfo(info), nil
}

// PutReader stores an object from a reader.
func (b *JetStreamBackend) PutReader(key string, reader io.Reader, exp time.Duration, opts ...storage.PutOption) (*storage.ObjectInfo, error) {
	return b.PutReaderWithContext(context.Background(), key, reader, exp, opts...)
}

// =============================================================================
// Helper Functions
// =============================================================================

// convertToStorageObjectInfo converts JetStream ObjectInfo to storage.ObjectInfo.
func convertToStorageObjectInfo(info *jetstream.ObjectInfo) *storage.ObjectInfo {
	// Safe uint64 to int64 conversion - cap at MaxInt64 if overflow would occur
	size := int64(math.MaxInt64)
	if info.Size <= math.MaxInt64 {
		size = int64(info.Size)
	}
	result := &storage.ObjectInfo{
		Bucket:      info.Bucket,
		Name:        info.Name,
		Size:        size,
		Digest:      info.Digest,
		ModTime:     info.ModTime,
		Deleted:     info.Deleted,
		Description: info.Description,
		Chunks:      info.Chunks,
	}

	if len(info.Headers) > 0 {
		result.Headers = make(map[string]string)
		for k, v := range info.Headers {
			if len(v) > 0 {
				result.Headers[k] = v[0]
			}
		}
	}

	return result
}
